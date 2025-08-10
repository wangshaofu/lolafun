# TODO: s --> ms and find the negative funding rate
import asyncio
import json
import logging
import time
import websockets
import aiohttp
from datetime import datetime
import statistics
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.ntp_sync import NTPTimeSync

# Binance USDT-M Mark Price WebSocket 多流
BINANCE_WS = "wss://fstream.binance.com/stream?streams="
SYMBOLS_TARGET = ["PLAYUSDT"]                 # 目標幣種（可自行調整）
SYMBOLS_CANARY = ["BTCUSDT", "ETHUSDT"]      # 金絲雀幣種（高流動性）
SYMBOLS = [s.lower() for s in (SYMBOLS_TARGET + SYMBOLS_CANARY)]
TARGET_SET = set([s.lower() for s in SYMBOLS_TARGET])
CANARY_SET = set([s.lower() for s in SYMBOLS_CANARY])

STREAM_FMT  = "{}@markPrice@1s"           # 每秒接收 markPrice+fundingRate
AGGTRADE_FMT = "{}@aggTrade"             # 即時成交量 stream
BOOKTICKER_FMT = "{}@bookTicker"          # 最佳買一/賣一（主要心跳）

SAFETY_MARGIN = 1.0            # extra slack (s) on top of dynamic 2*OWD

# Guard-window policy
TARGET_GUARD_ONLY = True   # True: 只以目標幣的 T 開窗（建議）。False: 任何幣進入其 T 皆視為守門窗啟動。
CANARY_REQUIRE_OVERLAP = False  # True: 只有當金絲雀也落在自己的 T 守門窗才算同步；False: 只要在目標幣守門窗同秒內出現異常即算同步。

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("FW-Heartbeat")

ntp_sync = NTPTimeSync()
clock_offset = 0.0                 # NTP offset (seconds)
latency_samples = asyncio.Queue()   # store last 50 one‑way delays

# ---- Server time alignment (epoch-based) ------------------------------------
server_time_offset = 0.0  # seconds; Binance server epoch - local epoch

def server_now() -> float:
    """Return current epoch seconds aligned to Binance server time."""
    return time.time() + server_time_offset

async def _fetch_server_time_offset(session: aiohttp.ClientSession) -> float | None:
    """Query Binance server time and return offset (server - local) in seconds."""
    url = "https://fapi.binance.com/fapi/v1/time"
    try:
        async with session.get(url, timeout=10) as resp:
            js = await resp.json()
            srv = js.get("serverTime", 0) / 1000.0
            if srv:
                return srv - time.time()
    except Exception:
        return None
    return None

async def maintain_server_time_offset(interval_sec: int = 300):
    """Background task to keep `server_time_offset` fresh using REST /fapi/v1/time."""
    global server_time_offset
    async with aiohttp.ClientSession() as session:
        while True:
            off = await _fetch_server_time_offset(session)
            if off is not None:
                server_time_offset = off
                logger.info(f"Binance server_time_offset = {server_time_offset:.3f}s")
            await asyncio.sleep(interval_sec)


def current_heartbeat_threshold() -> float:
    """Return adaptive heartbeat threshold based on recent one‑way latency."""
    try:
        # get up to 50 recent latency samples without blocking
        samples = []
        while not latency_samples.empty() and len(samples) < 50:
            samples.append(latency_samples.get_nowait())
        for s in samples:
            latency_samples.put_nowait(s)   # put back
        if not samples:
            return 3.0                      # fallback
        owd_med = statistics.median(samples)
        return 2 * owd_med + SAFETY_MARGIN
    except Exception:
        return 3.0


async def monitor_funding():
    # Step 1: NTP 校時
    logger.info("執行 NTP 校時...")
    offset_ms, delay_ms = ntp_sync.sync_time()
    global clock_offset
    clock_offset = offset_ms / 1000  # 轉為秒
    logger.info(f"NTP offset 設定為 {clock_offset:.3f} 秒 (delay {delay_ms:.1f} ms)")
    logger.info("啟動 Binance 伺服器時間對齊背景任務…")
    server_time_task = asyncio.create_task(maintain_server_time_offset())

    # 組合訂閱串，包含 markPrice@1s 與 aggTrade
    streams = []
    for s in SYMBOLS:
        streams.append(STREAM_FMT.format(s))
        streams.append(BOOKTICKER_FMT.format(s))
        streams.append(AGGTRADE_FMT.format(s))
    url = BINANCE_WS + "/".join(streams)

    last_bkt_ts = {s: time.time() for s in SYMBOLS}  # 每幣種 bookTicker 最近到達時間（epoch 秒）
    downtime_start = {s: None for s in SYMBOLS}      # 每幣種停機起點

    last_msg_ts = time.time()
    last_trade_ts = {s: time.time() for s in SYMBOLS}  # 每幣種 aggTrade 最近到達時間（epoch 秒）
    funding_time = {}

    async with websockets.connect(url, ping_interval=None) as ws:
        logger.info("WebSocket 已連線，開始監聽…")

        last_pong_ts = time.time()

        async def watch_heartbeat():
            nonlocal last_msg_ts, last_pong_ts, funding_time
            GUARD_PRE, GUARD_POST = 7, 5  # 結算守門窗（秒）
            while True:
                await asyncio.sleep(1)
                thresh = current_heartbeat_threshold()
                now_local = time.time()
                now_server = server_now()

                # 統計：目前哪些幣的 bookTicker 超門檻
                frozen_bkt = {sym for sym, ts0 in last_bkt_ts.items() if now_local - ts0 > thresh}

                for sym in list(last_bkt_ts.keys()):
                    bkt_gap = now_local - last_bkt_ts[sym]
                    trd_gap = now_local - last_trade_ts.get(sym, 0.0)
                    in_guard = False
                    ft = funding_time.get(sym)
                    if ft is not None:
                        in_guard = (ft - GUARD_PRE) <= now_server < (ft + GUARD_POST)

                    # 目標幣的守門窗（用於 canary 同步判定時的錨）
                    target_guard = False
                    if sym in TARGET_SET and funding_time.get(sym) is not None:
                        tft = funding_time[sym]
                        target_guard = (tft - GUARD_PRE) <= now_server < (tft + GUARD_POST)

                    # 主要依據：bookTicker 是否凍結
                    if bkt_gap > thresh:
                        # 判斷多訊號一致性（bookTicker + aggTrade 同凍結）
                        dual = trd_gap > thresh
                        # 若是目標幣，且有任一金絲雀同步凍結，提升信心
                        canary_sync = False
                        if sym in TARGET_SET and target_guard:
                            # 當前在目標幣守門窗內，檢查金絲雀是否同秒出現異常
                            for c in CANARY_SET:
                                if c not in last_bkt_ts:
                                    continue
                                c_freeze = (now_local - last_bkt_ts[c]) > thresh
                                if not c_freeze:
                                    continue
                                if CANARY_REQUIRE_OVERLAP:
                                    cft = funding_time.get(c)
                                    c_guard = False
                                    if cft is not None:
                                        c_guard = (cft - GUARD_PRE) <= now_server < (cft + GUARD_POST)
                                    if not c_guard:
                                        continue
                                canary_sync = True
                                break

                        level = "LOW"
                        if in_guard and dual and canary_sync:
                            level = "VERY HIGH"
                        elif in_guard and dual:
                            level = "HIGH"
                        elif in_guard:
                            level = "MEDIUM"

                        msg = (
                            f"[freeze:{level}] {sym} bookTicker gap={bkt_gap:.2f}s (> {thresh:.2f}s)"
                        )
                        if dual:
                            msg += f", aggTrade gap={trd_gap:.2f}s"
                        if in_guard:
                            msg += " — SETTLEMENT WINDOW"
                        if canary_sync:
                            can_list = sorted(list(CANARY_SET & frozen_bkt))
                            if can_list:
                                msg += f"; canary sync: {','.join(can_list)}"
                        logger.warning(msg)

                    else:
                        # 復原訊息（只針對 bookTicker 連續靜默後恢復）
                        # 我們以 downtime_start[sym] 是否曾設置來判定
                        if downtime_start[sym] is not None:
                            duration = now_local - downtime_start[sym]
                            logger.info(f"[restore] {sym} bookTicker restored after {duration:.2f}s downtime")
                            downtime_start[sym] = None

                    # 如剛發生凍結，標記起點（僅針對 bookTicker）
                    if bkt_gap > thresh and downtime_start[sym] is None:
                        downtime_start[sym] = now_local

                # 補充：ping/pong 超時提醒
                if now_local - last_pong_ts > 30:
                    logger.warning(f"[ping/pong] no pong for {now_local - last_pong_ts:.1f}s – possible network issue")

        async def send_ping():
            nonlocal last_pong_ts
            while True:
                now_s = server_now()
                guard_active = False
                items = funding_time.items()
                if TARGET_GUARD_ONLY:
                    items = ((s, ft) for s, ft in funding_time.items() if s in TARGET_SET)
                for sym, ft in items:
                    if ft is None:
                        continue
                    if (ft - 7) <= now_s < (ft + 5):
                        guard_active = True
                        break
                await asyncio.sleep(1 if guard_active else 10)
                try:
                    t0 = time.time()
                    pong_waiter = await ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=5)
                    rtt_ms = (time.time() - t0) * 1000.0
                    last_pong_ts = time.time()
                    logger.debug(f"[ping/pong] RTT {rtt_ms:.1f} ms")
                except Exception as e:
                    print(f"[ping/pong] ping failed: {e}")

        # 背景心跳檢測任務
        hb_task = asyncio.create_task(watch_heartbeat())
        ping_task = asyncio.create_task(send_ping())

        try:
            async for raw in ws:
                now = time.time()
                last_msg_ts = now
                # 若剛恢復，watch_heartbeat 會在下個 tick 印出恢復訊息
                msg = json.loads(raw)
                stream_type = msg.get("stream", "")
                data = msg.get("data", {})

                # markPrice stream 處理
                if stream_type.endswith("markPrice@1s"):
                    # ---- latency measurement --------------------------------
                    event_ts = data.get("E", 0) / 1000.0   # server epoch seconds
                    if event_ts:
                        recv_server_time = server_now()
                        owd = recv_server_time - event_ts
                        if owd >= 0:
                            if latency_samples.qsize() >= 50:
                                latency_samples.get_nowait()
                            latency_samples.put_nowait(owd)

                    symbol = data.get("s", "").lower()
                    ts = int(data.get("E", 0) // 1000)
                    funding_rate  = float(data.get("r", 0.0))
                    settle_time   = data.get("T", 0) // 1000     # funding 結算時間 (s)

                    # 初次或結算前 5 秒內，鎖定下一次結算時間
                    if (symbol not in funding_time) or (settle_time - 5 <= ts < settle_time):
                        funding_time[symbol] = settle_time
                        dt_local = datetime.fromtimestamp(settle_time).strftime("%Y-%m-%d %H:%M:%S")
                        logger.info(f"{symbol} 下次 funding 結算預定於 {dt_local}")

                    # 當前時間已過 funding_time，表示剛好落在結算時段
                    if (symbol in funding_time) and ts >= funding_time[symbol]:
                        logger.info(f"{symbol} 進入 funding 結算時間 ({funding_rate*100:.3f}%)")
                        # 此時若心跳斷裂，心跳檢測自動會在 3s 後通報

                elif stream_type.endswith("bookTicker"):
                    symbol = data.get("s", "").lower()
                    # 更新該幣種 bookTicker 最近到達時間（作為主要心跳）
                    if symbol:
                        last_bkt_ts[symbol] = time.time()
                    # 可選：記錄最優買賣報價
                    try:
                        bid = float(data.get("b", 0.0)); bid_qty = float(data.get("B", 0.0))
                        ask = float(data.get("a", 0.0)); ask_qty = float(data.get("A", 0.0))
                        logger.debug(f"{symbol} bookTicker: bid {bid} x {bid_qty} | ask {ask} x {ask_qty}")
                    except Exception:
                        pass

                # aggTrade stream 處理
                elif stream_type.endswith("aggTrade"):
                    symbol = data.get("s", "").lower()
                    price = float(data.get("p", 0.0))
                    volume = float(data.get("q", 0.0))
                    trade_time = int(data.get("T", 0) // 1000)
                    last_trade_ts[symbol] = time.time()
                    logger.info(f"{symbol} 即時成交量: {volume} @ {price} (time: {trade_time})")
        finally:
            server_time_task.cancel()
            hb_task.cancel()
            ping_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(monitor_funding())
    except KeyboardInterrupt:
        logger.info("主程式中斷，退出。")