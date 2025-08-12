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
import argparse
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.ntp_sync import NTPTimeSync


# 解析命令列參數
parser = argparse.ArgumentParser(description="Binance Funding Monitor")
parser.add_argument('--symbols', type=str, default=None, help='以逗號分隔的幣種列表，例如 BTCUSDT,ETHUSDT')
args, unknown = parser.parse_known_args()

# 預設目標幣種
DEFAULT_SYMBOLS_TARGET = ["FUNUSDT"]
if args.symbols:
    SYMBOLS_TARGET = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]
else:
    SYMBOLS_TARGET = DEFAULT_SYMBOLS_TARGET

BINANCE_WS = "wss://fstream.binance.com/stream?streams="
SYMBOLS = [s.lower() for s in SYMBOLS_TARGET]
TARGET_SET = set([s.lower() for s in SYMBOLS_TARGET])


BOOKTICKER_FMT = "{}@bookTicker"          # 最佳買一/賣一（主要心跳）

SAFETY_MARGIN = 1.0            # extra slack (s) on top of dynamic 2*OWD

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

    # 只組合目標幣 bookTicker stream
    streams = [BOOKTICKER_FMT.format(s) for s in SYMBOLS]
    url = BINANCE_WS + "/".join(streams)
    
    logger.info(f"Target Symbol : {SYMBOLS_TARGET}")
    last_bkt_ts = {s: time.time() for s in SYMBOLS}  # 每幣種 bookTicker 最近到達時間（epoch 秒）
    downtime_start = {s: None for s in SYMBOLS}      # 每幣種停機起點
    last_msg_ts = time.time()

    async with websockets.connect(url, ping_interval=None) as ws:
        logger.info("WebSocket connected, listening…")

        last_pong_ts = time.time()

        async def watch_heartbeat():
            nonlocal last_msg_ts, last_pong_ts
            while True:
                await asyncio.sleep(1)
                thresh = current_heartbeat_threshold()
                now_local = time.time()
                # 統計：目前哪些幣的 bookTicker 超門檻
                for sym in list(last_bkt_ts.keys()):
                    bkt_gap = now_local - last_bkt_ts[sym]
                    if bkt_gap > thresh:
                        msg = f"[freeze] {sym} bookTicker gap={bkt_gap:.2f}s (> {thresh:.2f}s)"
                        logger.warning(msg)
                        if downtime_start[sym] is None:
                            downtime_start[sym] = now_local
                    else:
                        if downtime_start[sym] is not None:
                            duration = now_local - downtime_start[sym]
                            logger.info(f"[restore] {sym} bookTicker restored after {duration:.2f}s downtime")
                            downtime_start[sym] = None
                # 補充：ping/pong 超時提醒
                if now_local - last_pong_ts > 30:
                    logger.warning(f"[ping/pong] no pong for {now_local - last_pong_ts:.1f}s – possible network issue")

        async def send_ping():
            nonlocal last_pong_ts
            while True:
                await asyncio.sleep(10)
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
                msg = json.loads(raw)
                stream_type = msg.get("stream", "")
                data = msg.get("data", {})
                if stream_type.endswith("bookTicker"):
                    symbol = data.get("s", "").lower()
                    if symbol:
                        last_bkt_ts[symbol] = time.time()
                    try:
                        bid = float(data.get("b", 0.0)); bid_qty = float(data.get("B", 0.0))
                        ask = float(data.get("a", 0.0)); ask_qty = float(data.get("A", 0.0))
                        logger.debug(f"{symbol} bookTicker: bid {bid} x {bid_qty} | ask {ask} x {ask_qty}")
                    except Exception:
                        pass
        finally:
            server_time_task.cancel()
            hb_task.cancel()
            ping_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(monitor_funding())
    except KeyboardInterrupt:
        logger.info("主程式中斷，退出。")