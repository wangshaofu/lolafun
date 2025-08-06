import asyncio
import json
import logging
import websockets
from datetime import datetime
import statistics
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.ntp_sync import NTPTimeSync

# Binance USDT-M Mark Price WebSocket 多流
BINANCE_WS = "wss://fstream.binance.com/stream?streams="
SYMBOLS     = ["OMNIUSDT","ALPACAUSDT"]      # 欲監控幣種 "btcusdt", "ethusdt", "nknusdt", etc. 
SYMBOLS = [s.lower() for s in SYMBOLS]

STREAM_FMT  = "{}@markPrice@1s"           # 每秒接收 markPrice+fundingRate
AGGTRADE_FMT = "{}@aggTrade"             # 即時成交量 stream

SAFETY_MARGIN = 1.0            # extra slack (s) on top of dynamic 2*OWD
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("FW-Heartbeat")

ntp_sync = NTPTimeSync()
clock_offset = 0.0                 # NTP offset (seconds)
latency_samples = asyncio.Queue()   # store last 50 one‑way delays




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

    # 組合訂閱串，包含 markPrice@1s 與 aggTrade
    streams = []
    for s in SYMBOLS:
        streams.append(STREAM_FMT.format(s))
        streams.append(AGGTRADE_FMT.format(s))
    url = BINANCE_WS + "/".join(streams)

    last_msg_ts = asyncio.get_event_loop().time()
    funding_time = None

    async with websockets.connect(url, ping_interval=None) as ws:
        logger.info("WebSocket 已連線，開始監聽…")

        async def watch_heartbeat():
            nonlocal last_msg_ts
            while True:
                await asyncio.sleep(1)
                thresh = current_heartbeat_threshold()
                now = asyncio.get_event_loop().time()
                if now - last_msg_ts > thresh:
                    print(f"[heartbeat] no message for {now - last_msg_ts:.1f}s "
                          f"(threshold {thresh:.2f}s) – possible downtime")
                    last_msg_ts = now  # reset so we don't spam

        # 背景心跳檢測任務
        hb_task = asyncio.create_task(watch_heartbeat())

        try:
            async for raw in ws:
                last_msg_ts = asyncio.get_event_loop().time()
                msg = json.loads(raw)
                stream_type = msg.get("stream", "")
                data = msg.get("data", {})

                # markPrice stream 處理
                if stream_type.endswith("markPrice@1s"):
                    # ---- latency measurement --------------------------------
                    local_now = asyncio.get_event_loop().time()
                    event_ts = data.get("E", 0) / 1000   # server epoch (s)
                    if event_ts:
                        owd = local_now - (event_ts + clock_offset)
                        if owd >= 0:
                            if latency_samples.qsize() >= 50:
                                latency_samples.get_nowait()
                            latency_samples.put_nowait(owd)

                    symbol        = data.get("s", "").lower()
                    ts            = data.get("E", 0) // 1000     # event time (s)
                    funding_rate  = float(data.get("r", 0.0))
                    settle_time   = data.get("T", 0) // 1000     # funding 結算時間 (s)

                    # 初次或接近結算前 5 秒，自動記錄下一次結算
                    if funding_time is None or ts < settle_time - 5 <= ts:
                        funding_time = settle_time
                        dt_local = datetime.fromtimestamp(settle_time).strftime("%Y-%m-%d %H:%M:%S")
                        logger.info(f"{symbol} 下次 funding 結算預定於 {dt_local}")

                    # 當前時間已過 funding_time，表示剛好落在結算時段
                    if funding_time and ts >= funding_time:
                        logger.info(f"{symbol} 進入 funding 結算時間 ({funding_rate*100:.3f}%)")
                        # 此時若心跳斷裂，心跳檢測自動會在 3s 後通報

                # aggTrade stream 處理
                elif stream_type.endswith("aggTrade"):
                    symbol = data.get("s", "").lower()
                    price = float(data.get("p", 0.0))
                    volume = float(data.get("q", 0.0))
                    trade_time = data.get("T", 0) // 1000
                    logger.info(f"{symbol} 即時成交量: {volume} @ {price} (time: {trade_time})")
        finally:
            hb_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(monitor_funding())
    except KeyboardInterrupt:
        logger.info("主程式中斷，退出。")