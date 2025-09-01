# Integrated funding-settlement monitor
# - Find most positive/negative funding rate and its next settlement time
# - Wait until 1 minute before settlement
# - Listen to bookTicker for that symbol with server-time alignment
# - Capture data around settlement and generate two plots to reveal short freezes

import asyncio
import json
import logging
import time
import websockets
import aiohttp
from datetime import datetime, timezone
import statistics
import sys
import os
import argparse
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.ntp_sync import NTPTimeSync


# 解析命令列參數
parser = argparse.ArgumentParser(description="Binance Funding Settlement Freeze Monitor")
parser.add_argument('--symbols', type=str, default=None, help='以逗號分隔的幣種列表，例如 BTCUSDT,ETHUSDT')
parser.add_argument('--window-pre', type=float, default=15.0, help='結算前收集秒數')
parser.add_argument('--window-post', type=float, default=15.0, help='結算後收集秒數')
parser.add_argument('--start-before', type=float, default=60.0, help='提早於結算多少秒開始監聽')
parser.add_argument('--output-dir', type=str, default='analysis/plots', help='輸出圖片資料夾')
parser.add_argument('--log-dir', type=str, default='analysis/logs', help='極值funding紀錄資料夾，會輸出 most_positive.csv / most_negative.csv')
parser.add_argument('--pick', type=str, default='both', choices=['abs','pos','neg','both'], help='選擇最正、最負、同時兩者或絕對值最大的 funding rate')
parser.add_argument('--min-quote-volume', type=float, default=15000000.0, help='24h USDT 成交額(quoteVolume)最小門檻，低於此值會改用第二順位')
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


def _append_csv(path: Path, row: list[str], header: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open('a', encoding='utf-8') as f:
        if not exists:
            f.write(','.join(header) + '\n')
        f.write(','.join(row) + '\n')


# ---- Funding snapshot + picking helpers -------------------------------------
async def _fetch_snapshot(session: aiohttp.ClientSession):
    """Fetch funding (premiumIndex) and 24h volumes.
    Returns (sorted_by_rate, vol_by_symbol, most_negative, most_positive).
    """
    url = "https://fapi.binance.com/fapi/v1/premiumIndex"
    async with session.get(url, timeout=10) as resp:
        all_items = await resp.json()
    url24 = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    async with session.get(url24, timeout=10) as resp2:
        tick24 = await resp2.json()

    filtered = [x for x in all_items if 'lastFundingRate' in x and 'nextFundingTime' in x]
    sorted_by_rate = sorted(filtered, key=lambda x: float(x['lastFundingRate']))
    most_negative = sorted_by_rate[0]
    most_positive = sorted_by_rate[-1]

    vol_by_symbol: dict[str, float] = {}
    try:
        for t in tick24:
            sym = t.get('symbol')
            qv = t.get('quoteVolume')
            if sym is not None and qv is not None:
                try:
                    vol_by_symbol[sym] = float(qv)
                except Exception:
                    pass
    except Exception:
        pass

    return sorted_by_rate, vol_by_symbol, most_negative, most_positive


def _pick_with_volume(sorted_list: list[dict], side: str, vol_by_symbol: dict[str, float], min_qv: float) -> tuple[dict, float, dict|None]:
    """依 funding rate 極值方向挑選，若成交額不足則往次佳遞補。
    回傳 (挑選項目, quoteVolume, 被淘汰的原始極值或 None)
    """
    if side == 'pos':
        ordered = list(reversed(sorted_list))  # 高 -> 低
    else:
        ordered = list(sorted_list)            # 低 -> 高
    rejected = None
    for idx, x in enumerate(ordered):
        sym = x['symbol']
        qv = vol_by_symbol.get(sym, 0.0)
        if qv >= min_qv:
            if idx == 0:
                return x, qv, None
            rejected = ordered[0]
            return x, qv, rejected
    x = ordered[0]
    return x, vol_by_symbol.get(x['symbol'], 0.0), None


async def _monitor_symbol(symbol: str, next_time_ms: int):
    # 3) 等到結算前 start-before 秒
    desired_start = next_time_ms / 1000 - float(args.start_before)
    wait_s = desired_start - server_now()
    if wait_s > 0:
        logger.info(f"等待 {wait_s:.1f}s 至結算前 {args.start_before:.0f}s 開始監聽 {symbol}…")
        await asyncio.sleep(wait_s)
    else:
        logger.info(f"{symbol} 已在結算前窗口內，直接開始監聽…")

    # 若接近結算，強制再做一次 NTP 對時
    try:
        ntp_sync.force_sync_before_settlement(next_time_ms)
    except Exception:
        pass

    # 4) 監聽目標 symbol 的 bookTicker 並收集資料
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    url = BINANCE_WS + BOOKTICKER_FMT.format(symbol.lower())
    logger.info(f"開始收集 {symbol} bookTicker：{url}")

    # Setup a dedicated log file for this symbol's bookTicker stream
    bt_log_dir = Path(args.log_dir) / "bookticker"
    bt_log_dir.mkdir(parents=True, exist_ok=True)
    bt_log_path = bt_log_dir / f"{symbol}_bookticker_{int(next_time_ms/1000)}.log"
    bt_file_handler = logging.FileHandler(bt_log_path, encoding='utf-8')
    bt_file_handler.setLevel(logging.INFO)
    bt_file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(bt_file_handler)

    # Keep full info for each tick:
    # (recv_ts_sec_server_aligned, event_ts_ms_from_binance, update_id, bid, ask)
    records = []
    start_collect = next_time_ms/1000 - float(args.window_pre)
    end_collect = next_time_ms/1000 + float(args.window_post)

    async with websockets.connect(url, ping_interval=None) as ws:
        try:
            async for raw in ws:
                now_srv = server_now()
                msg = json.loads(raw)
                data = msg.get('data', msg)  # 支援 combined / 單一
                if 's' in data and ('b' in data or 'B' in data) and ('a' in data or 'A' in data):
                    # Prices
                    bid = float(data.get('b', data.get('B', 0)))
                    ask = float(data.get('a', data.get('A', 0)))
                    # Quantities (best bid/ask qty)
                    bid_qty = float(data.get('B', data.get('q', 0)))
                    ask_qty = float(data.get('A', data.get('Q', 0)))
                    # Update id
                    u = int(data.get('u', 0))

                    # Binance timestamp in milliseconds per docs (minimal unit)
                    # Choose the smaller of E (event time) and T (transaction time) if both exist
                    ev_candidates = []
                    if 'E' in data:
                        try:
                            ev_candidates.append(int(data['E']))
                        except Exception:
                            pass
                    if 'T' in data:
                        try:
                            ev_candidates.append(int(data['T']))
                        except Exception:
                            pass
                    if ev_candidates:
                        ev_ts_ms = min(ev_candidates)
                    else:
                        ev_ts_ms = int(now_srv * 1000)

                    # Log every bookTicker tick with event time and quantities
                    try:
                        s_lower = str(data.get('s', symbol)).lower()
                        logger.info(f"[成交量] {s_lower} ts={ev_ts_ms} bid_qty={bid_qty} ask_qty={ask_qty}")
                    except Exception:
                        pass

                    # Keep records for plotting within the collection window
                    if start_collect <= now_srv <= end_collect:
                        records.append((now_srv, ev_ts_ms, u, bid, ask))
                # 收到結束時間之後結束
                if now_srv > end_collect:
                    break
        finally:
            # Ensure log file handler is removed and closed even on error
            try:
                logger.removeHandler(bt_file_handler)
                bt_file_handler.close()
            except Exception:
                pass

    # After collection, trigger plotting in background so we can proceed immediately
    if not records:
        logger.warning(f"{symbol} Window 內沒有任何 bookTicker 紀錄，無法繪圖。")
        return

    async def _plot_async():
        await asyncio.to_thread(_plot_bookticker_sync, records, symbol, next_time_ms, out_dir, bt_log_path)
    asyncio.create_task(_plot_async())


def _plot_bookticker_sync(records, symbol: str, next_time_ms: int, out_dir: Path, bt_log_path: Path):
    # Arrival times (server-aligned) for plotting x-axis
    times = [datetime.fromtimestamp(t_recv) for (t_recv, _, _, _, _) in records]

    # 1) True one-way latency (arrival_time - event_time)
    latency_ms = [(t_recv * 1000.0) - ev_ms for (t_recv, ev_ms, *_rest) in records]

    # 3) Event-time gap (ms) – exchange side pacing
    event_gap_ms = [0.0]
    for i in range(1, len(records)):
        event_gap_ms.append(records[i][1] - records[i-1][1])

    # 以結算時刻之後的資料為主進行極值觀測
    settle_ts = next_time_ms / 1000.0
    # 第一個落在結算後的索引（以接收時間判斷）
    first_post_idx = None
    for i, (t_recv, *_rest) in enumerate(records):
        if t_recv >= settle_ts:
            first_post_idx = i
            break
    post_indices = list(range(first_post_idx or 0, len(records))) if first_post_idx is not None else list(range(len(records)))
    # 事件時間差需要兩個點皆在結算後，因此從 first_post_idx+1 開始
    post_evt_gap_start = (first_post_idx + 1) if first_post_idx is not None else 1
    post_evt_gap_indices = list(range(post_evt_gap_start, len(records))) if len(records) > 1 else []

    # 找出最大延遲與最大事件時間差（以結算後為主，若無則全窗）
    if post_indices:
        max_idx = max(post_indices, key=lambda i: latency_ms[i])
    else:
        max_idx = max(range(len(latency_ms)), key=lambda i: latency_ms[i])
    max_t, _max_ev, max_u, max_bid, max_ask = records[max_idx]

    if post_evt_gap_indices:
        evt_gap_idx = max(post_evt_gap_indices, key=lambda i: event_gap_ms[i])
    else:
        evt_gap_idx = max(range(len(event_gap_ms)), key=lambda i: event_gap_ms[i])
    evt_gap_t = records[evt_gap_idx][0]

    # Plot 1: BookTicker Latency（單向延遲）
    plt.figure(figsize=(12,6))
    plt.title(f"{symbol} BookTicker Latency (ms)")
    plt.scatter(times, latency_ms, s=10)
    # 結算時間垂直線
    funding_dt = datetime.fromtimestamp(settle_ts)
    plt.axvline(funding_dt, color='k', linestyle='--', alpha=0.6, label='Funding Time')
    plt.xlabel("Event Time (datetime)")
    plt.ylabel("Latency (ms)")
    plt.grid(True, alpha=0.2)
    # 標示最大延遲（結算後）
    plt.scatter([datetime.fromtimestamp(max_t)], [latency_ms[max_idx]], color='orange', s=120, label="Largest Post-Settlement Latency")
    plt.legend(title=f"Largest Post Latency\norder_book_update_id: {max_u}\nBid: {max_bid:.6f}\nAsk: {max_ask:.6f}")
    f1 = out_dir / f"{symbol}_latency_{int(next_time_ms/1000)}.png"
    plt.tight_layout()
    plt.savefig(f1)
    plt.close()
    logger.info(f"本次 bookTicker 詳細時間戳已寫入：{bt_log_path}")

    # Plot 2: Event Time Differences (exchange-side pacing)
    plt.figure(figsize=(12,6))
    plt.title(f"{symbol} Event Time Differences (ms)")
    plt.scatter(times, event_gap_ms, s=10)
    plt.xlabel("Event Time (datetime)")
    plt.ylabel("Difference to Previous Event (ms)")
    plt.grid(True, alpha=0.2)
    # 結算時間垂直線
    plt.axvline(funding_dt, color='k', linestyle='--', alpha=0.6, label='Funding Time')
    # 標示最大事件時間差（結算後）
    plt.scatter([datetime.fromtimestamp(evt_gap_t)], [event_gap_ms[evt_gap_idx]], color='red', s=120, label="Largest Post-Settlement Event Gap")
    plt.legend(title=f"Largest Post Event Gap = {event_gap_ms[evt_gap_idx]:.1f} ms")
    f2 = out_dir / f"{symbol}_event_gap_{int(next_time_ms/1000)}.png"
    plt.tight_layout()
    plt.savefig(f2)
    plt.close()

    logger.info(f"已輸出圖檔：{f1} , {f2}")
async def _watch_loop(side: str):
    """Continuously pick and monitor the next settlement for given side ('pos' or 'neg')."""
    assert side in ('pos', 'neg')
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                sorted_by_rate, vol_by_symbol, most_negative, most_positive = await _fetch_snapshot(session)

            # 紀錄一次極值到 CSV（供追蹤使用）
            ts_iso = datetime.now(timezone.utc).isoformat()
            log_dir = Path(args.log_dir)
            _append_csv(log_dir / 'most_positive.csv',
                        [ts_iso, most_positive['symbol'], f"{float(most_positive['lastFundingRate']):.8f}", str(int(most_positive['nextFundingTime']))],
                        ["timestamp","symbol","lastFundingRate","nextFundingTime"])
            _append_csv(log_dir / 'most_negative.csv',
                        [ts_iso, most_negative['symbol'], f"{float(most_negative['lastFundingRate']):.8f}", str(int(most_negative['nextFundingTime']))],
                        ["timestamp","symbol","lastFundingRate","nextFundingTime"])

            pick, qv, rejected = _pick_with_volume(sorted_by_rate, side, vol_by_symbol, float(args.min_quote_volume))
            if rejected is not None:
                logger.info(f"{side.upper()} 極值 {rejected['symbol']} qv={vol_by_symbol.get(rejected['symbol'],0.0):,.0f} 低於門檻，改監聽 {pick['symbol']} qv={qv:,.0f}")
            else:
                logger.info(f"{side.upper()} 監聽 {pick['symbol']} fr={float(pick['lastFundingRate']):+,.6f} qv={qv:,.0f}")

            await _monitor_symbol(pick['symbol'], int(pick['nextFundingTime']))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"{side.upper()} watcher 發生錯誤：{e}")
            await asyncio.sleep(5)



async def monitor_funding():
    # 1) NTP 校時 + 伺服器時間對齊
    logger.info("執行 NTP 校時…")
    offset_ms, delay_ms = ntp_sync.sync_time()
    global clock_offset
    clock_offset = offset_ms / 1000  # 轉為秒
    logger.info(f"NTP offset 設定為 {clock_offset:.3f} 秒 (delay {delay_ms:.1f} ms)")
    logger.info("啟動 Binance 伺服器時間對齊背景任務…")
    server_time_task = asyncio.create_task(maintain_server_time_offset())

    # 2) 長期監聽（正、負獨立）
    tasks = []
    if args.pick in ('both', 'pos', 'abs'):
        tasks.append(asyncio.create_task(_watch_loop('pos')))
    if args.pick in ('both', 'neg', 'abs'):
        tasks.append(asyncio.create_task(_watch_loop('neg')))

    try:
        await asyncio.gather(*tasks)
    finally:
        server_time_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(monitor_funding())
    except KeyboardInterrupt:
        logger.info("主程式中斷，退出。")
