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
parser.add_argument('--start-before', type=float, default=60.0, help='提早於結算多少秒開始監聯')
parser.add_argument('--output-dir', type=str, default='analysis/plots', help='輸出圖片資料夾')
parser.add_argument('--log-dir', type=str, default='analysis/logs', help='極值funding紀錄資料夾，會輸出 most_positive.csv / most_negative.csv')
parser.add_argument('--pick', type=str, default='both', choices=['abs','pos','neg','both'], help='選擇最正、最負、同時兩者或絕對值最大的 funding rate')
parser.add_argument('--min-quote-volume', type=float, default=15000000.0, help='24h USDT 成交額(quoteVolume)最小門檻，低於此值會改用第二順位')
# === 新增：結算時刻精細分析參數 ===
parser.add_argument('--critical-window', type=float, default=2.0, help='結算時刻前後的關鍵視窗(秒)，用於精細分析凍結現象')
parser.add_argument('--freeze-threshold-ms', type=float, default=200.0, help='event gap 超過此毫秒數視為「凍結」')
parser.add_argument('--latency-spike-threshold-ms', type=float, default=50.0, help='延遲超過此毫秒數視為「延遲飆升」')
parser.add_argument('--multi-stream', action='store_true', default=False, help='啟用多 Stream 監控模式（同時監聽 bookTicker, aggTrade, markPrice, depth）')
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

# Stream 格式定義
BOOKTICKER_FMT = "{}@bookTicker"          # 最佳買一/賣一（主要心跳）
AGGTRADE_FMT = "{}@aggTrade"              # 聚合成交（用於交叉比對）
MARKPRICE_FMT = "{}@markPrice@1s"         # Mark Price（每秒推送，含 funding rate）
DEPTH_FMT = "{}@depth@100ms"              # 深度更新（100ms 推送）

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


# ======================== 多 Stream 延遲偵測方法 ========================

class MultiStreamDelayDetector:
    """
    使用多種 Stream 交叉比對來偵測 Binance 伺服器的 time delay
    
    偵測方法：
    1. bookTicker latency - 單向延遲 (recv_time - event_time)
    2. event_gap - 交易所端事件時間間隔異常
    3. update_id 跳號 - 檢查 update_id 是否有缺失
    4. aggTrade vs bookTicker 時間差 - 兩個 stream 的事件時間比對
    5. markPrice 更新延遲 - funding rate 結算時 mark price 的更新時機
    6. depth 更新頻率 - 深度更新的間隔異常
    """
    
    def __init__(self, symbol: str, next_time_ms: int, out_dir: Path, log_dir: Path):
        self.symbol = symbol.lower()
        self.next_time_ms = next_time_ms
        self.settle_ts = next_time_ms / 1000.0
        self.out_dir = out_dir
        self.log_dir = log_dir
        
        # 各 stream 的紀錄
        self.bookticker_records = []   # (recv_ts, event_ts_ms, update_id, bid, ask, bid_qty, ask_qty)
        self.aggtrade_records = []     # (recv_ts, event_ts_ms, trade_id, price, qty, is_buyer_maker)
        self.markprice_records = []    # (recv_ts, event_ts_ms, mark_price, funding_rate, next_funding_time)
        self.depth_records = []        # (recv_ts, event_ts_ms, first_update_id, final_update_id)
        
        # Update ID 追蹤（用於跳號偵測）
        self.last_bookticker_uid = None
        self.last_depth_uid = None
        self.uid_gaps = []  # 記錄跳號事件
        
        # 即時統計
        self.freeze_events = []
        self.latency_spikes = []
        self.cross_stream_anomalies = []
        
        # 設定日誌
        self._setup_logger()
    
    def _setup_logger(self):
        self.bt_log_dir = self.log_dir / "multi_stream"
        self.bt_log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.bt_log_dir / f"{self.symbol}_multi_stream_{int(self.next_time_ms/1000)}.log"
        self.file_handler = logging.FileHandler(self.log_path, encoding='utf-8')
        self.file_handler.setLevel(logging.INFO)
        self.file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(self.file_handler)
    
    def cleanup_logger(self):
        try:
            logger.removeHandler(self.file_handler)
            self.file_handler.close()
        except Exception:
            pass
    
    def process_bookticker(self, data: dict, recv_ts: float, now_srv: float):
        """處理 bookTicker 訊息"""
        bid = float(data.get('b', data.get('B', 0)))
        ask = float(data.get('a', data.get('A', 0)))
        bid_qty = float(data.get('B', data.get('q', 0)))
        ask_qty = float(data.get('A', data.get('Q', 0)))
        u = int(data.get('u', 0))
        
        ev_ts_ms = self._get_event_time(data, now_srv)
        
        # Update ID 跳號偵測
        if self.last_bookticker_uid is not None and u > 0:
            gap = u - self.last_bookticker_uid
            if gap > 1:
                rel_t = now_srv - self.settle_ts
                self.uid_gaps.append(('bookticker', rel_t, self.last_bookticker_uid, u, gap))
                logger.warning(f"[UID跳號] bookTicker uid 從 {self.last_bookticker_uid} 跳至 {u}，缺失 {gap-1} 筆 (t={rel_t:+.3f}s)")
        self.last_bookticker_uid = u
        
        self.bookticker_records.append((now_srv, ev_ts_ms, u, bid, ask, bid_qty, ask_qty))
        return ev_ts_ms
    
    def process_aggtrade(self, data: dict, recv_ts: float, now_srv: float):
        """處理 aggTrade 訊息"""
        trade_id = int(data.get('a', 0))
        price = float(data.get('p', 0))
        qty = float(data.get('q', 0))
        is_buyer_maker = data.get('m', False)
        
        ev_ts_ms = self._get_event_time(data, now_srv)
        trade_ts_ms = int(data.get('T', ev_ts_ms))  # Trade time
        
        self.aggtrade_records.append((now_srv, ev_ts_ms, trade_ts_ms, trade_id, price, qty, is_buyer_maker))
        return ev_ts_ms
    
    def process_markprice(self, data: dict, recv_ts: float, now_srv: float):
        """處理 markPrice 訊息"""
        mark_price = float(data.get('p', 0))
        funding_rate = float(data.get('r', 0))
        next_funding_time = int(data.get('T', 0))
        
        ev_ts_ms = self._get_event_time(data, now_srv)
        
        self.markprice_records.append((now_srv, ev_ts_ms, mark_price, funding_rate, next_funding_time))
        
        # 檢查 funding rate 是否剛結算（next_funding_time 改變）
        if len(self.markprice_records) > 1:
            prev_next_funding = self.markprice_records[-2][4]
            if next_funding_time != prev_next_funding and next_funding_time > prev_next_funding:
                rel_t = now_srv - self.settle_ts
                latency = (now_srv * 1000.0) - ev_ts_ms
                logger.info(f"[Funding結算] markPrice 顯示 funding 已結算! t={rel_t:+.3f}s latency={latency:.1f}ms new_next_funding={next_funding_time}")
        
        return ev_ts_ms
    
    def process_depth(self, data: dict, recv_ts: float, now_srv: float):
        """處理 depth 訊息"""
        first_uid = int(data.get('U', 0))
        final_uid = int(data.get('u', 0))
        prev_uid = int(data.get('pu', 0))
        
        ev_ts_ms = self._get_event_time(data, now_srv)
        
        # 檢查 depth 的 previous update id 連續性
        if self.last_depth_uid is not None and prev_uid > 0:
            if prev_uid != self.last_depth_uid:
                rel_t = now_srv - self.settle_ts
                self.uid_gaps.append(('depth', rel_t, self.last_depth_uid, first_uid, prev_uid))
                logger.warning(f"[Depth不連續] pu={prev_uid} 不等於上次 u={self.last_depth_uid} (t={rel_t:+.3f}s)")
        self.last_depth_uid = final_uid
        
        self.depth_records.append((now_srv, ev_ts_ms, first_uid, final_uid, prev_uid))
        return ev_ts_ms
    
    def _get_event_time(self, data: dict, now_srv: float) -> int:
        """從訊息中取得事件時間"""
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
        return min(ev_candidates) if ev_candidates else int(now_srv * 1000)
    
    def cross_stream_analysis(self):
        """進行跨 stream 分析"""
        results = {
            'bookticker_count': len(self.bookticker_records),
            'aggtrade_count': len(self.aggtrade_records),
            'markprice_count': len(self.markprice_records),
            'depth_count': len(self.depth_records),
            'uid_gap_count': len(self.uid_gaps),
        }
        
        # 1. bookTicker 與 aggTrade 時間差分析
        if self.bookticker_records and self.aggtrade_records:
            # 找出結算時刻附近的記錄
            crit_bt = [(r[0], r[1]) for r in self.bookticker_records 
                       if abs(r[0] - self.settle_ts) <= float(args.critical_window)]
            crit_at = [(r[0], r[1]) for r in self.aggtrade_records 
                       if abs(r[0] - self.settle_ts) <= float(args.critical_window)]
            
            if crit_bt and crit_at:
                # 計算同一時刻兩個 stream 的事件時間差
                bt_evt_times = {int(r[0] * 10): r[1] for r in crit_bt}  # 以 100ms 為桶
                at_evt_times = {int(r[0] * 10): r[1] for r in crit_at}
                
                common_keys = set(bt_evt_times.keys()) & set(at_evt_times.keys())
                if common_keys:
                    diffs = [abs(bt_evt_times[k] - at_evt_times[k]) for k in common_keys]
                    results['cross_stream_max_diff_ms'] = max(diffs)
                    results['cross_stream_avg_diff_ms'] = statistics.mean(diffs)
                    logger.info(f"[跨Stream分析] bookTicker vs aggTrade 事件時間差: 最大={max(diffs)}ms, 平均={statistics.mean(diffs):.1f}ms")
        
        # 2. markPrice 結算時刻分析
        if self.markprice_records:
            # 找出結算時刻最近的 markPrice
            post_settle = [r for r in self.markprice_records if r[0] >= self.settle_ts]
            if post_settle:
                first_post = post_settle[0]
                delay_after_settle = (first_post[0] - self.settle_ts) * 1000
                results['markprice_first_post_settle_delay_ms'] = delay_after_settle
                logger.info(f"[MarkPrice] 結算後第一筆 markPrice 延遲: {delay_after_settle:.1f}ms")
        
        # 3. Update ID 跳號統計
        if self.uid_gaps:
            results['uid_gaps'] = self.uid_gaps
            logger.warning(f"[UID跳號統計] 共偵測到 {len(self.uid_gaps)} 次 update_id 跳號")
        
        return results
    
    def generate_multi_stream_plots(self):
        """生成多 stream 比較圖表"""
        if not self.bookticker_records:
            return
        
        fig, axes = plt.subplots(4, 1, figsize=(16, 16), sharex=True)
        fig.suptitle(f"{self.symbol.upper()} Multi-Stream Delay Analysis (Settlement at t=0)", fontsize=14, fontweight='bold')
        
        # 1. bookTicker Latency
        bt_rel_times = [(r[0] - self.settle_ts) for r in self.bookticker_records]
        bt_latency = [(r[0] * 1000.0) - r[1] for r in self.bookticker_records]
        axes[0].scatter(bt_rel_times, bt_latency, s=10, alpha=0.7, label='bookTicker')
        axes[0].axvline(0, color='red', linestyle='--', linewidth=2, label='Settlement')
        axes[0].set_ylabel('Latency (ms)')
        axes[0].set_title('bookTicker One-Way Latency')
        axes[0].legend()
        axes[0].grid(True, alpha=0.3)
        
        # 2. aggTrade Latency（如果有）
        if self.aggtrade_records:
            at_rel_times = [(r[0] - self.settle_ts) for r in self.aggtrade_records]
            at_latency = [(r[0] * 1000.0) - r[1] for r in self.aggtrade_records]
            axes[1].scatter(at_rel_times, at_latency, s=10, alpha=0.7, color='green', label='aggTrade')
        axes[1].axvline(0, color='red', linestyle='--', linewidth=2)
        axes[1].set_ylabel('Latency (ms)')
        axes[1].set_title('aggTrade One-Way Latency')
        axes[1].legend()
        axes[1].grid(True, alpha=0.3)
        
        # 3. markPrice 更新間隔
        if len(self.markprice_records) > 1:
            mp_rel_times = [(r[0] - self.settle_ts) for r in self.markprice_records[1:]]
            mp_gaps = [(self.markprice_records[i][0] - self.markprice_records[i-1][0]) * 1000 
                       for i in range(1, len(self.markprice_records))]
            axes[2].scatter(mp_rel_times, mp_gaps, s=20, alpha=0.7, color='purple', label='markPrice')
            axes[2].axhline(1000, color='orange', linestyle=':', alpha=0.7, label='Expected (1s)')
        axes[2].axvline(0, color='red', linestyle='--', linewidth=2)
        axes[2].set_ylabel('Update Gap (ms)')
        axes[2].set_title('markPrice Update Interval (expected: 1000ms)')
        axes[2].legend()
        axes[2].grid(True, alpha=0.3)
        
        # 4. Update ID 跳號時間線
        if self.uid_gaps:
            gap_times = [g[1] for g in self.uid_gaps]
            gap_sizes = [g[4] for g in self.uid_gaps]
            axes[3].bar(gap_times, gap_sizes, width=0.1, alpha=0.7, color='red', label='UID Gaps')
        axes[3].axvline(0, color='red', linestyle='--', linewidth=2)
        axes[3].set_xlabel('Time Relative to Settlement (seconds)')
        axes[3].set_ylabel('Gap Size')
        axes[3].set_title('Update ID Gaps (missing updates)')
        axes[3].legend()
        axes[3].grid(True, alpha=0.3)
        
        f_out = self.out_dir / f"{self.symbol}_multi_stream_{int(self.next_time_ms/1000)}.png"
        plt.tight_layout()
        plt.savefig(f_out, dpi=150)
        plt.close()
        logger.info(f"已輸出多 Stream 分析圖：{f_out}")
        
        return f_out


async def _monitor_symbol_multi_stream(symbol: str, next_time_ms: int):
    """使用多個 stream 監控結算時刻的延遲"""
    # 等到結算前 start-before 秒
    desired_start = next_time_ms / 1000 - float(args.start_before)
    wait_s = desired_start - server_now()
    if wait_s > 0:
        logger.info(f"等待 {wait_s:.1f}s 至結算前 {args.start_before:.0f}s 開始多Stream監聽 {symbol}…")
        await asyncio.sleep(wait_s)
    else:
        logger.info(f"{symbol} 已在結算前窗口內，直接開始多Stream監聽…")

    # NTP 對時
    try:
        ntp_sync.force_sync_before_settlement(next_time_ms)
    except Exception:
        pass

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    log_dir = Path(args.log_dir)
    
    detector = MultiStreamDelayDetector(symbol, next_time_ms, out_dir, log_dir)
    
    # 建構多 stream URL
    streams = [
        BOOKTICKER_FMT.format(symbol.lower()),
        AGGTRADE_FMT.format(symbol.lower()),
        MARKPRICE_FMT.format(symbol.lower()),
        DEPTH_FMT.format(symbol.lower()),
    ]
    url = BINANCE_WS + "/".join(streams)
    logger.info(f"開始多 Stream 監聽 {symbol}：{url}")
    
    start_collect = next_time_ms/1000 - float(args.window_pre)
    end_collect = next_time_ms/1000 + float(args.window_post)
    critical_start = next_time_ms/1000 - float(args.critical_window)
    critical_end = next_time_ms/1000 + float(args.critical_window)

    try:
        async with websockets.connect(url, ping_interval=None) as ws:
            async for raw in ws:
                recv_ts = time.time()
                now_srv = server_now()
                
                try:
                    msg = json.loads(raw)
                    stream = msg.get('stream', '')
                    data = msg.get('data', msg)
                    
                    # 根據 stream 類型處理
                    if 'bookTicker' in stream:
                        ev_ts = detector.process_bookticker(data, recv_ts, now_srv)
                        # 關鍵視窗即時日誌
                        if critical_start <= now_srv <= critical_end:
                            lat = (now_srv * 1000.0) - ev_ts
                            rel_t = now_srv - (next_time_ms/1000)
                            logger.info(f"[關鍵視窗][bookTicker] t={rel_t:+.3f}s latency={lat:.1f}ms")
                    
                    elif 'aggTrade' in stream:
                        ev_ts = detector.process_aggtrade(data, recv_ts, now_srv)
                        if critical_start <= now_srv <= critical_end:
                            lat = (now_srv * 1000.0) - ev_ts
                            rel_t = now_srv - (next_time_ms/1000)
                            logger.info(f"[關鍵視窗][aggTrade] t={rel_t:+.3f}s latency={lat:.1f}ms")
                    
                    elif 'markPrice' in stream:
                        detector.process_markprice(data, recv_ts, now_srv)
                        if critical_start <= now_srv <= critical_end:
                            rel_t = now_srv - (next_time_ms/1000)
                            fr = float(data.get('r', 0))
                            logger.info(f"[關鍵視窗][markPrice] t={rel_t:+.3f}s funding_rate={fr:.8f}")
                    
                    elif 'depth' in stream:
                        detector.process_depth(data, recv_ts, now_srv)
                
                except Exception as e:
                    logger.debug(f"處理訊息時發生錯誤：{e}")
                
                if now_srv > end_collect:
                    break
    
    except Exception as e:
        logger.error(f"WebSocket 連線錯誤：{e}")
    
    finally:
        detector.cleanup_logger()
    
    # 進行跨 stream 分析
    if detector.bookticker_records:
        logger.info("開始跨 Stream 分析...")
        analysis_results = detector.cross_stream_analysis()
        
        # 生成圖表
        await asyncio.to_thread(detector.generate_multi_stream_plots)
        
        # 同時也用原本的 bookTicker 分析
        bt_records = [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], recv_ts) 
                      for r, recv_ts in zip(detector.bookticker_records, 
                                            [r[0] for r in detector.bookticker_records])]
        await asyncio.to_thread(_plot_bookticker_sync, bt_records, symbol, next_time_ms, out_dir, detector.log_path)


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
    # (recv_ts_sec_server_aligned, event_ts_ms_from_binance, update_id, bid, ask, bid_qty, ask_qty, raw_recv_ts)
    records = []
    start_collect = next_time_ms/1000 - float(args.window_pre)
    end_collect = next_time_ms/1000 + float(args.window_post)
    
    # 計算關鍵視窗時間範圍（用於即時日誌）
    critical_window = float(args.critical_window)
    critical_start = next_time_ms/1000 - critical_window
    critical_end = next_time_ms/1000 + critical_window

    async with websockets.connect(url, ping_interval=None) as ws:
        try:
            last_event_ts = None
            async for raw in ws:
                raw_recv_ts = time.time()  # 未校正的本機接收時間
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

                    # 計算即時延遲和 event gap
                    latency_now = (now_srv * 1000.0) - ev_ts_ms
                    event_gap = (ev_ts_ms - last_event_ts) if last_event_ts else 0
                    last_event_ts = ev_ts_ms
                    
                    # 關鍵視窗內的即時日誌（更詳細）
                    if critical_start <= now_srv <= critical_end:
                        rel_to_settle = now_srv - (next_time_ms/1000)
                        s_lower = str(data.get('s', symbol)).lower()
                        logger.info(f"[關鍵視窗] {s_lower} t={rel_to_settle:+.3f}s latency={latency_now:.1f}ms event_gap={event_gap}ms bid={bid} ask={ask}")
                    else:
                        # Log every bookTicker tick with event time and quantities
                        try:
                            s_lower = str(data.get('s', symbol)).lower()
                            logger.info(f"[成交量] {s_lower} ts={ev_ts_ms} bid_qty={bid_qty} ask_qty={ask_qty}")
                        except Exception:
                            pass

                    # Keep records for plotting within the collection window
                    if start_collect <= now_srv <= end_collect:
                        records.append((now_srv, ev_ts_ms, u, bid, ask, bid_qty, ask_qty, raw_recv_ts))
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
    """繪製 bookTicker 延遲分析圖，包含結算時刻 ±2 秒的精細分析"""
    settle_ts = next_time_ms / 1000.0
    critical_window = float(args.critical_window)  # 結算時刻前後的關鍵視窗
    freeze_threshold = float(args.freeze_threshold_ms)
    latency_spike_threshold = float(args.latency_spike_threshold_ms)
    
    # records 結構: (recv_ts_sec_server_aligned, event_ts_ms, update_id, bid, ask, bid_qty, ask_qty, raw_recv_ts)
    # Arrival times (server-aligned) for plotting x-axis
    times = [datetime.fromtimestamp(r[0]) for r in records]
    times_sec = [r[0] for r in records]  # epoch seconds
    
    # 相對於結算時刻的時間差（秒）
    relative_times = [t - settle_ts for t in times_sec]

    # 1) True one-way latency (arrival_time - event_time)
    latency_ms = [(r[0] * 1000.0) - r[1] for r in records]

    # 3) Event-time gap (ms) – exchange side pacing
    event_gap_ms = [0.0]
    for i in range(1, len(records)):
        event_gap_ms.append(records[i][1] - records[i-1][1])

    # 4) Arrival gap (ms) - 接收端間隔
    arrival_gap_ms = [0.0]
    for i in range(1, len(records)):
        arrival_gap_ms.append((records[i][0] - records[i-1][0]) * 1000.0)

    # ========== 關鍵視窗分析 (結算時刻 ± critical_window 秒) ==========
    critical_start = settle_ts - critical_window
    critical_end = settle_ts + critical_window
    
    # 篩選關鍵視窗內的資料
    critical_indices = [i for i, t in enumerate(times_sec) if critical_start <= t <= critical_end]
    critical_records = [(i, records[i], latency_ms[i], event_gap_ms[i], arrival_gap_ms[i], relative_times[i]) 
                        for i in critical_indices]
    
    # 凍結偵測：event gap 超過閾值
    freeze_events = [(i, event_gap_ms[i], relative_times[i]) 
                     for i in critical_indices if event_gap_ms[i] > freeze_threshold]
    
    # 延遲飆升偵測
    latency_spikes = [(i, latency_ms[i], relative_times[i]) 
                      for i in critical_indices if latency_ms[i] > latency_spike_threshold]
    
    # 統計關鍵視窗內的指標
    if critical_indices:
        critical_latencies = [latency_ms[i] for i in critical_indices]
        critical_event_gaps = [event_gap_ms[i] for i in critical_indices if i > 0]
        
        stats = {
            'count': len(critical_indices),
            'latency_mean': statistics.mean(critical_latencies),
            'latency_max': max(critical_latencies),
            'latency_min': min(critical_latencies),
            'latency_stdev': statistics.stdev(critical_latencies) if len(critical_latencies) > 1 else 0,
            'event_gap_mean': statistics.mean(critical_event_gaps) if critical_event_gaps else 0,
            'event_gap_max': max(critical_event_gaps) if critical_event_gaps else 0,
            'freeze_count': len(freeze_events),
            'spike_count': len(latency_spikes),
        }
        
        # ========== 找出真正的 Settlement 時間點 ==========
        # 方法：找出最大 event_gap 發生的位置，那就是凍結開始的時間點
        # 凍結開始 = 真正的 settlement 處理開始
        if freeze_events:
            # 找出最大的 freeze event
            max_freeze = max(freeze_events, key=lambda x: x[1])
            max_freeze_idx, max_freeze_gap, max_freeze_rel_t = max_freeze
            
            # 凍結開始時間 = 上一筆的事件時間
            if max_freeze_idx > 0:
                prev_event_ts_ms = records[max_freeze_idx - 1][1]
                freeze_start_rel_ms = (prev_event_ts_ms - next_time_ms)  # 相對於預期結算時間的毫秒數
            else:
                freeze_start_rel_ms = max_freeze_rel_t * 1000
            
            # 凍結結束時間 = 這筆的事件時間
            curr_event_ts_ms = records[max_freeze_idx][1]
            freeze_end_rel_ms = (curr_event_ts_ms - next_time_ms)
            
            stats['freeze_start_rel_ms'] = freeze_start_rel_ms
            stats['freeze_end_rel_ms'] = freeze_end_rel_ms
            stats['freeze_duration_ms'] = max_freeze_gap
            stats['actual_settlement_offset_ms'] = freeze_start_rel_ms  # 真正結算相對於預期的偏移
            
            logger.info(f"  🎯 推測真正結算時間點:")
            logger.info(f"    凍結開始: {freeze_start_rel_ms:+.1f}ms (相對預期結算)")
            logger.info(f"    凍結結束: {freeze_end_rel_ms:+.1f}ms (相對預期結算)")
            logger.info(f"    凍結持續: {max_freeze_gap:.1f}ms")
        else:
            stats['freeze_start_rel_ms'] = None
            stats['freeze_end_rel_ms'] = None
            stats['freeze_duration_ms'] = None
            stats['actual_settlement_offset_ms'] = None
        
        # ========== 輸出長期統計 CSV（追加模式）==========
        settlement_stats_path = out_dir.parent / "logs" / "settlement_stats.csv"
        settlement_stats_path.parent.mkdir(parents=True, exist_ok=True)
        
        settle_dt = datetime.fromtimestamp(settle_ts, tz=timezone.utc)
        stats_row = [
            settle_dt.isoformat(),
            symbol,
            str(next_time_ms),
            f"{stats['count']}",
            f"{stats['latency_mean']:.2f}",
            f"{stats['latency_max']:.2f}",
            f"{stats['latency_min']:.2f}",
            f"{stats['latency_stdev']:.2f}",
            f"{stats['event_gap_mean']:.2f}",
            f"{stats['event_gap_max']:.2f}",
            f"{stats['freeze_count']}",
            f"{stats['spike_count']}",
            f"{stats['freeze_start_rel_ms']:.1f}" if stats['freeze_start_rel_ms'] is not None else "",
            f"{stats['freeze_end_rel_ms']:.1f}" if stats['freeze_end_rel_ms'] is not None else "",
            f"{stats['freeze_duration_ms']:.1f}" if stats['freeze_duration_ms'] is not None else "",
        ]
        stats_header = [
            "settlement_time_utc", "symbol", "expected_settle_ms",
            "tick_count", "latency_mean_ms", "latency_max_ms", "latency_min_ms", "latency_stdev_ms",
            "event_gap_mean_ms", "event_gap_max_ms",
            "freeze_count", "spike_count",
            "freeze_start_rel_ms", "freeze_end_rel_ms", "freeze_duration_ms"
        ]
        _append_csv(settlement_stats_path, stats_row, stats_header)
        logger.info(f"已追加結算統計到：{settlement_stats_path}")
        
        # 輸出關鍵視窗統計到日誌
        logger.info(f"=== {symbol} 結算時刻 ±{critical_window}s 關鍵視窗統計 ===")
        logger.info(f"  Tick 數量: {stats['count']}")
        logger.info(f"  延遲 (ms): 平均={stats['latency_mean']:.2f}, 最大={stats['latency_max']:.2f}, 最小={stats['latency_min']:.2f}, 標準差={stats['latency_stdev']:.2f}")
        logger.info(f"  Event Gap (ms): 平均={stats['event_gap_mean']:.2f}, 最大={stats['event_gap_max']:.2f}")
        logger.info(f"  凍結事件 (>{freeze_threshold}ms): {stats['freeze_count']} 次")
        logger.info(f"  延遲飆升 (>{latency_spike_threshold}ms): {stats['spike_count']} 次")
        
        if freeze_events:
            logger.warning(f"  ⚠️ 偵測到凍結事件:")
            for idx, gap, rel_t in freeze_events:
                logger.warning(f"    t={rel_t:+.3f}s (相對結算), event_gap={gap:.1f}ms")
        
        if latency_spikes:
            logger.warning(f"  ⚠️ 偵測到延遲飆升:")
            for idx, lat, rel_t in latency_spikes:
                logger.warning(f"    t={rel_t:+.3f}s (相對結算), latency={lat:.1f}ms")
    else:
        stats = None
        logger.warning(f"{symbol} 關鍵視窗內無資料")

    # ========== 輸出關鍵視窗詳細 CSV ==========
    csv_path = out_dir / f"{symbol}_critical_window_{int(next_time_ms/1000)}.csv"
    with csv_path.open('w', encoding='utf-8') as f:
        f.write("index,recv_time_epoch,event_time_ms,update_id,bid,ask,bid_qty,ask_qty,raw_recv_ts,latency_ms,event_gap_ms,arrival_gap_ms,relative_to_settle_sec\n")
        for i, rec in enumerate(records):
            t_recv, ev_ms, u, bid, ask, bid_qty, ask_qty, raw_recv_ts = rec
            rel_t = t_recv - settle_ts
            f.write(f"{i},{t_recv:.6f},{ev_ms},{u},{bid},{ask},{bid_qty},{ask_qty},{raw_recv_ts:.6f},{latency_ms[i]:.3f},{event_gap_ms[i]:.3f},{arrival_gap_ms[i]:.3f},{rel_t:.6f}\n")
    logger.info(f"已輸出完整 bookTicker 資料 CSV：{csv_path}")

    # ========== 以結算時刻之後的資料為主進行極值觀測 ==========
    first_post_idx = None
    for i, rec in enumerate(records):
        if rec[0] >= settle_ts:
            first_post_idx = i
            break
    post_indices = list(range(first_post_idx or 0, len(records))) if first_post_idx is not None else list(range(len(records)))
    post_evt_gap_start = (first_post_idx + 1) if first_post_idx is not None else 1
    post_evt_gap_indices = list(range(post_evt_gap_start, len(records))) if len(records) > 1 else []

    # 找出最大延遲與最大事件時間差（以結算後為主，若無則全窗）
    if post_indices:
        max_idx = max(post_indices, key=lambda i: latency_ms[i])
    else:
        max_idx = max(range(len(latency_ms)), key=lambda i: latency_ms[i])
    max_rec = records[max_idx]
    max_t, _max_ev, max_u, max_bid, max_ask = max_rec[0], max_rec[1], max_rec[2], max_rec[3], max_rec[4]

    if post_evt_gap_indices:
        evt_gap_idx = max(post_evt_gap_indices, key=lambda i: event_gap_ms[i])
    else:
        evt_gap_idx = max(range(len(event_gap_ms)), key=lambda i: event_gap_ms[i])
    evt_gap_t = records[evt_gap_idx][0]

    funding_dt = datetime.fromtimestamp(settle_ts)

    # ========== Plot 1: BookTicker Latency（全視窗單向延遲）==========
    plt.figure(figsize=(14,7))
    plt.title(f"{symbol} BookTicker Latency (ms) - Full Window")
    plt.scatter(times, latency_ms, s=10, alpha=0.7, label='Latency')
    plt.axvline(funding_dt, color='k', linestyle='--', linewidth=2, label='Settlement Time')
    plt.axhline(latency_spike_threshold, color='r', linestyle=':', alpha=0.5, label=f'Spike Threshold ({latency_spike_threshold}ms)')
    plt.xlabel("Time")
    plt.ylabel("Latency (ms)")
    plt.grid(True, alpha=0.3)
    plt.scatter([datetime.fromtimestamp(max_t)], [latency_ms[max_idx]], color='orange', s=150, zorder=5, label=f"Max Post-Settlement: {latency_ms[max_idx]:.1f}ms")
    plt.legend(loc='upper right')
    f1 = out_dir / f"{symbol}_latency_{int(next_time_ms/1000)}.png"
    plt.tight_layout()
    plt.savefig(f1, dpi=150)
    plt.close()

    # ========== Plot 2: Event Time Differences（全視窗）==========
    plt.figure(figsize=(14,7))
    plt.title(f"{symbol} Event Time Differences (ms) - Full Window")
    plt.scatter(times, event_gap_ms, s=10, alpha=0.7, label='Event Gap')
    plt.axvline(funding_dt, color='k', linestyle='--', linewidth=2, label='Settlement Time')
    plt.axhline(freeze_threshold, color='r', linestyle=':', alpha=0.5, label=f'Freeze Threshold ({freeze_threshold}ms)')
    plt.xlabel("Time")
    plt.ylabel("Event Gap (ms)")
    plt.grid(True, alpha=0.3)
    plt.scatter([datetime.fromtimestamp(evt_gap_t)], [event_gap_ms[evt_gap_idx]], color='red', s=150, zorder=5, label=f"Max Post-Settlement: {event_gap_ms[evt_gap_idx]:.1f}ms")
    plt.legend(loc='upper right')
    f2 = out_dir / f"{symbol}_event_gap_{int(next_time_ms/1000)}.png"
    plt.tight_layout()
    plt.savefig(f2, dpi=150)
    plt.close()

    # ========== Plot 3: 關鍵視窗放大圖 - Latency ==========
    if critical_indices:
        crit_times = [relative_times[i] for i in critical_indices]
        crit_latency = [latency_ms[i] for i in critical_indices]
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
        fig.suptitle(f"{symbol} Settlement Critical Window (±{critical_window}s)", fontsize=14, fontweight='bold')
        
        # 上圖：延遲
        ax1.scatter(crit_times, crit_latency, s=20, c='blue', alpha=0.7, label='Latency')
        ax1.axvline(0, color='red', linestyle='--', linewidth=2, label='Settlement (t=0)')
        ax1.axhline(latency_spike_threshold, color='orange', linestyle=':', alpha=0.7, label=f'Spike Threshold ({latency_spike_threshold}ms)')
        # 標記延遲飆升
        for idx, lat, rel_t in latency_spikes:
            ax1.scatter([rel_t], [lat], s=100, c='red', marker='^', zorder=5)
        ax1.set_ylabel('Latency (ms)')
        ax1.set_title('One-Way Latency (recv_time - event_time)')
        ax1.grid(True, alpha=0.3)
        ax1.legend(loc='upper right')
        
        # 下圖：Event Gap
        crit_event_gap = [event_gap_ms[i] for i in critical_indices]
        ax2.scatter(crit_times, crit_event_gap, s=20, c='green', alpha=0.7, label='Event Gap')
        ax2.axvline(0, color='red', linestyle='--', linewidth=2, label='Settlement (t=0)')
        ax2.axhline(freeze_threshold, color='orange', linestyle=':', alpha=0.7, label=f'Freeze Threshold ({freeze_threshold}ms)')
        # 標記凍結事件
        for idx, gap, rel_t in freeze_events:
            ax2.scatter([rel_t], [gap], s=100, c='red', marker='v', zorder=5)
        ax2.set_xlabel('Time Relative to Settlement (seconds)')
        ax2.set_ylabel('Event Gap (ms)')
        ax2.set_title('Event Time Gap (consecutive event timestamps)')
        ax2.grid(True, alpha=0.3)
        ax2.legend(loc='upper right')
        
        # 添加統計資訊
        if stats:
            stats_text = (f"Ticks: {stats['count']} | "
                         f"Latency: avg={stats['latency_mean']:.1f}ms, max={stats['latency_max']:.1f}ms | "
                         f"Event Gap: avg={stats['event_gap_mean']:.1f}ms, max={stats['event_gap_max']:.1f}ms | "
                         f"Freezes: {stats['freeze_count']} | Spikes: {stats['spike_count']}")
            fig.text(0.5, 0.01, stats_text, ha='center', fontsize=10, style='italic')
        
        f3 = out_dir / f"{symbol}_critical_window_{int(next_time_ms/1000)}.png"
        plt.tight_layout(rect=[0, 0.03, 1, 0.97])
        plt.savefig(f3, dpi=150)
        plt.close()
        logger.info(f"已輸出關鍵視窗放大圖：{f3}")

    # ========== Plot 4: 結算前後 1 秒的超精細視圖 ==========
    ultra_window = 1.0  # ±1秒
    ultra_start = settle_ts - ultra_window
    ultra_end = settle_ts + ultra_window
    ultra_indices = [i for i, t in enumerate(times_sec) if ultra_start <= t <= ultra_end]
    
    if len(ultra_indices) >= 2:
        ultra_times = [relative_times[i] * 1000 for i in ultra_indices]  # 轉換為毫秒
        ultra_latency = [latency_ms[i] for i in ultra_indices]
        ultra_event_gap = [event_gap_ms[i] for i in ultra_indices]
        
        fig, axes = plt.subplots(3, 1, figsize=(16, 12), sharex=True)
        fig.suptitle(f"{symbol} Ultra-Fine Settlement View (±1 second = ±1000ms)", fontsize=14, fontweight='bold')
        
        # 1. Latency
        axes[0].scatter(ultra_times, ultra_latency, s=30, c='blue', alpha=0.8)
        axes[0].plot(ultra_times, ultra_latency, 'b-', alpha=0.3)
        axes[0].axvline(0, color='red', linestyle='--', linewidth=2, label='Settlement (t=0)')
        axes[0].set_ylabel('Latency (ms)')
        axes[0].set_title('One-Way Latency')
        axes[0].grid(True, alpha=0.3)
        axes[0].legend()
        
        # 2. Event Gap
        axes[1].scatter(ultra_times, ultra_event_gap, s=30, c='green', alpha=0.8)
        axes[1].plot(ultra_times, ultra_event_gap, 'g-', alpha=0.3)
        axes[1].axvline(0, color='red', linestyle='--', linewidth=2, label='Settlement (t=0)')
        axes[1].set_ylabel('Event Gap (ms)')
        axes[1].set_title('Event Time Gap')
        axes[1].grid(True, alpha=0.3)
        axes[1].legend()
        
        # 3. Arrival Gap（實際接收間隔）
        ultra_arrival_gap = [arrival_gap_ms[i] for i in ultra_indices]
        axes[2].scatter(ultra_times, ultra_arrival_gap, s=30, c='purple', alpha=0.8)
        axes[2].plot(ultra_times, ultra_arrival_gap, 'm-', alpha=0.3)
        axes[2].axvline(0, color='red', linestyle='--', linewidth=2, label='Settlement (t=0)')
        axes[2].set_xlabel('Time Relative to Settlement (milliseconds)')
        axes[2].set_ylabel('Arrival Gap (ms)')
        axes[2].set_title('Arrival Time Gap (local reception interval)')
        axes[2].grid(True, alpha=0.3)
        axes[2].legend()
        
        f4 = out_dir / f"{symbol}_ultra_fine_{int(next_time_ms/1000)}.png"
        plt.tight_layout()
        plt.savefig(f4, dpi=150)
        plt.close()
        logger.info(f"已輸出超精細視圖：{f4}")
    
    logger.info(f"本次 bookTicker 詳細時間戳已寫入：{bt_log_path}")
    logger.info(f"已輸出圖檔：{f1} , {f2}")


async def _watch_loop(side: str):
    """Continuously pick and monitor the next settlement for given side ('pos' or 'neg')."""
    assert side in ('pos', 'neg')
    use_multi_stream = args.multi_stream
    
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

            # 根據模式選擇監控函數
            if use_multi_stream:
                logger.info(f"使用多 Stream 模式監控 {pick['symbol']}")
                await _monitor_symbol_multi_stream(pick['symbol'], int(pick['nextFundingTime']))
            else:
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
