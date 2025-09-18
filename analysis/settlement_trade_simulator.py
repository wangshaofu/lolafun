#!/usr/bin/env python3
"""
Settlement Trade Simulator (maker-first)

Mini README
- Purpose: Collect Binance Futures bookTicker ticks around funding settlement, align to server time, and simulate a post-settlement maker entry with configurable exit horizons to estimate fill probability, latency, and net PnL after fees/slippage.

- Quick Usage:
  1) Auto-pick extremes by funding (both sides, continuous):
     python analysis/settlement_trade_simulator.py --pick both --write-raw

  2) Run a fixed symbol list (one-off run per symbol):
     python analysis/settlement_trade_simulator.py --symbols JELLYJELLYUSDT,MUSDT --write-raw \
         --peak-window 2 --exit-windows 2,10 --taker-fee-bps 4 --slippage-bps-exit 0

- Key Arguments:
  --symbols: Comma-separated symbols to run once; omit to auto-pick extremes.
  --pick:    Auto-pick mode among {pos, neg, both, abs}; default both.
  --min-quote-volume: 24h quoteVolume threshold for auto-pick (default 15,000,000).
  --start-before / --window-pre / --window-post: Capture windows around settlement (60 / 15 / 15s default).
  --peak-window: Seconds for the “2s window” style metrics and maker fill check (default 2).
  --exit-windows: Comma list of horizons in seconds for PnL, e.g. 1,2,10 (default 2,10).
  --maker-fee-bps / --taker-fee-bps / --slippage-bps-exit: Costs modeling (defaults 0 / 4 / 0 bps).
  --write-raw: If set, writes per-event raw bookTicker CSV for later inspection.

- Outputs:
  1) Aggregated CSV: analysis/logs/settlement_sim_results.csv
     Columns include funding info, pre/mid prices, amplitude within 2s/10s, max latencies, maker side & price,
     whether filled within peak window, and for each exit window the exit price and net PnL% (if a fill occurred).

  2) Raw ticks (optional): analysis/logs/bookticker/records_<symbol>_<funding_ts>.csv
     Columns: recv_ts,event_ts_ms,update_id,bid,ask (server-aligned receive time).

- Mechanics Summary:
  - Direction: lastFundingRate < 0 => SELL at ask0, else BUY at bid0, immediately after settlement tick.
  - Fill check: Within --peak-window, best-opposite quote must touch the maker price to be considered filled.
  - Exit/PnL: At each --exit-windows timestamp, exit via market (taker) with fee and optional slippage applied.
  - Latency & Amplitude: Records max one-way latency (event_ts -> receive_ts) and price amplitudes post-settlement.

- Notes:
  - Requires network access to Binance endpoints and WebSocket stream.
  - NTP/server-time alignment is attempted to reduce local clock skew.
  - If “maker_filled_2s” (or appropriate peak-window) is false, exit fields for that row are left blank.
  - Adjust --min-quote-volume and symbol selection to avoid illiquid tails.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Tuple, Dict

import aiohttp
import websockets
import os, sys

# Minimal imports from project
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.ntp_sync import NTPTimeSync

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("SettlementSim")

BINANCE_WS_BASE = "wss://fstream.binance.com/stream?streams="
BOOKTICKER_FMT = "{}@bookTicker"
PREMIUM_INDEX_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
SERVER_TIME_URL = "https://fapi.binance.com/fapi/v1/time"


@dataclass
class SymbolFunding:
    symbol: str
    last_funding_rate: float
    next_funding_time_ms: int


async def fetch_premium_index(session: aiohttp.ClientSession, symbol: str) -> Optional[SymbolFunding]:
    params = {"symbol": symbol.upper()}
    async with session.get(PREMIUM_INDEX_URL, params=params, timeout=10) as resp:
        js = await resp.json()
        try:
            return SymbolFunding(
                symbol=symbol.upper(),
                last_funding_rate=float(js.get("lastFundingRate", 0.0)),
                next_funding_time_ms=int(js.get("nextFundingTime")),
            )
        except Exception:
            return None


async def fetch_server_time_offset(session: aiohttp.ClientSession) -> Optional[float]:
    try:
        async with session.get(SERVER_TIME_URL, timeout=10) as resp:
            js = await resp.json()
            srv = js.get("serverTime", 0) / 1000.0
            return srv - time.time()
    except Exception:
        return None


def server_now(server_offset_s: float) -> float:
    return time.time() + server_offset_s


async def collect_bookticker(symbol: str,
                             funding_time_ms: int,
                             start_before_s: float = 60.0,
                             window_pre_s: float = 15.0,
                             window_post_s: float = 15.0,
                             write_raw_csv: bool = True
                             ) -> List[Tuple[float, int, int, float, float]]:
    """
    Collect bookTicker ticks around settlement.
    Returns list of tuples: (recv_ts_server_aligned, event_ts_ms, update_id, bid, ask)
    """
    records: List[Tuple[float, int, int, float, float]] = []
    async with aiohttp.ClientSession() as session:
        # Align to server time and do NTP sync
        ntp = NTPTimeSync()
        try:
            ntp.sync_time()
        except Exception:
            pass
        server_offset = await fetch_server_time_offset(session) or 0.0

        # Wait until start time
        desired_start = (funding_time_ms / 1000.0) - start_before_s
        wait_s = desired_start - server_now(server_offset)
        if wait_s > 0:
            logger.info(f"等待 {wait_s:.1f}s 至結算前 {start_before_s:.0f}s 開始監聽 {symbol}…")
            await asyncio.sleep(wait_s)

        # Force one more NTP sync close to settlement
        try:
            ntp.force_sync_before_settlement(funding_time_ms)
        except Exception:
            pass

        # Set collection window
        start_collect = funding_time_ms/1000.0 - window_pre_s
        end_collect = funding_time_ms/1000.0 + window_post_s

        url = BINANCE_WS_BASE + BOOKTICKER_FMT.format(symbol.lower())
        logger.info(f"開始收集 {symbol} bookTicker：{url}")

        async with websockets.connect(url, ping_interval=None) as ws:
            async for raw in ws:
                now_srv = server_now(server_offset)
                data = json.loads(raw).get('data', {})
                if 's' in data and ('b' in data or 'B' in data) and ('a' in data or 'A' in data):
                    bid = float(data.get('b', data.get('B', 0)))
                    ask = float(data.get('a', data.get('A', 0)))
                    u = int(data.get('u', 0))
                    ev_candidates = []
                    for k in ('E', 'T'):
                        if k in data:
                            try:
                                ev_candidates.append(int(data[k]))
                            except Exception:
                                pass
                    ev_ts_ms = min(ev_candidates) if ev_candidates else int(now_srv * 1000)
                    if start_collect <= now_srv <= end_collect:
                        records.append((now_srv, ev_ts_ms, u, bid, ask))
                if now_srv > end_collect:
                    break

    # Persist raw ticks for later analysis
    if write_raw_csv and records:
        out_dir = Path("analysis/logs/bookticker")
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"records_{symbol}_{int(funding_time_ms/1000)}.csv"
        with csv_path.open('w', encoding='utf-8') as f:
            f.write("recv_ts,event_ts_ms,update_id,bid,ask\n")
            for r in records:
                f.write(f"{r[0]:.6f},{r[1]},{r[2]},{r[3]},{r[4]}\n")
        logger.info(f"已輸出 ticks CSV: {csv_path}")

    return records


def analyze_post_settlement(
    records: List[Tuple[float, int, int, float, float]],
    funding_time_ms: int,
    last_funding_rate: float,
    peak_window_s: float = 2.0,
    exit_windows_s: Optional[List[float]] = None,
    maker_fee_bps: float = 0.0,
    taker_fee_bps: float = 4.0,
    exit_slippage_bps: float = 0.0,
) -> dict:
    """Compute latency and price-reaction metrics around settlement."""
    if not records:
        return {}

    t0 = funding_time_ms / 1000.0
    # Convert to series
    recv_ts = [r[0] for r in records]
    ev_ts_ms = [r[1] for r in records]
    bids = [r[3] for r in records]
    asks = [r[4] for r in records]
    mids = [(b + a) / 2.0 for b, a in zip(bids, asks)]

    # One-way latency (ms)
    latency_ms = [(rt * 1000.0) - et for rt, et in zip(recv_ts, ev_ts_ms)]

    # Pre- and post-indices
    post_idx = [i for i, t in enumerate(recv_ts) if t >= t0]
    if not post_idx:
        return {}
    first_post = post_idx[0]

    # Baseline pre-settlement mid (use last tick before t0 if available)
    pre_mid = mids[first_post - 1] if first_post > 0 else mids[first_post]
    post_window_idx = [i for i, t in enumerate(recv_ts) if t0 <= t <= (t0 + peak_window_s)]
    wider_idx = [i for i, t in enumerate(recv_ts) if t0 <= t <= (t0 + 10.0)]

    def pct(x):
        return (x - pre_mid) / pre_mid * 100.0 if pre_mid else 0.0

    max_up_2s = max((mids[i] for i in post_window_idx), default=pre_mid)
    min_dn_2s = min((mids[i] for i in post_window_idx), default=pre_mid)
    max_up_10s = max((mids[i] for i in wider_idx), default=pre_mid)
    min_dn_10s = min((mids[i] for i in wider_idx), default=pre_mid)

    amp_2s = max(abs(pct(max_up_2s)), abs(pct(min_dn_2s)))
    amp_10s = max(abs(pct(max_up_10s)), abs(pct(min_dn_10s)))

    # Latency maxima
    post_latency = [latency_ms[i] for i in post_idx]
    max_lat_all = max(post_latency) if post_latency else 0.0
    within_2s_idx = [i for i, t in enumerate(recv_ts) if t0 <= t <= (t0 + peak_window_s)]
    max_lat_2s = max((latency_ms[i] for i in within_2s_idx), default=0.0)
    is_max_latency_in_2s = (abs(max_lat_2s - max_lat_all) < 1e-6) if post_latency else False

    # Maker simulation: after settlement, place SELL at ask0 if funding < 0 else BUY at bid0
    side = 'SELL' if last_funding_rate < 0 else 'BUY'
    ask0 = asks[first_post]
    bid0 = bids[first_post]
    if side == 'SELL':
        sim_price = ask0
        # Approximate fill if best bid touches our ask within the window
        max_bid_2s = max((bids[i] for i in post_window_idx), default=bid0)
        filled_2s = max_bid_2s >= sim_price
    else:
        sim_price = bid0
        min_ask_2s = min((asks[i] for i in post_window_idx), default=ask0)
        filled_2s = min_ask_2s <= sim_price

    # Exit/PnL backtest
    if exit_windows_s is None:
        exit_windows_s = [2.0, 10.0]

    # Helper to find first index at or after specific time
    def first_idx_at_or_after(t_target: float) -> Optional[int]:
        for i, t in enumerate(recv_ts):
            if t >= t_target:
                return i
        return None

    # Effective prices with fees and slippage
    m_fee = maker_fee_bps / 10000.0
    t_fee = taker_fee_bps / 10000.0
    s_exit = exit_slippage_bps / 10000.0

    # Entry effective price (includes maker fee if any)
    entry_price = sim_price
    if side == 'BUY':
        entry_eff = entry_price * (1.0 + m_fee)
    else:  # SELL entry
        entry_eff = entry_price * (1.0 - m_fee)

    # Compute exit price and net returns for each window
    pnl_by_window: Dict[float, Optional[float]] = {}
    exit_price_by_window: Dict[float, Optional[float]] = {}
    for w in exit_windows_s:
        i_exit = first_idx_at_or_after(t0 + float(w))
        if i_exit is None:
            pnl_by_window[w] = None
            exit_price_by_window[w] = None
            continue

        if side == 'BUY':
            # Exit with market sell: you hit bid, slippage and taker fee reduce proceeds
            raw_exit = bids[i_exit]
            exit_eff = raw_exit * (1.0 - s_exit) * (1.0 - t_fee)
            pnl = (exit_eff / entry_eff) - 1.0
            exit_price_by_window[w] = raw_exit
            pnl_by_window[w] = pnl
        else:
            # Short exit with market buy: you hit ask, slippage and taker fee increase cost
            raw_exit = asks[i_exit]
            exit_eff = raw_exit * (1.0 + s_exit) * (1.0 + t_fee)
            pnl = (entry_eff / exit_eff) - 1.0
            exit_price_by_window[w] = raw_exit
            pnl_by_window[w] = pnl

    return {
        'symbol': None,  # filled later by caller
        'funding_time': int(funding_time_ms/1000),
        'funding_rate': last_funding_rate,
        'pre_mid': pre_mid,
        'mid_at_t0': mids[first_post],
        'max_up_2s_pct': pct(max_up_2s),
        'min_dn_2s_pct': pct(min_dn_2s),
        'amp_2s_pct': amp_2s,
        'amp_10s_pct': amp_10s,
        'max_latency_ms_post': max_lat_all,
        'max_latency_ms_2s': max_lat_2s,
        'is_max_latency_in_2s': is_max_latency_in_2s,
        'maker_side': side,
        'maker_sim_price': sim_price,
        'maker_filled_within_2s': filled_2s,
        # PnL related
        'entry_eff_price': entry_eff,
        'exit_slippage_bps': exit_slippage_bps,
        'taker_fee_bps': taker_fee_bps,
        'maker_fee_bps': maker_fee_bps,
        'exit_price_by_window': exit_price_by_window,
        'pnl_by_window': pnl_by_window,
    }


async def _fetch_snapshot(session: aiohttp.ClientSession) -> Tuple[List[Dict], Dict[str, float], Dict, Dict]:
    """Fetch all premiumIndex and 24h tickers, return funding sorted list and volume map.
    Returns (sorted_by_rate, vol_by_symbol, most_negative, most_positive).
    """
    # All futures premium index
    async with session.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=10) as resp:
        all_items = await resp.json()
    # 24h ticker for quoteVolume filtering
    async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=10) as resp2:
        tick24 = await resp2.json()

    filtered = [x for x in all_items if 'lastFundingRate' in x and 'nextFundingTime' in x]
    sorted_by_rate = sorted(filtered, key=lambda x: float(x['lastFundingRate']))
    most_negative = sorted_by_rate[0] if sorted_by_rate else None
    most_positive = sorted_by_rate[-1] if sorted_by_rate else None

    vol_by_symbol: Dict[str, float] = {}
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


def _pick_with_volume(sorted_list: List[Dict], side: str, vol_by_symbol: Dict[str, float], min_qv: float) -> Tuple[Dict, float, Optional[Dict]]:
    """Pick extreme by side with volume threshold. Returns (item, qv, rejected_or_none)."""
    ordered = list(reversed(sorted_list)) if side == 'pos' else list(sorted_list)
    rejected = None
    for idx, x in enumerate(ordered):
        sym = x.get('symbol')
        qv = vol_by_symbol.get(sym, 0.0)
        if qv >= min_qv:
            if idx == 0:
                return x, qv, None
            rejected = ordered[0]
            return x, qv, rejected
    x = ordered[0]
    return x, vol_by_symbol.get(x.get('symbol', ''), 0.0), None


def _parse_exit_windows(arg_val: Optional[str]) -> List[float]:
    if not arg_val:
        return [2.0, 10.0]
    out: List[float] = []
    for part in str(arg_val).split(','):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(float(part))
        except Exception:
            continue
    return out or [2.0, 10.0]


def _suffix_for_window(w: float) -> str:
    # Sanitize float to column suffix, e.g., 2.5 -> "2p5s"
    if abs(w - int(w)) < 1e-9:
        return f"{int(w)}s"
    s = str(w).replace('.', 'p')
    return f"{s}s"


async def _watch_loop(side: str, args):
    assert side in ('pos', 'neg')
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                sorted_by_rate, vol_by_symbol, most_negative, most_positive = await _fetch_snapshot(session)
            if not sorted_by_rate:
                logger.warning("取不到 premiumIndex，5 秒後重試…")
                await asyncio.sleep(5)
                continue

            pick, qv, rejected = _pick_with_volume(sorted_by_rate, side, vol_by_symbol, float(args.min_quote_volume))
            sym = pick['symbol']
            fr = float(pick['lastFundingRate'])
            nft = int(pick['nextFundingTime'])
            if rejected is not None:
                logger.info(f"{side.upper()} 極值 {rejected['symbol']} qv={vol_by_symbol.get(rejected['symbol'],0.0):,.0f} 低於門檻，改監聽 {sym} qv={qv:,.0f}")
            else:
                logger.info(f"{side.upper()} 監聽 {sym} fr={fr:+.6f} qv={qv:,.0f}")

            # Collect ticks and analyze
            ticks = await collect_bookticker(
                sym,
                nft,
                start_before_s=float(args.start_before),
                window_pre_s=float(args.window_pre),
                window_post_s=float(args.window_post),
                write_raw_csv=bool(args.write_raw)
            )

            exit_windows = _parse_exit_windows(getattr(args, 'exit_windows', None))
            metrics = analyze_post_settlement(
                ticks,
                nft,
                fr,
                peak_window_s=float(args.peak_window),
                exit_windows_s=exit_windows,
                maker_fee_bps=float(getattr(args, 'maker_fee_bps', 0.0)),
                taker_fee_bps=float(getattr(args, 'taker_fee_bps', 4.0)),
                exit_slippage_bps=float(getattr(args, 'slippage_bps_exit', 0.0)),
            )
            if not metrics:
                logger.warning(f"{sym} 沒有足夠資料可供分析，繼續下一輪…")
                await asyncio.sleep(2)
                continue
            metrics['symbol'] = sym

            # Append to results CSV
            results_csv = Path('analysis/logs/settlement_sim_results.csv')
            results_csv.parent.mkdir(parents=True, exist_ok=True)
            if not results_csv.exists():
                base_cols = [
                    'timestamp_utc','symbol','funding_time','lastFundingRate','pre_mid','mid_at_t0',
                    'max_up_2s_pct','min_dn_2s_pct','amp_2s_pct','amp_10s_pct',
                    'max_latency_ms_post','max_latency_ms_2s','is_max_latency_in_2s',
                    'maker_side','maker_sim_price','maker_filled_2s',
                    'maker_fee_bps','taker_fee_bps','exit_slippage_bps','entry_eff_price'
                ]
                # Dynamic exit columns
                exit_cols = []
                for w in exit_windows:
                    suf = _suffix_for_window(w)
                    exit_cols.append(f"exit_price_{suf}")
                    exit_cols.append(f"pnl_net_{suf}_pct")
                header = ','.join(base_cols + exit_cols) + '\\n'
                results_csv.write_text(header, encoding='utf-8')
            ts_iso = datetime.now(timezone.utc).isoformat()
            # Base values
            fields = [
                ts_iso,
                sym,
                str(metrics['funding_time']),
                f"{metrics['funding_rate']:.8f}",
                f"{metrics['pre_mid']:.8f}",
                f"{metrics['mid_at_t0']:.8f}",
                f"{metrics['max_up_2s_pct']:.6f}",
                f"{metrics['min_dn_2s_pct']:.6f}",
                f"{metrics['amp_2s_pct']:.6f}",
                f"{metrics['amp_10s_pct']:.6f}",
                f"{metrics['max_latency_ms_post']:.1f}",
                f"{metrics['max_latency_ms_2s']:.1f}",
                str(int(metrics['is_max_latency_in_2s'])),
                metrics['maker_side'],
                f"{metrics['maker_sim_price']:.8f}",
                str(int(metrics['maker_filled_within_2s'])),
                f"{metrics['maker_fee_bps']:.2f}",
                f"{metrics['taker_fee_bps']:.2f}",
                f"{metrics['exit_slippage_bps']:.2f}",
                f"{metrics['entry_eff_price']:.8f}",
            ]
            # Dynamic exit fields
            for w in exit_windows:
                suf = _suffix_for_window(w)
                ex_price = metrics['exit_price_by_window'].get(w)
                pnl = metrics['pnl_by_window'].get(w)
                if metrics['maker_filled_within_2s'] and ex_price is not None and pnl is not None:
                    fields.append(f"{ex_price:.8f}")
                    fields.append(f"{pnl*100.0:.6f}")
                else:
                    fields.append("")
                    fields.append("")
            line = ','.join(fields) + "\n"
            with results_csv.open('a', encoding='utf-8') as f:
                f.write(line)
            logger.info(f"✅ {sym} 模擬完成，已寫入 {results_csv}；繼續下一輪…")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"{side.upper()} watcher 發生錯誤：{e}")
            await asyncio.sleep(5)


async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Settlement maker simulator with fee/slippage PnL backtest")
    parser.add_argument('--symbols', type=str, default=None, help='Comma separated symbols e.g. BTCUSDT,ETHUSDT. If omitted, auto-pick extreme funding.')
    parser.add_argument('--pick', type=str, default='both', choices=['abs','pos','neg','both'], help='auto-pick: choose most pos/neg/both/abs (abs runs both watchers)')
    parser.add_argument('--min-quote-volume', type=float, default=15000000.0, help='24h USDT quoteVolume minimum; below it will fallback to next best')
    parser.add_argument('--start-before', type=float, default=60.0, help='seconds before settlement to start listening')
    parser.add_argument('--window-pre', type=float, default=15.0, help='seconds to keep before settlement')
    parser.add_argument('--window-post', type=float, default=15.0, help='seconds to keep after settlement')
    parser.add_argument('--peak-window', type=float, default=2.0, help='post-settlement window to test peak (seconds)')
    parser.add_argument('--write-raw', action='store_true', help='write raw bookTicker ticks to CSV')
    # Fees & slippage & exit
    parser.add_argument('--maker-fee-bps', type=float, default=0.0, help='maker fee in bps (default 0)')
    parser.add_argument('--taker-fee-bps', type=float, default=4.0, help='taker fee in bps (default 4 = 0.04%)')
    parser.add_argument('--slippage-bps-exit', type=float, default=0.0, help='exit slippage in bps applied on market exit')
    parser.add_argument('--exit-windows', type=str, default='2,10', help='comma-separated exit horizons in seconds for PnL, e.g. 2,10')
    args, _ = parser.parse_known_args()

    # If symbols provided: run once for list; else auto-pick extremes repeatedly
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]
        results_csv = Path('analysis/logs/settlement_sim_results.csv')
        results_csv.parent.mkdir(parents=True, exist_ok=True)
        if not results_csv.exists():
            exit_windows = _parse_exit_windows(args.exit_windows)
            base_cols = [
                'timestamp_utc','symbol','funding_time','lastFundingRate','pre_mid','mid_at_t0',
                'max_up_2s_pct','min_dn_2s_pct','amp_2s_pct','amp_10s_pct',
                'max_latency_ms_post','max_latency_ms_2s','is_max_latency_in_2s',
                'maker_side','maker_sim_price','maker_filled_2s',
                'maker_fee_bps','taker_fee_bps','exit_slippage_bps','entry_eff_price'
            ]
            exit_cols = []
            for w in exit_windows:
                suf = _suffix_for_window(w)
                exit_cols.append(f"exit_price_{suf}")
                exit_cols.append(f"pnl_net_{suf}_pct")
            header = ','.join(base_cols + exit_cols) + '\\n'
            results_csv.write_text(header, encoding='utf-8')

        async with aiohttp.ClientSession() as session:
            for sym in symbols:
                info = await fetch_premium_index(session, sym)
                if not info:
                    logger.warning(f"無法取得 {sym} premiumIndex，跳過")
                    continue

                # Collect and analyze once
                ticks = await collect_bookticker(
                    sym,
                    info.next_funding_time_ms,
                    start_before_s=float(args.start_before),
                    window_pre_s=float(args.window_pre),
                    window_post_s=float(args.window_post),
                    write_raw_csv=bool(args.write_raw),
                )

                exit_windows = _parse_exit_windows(args.exit_windows)
                metrics = analyze_post_settlement(
                    ticks,
                    info.next_funding_time_ms,
                    info.last_funding_rate,
                    peak_window_s=float(args.peak_window),
                    exit_windows_s=exit_windows,
                    maker_fee_bps=float(args.maker_fee_bps),
                    taker_fee_bps=float(args.taker_fee_bps),
                    exit_slippage_bps=float(args.slippage_bps_exit),
                )
                if not metrics:
                    logger.warning(f"{sym} 沒有足夠資料可供分析")
                    continue
                metrics['symbol'] = sym

                ts_iso = datetime.now(timezone.utc).isoformat()
                fields = [
                    ts_iso,
                    sym,
                    str(metrics['funding_time']),
                    f"{metrics['funding_rate']:.8f}",
                    f"{metrics['pre_mid']:.8f}",
                    f"{metrics['mid_at_t0']:.8f}",
                    f"{metrics['max_up_2s_pct']:.6f}",
                    f"{metrics['min_dn_2s_pct']:.6f}",
                    f"{metrics['amp_2s_pct']:.6f}",
                    f"{metrics['amp_10s_pct']:.6f}",
                    f"{metrics['max_latency_ms_post']:.1f}",
                    f"{metrics['max_latency_ms_2s']:.1f}",
                    str(int(metrics['is_max_latency_in_2s'])),
                    metrics['maker_side'],
                    f"{metrics['maker_sim_price']:.8f}",
                    str(int(metrics['maker_filled_within_2s'])),
                    f"{metrics['maker_fee_bps']:.2f}",
                    f"{metrics['taker_fee_bps']:.2f}",
                    f"{metrics['exit_slippage_bps']:.2f}",
                    f"{metrics['entry_eff_price']:.8f}",
                ]
                for w in exit_windows:
                    ex_price = metrics['exit_price_by_window'].get(w)
                    pnl = metrics['pnl_by_window'].get(w)
                    if metrics['maker_filled_within_2s'] and ex_price is not None and pnl is not None:
                        fields.append(f"{ex_price:.8f}")
                        fields.append(f"{pnl*100.0:.6f}")
                    else:
                        fields.append("")
                        fields.append("")
                line = ','.join(fields) + "\n"
                with results_csv.open('a', encoding='utf-8') as f:
                    f.write(line)
                logger.info(f"✅ {sym} 模擬完成，已寫入 {results_csv}")
    else:
        tasks = []
        if args.pick in ('both', 'pos', 'abs'):
            tasks.append(asyncio.create_task(_watch_loop('pos', args)))
        if args.pick in ('both', 'neg', 'abs'):
            tasks.append(asyncio.create_task(_watch_loop('neg', args)))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
            raise


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("中斷執行，退出。")
