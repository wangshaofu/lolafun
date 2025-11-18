#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Settlement Real Trade Executor (taker-entry 30ms after funding, maker-exit via FundingRateAnalyzer)

- 實盤邏輯：
  1) 自動挑選 funding rate 最負的合約（只做空）。
  2) 在 funding 結算時間附近訂閱 bookTicker。
  3) 一偵測到結算後的第一筆 tick，就視為結算點，使用該時刻 bid 價「用 taker 市價做空」。
     （程式中把這個視為 30 ms 的固定延遲，可視需要調參）
  4) 透過 FundingRateAnalyzer 計算 stop loss / take profit 價格，
     利用 maker limit 掛出 TP / SL 兩張出場單。

- 回測邏輯：
  - `analyze_post_settlement` 仍可用於分析結算前後 2s/10s 的價格變動與模擬 PnL。
"""

import asyncio
import configparser
import json
import logging
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

import aiohttp
import ccxt
import websockets
import os
import sys

# ---- 專案內部模組 ----
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.ntp_sync import NTPTimeSync
from trading.funding_analyzer import FundingRateAnalyzer
from trading.precision_manager import SymbolPrecisionManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("SettlementRealTrade")

BINANCE_WS_BASE = "wss://fstream.binance.com/stream?streams="
BOOKTICKER_FMT = "{}@bookTicker"
PREMIUM_INDEX_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
SERVER_TIME_URL = "https://fapi.binance.com/fapi/v1/time"


# ==================== Data classes ====================

@dataclass
class SymbolFunding:
    symbol: str
    last_funding_rate: float
    next_funding_time_ms: int


# ==================== Binance 實盤交易封裝 ====================

class BinanceRealTrader:
    """Handle real Binance Futures trades for settlement strategy."""

    def __init__(self, client: ccxt.binance, precision_manager: SymbolPrecisionManager):
        self.client = client
        self.precision_manager = precision_manager

    async def execute_short_trade(
        self,
        symbol: str,
        entry_price: float,
        investment_amount: float,
        leverage: int,
        margin_mode: str,
    ) -> Dict[str, Any]:
        """
        真實做空：taker 市價單。
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            self._execute_short_trade_sync,
            symbol.upper(),
            entry_price,
            float(investment_amount),
            int(leverage),
            margin_mode,
        )

    def _execute_short_trade_sync(
        self,
        symbol: str,
        entry_price: float,
        investment_amount: float,
        leverage: int,
        margin_mode: str,
    ) -> Dict[str, Any]:
        if entry_price <= 0:
            raise ValueError("Entry price must be positive for real trade execution")
        if investment_amount <= 0:
            raise ValueError("Investment amount must be positive for real trade execution")

        lev = max(1, min(int(leverage), 125))
        notional = investment_amount * lev
        quantity = notional / entry_price

        formatted_qty = self.precision_manager.format_quantity(symbol, quantity)
        qty_value = float(formatted_qty)
        if qty_value <= 0:
            raise ValueError("Calculated order quantity is too small")

        margin_type = self._normalize_margin_mode(margin_mode)
        self._ensure_margin_type(symbol, margin_type)
        self._ensure_leverage(symbol, lev)

        logger.info(
            f"🚀 準備下真實 SELL 單 {symbol}: 數量 {formatted_qty}, 槓桿 {lev}x, 模式 {margin_type}"
        )

        # ccxt futures 市價做空
        params = {"newOrderRespType": "RESULT", "reduceOnly": False}
        response = self.client.create_order(
            symbol=symbol,
            type="market",
            side="sell",
            amount=qty_value,
            params=params,
        )

        avg_price = (
            response.get("average")
            or response.get("price")
            or response.get("info", {}).get("avgPrice")
        )
        executed_qty = (
            response.get("amount")
            or response.get("filled")
            or response.get("info", {}).get("executedQty")
        )
        order_id = response.get("id") or response.get("info", {}).get("orderId")
        logger.info(
            f"🎯 Binance 回傳 avgPrice={avg_price}, executedQty={executed_qty}, orderId={order_id}"
        )
        return response

    def _ensure_margin_type(self, symbol: str, margin_type: str):
        try:
            setter = getattr(self.client, "set_margin_mode", None)
            if setter:
                setter(margin_type, symbol)
                logger.info(f"🔧 已設置 {symbol} margin 模式為 {margin_type}")
            else:
                logger.debug("當前客戶端不支援 margin mode 設定，跳過")
        except ccxt.BaseError as exc:
            message = str(exc)
            if "No need to change margin type" in message or "no change" in message.lower():
                logger.debug(f"{symbol} margin 模式已是 {margin_type}")
            else:
                raise

    def _ensure_leverage(self, symbol: str, leverage: int):
        try:
            setter = getattr(self.client, "set_leverage", None)
            if setter:
                setter(leverage, symbol)
                logger.info(f"⚙️ 已設置 {symbol} 槓桿為 {leverage}x")
            else:
                logger.debug("當前客戶端不支援槓桿設定，跳過")
        except ccxt.BaseError as exc:
            logger.error(f"設定槓桿失敗: {exc}")
            raise

    def _normalize_margin_mode(self, margin_mode: str) -> str:
        text = (margin_mode or "isolated").lower()
        if text.startswith("cross"):
            return "cross"
        return "isolated"

    def _format_price(self, symbol: str, price: float) -> float:
        """
        優先使用 precision_manager 的 price formatter，如果沒有就用交易所的 tickSize。
        """
        fmt_price = None
        if hasattr(self.precision_manager, "format_price"):
            try:
                fmt_price = self.precision_manager.format_price(symbol, price)
            except Exception:
                fmt_price = None

        if fmt_price is not None:
            return float(fmt_price)

        # 從 ccxt markets 中抓 tickSize
        mkt = self.client.markets.get(symbol, {})
        precision = mkt.get("precision", {})
        price_prec = precision.get("price")
        if price_prec is not None:
            return float(f"{price:.{int(price_prec)}f}")
        return float(price)

    def place_exit_orders_for_short(
        self,
        symbol: str,
        entry_price: float,
        funding_rate: float,
        quantity: float,
    ) -> Dict[str, Any]:
        """
        依 FundingRateAnalyzer 計算止盈止損，掛 maker 單。
        回傳 dict: {"take_profit": {...}, "stop_loss": {...}}
        """
        analyzer = FundingRateAnalyzer()
        stop_loss_price, take_profit_price = analyzer.calculate_trade_levels(
            funding_rate,
            entry_price
        )

        tp_price_fmt = self._format_price(symbol, take_profit_price)
        sl_price_fmt = self._format_price(symbol, stop_loss_price)
        qty_fmt = self.precision_manager.format_quantity(symbol, quantity)
        qty_val = float(qty_fmt)

        logger.info(
            f"📌 準備掛出場單 {symbol}: "
            f"TP={tp_price_fmt}, SL={sl_price_fmt}, qty={qty_fmt}"
        )

        results: Dict[str, Any] = {}
        params_common = {
            # 「只減倉」避免反向加倉
            "reduceOnly": True,
            "postOnly": True, 
            # 可以視情況加上 timeInForce / postOnly 等
            # "timeInForce": "GTX",  # POST_ONLY for Binance futures (視 ccxt 支援情況)
        }

        # 做空 → 出場都是 BUY limit single-side
        tp_params = dict(params_common)
        sl_params = dict(params_common)

        tp_order = self.client.create_order(
            symbol=symbol,
            type="limit",
            side="buy",
            amount=qty_val,
            price=tp_price_fmt,
            params=tp_params,
        )
        results["take_profit"] = tp_order

        sl_order = self.client.create_order(
            symbol=symbol,
            type="limit",
            side="buy",
            amount=qty_val,
            price=sl_price_fmt,
            params=sl_params,
        )
        results["stop_loss"] = sl_order

        logger.info(
            f"✅ 已掛出場單: TP id={tp_order.get('id') or tp_order.get('info', {}).get('orderId')}, "
            f"SL id={sl_order.get('id') or sl_order.get('info', {}).get('orderId')}"
        )
        results["tp_price_fmt"] = tp_price_fmt
        results["sl_price_fmt"] = sl_price_fmt
        return results


# ==================== REST / WS 公用函式 ====================

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


async def collect_bookticker(
    symbol: str,
    funding_time_ms: int,
    start_before_s: float = 60.0,
    window_pre_s: float = 15.0,
    window_post_s: float = 15.0,
    write_raw_csv: bool = True,
) -> List[Tuple[float, int, int, float, float]]:
    """
    收集結算前後的 bookTicker 資料：
    回傳 [(recv_ts_aligned, event_ts_ms, update_id, bid, ask), ...]
    """
    records: List[Tuple[float, int, int, float, float]] = []
    async with aiohttp.ClientSession() as session:
        # NTP + server time 對齊
        ntp = NTPTimeSync()
        try:
            ntp.sync_time()
        except Exception:
            pass
        server_offset = await fetch_server_time_offset(session) or 0.0

        desired_start = (funding_time_ms / 1000.0) - start_before_s
        wait_s = desired_start - server_now(server_offset)
        if wait_s > 0:
            logger.info(f"等待 {wait_s:.1f}s 至結算前 {start_before_s:.0f}s 開始監聽 {symbol}…")
            await asyncio.sleep(wait_s)

        try:
            ntp.force_sync_before_settlement(funding_time_ms)
        except Exception:
            pass

        start_collect = funding_time_ms / 1000.0 - window_pre_s
        end_collect = funding_time_ms / 1000.0 + window_post_s

        url = BINANCE_WS_BASE + BOOKTICKER_FMT.format(symbol.lower())
        logger.info(f"開始收集 {symbol} bookTicker：{url}")

        async with websockets.connect(url, ping_interval=None) as ws:
            async for raw in ws:
                now_srv = server_now(server_offset)
                data = json.loads(raw).get("data", {})
                if "s" in data and ("b" in data or "B" in data) and ("a" in data or "A" in data):
                    bid = float(data.get("b", data.get("B", 0)))
                    ask = float(data.get("a", data.get("A", 0)))
                    u = int(data.get("u", 0))
                    ev_candidates = []
                    for k in ("E", "T"):
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

    if write_raw_csv and records:
        out_dir = Path("analysis/logs/bookticker")
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"records_{symbol}_{int(funding_time_ms/1000)}.csv"
        with csv_path.open("w", encoding="utf-8") as f:
            f.write("recv_ts,event_ts_ms,update_id,bid,ask\n")
            for r in records:
                f.write(f"{r[0]:.6f},{r[1]},{r[2]},{r[3]},{r[4]}\n")
        logger.info(f"已輸出 ticks CSV: {csv_path}")

    return records


# ==================== 分析 & 回測 ====================

def analyze_post_settlement(
    records: List[Tuple[float, int, int, float, float]],
    funding_time_ms: int,
    last_funding_rate: float,
    peak_window_s: float = 2.0,
    maker_fee_bps: float = 0.0,
    taker_fee_bps: float = 2.75,
    latency_entry_ms: float = 30.0,     # 預設 30ms 入場
    leverage_list: List[int] = [5, 10, 15, 20, 25],
) -> dict:
    """
    結算前後的價格反應與 PnL 模擬。
    這裡仍保留邏輯，但「實盤入場」不再靠 latency spike，而是固定 30ms。
    """
    if not records:
        return {}

    t0 = funding_time_ms / 1000.0
    recv_ts = [r[0] for r in records]
    ev_ts_ms = [r[1] for r in records]
    bids = [r[3] for r in records]
    asks = [r[4] for r in records]
    mids = [(b + a) / 2.0 for b, a in zip(bids, asks)]
    latency_ms = [(rt * 1000.0) - et for rt, et in zip(recv_ts, ev_ts_ms)]

    post_idx = [i for i, t in enumerate(recv_ts) if t >= t0]
    if not post_idx:
        return {}
    first_post = post_idx[0]

    pre_mid = mids[first_post - 1] if first_post > 0 else mids[first_post]

    post_window_idx = [i for i, t in enumerate(recv_ts) if t0 <= t <= (t0 + peak_window_s)]
    wider_idx = [i for i, t in enumerate(recv_ts) if t0 <= t <= (t0 + 10.0)]

    def pct(x: float) -> float:
        return (x - pre_mid) / pre_mid * 100.0 if pre_mid else 0.0

    max_up_2s = max((mids[i] for i in post_window_idx), default=pre_mid)
    min_dn_2s = min((mids[i] for i in post_window_idx), default=pre_mid)
    max_up_10s = max((mids[i] for i in wider_idx), default=pre_mid)
    min_dn_10s = min((mids[i] for i in wider_idx), default=pre_mid)

    amp_2s = max(abs(pct(max_up_2s)), abs(pct(min_dn_2s)))
    amp_10s = max(abs(pct(max_up_10s)), abs(pct(min_dn_10s)))

    pre_idx = [i for i, t in enumerate(recv_ts) if t < t0]
    pre_latency_samples = [latency_ms[i] for i in pre_idx if latency_ms[i] >= 0]
    latency_baseline = (
        statistics.median(pre_latency_samples)
        if pre_latency_samples else max(latency_entry_ms, 1.0)
    )

    post_latency = [latency_ms[i] for i in post_idx]
    max_lat_all = max(post_latency) if post_latency else 0.0
    within_2s_idx = [i for i, t in enumerate(recv_ts) if t0 <= t <= (t0 + peak_window_s)]
    max_lat_2s = max((latency_ms[i] for i in within_2s_idx), default=0.0)
    is_max_latency_in_2s = (abs(max_lat_2s - max_lat_all) < 1e-6) if post_latency else False

    if last_funding_rate >= 0:
        logger.info("⚠️ Funding rate >= 0，策略僅支援做空，跳過此次分析")
        return {}

    # === 固定 30ms 入場邏輯 ===
    entry_idx: Optional[int] = None
    entry_latency: float = latency_entry_ms

    # 找結算後第一筆 tick 視為入場
    for idx in post_idx:
        entry_idx = idx
        break

    if entry_idx is None:
        return {
            "symbol": None,
            "funding_time": int(funding_time_ms / 1000),
            "funding_rate": last_funding_rate,
            "pre_mid": pre_mid,
            "mid_at_t0": mids[first_post],
            "max_up_2s_pct": pct(max_up_2s),
            "min_dn_2s_pct": pct(min_dn_2s),
            "amp_2s_pct": amp_2s,
            "amp_10s_pct": amp_10s,
            "max_latency_ms_post": max_lat_all,
            "max_latency_ms_2s": max_lat_2s,
            "is_max_latency_in_2s": is_max_latency_in_2s,
            "latency_baseline_ms": latency_baseline,
            "maker_side": "SELL",
            "entry_triggered": False,
            "order_status": "NO_ENTRY",
            "entry_latency_ms": 0.0,
            "entry_price": 0.0,
            "entry_eff_price": 0.0,
            "stop_loss_price": 0.0,
            "take_profit_price": 0.0,
            "exit_type": None,
            "exit_price": 0.0,
            "exit_eff_price": 0.0,
            "slippage_occurred": False,
            "hold_time_s": None,
            "maker_fee_bps": maker_fee_bps,
            "taker_fee_bps": taker_fee_bps,
            "leverage_results": {},
        }

    entry_price = bids[entry_idx]
    logger.info(
        "✅ 模擬入場：假設結算後 30ms 用 SELL taker 進場，價格 %.8f (idx=%d)",
        entry_price,
        entry_idx,
    )

    analyzer = FundingRateAnalyzer()
    stop_loss_price, take_profit_price = analyzer.calculate_trade_levels(
        last_funding_rate,
        entry_price
    )

    t_fee = taker_fee_bps / 10000.0
    entry_eff = entry_price * (1.0 - t_fee)

    order_status = "PENDING"
    exit_idx: Optional[int] = None
    exit_price: Optional[float] = None
    exit_type: Optional[str] = None
    slippage_occurred = False

    price_history: List[Dict[str, float]] = []
    for idx in range(entry_idx + 1, len(records)):
        current_bid = bids[idx]
        current_ask = asks[idx]
        current_mid = mids[idx]
        price_history.append(
            {
                "idx": idx,
                "bid": current_bid,
                "ask": current_ask,
                "mid": current_mid,
                "time": recv_ts[idx],
            }
        )

        stop_condition = current_ask >= stop_loss_price
        profit_condition = current_ask <= take_profit_price
        book_price_for_slippage = current_ask

        if stop_condition:
            exit_idx = idx
            exit_price = stop_loss_price
            exit_type = "STOP_LOSS"
            if book_price_for_slippage > stop_loss_price * 1.001:
                slippage_occurred = True
            break
        elif profit_condition:
            exit_idx = idx
            exit_price = take_profit_price
            exit_type = "TAKE_PROFIT"
            if book_price_for_slippage < take_profit_price * 0.999:
                slippage_occurred = True
            break

    leverage_results: Dict[int, Dict[str, Any]] = {}
    exit_eff = None
    hold_time_s = None

    if exit_idx is not None:
        order_status = "FILLED"
        exit_m_fee = maker_fee_bps / 10000.0
        exit_eff = exit_price * (1.0 + exit_m_fee)

        base_pnl = (entry_eff / exit_eff) - 1.0

        for lev in leverage_list:
            liquidation_price = entry_price * (1.0 + 1.0 / lev)
            liquidated = False
            liquidation_idx = None

            for p in price_history:
                if p["ask"] >= liquidation_price:
                    liquidated = True
                    liquidation_idx = p["idx"]
                    break

            if liquidated:
                lev_pnl = -1.0
                lev_status = "LIQUIDATED"
                actual_exit_idx = liquidation_idx
            else:
                lev_pnl = base_pnl * lev
                lev_pnl = max(lev_pnl, -1.0)
                lev_status = exit_type
                actual_exit_idx = exit_idx

            leverage_results[lev] = {
                "pnl_pct": lev_pnl * 100.0,
                "status": lev_status,
                "liquidated": liquidated,
                "liquidation_price": liquidation_price,
                "exit_idx": actual_exit_idx,
            }

        hold_time_s = recv_ts[exit_idx] - recv_ts[entry_idx]
    else:
        order_status = "UNFILLED"
        for lev in leverage_list:
            leverage_results[lev] = {
                "pnl_pct": None,
                "status": "UNFILLED",
                "liquidated": False,
                "liquidation_price": None,
                "exit_idx": None,
            }

    return {
        "symbol": None,
        "funding_time": int(funding_time_ms / 1000),
        "funding_rate": last_funding_rate,
        "pre_mid": pre_mid,
        "mid_at_t0": mids[first_post],
        "max_up_2s_pct": pct(max_up_2s),
        "min_dn_2s_pct": pct(min_dn_2s),
        "amp_2s_pct": amp_2s,
        "amp_10s_pct": amp_10s,
        "max_latency_ms_post": max_lat_all,
        "max_latency_ms_2s": max_lat_2s,
        "is_max_latency_in_2s": is_max_latency_in_2s,
        "latency_baseline_ms": latency_baseline,
        "entry_triggered": True,
        "entry_latency_ms": entry_latency,
        "maker_side": "SELL",
        "entry_price": entry_price,
        "entry_eff_price": entry_eff,
        "stop_loss_price": stop_loss_price,
        "take_profit_price": take_profit_price,
        "order_status": order_status,
        "exit_type": exit_type,
        "exit_price": exit_price,
        "exit_eff_price": exit_eff,
        "slippage_occurred": slippage_occurred,
        "hold_time_s": hold_time_s,
        "maker_fee_bps": maker_fee_bps,
        "taker_fee_bps": taker_fee_bps,
        "leverage_results": leverage_results,
    }


# ==================== funding snapshot & symbol 選擇 ====================

async def _fetch_snapshot(session: aiohttp.ClientSession) -> Tuple[List[Dict], Dict[str, float], Dict, Dict]:
    """
    取得所有合約的 funding 與 24h 量。
    回傳: (sorted_by_rate, vol_by_symbol, most_negative, most_positive)
    """
    async with session.get(PREMIUM_INDEX_URL, timeout=10) as resp:
        all_items = await resp.json()
    async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=10) as resp2:
        tick24 = await resp2.json()

    filtered = [x for x in all_items if "lastFundingRate" in x and "nextFundingTime" in x]
    sorted_by_rate = sorted(filtered, key=lambda x: float(x["lastFundingRate"]))
    most_negative = sorted_by_rate[0] if sorted_by_rate else None
    most_positive = sorted_by_rate[-1] if sorted_by_rate else None

    vol_by_symbol: Dict[str, float] = {}
    try:
        for t in tick24:
            sym = t.get("symbol")
            qv = t.get("quoteVolume")
            if sym is not None and qv is not None:
                try:
                    vol_by_symbol[sym] = float(qv)
                except Exception:
                    pass
    except Exception:
        pass

    return sorted_by_rate, vol_by_symbol, most_negative, most_positive


def _pick_with_volume(
    sorted_list: List[Dict],
    side: str,
    vol_by_symbol: Dict[str, float],
    min_qv: float,
) -> Tuple[Dict, float, Optional[Dict]]:
    """
    根據 side ('pos' or 'neg') 挑 funding 極值，並檢查 24h 量門檻。
    """
    ordered = list(reversed(sorted_list)) if side == "pos" else list(sorted_list)
    rejected = None
    for idx, x in enumerate(ordered):
        sym = x.get("symbol")
        qv = vol_by_symbol.get(sym, 0.0)
        if qv >= min_qv:
            if idx == 0:
                return x, qv, None
            rejected = ordered[0]
            return x, qv, rejected
    x = ordered[0]
    return x, vol_by_symbol.get(x.get("symbol", ""), 0.0), None


# ==================== Binanace client & config ====================

def create_binance_futures_client(
    api_key: str,
    api_secret: str,
    testnet_mode: bool,
) -> ccxt.binance:
    client = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "future"},
            "headers": {"User-Agent": "Mozilla/5.0"},
        }
    )
    client.set_sandbox_mode(testnet_mode)
    client.load_markets()
    return client


def load_api_credentials_from_config(config_path: Path) -> Tuple[Optional[str], Optional[str], bool]:
    if not config_path.exists():
        return None, None, False

    parser = configparser.ConfigParser()
    parser.read(config_path)
    if "FUTURE_ACCOUNT" not in parser:
        return None, None, False

    api_key = parser["FUTURE_ACCOUNT"].get("BINANCE_API_KEY_FUTURE")
    api_secret = parser["FUTURE_ACCOUNT"].get("BINANCE_SECRET_FUTURE")

    if not api_secret:
        key_path_value = parser["FUTURE_ACCOUNT"].get("PrivateKeyPath")
        if key_path_value:
            key_path = Path(key_path_value)
            if not key_path.is_absolute():
                key_path = config_path.parent / key_path
            if key_path.exists():
                api_secret = key_path.read_text(encoding="utf-8").strip()

    testnet_mode = False
    if parser.has_section("EXCHANGE"):
        testnet_value = parser["EXCHANGE"].get("TestnetMode", "False")
        testnet_mode = str(testnet_value).strip().lower() in {"true", "1", "yes"}

    return api_key, api_secret, testnet_mode


# ==================== watcher：實盤 + 寫入 CSV ====================

async def _watch_loop(side: str, args, real_trader: Optional[BinanceRealTrader] = None):
    assert side == "neg", "Only SELL direction (neg funding) is supported for live trades"

    leverage_list = [5, 10, 15, 20, 25]

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                sorted_by_rate, vol_by_symbol, most_negative, most_positive = await _fetch_snapshot(session)

            if not sorted_by_rate:
                logger.warning("取不到 premiumIndex，5 秒後重試…")
                await asyncio.sleep(5)
                continue

            pick, qv, rejected = _pick_with_volume(
                sorted_by_rate,
                side,
                vol_by_symbol,
                float(args.min_quote_volume),
            )
            sym = pick["symbol"]
            fr = float(pick["lastFundingRate"])
            nft = int(pick["nextFundingTime"])

            if rejected is not None:
                logger.info(
                    f"{side.upper()} 極值 {rejected['symbol']} qv={vol_by_symbol.get(rejected['symbol'],0.0):,.0f} "
                    f"低於門檻，改監聽 {sym} qv={qv:,.0f}"
                )
            else:
                logger.info(f"{side.upper()} 監聽 {sym} fr={fr:+.6f} qv={qv:,.0f}")

            # 收集結算前後 ticks
            ticks = await collect_bookticker(
                sym,
                nft,
                start_before_s=float(args.start_before),
                window_pre_s=float(args.window_pre),
                window_post_s=float(args.window_post),
                write_raw_csv=bool(args.write_raw),
            )

            metrics = analyze_post_settlement(
                ticks,
                nft,
                fr,
                peak_window_s=float(args.peak_window),
                maker_fee_bps=float(getattr(args, "maker_fee_bps", 0.0)),
                taker_fee_bps=float(getattr(args, "taker_fee_bps", 2.75)),
                latency_entry_ms=float(getattr(args, "entry_latency_ms", 30.0)),
                leverage_list=leverage_list,
            )
            if not metrics:
                logger.warning(f"{sym} 沒有足夠資料可供分析，繼續下一輪…")
                await asyncio.sleep(2)
                continue
            metrics["symbol"] = sym

            # ====== 實盤入場 + 掛出場單 ======
            if real_trader and metrics.get("entry_triggered") and metrics.get("entry_price"):
                try:
                    # 以「實際要投入的 investment_amount & leverage」再算一次數量
                    entry_price = float(metrics["entry_price"])
                    lev = int(args.leverage)
                    notional = float(args.investment_amount) * lev
                    qty = notional / entry_price

                    # 真實做空
                    trade_result = await real_trader.execute_short_trade(
                        symbol=sym,
                        entry_price=entry_price,
                        investment_amount=float(args.investment_amount),
                        leverage=lev,
                        margin_mode=args.margin_mode,
                    )
                    taker_time_iso = datetime.now(timezone.utc).isoformat()
                    taker_price = (
                        trade_result.get("average")
                        or trade_result.get("price")
                        or trade_result.get("info", {}).get("avgPrice")
                        or entry_price
                    )
                    taker_qty = (
                        trade_result.get("amount")
                        or trade_result.get("filled")
                        or trade_result.get("info", {}).get("executedQty")
                        or qty
                    )
                    logger.info(
                        f"🕒 Taker 入場時間={taker_time_iso}, 價格={taker_price}, 數量={taker_qty}"
                    )

                    # 下 maker 出場單（TP / SL）
                    exit_orders = real_trader.place_exit_orders_for_short(
                        symbol=sym,
                        entry_price=entry_price,
                        funding_rate=fr,
                        quantity=qty,
                    )
                    tp_price = exit_orders.get("tp_price_fmt")
                    sl_price = exit_orders.get("sl_price_fmt")
                    logger.info(
                        f"🏷️ Maker 掛單價格: TP={tp_price}, SL={sl_price}"
                    )

                    metrics["live_trade_result"] = trade_result
                    metrics["live_exit_orders"] = exit_orders
                except Exception as exc:
                    logger.exception(f"{sym} 真實交易或掛單失敗：{exc}")

            # ====== 寫入 CSV ======
            results_csv = Path("analysis/logs/settlement_sim_results.csv")
            results_csv.parent.mkdir(parents=True, exist_ok=True)
            if not results_csv.exists():
                base_header = [
                    "timestamp_utc",
                    "symbol",
                    "funding_time",
                    "lastFundingRate",
                    "pre_mid",
                    "mid_at_t0",
                    "max_up_2s_pct",
                    "min_dn_2s_pct",
                    "amp_2s_pct",
                    "amp_10s_pct",
                    "max_latency_ms_post",
                    "max_latency_ms_2s",
                    "latency_baseline_ms",
                    "entry_latency_ms",
                    "is_max_latency_in_2s",
                    "maker_side",
                    "entry_triggered",
                    "entry_price",
                    "entry_eff_price",
                    "stop_loss_price",
                    "take_profit_price",
                    "order_status",
                    "exit_type",
                    "exit_price",
                    "exit_eff_price",
                    "slippage_occurred",
                    "hold_time_s",
                    "maker_fee_bps",
                    "taker_fee_bps",
                ]
                leverage_header = []
                for lev in leverage_list:
                    leverage_header.extend(
                        [
                            f"lev_{lev}x_pnl_pct",
                            f"lev_{lev}x_status",
                            f"lev_{lev}x_liquidated",
                            f"lev_{lev}x_liq_price",
                        ]
                    )
                header = ",".join(base_header + leverage_header) + "\n"
                results_csv.write_text(header, encoding="utf-8")

            ts_iso = datetime.now(timezone.utc).isoformat()
            fields = [
                ts_iso,
                sym,
                str(metrics["funding_time"]),
                f"{metrics['funding_rate']:.8f}",
                f"{metrics['pre_mid']:.8f}",
                f"{metrics['mid_at_t0']:.8f}",
                f"{metrics['max_up_2s_pct']:.6f}",
                f"{metrics['min_dn_2s_pct']:.6f}",
                f"{metrics['amp_2s_pct']:.6f}",
                f"{metrics['amp_10s_pct']:.6f}",
                f"{metrics['max_latency_ms_post']:.1f}",
                f"{metrics['max_latency_ms_2s']:.1f}",
                f"{metrics.get('latency_baseline_ms', 0):.1f}",
                f"{metrics.get('entry_latency_ms', 0):.1f}",
                str(int(metrics["is_max_latency_in_2s"])),
                metrics["maker_side"],
                str(int(metrics["entry_triggered"])),
                f"{metrics.get('entry_price', 0):.8f}",
                f"{metrics.get('entry_eff_price', 0):.8f}",
                f"{metrics.get('stop_loss_price', 0):.8f}",
                f"{metrics.get('take_profit_price', 0):.8f}",
                metrics.get("order_status", "UNKNOWN"),
                metrics.get("exit_type", "") or "",
                f"{metrics.get('exit_price', 0):.8f}" if metrics.get("exit_price") else "",
                f"{metrics.get('exit_eff_price', 0):.8f}" if metrics.get("exit_eff_price") else "",
                str(int(metrics.get("slippage_occurred", False))),
                f"{metrics.get('hold_time_s', 0):.2f}" if metrics.get("hold_time_s") else "",
                f"{metrics.get('maker_fee_bps', 0.0):.2f}",
                f"{metrics.get('taker_fee_bps', 0.0):.2f}",
            ]

            lev_results = metrics.get("leverage_results", {})
            for lev in leverage_list:
                lev_data = lev_results.get(lev, {})
                pnl = lev_data.get("pnl_pct")
                fields.extend(
                    [
                        f"{pnl:.6f}" if pnl is not None else "",
                        lev_data.get("status", ""),
                        str(int(lev_data.get("liquidated", False))),
                        f"{lev_data.get('liquidation_price', 0):.8f}"
                        if lev_data.get("liquidation_price")
                        else "",
                    ]
                )

            line = ",".join(fields) + "\n"
            with results_csv.open("a", encoding="utf-8") as f:
                f.write(line)

            summary_parts = [f"✅ {sym} 模擬完成 | 狀態:{metrics.get('order_status')}"]
            if metrics.get("live_trade_result"):
                live = metrics["live_trade_result"]
                order_id = live.get("id") or live.get("info", {}).get("orderId")
                summary_parts.append(f"REAL orderId:{order_id}")
            for lev in leverage_list:
                lev_data = lev_results.get(lev, {})
                pnl = lev_data.get("pnl_pct")
                status = lev_data.get("status", "")
                if pnl is not None:
                    summary_parts.append(f"{lev}x:{pnl:+.2f}%({status})")
            summary = " | ".join(summary_parts) + f" | 已寫入 {results_csv}"
            logger.info(summary)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"{side.upper()} watcher 發生錯誤：{e}")
            await asyncio.sleep(5)


# ==================== main ====================

async def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Settlement real trade executor (taker-entry 30ms after funding, maker-exit via FundingRateAnalyzer)"
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma separated symbols e.g. BTCUSDT,ETHUSDT. If omitted, auto-pick extreme funding.",
    )
    parser.add_argument(
        "--pick",
        type=str,
        default="neg",
        choices=["abs", "pos", "neg", "both"],
        help="auto-pick: choose most pos/neg/both/abs (abs runs both watchers)",
    )
    parser.add_argument(
        "--min-quote-volume",
        type=float,
        default=15000000.0,
        help="24h USDT quoteVolume minimum; below it will fallback to next best",
    )
    parser.add_argument("--start-before", type=float, default=60.0, help="seconds before settlement to start listening")
    parser.add_argument("--window-pre", type=float, default=15.0, help="seconds to keep before settlement")
    parser.add_argument("--window-post", type=float, default=15.0, help="seconds to keep after settlement")
    parser.add_argument(
        "--peak-window",
        type=float,
        default=2.0,
        help="post-settlement window to test peak (seconds) for analysis only",
    )
    parser.add_argument(
        "--entry-latency-ms",
        type=float,
        default=30.0,
        help="assumed latency after funding (ms) for backtest metrics (real trade直接用第一筆post tick)",
    )
    parser.add_argument("--write-raw", action="store_true", help="write raw bookTicker ticks to CSV")
    parser.add_argument("--maker-fee-bps", type=float, default=0.0, help="maker fee in bps (for backtest)")
    parser.add_argument("--taker-fee-bps", type=float, default=2.75, help="taker fee in bps (for backtest)")
    parser.add_argument(
        "--live-trade",
        action="store_true",
        help="啟用真實 SELL 下單（會用 FundingAnalyzer 決定 TP/SL 的 maker 單）",
    )
    parser.add_argument("--leverage", type=int, default=20, help="真實下單的槓桿倍數")
    parser.add_argument(
        "--margin-mode",
        type=str,
        choices=["isolated", "cross"],
        default="isolated",
        help="選擇逐倉或全倉模式",
    )
    parser.add_argument("--investment-amount", type=float, default=1.0, help="每筆投入 USDT 保證金")

    args, _ = parser.parse_known_args()

    real_trader: Optional[BinanceRealTrader] = None
    if args.live_trade:
        config_path = Path(__file__).resolve().parents[1] / "config.ini"
        cfg_api_key, cfg_api_secret, cfg_testnet = load_api_credentials_from_config(config_path)

        api_key = (
            os.environ.get("BINANCE_API_KEY_FUTURE")
            or os.environ.get("BINANCE_API_KEY")
            or cfg_api_key
        )
        api_secret = (
            os.environ.get("BINANCE_SECRET_FUTURE")
            or os.environ.get("BINANCE_API_SECRET")
            or cfg_api_secret
        )
        testnet_mode = cfg_testnet
        env_testnet = os.environ.get("BINANCE_TESTNET_MODE")
        if env_testnet is not None:
            testnet_mode = env_testnet.strip().lower() in {"true", "1", "yes"}

        if not api_key or not api_secret:
            raise SystemExit("請先在環境變數或 config.ini 中提供 APIKey 與對應密鑰/檔案後再進行真實交易")

        futures_client = create_binance_futures_client(api_key, api_secret, testnet_mode)
        precision_manager = SymbolPrecisionManager(futures_client)
        initialized = await precision_manager.initialize()
        if not initialized:
            raise SystemExit("無法讀取交易所精度資料，停止真實交易")
        real_trader = BinanceRealTrader(futures_client, precision_manager)
        mode_text = "TESTNET" if testnet_mode else "LIVE"
        logger.info(f"✅ ccxt Binance Futures 客戶端已就緒（模式：{mode_text}）")

        if args.pick != "neg":
            logger.warning("真實交易僅支援 SELL ，已強制將 --pick 設為 neg")
            args.pick = "neg"

    # 這份檔案主要針對「自動挑選 funding 極值」+ 實盤模式
    if args.symbols:
        logger.warning(
            "目前版本的重點在自動挑選 funding 極值，--symbols 模式僅保留回測用途，如要實盤請走 auto-pick 模式。"
        )

    if args.pick != "neg":
        logger.warning("系統僅支援 SELL 方向，監控目標已調整為負 funding")
        args.pick = "neg"

    tasks = [asyncio.create_task(_watch_loop("neg", args, real_trader))]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("中斷執行，退出。")
