#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Settlement Real Trade Executor

設計重點：
- 只做 SELL（funding rate < 0）。
- 在 _watch_loop 選好 symbol 後，就先設定好 margin mode / leverage。
- 結算前開始訂閱 bookTicker，**第一筆結算後的 tick 當下**：
    1) 用 taker 市價 SELL 進場。
    2) 取得實際部位大小，依 FundingRateAnalyzer 計算 TP / SL 價格。
    3) 立刻下兩張 reduceOnly 的 TAKE_PROFIT_MARKET / STOP_MARKET 平倉單。
- 仍可留存結算前後的 ticks 到 CSV（純記錄用途），但不再做 window_post 的價格分析 / 回測。

使用前提：
- config.ini 中要有 FUTURE_ACCOUNT 欄位或環境變數提供 API key / secret。
"""

import asyncio
import configparser
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any, Callable, Awaitable

import aiohttp
import ccxt
import websockets
import os
import sys

# 專案內部模組
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
    """封裝 Binance Futures 實盤操作：進場市價單 + 出場 STOP/TP_MKT"""

    def __init__(self, client: ccxt.binance, precision_manager: SymbolPrecisionManager):
        self.client = client
        self.precision_manager = precision_manager

    # ---------- 進場：taker 市價 SELL ----------

    async def execute_short_trade(
        self,
        symbol: str,
        notional_margin: float,
        leverage: int,
        margin_mode: str,
        hint_price: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        用「保證金 × 槓桿」計算名目金額，再依當下價格計算數量，送出市場 SELL 單。
        - symbol: "BTCUSDT" 等
        - notional_margin: 你願意投多少 USDT 作為保證金（例如 1 USDT）
        - leverage: 槓桿倍數，例如 20
        - margin_mode: "isolated" 或 "cross"
        - hint_price: 可選，用於估算數量；實際成交價以交易所為準
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            self._execute_short_trade_sync,
            symbol.upper(),
            float(notional_margin),
            int(leverage),
            margin_mode,
            None if hint_price is None else float(hint_price),
        )

    def _execute_short_trade_sync(
        self,
        symbol: str,
        notional_margin: float,
        leverage: int,
        margin_mode: str,
        hint_price: Optional[float],
    ) -> Dict[str, Any]:
        if notional_margin <= 0:
            raise ValueError("notional_margin 必須為正值")

        lev = max(1, min(int(leverage), 125))
        notional = notional_margin * lev

        # 若沒有 hint_price，就抓當前市價
        if hint_price is None or hint_price <= 0:
            ticker = self.client.fetch_ticker(symbol)
            mid = (ticker.get("bid") or ticker.get("last") or ticker.get("ask") or 0)
            if mid <= 0:
                raise ValueError("無法取得合理市價作為數量估算基準")
            entry_price_est = float(mid)
        else:
            entry_price_est = hint_price

        raw_qty = notional / entry_price_est
        formatted_qty = self.precision_manager.format_quantity(symbol, raw_qty)
        qty_value = float(formatted_qty)
        if qty_value <= 0:
            raise ValueError(f"計算出來的下單數量過小: {qty_value}")

        margin_type = self._normalize_margin_mode(margin_mode)
        self._ensure_margin_type(symbol, margin_type)
        self._ensure_leverage(symbol, lev)

        logger.info(
            f"🚀 準備下真實 SELL 單 {symbol}: 數量 {formatted_qty}, 槓桿 {lev}x, 模式 {margin_type}"
        )

        params = {
            "newOrderRespType": "RESULT",
            "reduceOnly": False,  # 建倉單，不是平倉
        }
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

    # ---------- 出場：STOP_MARKET / TAKE_PROFIT_MARKET + reduceOnly ----------

    async def place_exit_market_orders_for_short(
        self,
        symbol: str,
        funding_rate: float,
    ) -> Dict[str, Any]:
        """
        讀取當前部位大小，根據 FundingRateAnalyzer 算出 TP / SL 價格，
        下兩張 reduceOnly 的 TAKE_PROFIT_MARKET / STOP_MARKET 平倉單。
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            self._place_exit_market_orders_for_short_sync,
            symbol.upper(),
            float(funding_rate),
        )

    def _place_exit_market_orders_for_short_sync(
        self,
        symbol: str,
        funding_rate: float,
    ) -> Dict[str, Any]:
        position_size = self._get_current_position_size(symbol)
        if position_size <= 0:
            raise ValueError(f"{symbol} 沒有可平倉的空單部位，無法掛出場單")

        # 用當下市價當 entry_price 近似，純為計算 TP/SL 門檻（方向由 FundingRateAnalyzer 控制）
        ticker = self.client.fetch_ticker(symbol)
        mid = (ticker.get("bid") or ticker.get("last") or ticker.get("ask") or 0)
        if mid <= 0:
            raise ValueError("無法取得市價作為 TP/SL 計算基準")
        entry_price_for_levels = float(mid)

        analyzer = FundingRateAnalyzer()
        stop_loss_price, take_profit_price = analyzer.calculate_trade_levels(
            funding_rate,
            entry_price_for_levels,
        )

        tp_price_fmt = self._format_price(symbol, take_profit_price)
        sl_price_fmt = self._format_price(symbol, stop_loss_price)

        qty_fmt = self.precision_manager.format_quantity(symbol, position_size)
        qty_val = float(qty_fmt)

        logger.info(
            f"📌 準備掛出場 STOP/TP 市價單 {symbol}: "
            f"TP(stopPrice)={tp_price_fmt}, SL(stopPrice)={sl_price_fmt}, qty={qty_fmt}"
        )

        results: Dict[str, Any] = {}

        # TAKE_PROFIT_MARKET（獲利出場）
        tp_params = {
            "stopPrice": tp_price_fmt,
            "reduceOnly": True,
        }
        tp_order = self.client.create_order(
            symbol=symbol,
            type="TAKE_PROFIT_MARKET",
            side="buy",        # 空單平倉 → BUY
            amount=qty_val,
            price=None,
            params=tp_params,
        )
        results["take_profit"] = tp_order

        # STOP_MARKET（止損出場）
        sl_params = {
            "stopPrice": sl_price_fmt,
            "reduceOnly": True,
        }
        sl_order = self.client.create_order(
            symbol=symbol,
            type="STOP_MARKET",
            side="buy",
            amount=qty_val,
            price=None,
            params=sl_params,
        )
        results["stop_loss"] = sl_order

        logger.info(
            "✅ 已掛出場 STOP/TP 單: "
            f"TP id={tp_order.get('id') or tp_order.get('info', {}).get('orderId')}, "
            f"SL id={sl_order.get('id') or sl_order.get('info', {}).get('orderId')}"
        )
        results["tp_price_fmt"] = tp_price_fmt
        results["sl_price_fmt"] = sl_price_fmt
        results["qty"] = qty_val
        return results

    # ---------- 工具函式 ----------

    def _get_current_position_size(self, symbol: str) -> float:
        """
        取得單向模式下該 symbol 的部位大小（contracts 數量，取絕對值）。
        若無部位則回傳 0。
        """
        positions = self.client.fetch_positions([symbol])
        for pos in positions:
            # 根據 ccxt binance 格式判斷
            sym = pos.get("symbol") or pos.get("info", {}).get("symbol")
            if sym != symbol:
                continue
            amt = None
            if "contracts" in pos and pos["contracts"] is not None:
                amt = float(pos["contracts"])
            else:
                info = pos.get("info", {})
                if "positionAmt" in info:
                    amt = float(info["positionAmt"])
            if amt is not None and amt != 0:
                return abs(amt)
        return 0.0

    def _ensure_margin_type(self, symbol: str, margin_type: str):
        try:
            setter = getattr(self.client, "set_margin_mode", None)
            if setter:
                setter(margin_type, symbol)
                logger.info(f"🔧 已設置 {symbol} margin 模式為 {margin_type}")
            else:
                logger.debug("當前客戶端不支援 margin mode 設定，跳過")
        except ccxt.BaseError as exc:
            msg = str(exc)
            if "No need to change margin type" in msg or "no need to change" in msg.lower():
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
        優先使用 precision_manager 的 price formatter，如果沒有就用交易所的 tickSize 精度。
        """
        fmt_price = None
        if hasattr(self.precision_manager, "format_price"):
            try:
                fmt_price = self.precision_manager.format_price(symbol, price)
            except Exception:
                fmt_price = None

        if fmt_price is not None:
            return float(fmt_price)

        mkt = self.client.markets.get(symbol, {})
        precision = mkt.get("precision", {})
        price_prec = precision.get("price")
        if price_prec is not None:
            return float(f"{price:.{int(price_prec)}f}")
        return float(price)


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


# ==================== Tick 收集（含實盤回呼） ====================

async def collect_bookticker(
    symbol: str,
    funding_time_ms: int,
    start_before_s: float,
    window_pre_s: float,
    window_post_s: float,
    write_raw_csv: bool,
    on_first_post_tick: Optional[
        Callable[[float, int, float, float], Awaitable[None]]
    ] = None,
) -> List[Tuple[float, int, int, float, float]]:
    """
    收集結算前後的 bookTicker。
    - on_first_post_tick 會在「第一筆 ev_ts_ms >= funding_time_ms 的 tick」觸發一次。
    - 仍會持續收集到 t0 + window_post，純紀錄用。
    回傳: [(recv_ts_aligned, event_ts_ms, update_id, bid, ask), ...]
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

        t0 = funding_time_ms / 1000.0
        desired_start = t0 - start_before_s
        wait_s = desired_start - server_now(server_offset)
        if wait_s > 0:
            logger.info(f"等待 {wait_s:.1f}s 至結算前 {start_before_s:.0f}s 開始監聽 {symbol}…")
            await asyncio.sleep(wait_s)

        try:
            ntp.force_sync_before_settlement(funding_time_ms)
        except Exception:
            pass

        start_collect = t0 - window_pre_s
        end_collect = t0 + window_post_s

        url = BINANCE_WS_BASE + BOOKTICKER_FMT.format(symbol.lower())
        logger.info(f"開始收集 {symbol} bookTicker：{url}")

        entry_callback_done = False

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

                    # 收集資料：僅在時間窗內記錄
                    if start_collect <= now_srv <= end_collect:
                        records.append((now_srv, ev_ts_ms, u, bid, ask))

                    # 第一筆「事件時間 >= funding_time」觸發實盤 callback
                    if (not entry_callback_done) and (ev_ts_ms >= funding_time_ms):
                        entry_callback_done = True
                        if on_first_post_tick is not None:
                            try:
                                await on_first_post_tick(now_srv, ev_ts_ms, bid, ask)
                            except Exception as exc:
                                logger.exception(f"{symbol} on_first_post_tick 執行失敗：{exc}")

                if now_srv > end_collect:
                    break

    # 寫出 raw ticks（純紀錄用）
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


# ==================== watcher：實盤 + 簡單紀錄 ====================

async def _watch_loop(side: str, args, real_trader: Optional[BinanceRealTrader] = None):
    assert side == "neg", "Only SELL direction (neg funding) is supported for live trades"

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

            if fr >= 0:
                logger.info(f"{sym} fundingRate={fr:+.6f} >= 0，僅做空策略，略過本輪")
                await asyncio.sleep(5)
                continue

            if rejected is not None:
                logger.info(
                    f"{side.upper()} 極值 {rejected['symbol']} qv={vol_by_symbol.get(rejected['symbol'],0.0):,.0f} "
                    f"低於門檻，改監聽 {sym} qv={qv:,.0f}"
                )
            else:
                logger.info(f"{side.upper()} 監聽 {sym} fr={fr:+.6f} qv={qv:,.0f}")

            # 真實交易模式：在結算前先把 margin mode / leverage 設好
            if real_trader is not None:
                try:
                    real_trader._ensure_margin_type(sym, args.margin_mode)
                    real_trader._ensure_leverage(sym, int(args.leverage))
                except Exception as exc:
                    logger.exception(f"{sym} 預先設定 margin/leverage 失敗：{exc}")
                    await asyncio.sleep(5)
                    continue

            # 實盤 entry callback：第一筆 post-settlement tick 就執行
            async def on_first_post_tick(
                recv_ts_srv: float,
                ev_ts_ms: int,
                bid: float,
                ask: float,
            ) -> None:
                if real_trader is None:
                    logger.info(
                        f"{sym} 第一筆 post tick（回測模式）:"
                        f" recv_ts={recv_ts_srv:.3f}, ev_ts_ms={ev_ts_ms}, bid={bid}, ask={ask}"
                    )
                    return

                try:
                    logger.info(
                        f"✅ 偵測到 {sym} 第一筆結算後 tick，準備立刻市價 SELL 入場…"
                        f" (ev_ts={ev_ts_ms}, funding_time={nft})"
                    )
                    # 以當下 bid 當作 hint price，實盤會以市場成交價為準
                    trade_result = await real_trader.execute_short_trade(
                        symbol=sym,
                        notional_margin=float(args.investment_amount),
                        leverage=int(args.leverage),
                        margin_mode=args.margin_mode,
                        hint_price=bid,
                    )

                    taker_time_iso = datetime.now(timezone.utc).isoformat()
                    taker_price = (
                        trade_result.get("average")
                        or trade_result.get("price")
                        or trade_result.get("info", {}).get("avgPrice")
                    )
                    taker_qty = (
                        trade_result.get("amount")
                        or trade_result.get("filled")
                        or trade_result.get("info", {}).get("executedQty")
                    )
                    order_id = trade_result.get("id") or trade_result.get("info", {}).get("orderId")

                    logger.info(
                        f"🕒 Taker 入場時間={taker_time_iso}, 價格={taker_price}, 數量={taker_qty}, orderId={order_id}"
                    )

                    # 下 STOP_MARKET / TAKE_PROFIT_MARKET 出場單
                    exit_orders = await real_trader.place_exit_market_orders_for_short(
                        symbol=sym,
                        funding_rate=fr,
                    )
                    tp_price = exit_orders.get("tp_price_fmt")
                    sl_price = exit_orders.get("sl_price_fmt")
                    logger.info(
                        f"🏷️ 已掛出場 STOP/TP 單 {sym}: "
                        f"TP(stopPrice)={tp_price}, SL(stopPrice)={sl_price}"
                    )

                    # 簡單紀錄到 CSV
                    trades_csv = Path("analysis/logs/settlement_real_trades.csv")
                    trades_csv.parent.mkdir(parents=True, exist_ok=True)
                    if not trades_csv.exists():
                        header = (
                            "timestamp_utc,symbol,funding_time,lastFundingRate,"
                            "entry_event_ts_ms,entry_recv_ts,entry_hint_bid,"
                            "taker_avg_price,taker_qty,tp_stop_price,sl_stop_price\n"
                        )
                        trades_csv.write_text(header, encoding="utf-8")

                    line = (
                        f"{taker_time_iso},{sym},{int(nft/1000)},{fr:.8f},"
                        f"{ev_ts_ms},{recv_ts_srv:.6f},{bid:.8f},"
                        f"{float(taker_price) if taker_price else 0:.8f},"
                        f"{float(taker_qty) if taker_qty else 0:.6f},"
                        f"{tp_price if tp_price is not None else ''},"
                        f"{sl_price if sl_price is not None else ''}\n"
                    )
                    with trades_csv.open("a", encoding="utf-8") as f:
                        f.write(line)

                except Exception as exc:
                    logger.exception(f"{sym} 真實交易流程（市價入場或掛 STOP/TP）失敗：{exc}")

            # 收集 ticks（下單邏輯已在 on_first_post_tick 中觸發）
            await collect_bookticker(
                sym,
                nft,
                start_before_s=float(args.start_before),
                window_pre_s=float(args.window_pre),
                window_post_s=float(args.window_post),
                write_raw_csv=bool(args.write_raw),
                on_first_post_tick=on_first_post_tick,
            )

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"{side.upper()} watcher 發生錯誤：{e}")
            await asyncio.sleep(5)


# ==================== main ====================

async def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Settlement real trade executor (taker-entry on first post-funding tick, exit via STOP/TP MARKET)"
    )
    parser.add_argument(
        "--pick",
        type=str,
        default="neg",
        choices=["abs", "pos", "neg", "both"],
        help="auto-pick: choose most pos/neg/both/abs（實盤僅支援 neg）",
    )
    parser.add_argument(
        "--min-quote-volume",
        type=float,
        default=15000000.0,
        help="24h USDT quoteVolume minimum; below it will fallback to next best",
    )
    parser.add_argument(
        "--start-before",
        type=float,
        default=60.0,
        help="seconds before settlement to start listening",
    )
    parser.add_argument(
        "--window-pre",
        type=float,
        default=15.0,
        help="seconds to keep before settlement (for raw CSV only)",
    )
    parser.add_argument(
        "--window-post",
        type=float,
        default=15.0,
        help="seconds to keep after settlement (for raw CSV only; 不再影響下單時間)",
    )
    parser.add_argument(
        "--write-raw",
        action="store_true",
        help="write raw bookTicker ticks to CSV（不影響實盤）",
    )
    parser.add_argument(
        "--live-trade",
        action="store_true",
        help="啟用真實 SELL 下單（停損/停利用 STOP/TAKE_PROFIT MARKET reduceOnly）",
    )
    parser.add_argument(
        "--leverage",
        type=int,
        default=20,
        help="真實下單的槓桿倍數",
    )
    parser.add_argument(
        "--margin-mode",
        type=str,
        choices=["isolated", "cross"],
        default="isolated",
        help="選擇逐倉或全倉模式",
    )
    parser.add_argument(
        "--investment-amount",
        type=float,
        default=1.0,
        help="每筆投入 USDT 保證金（例如 1 代表 1 USDT 保證金 × leverage）",
    )

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
            raise SystemExit("請先在環境變數或 config.ini 中提供 APIKey 與對應密鑰後再進行真實交易")

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

    # 實盤邏輯只用 auto-pick funding 最負的 symbol
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