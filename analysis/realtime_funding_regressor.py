#!/usr/bin/env python
"""
realtime_funding_regressor.py

1) 透過 Binance USDT-M Mark Price WebSocket 即時取得 markPrice 與 fundingRate
2) 緩衝每檔 symbol 120 分鐘價格，用於計算 slope 與 volatility
3) 於每次 fundingTime 觸發時，提取
      - fundingRate_now
      - price_slope_pre1h  （結算前 1h 單位斜率 %）
      - realized_vol_pre1h （結算前 1h 年化波動率 %）
      - price_return_prevFunding （與上一次結算時價格之差 %） ← 迴歸目標
4) 持續累積樣本；每累積 N=200 筆以上即可動態重訓 Ridge Regression
5) 可呼叫 predict_price_spread(-0.001) 取得 fundingRate = –0.1% 時的價差預估
"""

import asyncio, json, math, time, logging
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import websockets
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

BINANCE_WS = "wss://fstream.binance.com/stream?streams="
# 監聽多檔 symbol；可自行增減
SYMBOLS = ["btcusdt", "ethusdt"]
# 1000ms 更新速度
STREAM_SUFFIX = "@markPrice@1s"
# 緩衝 120 分鐘價格（7200 秒）
BUFFER_SEC = 7200
# 斜率／波動率計算視窗（3600 秒）
WINDOW_SEC = 3600

logger = logging.getLogger("FundingRegressor")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

class FundingRegressor:
    def __init__(self):
        # 每檔 symbol → deque[(ts, price)]
        self.price_buf = defaultdict(lambda: deque(maxlen=BUFFER_SEC))
        # 每檔 symbol → (last_funding_ts, last_funding_price)
        self.last_funding_ctx = {}
        # 訓練資料
        self.data = []
        # Sklearn pipeline
        self.model = Pipeline([
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=1.0))
        ])

    # --- 計算工具 ---------------------------------------------------------
    @staticmethod
    def _calc_slope(prices: pd.Series) -> float:
        # 線性回歸斜率 (%/sec)
        if len(prices) < 30:   # 至少 30 個樣本
            return 0.0
        x = np.arange(len(prices))
        y = prices.values
        slope, _ = np.polyfit(x, y, 1)
        return slope / prices.iloc[0] * 100  # 轉百分比/秒

    @staticmethod
    def _calc_vol(prices: pd.Series) -> float:
        # 年化波動率 (%)
        if len(prices) < 30:
            return 0.0
        log_ret = np.diff(np.log(prices))
        vol = np.std(log_ret) * math.sqrt(60*60*24*365) * 100  # 以秒頻率年化
        return vol

    # --- 資料處理 ---------------------------------------------------------
    def _process_mark(self, symbol: str, payload: dict):
        ts = payload["E"] // 1000              # stream time (s)
        price = float(payload["p"])
        funding_rate = float(payload["r"])
        funding_time = payload["T"] // 1000    # 預計結算時間 (s)

        # 寫入緩衝
        self.price_buf[symbol].append((ts, price))

        # 檢查是否進入新的 funding 事件 (ts >= funding_time)
        ctx = self.last_funding_ctx.get(symbol)
        if ctx is None or ts >= funding_time > ctx[0]:
            # 取前後一小時價格序列
            pre_prices = self._get_window_prices(symbol, ts - WINDOW_SEC, ts)
            post_prices = self._get_window_prices(symbol, ts, ts + WINDOW_SEC)

            if pre_prices.empty or post_prices.empty:
                return  # 資料不足，跳過

            price_slope = self._calc_slope(pre_prices)
            vol = self._calc_vol(pre_prices)

            # 目標值：與上一個 funding 結算價差
            prev_price = ctx[1] if ctx else price
            price_return = (price - prev_price) / prev_price * 100

            sample = {
                "symbol": symbol,
                "funding_rate": funding_rate * 100,  # 轉百分比
                "price_slope": price_slope,
                "volatility": vol,
                "price_return": price_return,
                "ts": ts
            }
            self.data.append(sample)
            logger.info(f"{symbol} | funding {funding_rate*100:.3f}% | "
                        f"Δ%={price_return:.3f} | slope={price_slope:.4f} | vol={vol:.2f}")

            # 更新 ctx
            self.last_funding_ctx[symbol] = (funding_time, price)

            # 若累積足夠樣本，重新訓練模型
            if len(self.data) >= 200:
                self._train()

    def _get_window_prices(self, symbol: str, start_ts: int, end_ts: int) -> pd.Series:
        buf = self.price_buf[symbol]
        data = [p for (t, p) in buf if start_ts <= t <= end_ts]
        return pd.Series(data, dtype="float64")

    # --- 模型 -------------------------------------------------------------
    def _train(self):
        df = pd.DataFrame(self.data)
        X = df[["funding_rate", "price_slope", "volatility"]]
        y = df["price_return"]
        self.model.fit(X, y)
        logger.info("模型已更新，R² = {:.4f}".format(self.model.score(X, y)))

    def predict_price_spread(self, funding_rate: float,
                             price_slope: float = 0.0,
                             volatility: float = 0.0):
        """預測在指定 funding_rate (%，如 -0.1) 時的期望價差 (%)"""
        X = np.array([[funding_rate*100, price_slope, volatility]])
        return float(self.model.predict(X)[0])

# -------------------------------------------------------------------------
async def main():
    # 建立多流 WebSocket 連線
    streams = "/".join([f"{s}{STREAM_SUFFIX}" for s in SYMBOLS])
    url = BINANCE_WS + streams
    reg = FundingRegressor()

    async with websockets.connect(url, max_queue=None, ping_interval=10) as ws:
        logger.info("WS connected")
        async for msg in ws:
            payload = json.loads(msg)
            data = payload["data"]
            symbol = data["s"].lower()
            reg._process_mark(symbol, data)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exit requested by user.")