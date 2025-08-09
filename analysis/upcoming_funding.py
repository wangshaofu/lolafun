#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
list top N upcoming funding rates from Binance USDT-M Futures.
This script fetches the next funding times for all symbols and lists the top N
'''

import requests
from datetime import datetime, timezone

import time
import statistics

# Binance USDT-M Futures Premium Index endpoint
API_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"

# Binance USDT-M Futures Kline endpoint
KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"

def fetch_premium_index():
    """向 Binance 取得所有期貨標的的 funding 資訊。"""
    resp = requests.get(API_URL, timeout=5)
    resp.raise_for_status()
    return resp.json()

def fetch_last_hour_volumes(symbol):
    """抓取指定 symbol 過去 1 小時的 1m K 線成交量 (volume)。"""
    # 取得今天 00:00:00 UTC 的 timestamp (ms)
    now = datetime.now(timezone.utc)
    today_utc = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    start_time = int(today_utc.timestamp() * 1000)
    end_time = int(time.time() * 1000)
    params = {
        "symbol": symbol,
        "interval": "1m",
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1440  # 一天最多 1440 筆
    }
    resp = requests.get(KLINE_URL, params=params, timeout=5)
    resp.raise_for_status()
    klines = resp.json()
    # volume 在 index 5，需轉 float
    volumes = [float(k[5]) for k in klines]
    # 只取最近 60 筆
    return volumes[-60:] if len(volumes) >= 60 else volumes

def list_upcoming_funding(top_n=10):
    """列出最先要結算 funding rate 的前 N 檔交易對。"""
    data = fetch_premium_index()
    # 將 nextFundingTime 由 ms 轉成整數，並排序
    # 過濾掉 nextFundingTime 為 0 的 symbol
    filtered = [x for x in data if int(x.get("nextFundingTime", 0)) > 0]
    sorted_list = sorted(
        filtered,
        key=lambda x: int(x.get("nextFundingTime", 0))
    )
    
    print(f"{'Symbol':<12} {'Next Funding Time (UTC)':<25} {'Local Time':<20} {'Funding Rate':<14} {'1h Avg Vol':<14} {'1h Vol Std':<14} {'Nonzero':<8} {'Std/Avg':<8}")
    print("-" * 150)
    stats_list = []
    for item in sorted_list:
        symbol = item["symbol"]
        nft_ms = int(item["nextFundingTime"])
        funding_rate = item.get("lastFundingRate", None)
        dt_utc = datetime.fromtimestamp(nft_ms/1000, tz=timezone.utc)
        dt_local = dt_utc.astimezone()
        funding_rate_str = f"{float(funding_rate):.6f}" if funding_rate is not None else "N/A"
        try:
            volumes = fetch_last_hour_volumes(symbol)
            avg_vol = statistics.mean(volumes) if volumes else 0
            std_vol = statistics.stdev(volumes) if len(volumes) > 1 else 0
            nonzero_count = sum(1 for v in volumes if v > 0)
            std_avg_ratio = std_vol / avg_vol if avg_vol > 0 else 0
        except Exception as e:
            avg_vol = std_vol = std_avg_ratio = nonzero_count = 0
        stats_list.append({
            "symbol": symbol,
            "dt_utc": dt_utc,
            "dt_local": dt_local,
            "funding_rate_str": funding_rate_str,
            "avg_vol": avg_vol,
            "std_vol": std_vol,
            "nonzero_count": nonzero_count,
            "std_avg_ratio": std_avg_ratio
        })

    # 依流動性排序：nonzero_count多 > std/avg小 > avg_vol大
    stats_list.sort(key=lambda x: (-x["nonzero_count"], x["std_avg_ratio"], -x["avg_vol"]))

    for s in stats_list[:top_n]:
        print(f"{s['symbol']:<12} {s['dt_utc'].strftime('%Y-%m-%d %H:%M:%S'):<25} {s['dt_local'].strftime('%Y-%m-%d %H:%M:%S'):<20} {s['funding_rate_str']:<14} {s['avg_vol']:<14.2f} {s['std_vol']:<14.2f} {s['nonzero_count']:<8} {s['std_avg_ratio']:<8.2f}")

if __name__ == "__main__":
    # 參數 top_n 可自由調整要列出的數量
    list_upcoming_funding(top_n=10)