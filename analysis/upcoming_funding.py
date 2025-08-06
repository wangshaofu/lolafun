#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
list top N upcoming funding rates from Binance USDT-M Futures.
This script fetches the next funding times for all symbols and lists the top N
'''

import requests
from datetime import datetime, timezone

# Binance USDT-M Futures Premium Index endpoint
API_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"

def fetch_premium_index():
    """向 Binance 取得所有期貨標的的 funding 資訊。"""
    resp = requests.get(API_URL, timeout=5)
    resp.raise_for_status()
    return resp.json()

def list_upcoming_funding(top_n=10):
    """列出最先要結算 funding rate 的前 N 檔交易對。"""
    data = fetch_premium_index()
    # 將 nextFundingTime 由 ms 轉成整數，並排序
    sorted_list = sorted(
        data,
        key=lambda x: int(x.get("nextFundingTime", 0))
    )
    
    print(f"{'Symbol':<12} {'Next Funding Time (UTC)':<25} {'Local Time'}")
    print("-" * 70)
    for item in sorted_list[:top_n]:
        symbol = item["symbol"]
        nft_ms = int(item["nextFundingTime"])
        # 轉為 UTC datetime，再轉成本地時區
        dt_utc = datetime.fromtimestamp(nft_ms/1000, tz=timezone.utc)
        dt_local = dt_utc.astimezone()  # 預設轉成本機時區
        print(f"{symbol:<12} {dt_utc.strftime('%Y-%m-%d %H:%M:%S'):<25} {dt_local.strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    # 參數 top_n 可自由調整要列出的數量
    list_upcoming_funding(top_n=10)