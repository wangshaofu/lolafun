import sys
import json
try:
    import websocket
except ImportError:
    websocket = None
# WebSocket 取得所有合約 funding rate
def run_funding_rate_websocket():
    if websocket is None:
        print("請先安裝 websocket-client：pip install websocket-client")
        return
    def on_message(ws, message):
        data = json.loads(message)
        for item in data:
            symbol = item['s']
            funding_rate = float(item['r'])
            next_funding_time = int(item['T'])
            dt_utc = datetime.fromtimestamp(next_funding_time/1000, tz=timezone.utc)
            print(f"{symbol:<12} funding_rate={funding_rate:>+9.6f} next_funding_time={dt_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print('-' * 80)
    def on_error(ws, error):
        print("WebSocket error:", error)
    def on_close(ws, close_status_code, close_msg):
        print("WebSocket closed")
    def on_open(ws):
        print("WebSocket connection opened")
    ws_url = "wss://fstream.binance.com/ws/!markPrice@arr"
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
即時取得 Binance USDT-M Futures 全部合約的 funding rate（WebSocket 版）
並即時顯示 funding rate 最負與最正的標的
'''

import websocket
import json
from datetime import datetime, timezone
import threading
import time

def on_message(ws, message):
    data = json.loads(message)
    # data 是 list，每個元素是一個 symbol 的 mark price/funding rate
    if not isinstance(data, list):
        return
    # 建立 symbol -> 資料 dict
    symbol_map = {item['s']: item for item in data if 'r' in item}
    # 只顯示一次最正極與最負的 funding rate（各一個）
    import pytz
    tz_taipei = pytz.timezone('Asia/Taipei')
    now_dt = datetime.now(timezone.utc)
    now_taipei = now_dt.astimezone(tz_taipei)
    now_str = now_taipei.strftime('%Y-%m-%d %H:%M:%S 台灣時間')
    sorted_by_rate = sorted(symbol_map.values(), key=lambda x: float(x['r']))
    most_positive = sorted_by_rate[-1]
    most_negative = sorted_by_rate[0]
    print(f"[{now_str}]\nMost Positive Funding Rate:")
    pos_time = datetime.fromtimestamp(int(most_positive['T'])/1000, tz=timezone.utc).astimezone(tz_taipei)
    print(f"  {most_positive['s']:<12} funding_rate={float(most_positive['r']):+9.6f} next_funding_time={pos_time.strftime('%Y-%m-%d %H:%M:%S 台灣時間')}")
    print('-' * 80)
    print(f"Most Negative Funding Rate:")
    neg_time = datetime.fromtimestamp(int(most_negative['T'])/1000, tz=timezone.utc).astimezone(tz_taipei)
    print(f"  {most_negative['s']:<12} funding_rate={float(most_negative['r']):+9.6f} next_funding_time={neg_time.strftime('%Y-%m-%d %H:%M:%S 台灣時間')}")
    print('-' * 80)

    # 列出六小時內即將結算的幣種，只顯示前三個，時間顯示台灣時間
    six_hours_ms = 6 * 60 * 60 * 1000
    upcoming = [item for item in symbol_map.values() if 0 <= int(item['T']) - int(now_dt.timestamp() * 1000) <= six_hours_ms]
    upcoming_sorted = sorted(upcoming, key=lambda x: int(x['T']))[:3]
    if upcoming_sorted:
        print(f"Upcoming settlements within 6 hours (Top 3):")
        for item in upcoming_sorted:
            symbol = item['s']
            funding_rate = float(item['r'])
            next_funding_time = int(item['T'])
            dt_taipei = datetime.fromtimestamp(next_funding_time/1000, tz=timezone.utc).astimezone(tz_taipei)
            mins_left = int((next_funding_time - int(now_dt.timestamp() * 1000)) / 60000)
            print(f"{symbol:<12} funding_rate={funding_rate:+9.6f} next_funding_time={dt_taipei.strftime('%Y-%m-%d %H:%M:%S 台灣時間')} (還有 {mins_left} 分鐘)")
        print('-' * 80)
    else:
        print("No settlements within 6 hours.")
        print('-' * 80)

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    print("WebSocket connection opened")

if __name__ == "__main__":
    ws_url = "wss://fstream.binance.com/ws/!markPrice@arr"
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()