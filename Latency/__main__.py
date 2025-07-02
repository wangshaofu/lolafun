import time
import json
import asyncio
import websockets
import numpy as np
import matplotlib.pyplot as plt
import requests
import ntplib


def get_ntp_offset(server="jp.pool.ntp.org"):
    """
    Get the time offset and delay to the given NTP server.
    Returns (offset_ms, delay_ms):
      offset_ms = local_time_ms - ntp_server_time_ms
      delay_ms  = round-trip delay in ms
    """
    client = ntplib.NTPClient()
    t0 = time.time() * 1000
    resp = client.request(server, version=3)
    t1 = time.time() * 1000
    server_time_ms = resp.tx_time * 1000
    # assume symmetric delay
    local_mid_ms = (t0 + t1) / 2
    offset_ms = local_mid_ms - server_time_ms
    delay_ms = resp.delay * 1000
    return offset_ms, delay_ms


# ----- Configuration -----
SYMBOL = "btcusdt"
URI = f"wss://fstream.binance.com/ws/{SYMBOL}@bookTicker"
NUM_ONE_WAY = 1000  # measure 500 one-way latency samples


# ----- Functions -----
async def measure_one_way(offset, n):
    latencies = []
    async with websockets.connect(URI) as ws:
        for _ in range(n):
            msg = await ws.recv()
            recv_ms = time.time() * 1000
            data = json.loads(msg)
            event_ms = data.get("E")
            if event_ms:
                adjusted_event = event_ms + offset
                latencies.append(recv_ms - adjusted_event)
    return latencies


def summarize(arr, name):
    arr = np.array(arr)
    print(f"=== {name} Statistics ===")
    print(f"Sample count: {len(arr)}")
    print(f"Mean: {arr.mean():.2f} ms")
    print(f"Median: {np.median(arr):.2f} ms")
    print(f"99th Percentile: {np.percentile(arr, 99):.2f} ms")
    print()


def plot_hist(arr, name):
    plt.hist(arr, bins=50, alpha=0.7)
    plt.title(f"{name} Distribution")
    plt.xlabel("Latency (ms)")
    plt.ylabel("Count")
    plt.grid(True, alpha=0.3)
    plt.show()


# ----- Main -----
async def main():
    ntp_offset, ntp_delay = get_ntp_offset("jp.pool.ntp.org")
    print(f"NTP offset: {ntp_offset:.2f} ms, NTP RTT delay: {ntp_delay:.2f} ms")

    print("Starting one-way latency measurement...")
    one_way = await measure_one_way(ntp_offset, NUM_ONE_WAY)
    summarize(one_way, "One-Way Latency")

    print("Plotting histogram for one-way latency...")
    plot_hist(one_way, "One-Way Latency")


if __name__ == "__main__":
    asyncio.run(main())