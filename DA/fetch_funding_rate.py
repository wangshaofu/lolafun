import configparser
from binance.um_futures import UMFutures
from datetime import datetime
import pprint
import os
import csv
import time


def date_to_milliseconds(date):
    """Convert a date string to milliseconds since epoch."""
    dt = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    return int(dt.timestamp() * 1000)


def load_config(path='../config.ini'):
    config = configparser.ConfigParser()
    config.read(path)
    return config


def fetch_history_funding_rate(pair, start_time=None, end_time=None, limit=1000):
    # split to loop for every month until end_time, then combine the results
    full_funding_rate_list = []
    if start_time is None:  # default is today
        today = datetime.now()
        start_time = date_to_milliseconds(today.strftime("%Y-%m-%d 00:00:00"))
    else:
        start_time = date_to_milliseconds(start_time)

    if end_time is None:  # default is current time
        end_time = int(datetime.now().timestamp() * 1000)
    else:
        end_time = date_to_milliseconds(end_time)

    while start_time < end_time:
        # Calculate the end of current batch (either next month or the final end_time)
        batch_end_time = min(start_time + 30 * 24 * 60 * 60 * 1000, end_time)

        funding_rate_list = um_futures_client.funding_rate(
            symbol=pair,
            startTime=start_time,
            endTime=batch_end_time,
            limit=limit
        )
        full_funding_rate_list.extend(funding_rate_list)
        start_time = batch_end_time  # Move to the next batch

        # Add a small delay to avoid rate limiting
        time.sleep(0.1)

    return full_funding_rate_list


def save_funding_rates_to_csv(symbol: str, funding_rates: list):
    # Ensure subdirectory exists
    subdir = "Funding Rate History"
    os.makedirs(subdir, exist_ok=True)

    # Define filename and path
    filename = f"{symbol}.csv"
    filepath = os.path.join(subdir, filename)

    # Write headers and rows
    with open(filepath, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['symbol', 'fundingRate', 'fundingTime', 'fundingTimeReadable', 'markPrice']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for entry in funding_rates:
            writer.writerow({
                'symbol': entry['symbol'],
                'fundingRate': entry['fundingRate'],
                'fundingTime': entry['fundingTime'],
                'fundingTimeReadable': datetime.utcfromtimestamp(entry['fundingTime'] // 1000).strftime(
                    '%Y-%m-%d %H:%M:%S'),
                'markPrice': entry['markPrice']
            })

    print(f"Saved funding rate history for {symbol} to {filepath}")


if __name__ == "__main__":
    config = load_config()
    um_futures_client = UMFutures(key=config['ACCOUNT']['APIKey'], secret=config['ACCOUNT']['APISecret'])
    exchange_info = um_futures_client.exchange_info()
    symbols = [info['symbol'] for info in exchange_info['symbols']]
    for symbol in symbols:
        print(f"Fetching funding rates for {symbol}...")
        funding_rates = fetch_history_funding_rate(
            pair=symbol,
            start_time='2025-01-01 00:00:00',
            end_time='2025-03-01 00:00:00'
        )
        save_funding_rates_to_csv(symbol, funding_rates)
        time.sleep(0.33)
    print("Funding rate history fetching completed.")
    # print(um_futures_client.ping())
    # print(funding_rates)
    # print(um_futures_client.agg_trades("BTCUSDT", limit=10))
