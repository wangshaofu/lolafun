import configparser
from binance.um_futures import UMFutures
from datetime import datetime, timedelta
import pprint
import os
import csv
import time


def load_config(path='../config.ini'):
    config = configparser.ConfigParser()
    config.read(path)
    return config


def find_negative_funding_rates(funding_rate_dir, threshold=-0.003):
    """Find all severely negative funding rates from existing CSV files."""
    negative_rates = []
    total_files_processed = 0
    total_rows_processed = 0

    print(f"Looking for funding rates <= {threshold}")
    print(f"Scanning directory: {funding_rate_dir}")

    if not os.path.exists(funding_rate_dir):
        print(f"ERROR: Directory does not exist: {funding_rate_dir}")
        return negative_rates

    csv_files = [f for f in os.listdir(funding_rate_dir) if f.endswith('.csv')]
    print(f"Found {len(csv_files)} CSV files to process")

    for csv_file in csv_files:
        symbol = csv_file.replace('.csv', '')
        filepath = os.path.join(funding_rate_dir, csv_file)
        total_files_processed += 1

        print(f"Processing {symbol}... ({total_files_processed}/{len(csv_files)})")

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                file_row_count = 0
                min_rate = float('inf')
                max_rate = float('-inf')

                for row in reader:
                    file_row_count += 1
                    total_rows_processed += 1

                    try:
                        funding_rate = float(row['fundingRate'])
                        min_rate = min(min_rate, funding_rate)
                        max_rate = max(max_rate, funding_rate)

                        if funding_rate <= threshold:
                            # Parse the funding time
                            funding_time_readable = row['fundingTimeReadable']

                            negative_rates.append({
                                'symbol': symbol,
                                'fundingRate': funding_rate,
                                'fundingTime': row['fundingTime'],
                                'fundingTimeReadable': funding_time_readable,
                                'markPrice': row['markPrice']
                            })

                            print(f"  ✅ Found negative rate: {funding_rate:.6f} ({funding_rate * 100:.4f}%) on {funding_time_readable}")
                    except (ValueError, KeyError) as e:
                        print(f"  ⚠️  Error processing row in {symbol}: {e}")
                        continue

                print(f"  📊 {symbol}: {file_row_count} rows, rate range: {min_rate:.6f} to {max_rate:.6f}")

        except Exception as e:
            print(f"  ❌ Error reading {csv_file}: {str(e)}")

    print(f"\n📈 Summary:")
    print(f"  Files processed: {total_files_processed}")
    print(f"  Total rows processed: {total_rows_processed}")
    print(f"  Negative rates found: {len(negative_rates)}")

    if len(negative_rates) == 0:
        print(f"  💡 No rates found <= {threshold}. Consider adjusting the threshold.")
        print(f"     Current threshold: {threshold}")
        print(f"     Try a less strict threshold like -0.001 or -0.0005")

    return negative_rates


def fetch_4h_kline_data(client, symbol, limit=24):
    """Fetch 4-hour kline data for a symbol (default 24 periods = 4 days)."""
    try:
        klines = client.klines(
            symbol=symbol,
            interval='4h',
            limit=limit
        )

        processed_data = []
        for kline in klines:
            processed_data.append({
                'symbol': symbol,
                'openTime': kline[0],
                'openTimeReadable': datetime.utcfromtimestamp(kline[0] // 1000).strftime('%Y-%m-%d %H:%M:%S'),
                'open': float(kline[1]),
                'high': float(kline[2]),
                'low': float(kline[3]),
                'close': float(kline[4]),
                'volume': float(kline[5]),
                'closeTime': kline[6],
                'quoteAssetVolume': float(kline[7]),
                'numberOfTrades': int(kline[8]),
                'takerBuyBaseAssetVolume': float(kline[9]),
                'takerBuyQuoteAssetVolume': float(kline[10])
            })

        return processed_data
    except Exception as e:
        print(f"Error fetching kline data for {symbol}: {e}")
        return []


def fetch_historical_4h_kline_data(client, symbol, funding_timestamp):
    """Fetch the single 4-hour kline that occurred immediately before the funding timestamp."""
    try:
        # Convert funding timestamp to milliseconds (it should already be in ms from CSV)
        funding_ts_ms = int(funding_timestamp)

        # Calculate end time (just before the funding rate)
        end_time = funding_ts_ms - 1  # 1ms before the funding rate

        # Debug output with readable time for human understanding
        readable_time = datetime.utcfromtimestamp(funding_ts_ms // 1000).strftime('%Y-%m-%d %H:%M:%S')
        print(f"    Fetching single 4h kline before timestamp: {funding_ts_ms} ({readable_time} UTC)")

        # Fetch only 1 kline ending at that time
        klines = client.klines(
            symbol=symbol,
            interval='4h',
            endTime=end_time,
            limit=1  # Only fetch the single candle before funding
        )

        if not klines:
            print(f"    No kline data found for {symbol} before {readable_time}")
            return None

        # Process the single kline
        kline = klines[0]
        processed_data = {
            'symbol': symbol,
            'openTime': kline[0],
            'openTimeReadable': datetime.utcfromtimestamp(kline[0] // 1000).strftime('%Y-%m-%d %H:%M:%S'),
            'open': float(kline[1]),
            'high': float(kline[2]),
            'low': float(kline[3]),
            'close': float(kline[4]),
            'volume': float(kline[5]),
            'closeTime': kline[6],
            'closeTimeReadable': datetime.utcfromtimestamp(kline[6] // 1000).strftime('%Y-%m-%d %H:%M:%S'),
            'quoteAssetVolume': float(kline[7]),
            'numberOfTrades': int(kline[8]),
            'takerBuyBaseAssetVolume': float(kline[9]),
            'takerBuyQuoteAssetVolume': float(kline[10])
        }

        return processed_data
    except Exception as e:
        print(f"Error fetching historical kline data for {symbol}: {e}")
        return None


def save_trading_volume_to_csv(symbol, kline_data, funding_events):
    """Save trading volume data to CSV file with funding rate info."""
    # Ensure subdirectory exists
    subdir = "Trading Volume History"
    os.makedirs(subdir, exist_ok=True)

    # Define filename and path
    filename = f"{symbol}_4h_volume.csv"
    filepath = os.path.join(subdir, filename)

    # Write headers and rows
    with open(filepath, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'symbol', 'negativeFundingCount', 'minFundingRate', 'avgFundingRate',
            'openTime', 'openTimeReadable', 'open', 'high', 'low', 'close',
            'volume', 'closeTime', 'quoteAssetVolume', 'numberOfTrades',
            'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        # Calculate funding stats
        funding_rates = [event['fundingRate'] for event in funding_events]
        min_funding = min(funding_rates)
        avg_funding = sum(funding_rates) / len(funding_rates)

        for entry in kline_data:
            row = entry.copy()
            row['negativeFundingCount'] = len(funding_events)
            row['minFundingRate'] = min_funding
            row['avgFundingRate'] = avg_funding
            writer.writerow(row)

    print(f"Saved 4h trading volume for {symbol} ({len(funding_events)} negative events, min rate: {min_funding:.6f}) to {filepath}")


def save_summary_to_csv(symbol_funding_groups):
    """Save summary of all negative funding pairs to a single CSV."""
    subdir = "Trading Volume History"
    os.makedirs(subdir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"negative_funding_summary_{timestamp}.csv"
    filepath = os.path.join(subdir, filename)

    with open(filepath, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['symbol', 'negativeEventCount', 'minFundingRate', 'avgFundingRate',
                     'minFundingRatePercent', 'avgFundingRatePercent', 'latestNegativeDate']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for symbol, events in symbol_funding_groups.items():
            funding_rates = [event['fundingRate'] for event in events]
            min_rate = min(funding_rates)
            avg_rate = sum(funding_rates) / len(funding_rates)
            latest_date = max([event['fundingTimeReadable'] for event in events])

            writer.writerow({
                'symbol': symbol,
                'negativeEventCount': len(events),
                'minFundingRate': min_rate,
                'avgFundingRate': avg_rate,
                'minFundingRatePercent': f"{min_rate * 100:.4f}%",
                'avgFundingRatePercent': f"{avg_rate * 100:.4f}%",
                'latestNegativeDate': latest_date
            })

    print(f"Saved summary of negative funding pairs to {filepath}")


def save_historical_trading_volume_to_csv(symbol, all_kline_data):
    """Save historical trading volume data for all funding events of a symbol."""
    # Ensure subdirectory exists
    subdir = "Trading Volume History"
    os.makedirs(subdir, exist_ok=True)

    # Define filename and path
    filename = f"{symbol}_historical_4h_volume.csv"
    filepath = os.path.join(subdir, filename)

    # Write headers and rows
    with open(filepath, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'symbol', 'fundingRate', 'fundingTime', 'fundingTimeReadable', 'negativeFundingCount',
            'openTime', 'openTimeReadable', 'closeTime', 'closeTimeReadable',
            'open', 'high', 'low', 'close', 'volume', 'quoteAssetVolume',
            'numberOfTrades', 'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for kline in all_kline_data:
            writer.writerow(kline)

    print(f"Saved historical 4h trading volume for {symbol} ({len(all_kline_data)} kline records) to {filepath}")


if __name__ == "__main__":
    # Set up directories
    script_dir = os.path.dirname(os.path.abspath(__file__))
    funding_rate_dir = os.path.join(script_dir, "Funding Rate History")

    print("=" * 60)
    print("NEGATIVE FUNDING RATE TRADING VOLUME ANALYZER")
    print("=" * 60)

    # Check if funding rate directory exists
    if not os.path.exists(funding_rate_dir):
        print(f"Error: Funding rate directory not found: {funding_rate_dir}")
        exit()

    # Find all negative funding rates from existing CSV files
    print("\nStep 1: Finding negative funding rates from existing CSV files...")
    threshold = -0.003  # -0.3%
    negative_rates = find_negative_funding_rates(funding_rate_dir, threshold)

    if not negative_rates:
        print("No negative funding rates found!")
        exit()

    print(f"\nFound {len(negative_rates)} negative funding rate instances!")

    # Group by symbol
    symbol_funding_groups = {}
    for rate in negative_rates:
        symbol = rate['symbol']
        if symbol not in symbol_funding_groups:
            symbol_funding_groups[symbol] = []
        symbol_funding_groups[symbol].append(rate)

    print(f"\nSummary by symbol:")
    for symbol, events in sorted(symbol_funding_groups.items()):
        min_rate = min([event['fundingRate'] for event in events])
        print(f"  {symbol}: {len(events)} events (min rate: {min_rate * 100:.4f}%)")

    # Save summary
    save_summary_to_csv(symbol_funding_groups)

    # Set up Binance client for fetching trading volume
    config = load_config()
    um_futures_client = UMFutures(key=config['ACCOUNT']['APIKey'], secret=config['ACCOUNT']['APISecret'])

    # Fetch 4-hour trading volume for each symbol with negative funding
    print(f"\nStep 2: Fetching historical 4-hour trading volume data for {len(symbol_funding_groups)} symbols...")
    for i, (symbol, events) in enumerate(symbol_funding_groups.items(), 1):
        print(f"\nProcessing [{i}/{len(symbol_funding_groups)}]: {symbol} ({len(events)} negative events)")

        # Process each funding event individually to get historical data
        all_kline_data = []
        for j, event in enumerate(events, 1):
            funding_time = event['fundingTime']
            funding_time_readable = event['fundingTimeReadable']
            funding_rate = event['fundingRate']

            print(f"  [{j}/{len(events)}] Processing funding event {funding_time_readable} (rate: {funding_rate * 100:.4f}%)")

            # Fetch single 4h kline before this specific funding event
            historical_kline = fetch_historical_4h_kline_data(um_futures_client, symbol, funding_time)

            if historical_kline:
                # Add funding event info to the kline
                historical_kline['fundingRate'] = funding_rate
                historical_kline['fundingTime'] = funding_time
                historical_kline['fundingTimeReadable'] = funding_time_readable
                historical_kline['negativeFundingCount'] = len(events)

                all_kline_data.append(historical_kline)
                print(f"    Found 4h kline: {historical_kline['volume']:,.0f} volume from {historical_kline['openTimeReadable']} to {historical_kline['closeTimeReadable']}")
            else:
                print(f"    No historical data found for funding event {funding_time_readable}")

            # Rate limiting
            time.sleep(0.5)

        if all_kline_data:
            save_historical_trading_volume_to_csv(symbol, all_kline_data)

            # Show summary of volumes
            total_volume = sum(kline['volume'] for kline in all_kline_data)
            avg_volume = total_volume / len(all_kline_data)
            print(f"  Summary: {len(all_kline_data)} events, average volume per 4h period: {avg_volume:,.2f} {symbol.replace('USDT', '').replace('USDC', '')}")
        else:
            print(f"  ❌ No historical trading volume data found for {symbol}")
