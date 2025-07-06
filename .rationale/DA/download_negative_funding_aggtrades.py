#!/usr/bin/env python

"""
Script to find severely negative funding rates (< -0.003) and download corresponding aggTrade data.
This script will:
1. Read all funding rate CSV files
2. Find entries with funding rates < -0.003 (very negative rates)
3. Download aggTrade data for those dates AND the previous day
4. Extract and organize the files by symbol/date structure
5. Merge all CSV files for each symbol into one large file
"""

import os
import sys
import csv
import zipfile
import subprocess
import shutil
import pandas as pd
import bisect
from datetime import datetime, timedelta
from pathlib import Path

# Add the binance-public-data-master directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'binance-public-data-master'))

from utility import download_file, get_path

# Threshold for severely negative funding rates
NEGATIVE_FUNDING_THRESHOLD = -0.003


def find_negative_funding_rates(funding_rate_dir, threshold=NEGATIVE_FUNDING_THRESHOLD):
    """Find all severely negative funding rates from CSV files."""
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
                            funding_datetime = datetime.strptime(funding_time_readable, '%Y-%m-%d %H:%M:%S')
                            funding_date = funding_datetime.strftime('%Y-%m-%d')

                            # Also get the previous day
                            previous_date = (funding_datetime - timedelta(days=1)).strftime('%Y-%m-%d')

                            negative_rates.append({
                                'symbol': symbol,
                                'funding_rate': funding_rate,
                                'funding_time': row['fundingTime'],  # Use the actual timestamp
                                'funding_time_readable': funding_time_readable,  # Keep readable for display
                                'date': funding_date,
                                'previous_date': previous_date
                            })

                            print(f"  ✅ Found severely negative rate: {funding_rate} on {funding_time_readable}")
                    except (ValueError, KeyError) as e:
                        print(f"  ⚠️  Error processing row in {symbol}: {e}")
                        continue

                print(f"  📊 {symbol}: {file_row_count} rows, rate range: {min_rate:.6f} to {max_rate:.6f}")

        except Exception as e:
            print(f"  ❌ Error reading {csv_file}: {str(e)}")

    print(f"\n📈 Summary:")
    print(f"  Files processed: {total_files_processed}")
    print(f"  Total rows processed: {total_rows_processed}")
    print(f"  Severely negative rates found: {len(negative_rates)}")

    if len(negative_rates) == 0:
        print(f"  💡 No rates found <= {threshold}. Consider adjusting the threshold.")
        print(f"     Current threshold: {NEGATIVE_FUNDING_THRESHOLD}")
        print(f"     Try a less strict threshold like -0.001 or -0.0005")

    return negative_rates


def extract_and_organize_zip_file(zip_path, symbol, date, base_extract_dir):
    """Extract a zip file and organize it by symbol/date structure."""
    try:
        # Create symbol directory
        symbol_dir = os.path.join(base_extract_dir, symbol)
        os.makedirs(symbol_dir, exist_ok=True)

        # Extract to temporary directory first
        temp_extract_dir = os.path.join(base_extract_dir, 'temp', f"{symbol}_{date}")
        os.makedirs(temp_extract_dir, exist_ok=True)

        # Use with statement to ensure proper file closure
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_extract_dir)
            print(f"  Extracted temporarily to: {temp_extract_dir}")

            # Move and rename CSV files to the symbol directory
            for file_name in zip_ref.namelist():
                if file_name.endswith('.csv'):
                    temp_file_path = os.path.join(temp_extract_dir, file_name)
                    final_file_path = os.path.join(symbol_dir, f"{date}.csv")

                    if os.path.exists(temp_file_path):
                        # If file already exists, skip or overwrite based on preference
                        if os.path.exists(final_file_path):
                            print(f"  File already exists, overwriting: {symbol}/{date}.csv")
                        shutil.move(temp_file_path, final_file_path)
                        print(f"  Organized: {symbol}/{date}.csv")
                    else:
                        print(f"  Warning: Expected CSV file not found: {temp_file_path}")

        # Clean up temporary directory
        shutil.rmtree(temp_extract_dir, ignore_errors=True)

        # Add a small delay before attempting to delete the zip file
        import time
        time.sleep(0.1)

        # Clean up the original zip file after successful extraction
        # Use a more robust deletion approach
        try:
            if os.path.exists(zip_path):
                os.remove(zip_path)
                print(f"  Cleaned up zip file: {zip_path}")
        except PermissionError:
            print(f"  Warning: Could not delete zip file (in use): {zip_path}")
            # Try to mark it for deletion on next reboot if needed
            try:
                import win32api
                win32api.MoveFileEx(zip_path, None, win32api.MOVEFILE_DELAY_UNTIL_REBOOT)
                print(f"  Scheduled zip file for deletion on reboot: {zip_path}")
            except ImportError:
                print(f"  Zip file will remain: {zip_path}")

    except Exception as e:
        print(f"  Error extracting {zip_path}: {str(e)}")


def download_aggtrades_for_negative_rates(negative_rates, output_dir):
    """Download aggTrade data for dates with severely negative funding rates."""
    os.makedirs(output_dir, exist_ok=True)

    # Group by symbol and date to avoid downloading the same file multiple times
    # Include both the funding date and previous date
    unique_downloads = {}
    for rate_data in negative_rates:
        symbol = rate_data['symbol']
        # Add both the funding date and previous date
        for date_key, date_value in [('date', rate_data['date']), ('previous_date', rate_data['previous_date'])]:
            key = f"{symbol}_{date_value}"
            if key not in unique_downloads:
                unique_downloads[key] = {
                    'symbol': symbol,
                    'date': date_value,
                    'funding_rate': rate_data['funding_rate'],
                    'funding_time': rate_data['funding_time'],
                    'is_previous_day': date_key == 'previous_date'
                }

    print(f"\nFound {len(unique_downloads)} unique symbol-date combinations to download")

    for i, (key, download_data) in enumerate(unique_downloads.items(), 1):
        symbol = download_data['symbol']
        date = download_data['date']
        day_type = "previous day" if download_data['is_previous_day'] else "funding day"

        print(f"\n[{i}/{len(unique_downloads)}] Downloading {symbol} aggTrades for {date} ({day_type})")

        try:
            # Use the download-aggTrade.py script approach
            # Format: SYMBOL-aggTrades-YYYY-MM-DD.zip
            file_name = f"{symbol}-aggTrades-{date}.zip"

            # Get the path for futures trading (um = USD-M futures)
            path = get_path("um", "aggTrades", "daily", symbol)

            # Download the file
            download_file(path, file_name, None, output_dir)

            # Extract and organize the zip file
            zip_path = os.path.join(output_dir, path, file_name)
            if os.path.exists(zip_path):
                extract_and_organize_zip_file(zip_path, symbol, date, output_dir)
            else:
                print(f"  Warning: Downloaded file not found at {zip_path}")

        except Exception as e:
            print(f"  Error downloading {symbol} for {date}: {str(e)}")


def merge_symbol_csv_files(output_dir):
    """Merge all CSV files for each symbol into one large file."""
    print(f"\nStep 3: Merging CSV files by symbol...")

    merged_dir = os.path.join(output_dir, "Merged")
    os.makedirs(merged_dir, exist_ok=True)

    # Get all symbol directories
    symbol_dirs = [d for d in os.listdir(output_dir)
                   if os.path.isdir(os.path.join(output_dir, d)) and d != "Merged" and d != "temp"]

    for symbol in symbol_dirs:
        symbol_path = os.path.join(output_dir, symbol)
        csv_files = [f for f in os.listdir(symbol_path) if f.endswith('.csv')]

        if not csv_files:
            print(f"  No CSV files found for {symbol}")
            continue

        print(f"  Merging {len(csv_files)} files for {symbol}...")

        # Read and combine all CSV files for this symbol
        all_dataframes = []

        for csv_file in sorted(csv_files):  # Sort to ensure consistent order
            csv_path = os.path.join(symbol_path, csv_file)
            try:
                # First, check if the CSV has headers by reading the first line
                with open(csv_path, 'r') as f:
                    first_line = f.readline().strip()

                # Check if first line looks like headers or data
                expected_headers = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'transact_time', 'is_buyer_maker']
                first_line_parts = first_line.split(',')

                # If first line doesn't contain expected headers, assume no header
                has_header = any(header in first_line.lower() for header in expected_headers)

                if has_header:
                    df = pd.read_csv(csv_path)
                    print(f"    Added {os.path.basename(csv_path)} ({len(df)} rows) - with headers")
                else:
                    # Read without header and assign column names
                    df = pd.read_csv(csv_path, header=None)
                    df.columns = expected_headers
                    print(f"    Added {os.path.basename(csv_path)} ({len(df)} rows) - added missing headers")

                # Verify we have the expected columns after loading
                if 'transact_time' not in df.columns:
                    print(f"    Warning: 'transact_time' column missing in {os.path.basename(csv_path)}")
                    continue

                all_dataframes.append(df)
            except Exception as e:
                print(f"    Error reading {csv_path}: {str(e)}")

        if all_dataframes:
            # Combine all dataframes
            merged_df = pd.concat(all_dataframes, ignore_index=True)

            # Sort by timestamp if available
            if 'T' in merged_df.columns:  # T is typically the timestamp column in aggTrades
                merged_df = merged_df.sort_values('T')

            # Save merged file
            merged_file_path = os.path.join(merged_dir, f"{symbol}_merged.csv")
            merged_df.to_csv(merged_file_path, index=False)
            print(f"    Saved merged file: {symbol}_merged.csv ({len(merged_df)} total rows)")

            # --- Filter for 10s before/after each negative funding event ---
            # Load negative funding events for this symbol
            negative_events = [e for e in negative_rates if e['symbol'] == symbol]
            for event in negative_events:
                funding_time = event['funding_time']
                # Convert funding_time to timestamp in ms
                funding_dt = datetime.strptime(funding_time, '%Y-%m-%d %H:%M:%S')
                funding_ts_ms = int(funding_dt.timestamp() * 1000)
                window_start = funding_ts_ms - 10_000
                window_end = funding_ts_ms + 10_000
                if 'T' in merged_df.columns:
                    filtered = merged_df[(merged_df['T'] >= window_start) & (merged_df['T'] <= window_end)]
                    if not filtered.empty:
                        outname = f"{symbol}_{funding_time.replace(':','-').replace(' ','_')}_window.csv"
                        outpath = os.path.join(merged_dir, outname)
                        filtered.to_csv(outpath, index=False)
                        print(f"    Saved filtered window: {outname} ({len(filtered)} rows)")
                    else:
                        print(f"    No aggTrades found in window for {symbol} at {funding_time}")
    # --- New code starts here ---
def process_negative_funding_events(negative_rates, output_dir):
    """Process each negative funding event: download, merge, filter, cleanup."""
    merged_dir = os.path.join(output_dir, "Merged")
    os.makedirs(merged_dir, exist_ok=True)
    for i, event in enumerate(negative_rates, 1):
        symbol = event['symbol']
        funding_time = event['funding_time']
        funding_dt = datetime.strptime(funding_time, '%Y-%m-%d %H:%M:%S')
        funding_ts_ms = int(funding_dt.timestamp() * 1000)
        window_start = funding_ts_ms - 10_000
        window_end = funding_ts_ms + 10_000
        for date in [event['date'], event['previous_date']]:
            print(f"\n[{i}] Processing {symbol} {date} for funding event at {funding_time}")
            # Download and extract aggTrade data for this symbol/date
            file_name = f"{symbol}-aggTrades-{date}.zip"
            path = get_path("um", "aggTrades", "daily", symbol)
            try:
                download_file(path, file_name, None, output_dir)
                zip_path = os.path.join(output_dir, path, file_name)
                if os.path.exists(zip_path):
                    extract_and_organize_zip_file(zip_path, symbol, date, output_dir)
                else:
                    print(f"  Warning: Downloaded file not found at {zip_path}")
            except Exception as e:
                print(f"  Error downloading {symbol} for {date}: {str(e)}")
                continue
            # Merge CSVs for this symbol/date
            symbol_path = os.path.join(output_dir, symbol)
            csv_path = os.path.join(symbol_path, f"{date}.csv")
            if not os.path.exists(csv_path):
                print(f"  No CSV file found for {symbol} {date}")
                continue
            try:
                df = pd.read_csv(csv_path)
                if 'T' not in df.columns:
                    print(f"  No 'T' column in {csv_path}")
                    continue
                # Use bisect to find window efficiently
                timestamps = df['T'].values
                left = bisect.bisect_left(timestamps, window_start)
                right = bisect.bisect_right(timestamps, window_end)
                filtered = df.iloc[left:right]
                if not filtered.empty:
                    outname = f"{symbol}_{funding_time.replace(':','-').replace(' ','_')}_{date}_window.csv"
                    outpath = os.path.join(merged_dir, outname)
                    filtered.to_csv(outpath, index=False)
                    print(f"    Saved filtered window: {outname} ({len(filtered)} rows)")
                else:
                    print(f"    No aggTrades found in window for {symbol} at {funding_time} on {date}")
            except Exception as e:
                print(f"  Error processing {csv_path}: {str(e)}")
            # Clean up CSV to save space
            try:
                os.remove(csv_path)
                print(f"  Deleted intermediate file: {csv_path}")
            except Exception as e:
                print(f"  Could not delete {csv_path}: {str(e)}")
    # --- New code ends here ---
# --- New code starts here ---
def process_all_pairs_negative_funding(negative_rates, output_dir):
    """For each symbol, download all severe dates, merge, bisect for all intervals, then cleanup."""
    import bisect
    from collections import defaultdict

    symbol_events = defaultdict(list)
    for event in negative_rates:
        symbol_events[event['symbol']].append(event)

    for symbol, events in symbol_events.items():
        print(f"\nProcessing symbol: {symbol} ({len(events)} severe events)")

        # Create symbol-specific directory structure
        symbol_output_dir = os.path.join(output_dir, symbol)
        os.makedirs(symbol_output_dir, exist_ok=True)
        print(f"  Created output directory: {symbol_output_dir}")

        # Collect all unique dates (including previous days)
        all_dates = set()
        for e in events:
            all_dates.add(e['date'])
            all_dates.add(e['previous_date'])

        print(f"  Need to download {len(all_dates)} unique dates: {sorted(all_dates)}")

        # Download and extract all relevant aggTrade data for this symbol
        for i, date in enumerate(sorted(all_dates), 1):
            print(f"  [{i}/{len(all_dates)}] Downloading {symbol} for {date}...")
            file_name = f"{symbol}-aggTrades-{date}.zip"
            path = get_path("um", "aggTrades", "daily", symbol)
            try:
                download_file(path, file_name, None, output_dir)
                zip_path = os.path.join(output_dir, path, file_name)
                if os.path.exists(zip_path):
                    extract_and_organize_zip_file(zip_path, symbol, date, output_dir)
                    print(f"    ✅ Successfully downloaded and extracted {date}")
                else:
                    print(f"    ⚠️ WARNING: Downloaded file not found at {zip_path}")
            except Exception as e:
                print(f"    ❌ ERROR: downloading {symbol} for {date}: {str(e)}")

        # Merge all CSVs for this symbol
        symbol_path = os.path.join(output_dir, symbol)
        csv_files = [os.path.join(symbol_path, f) for f in os.listdir(symbol_path) if f.endswith('.csv')]
        if not csv_files:
            print(f"  ❌ ERROR: No CSV files found for {symbol}")
            continue

        print(f"  Merging {len(csv_files)} CSV files...")
        all_dfs = []
        for csv_path in sorted(csv_files):
            try:
                # First, check if the CSV has headers by reading the first line
                with open(csv_path, 'r') as f:
                    first_line = f.readline().strip()

                # Check if first line looks like headers or data
                expected_headers = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'transact_time', 'is_buyer_maker']

                # If first line doesn't contain expected headers, assume no header
                has_header = any(header in first_line.lower() for header in expected_headers)

                if has_header:
                    df = pd.read_csv(csv_path)
                    print(f"    ✅ Added {os.path.basename(csv_path)} ({len(df):,} rows) - with headers")
                else:
                    # Read without header and assign column names
                    df = pd.read_csv(csv_path, header=None)
                    df.columns = expected_headers
                    print(f"    ✅ Added {os.path.basename(csv_path)} ({len(df):,} rows) - added missing headers")

                # Verify we have the expected columns after loading
                if 'transact_time' not in df.columns:
                    print(f"    ⚠️ WARNING: 'transact_time' column missing in {os.path.basename(csv_path)}")
                    continue

                all_dfs.append(df)
            except Exception as e:
                print(f"    ❌ ERROR: reading {csv_path}: {str(e)}")

        if not all_dfs:
            print(f"  ❌ ERROR: No valid data for {symbol}")
            continue

        # Check total rows before merging to handle large files
        total_rows = sum(len(df) for df in all_dfs)
        print(f"  Total rows to merge: {total_rows:,}")

        # For very large datasets, use chunked processing
        if total_rows > 50_000_000:  # 50M rows threshold
            print(f"  Large dataset detected ({total_rows:,} rows). Using chunked processing...")
            # Create temporary merged file
            temp_merged_path = os.path.join(symbol_output_dir, f"temp_merged.csv")

            # Write first chunk with header
            first_df = all_dfs[0]
            if 'transact_time' not in first_df.columns:
                print(f"  ❌ ERROR: No 'transact_time' column in aggTrades for {symbol}")
                continue
            first_df = first_df.sort_values('transact_time')
            first_df.to_csv(temp_merged_path, index=False, mode='w')
            print(f"    ✅ Wrote chunk 1/{len(all_dfs)}: {len(first_df):,} rows")

            # Append remaining chunks without header
            for i, df in enumerate(all_dfs[1:], 2):
                if 'transact_time' in df.columns:
                    df = df.sort_values('transact_time')
                    df.to_csv(temp_merged_path, index=False, mode='a', header=False)
                    print(f"    ✅ Wrote chunk {i}/{len(all_dfs)}: {len(df):,} rows")

            print(f"  ✅ Created temporary merged file ({total_rows:,} rows)")

            # For large files, process filtering in chunks too
            print(f"  Processing large file filtering in chunks...")
            filtered_all = []
            chunk_size = 1_000_000  # 1M rows per chunk

            for j, event in enumerate(events, 1):
                funding_time = event['funding_time']
                funding_time_readable = event['funding_time_readable']
                print(f"    [{j}/{len(events)}] Processing funding event {funding_time_readable}...")
                # Use fundingTime directly (should be timestamp in ms)
                funding_ts_ms = int(event['funding_time'])  # Use the timestamp directly
                window_start = funding_ts_ms - 10_000
                window_end = funding_ts_ms + 10_000

                event_filtered = []
                # Read and filter in chunks
                for chunk in pd.read_csv(temp_merged_path, chunksize=chunk_size):
                    if 'transact_time' in chunk.columns:
                        chunk_filtered = chunk[(chunk['transact_time'] >= window_start) &
                                             (chunk['transact_time'] <= window_end)]
                        if not chunk_filtered.empty:
                            event_filtered.append(chunk_filtered)

                if event_filtered:
                    event_df = pd.concat(event_filtered, ignore_index=True)
                    # Save individual windowed file to symbol directory
                    window_filename = f"{symbol}_{funding_time}_window.csv"
                    window_path = os.path.join(symbol_output_dir, window_filename)
                    event_df.to_csv(window_path, index=False)
                    print(f"      ✅ Funding event {funding_time_readable}: {len(event_df):,} rows found, saved to {window_filename}")
                else:
                    print(f"      ⚠️ WARNING: Funding event {funding_time_readable}: 0 rows found (no trading activity in ±10s window)")

            # Delete temporary merged file
            try:
                os.remove(temp_merged_path)
                print(f"  ✅ Deleted temporary merged file")
            except Exception:
                pass

        else:
            # Normal processing for smaller datasets
            merged_df = pd.concat(all_dfs, ignore_index=True)
            if 'transact_time' not in merged_df.columns:
                print(f"  ❌ ERROR: No 'transact_time' column in merged aggTrades for {symbol}")
                continue
            merged_df = merged_df.sort_values('transact_time')
            print(f"  ✅ Merged {len(merged_df):,} rows for {symbol} from {len(csv_files)} files.")

            # For each event, bisect to find the window and save individual files
            timestamps = merged_df['transact_time'].values
            print(f"  Processing {len(events)} funding events...")

            for j, event in enumerate(events, 1):
                funding_time = event['funding_time']
                funding_time_readable = event['funding_time_readable']
                print(f"    [{j}/{len(events)}] Processing funding event {funding_time_readable}...")
                # Use fundingTime directly (should be timestamp in ms)
                funding_ts_ms = int(event['funding_time'])
                window_start = funding_ts_ms - 10_000
                window_end = funding_ts_ms + 10_000
                left = bisect.bisect_left(timestamps, window_start)
                right = bisect.bisect_right(timestamps, window_end)
                filtered = merged_df.iloc[left:right]

                if not filtered.empty:
                    # Save individual windowed file to symbol directory
                    window_filename = f"{symbol}_{funding_time}_window.csv"
                    window_path = os.path.join(symbol_output_dir, window_filename)
                    filtered.to_csv(window_path, index=False)
                    print(f"      ✅ Funding event {funding_time_readable}: {len(filtered):,} rows in window, saved to {window_filename}")
                else:
                    print(f"      ⚠️ WARNING: Funding event {funding_time_readable}: 0 rows in window - NO TRADING ACTIVITY")

        # Clean up all intermediate CSVs for this symbol (only keep windowed files)
        print(f"  Cleaning up intermediate files...")
        deleted_count = 0
        for csv_path in csv_files:
            try:
                os.remove(csv_path)
                deleted_count += 1
                print(f"    ✅ Deleted raw file: {os.path.basename(csv_path)}")
            except Exception:
                print(f"    ⚠️ WARNING: Could not delete {os.path.basename(csv_path)}")

        # Also clean up the symbol directory (should be empty now except for windowed files)
        try:
            if os.path.exists(symbol_path) and not os.listdir(symbol_path):
                os.rmdir(symbol_path)
                print(f"  ✅ Removed empty intermediate directory")
        except Exception:
            pass

        print(f"  ✅ Completed {symbol}: windowed files saved to {symbol_output_dir}")
        print(f"  Deleted {deleted_count} intermediate files, kept windowed results only")
        print(f"  {'='*60}")
# --- New code ends here ---

def main():
    """Main function to execute the negative funding rate analysis and download."""
    # Set up directories
    script_dir = os.path.dirname(os.path.abspath(__file__))
    funding_rate_dir = os.path.join(script_dir, "Funding Rate History")
    output_dir = os.path.join(script_dir, "Negative Funding AggTrades")

    print("=" * 60)
    print("SEVERELY NEGATIVE FUNDING RATE AGGTRADES DOWNLOADER")
    print("=" * 60)
    print(f"Threshold: {NEGATIVE_FUNDING_THRESHOLD}")
    print(f"Source directory: {funding_rate_dir}")
    print(f"Output directory: {output_dir}")
    print("=" * 60)

    # Check if funding rate directory exists
    if not os.path.exists(funding_rate_dir):
        print(f"Error: Funding rate directory not found: {funding_rate_dir}")
        return

    # Find all severely negative funding rates
    print("\nStep 1: Finding severely negative funding rates...")
    negative_rates = find_negative_funding_rates(funding_rate_dir)

    if not negative_rates:
        print("No severely negative funding rates found!")
        return

    print(f"\nFound {len(negative_rates)} severely negative funding rate instances!")

    # Show summary by symbol
    symbol_counts = {}
    for rate in negative_rates:
        symbol = rate['symbol']
        symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1

    print(f"\nSummary by symbol:")
    for symbol, count in sorted(symbol_counts.items()):
        print(f"  {symbol}: {count} instances")

    # --- Only use the new per-symbol logic ---
    print(f"\nStep 2: Processing all pairs of negative funding events...")
    process_all_pairs_negative_funding(negative_rates, output_dir)

    print(f"\nCompleted! Check the files in:")
    print(f"  Merged files: {os.path.join(output_dir, 'Merged')}")
    print("File structure:")
    print("  Merged: Merged/{symbol}_merged.csv")


if __name__ == "__main__":
    main()
