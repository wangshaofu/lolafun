import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error
import numpy as np
import os
import glob
from scipy import stats
import csv

# --- Configuration ---
# Directories
script_dir = os.path.dirname(os.path.abspath(__file__))
aggTrades_dir = os.path.join(script_dir, 'Negative Funding AggTrades')
fundingRate_dir = os.path.join(script_dir, 'Funding Rate History')
output_file = os.path.join(os.path.dirname(script_dir), 'fundingrate_vs_delta.csv')

# Time window in seconds to look for max price change after settlement
time_window_seconds = 10


def get_funding_rate(symbol, settlement_timestamp_ms):
    """
    Get the funding rate by finding the closest fundingTime to settlement timestamp
    Uses a flexible matching approach to handle slight timestamp differences
    """
    funding_file = os.path.join(fundingRate_dir, f"{symbol}.csv")

    if not os.path.exists(funding_file):
        print(f"Funding rate file not found for {symbol}")
        return None

    try:
        funding_df = pd.read_csv(funding_file)

        # Convert fundingTime to numeric for calculations
        funding_df['fundingTime'] = pd.to_numeric(funding_df['fundingTime'], errors='coerce')

        # Remove any rows with invalid timestamps
        funding_df = funding_df.dropna(subset=['fundingTime'])

        if funding_df.empty:
            print(f"No valid funding time data for {symbol}")
            return None

        # Method 1: Try exact prefix match first (original method)
        settlement_prefix = str(settlement_timestamp_ms)[:9]
        exact_matches = funding_df[funding_df['fundingTime'].astype(str).str[:9] == settlement_prefix]

        if not exact_matches.empty:
            funding_rate = exact_matches.iloc[0]['fundingRate'] * 100
            return funding_rate

        # Method 2: Find the closest timestamp within a reasonable range
        # Allow up to 3 minutes (180 seconds = 180000 ms) difference
        tolerance_ms = 3 * 60 * 1000  # 3 minutes

        # Calculate absolute differences
        funding_df['time_diff'] = abs(funding_df['fundingTime'] - settlement_timestamp_ms)

        # Filter within tolerance
        within_tolerance = funding_df[funding_df['time_diff'] <= tolerance_ms]

        if within_tolerance.empty:
            # Method 3: If no close match, try broader prefix matching
            # Try 8 digits, then 7 digits
            for prefix_len in [8, 7]:
                settlement_prefix_broad = str(settlement_timestamp_ms)[:prefix_len]
                broad_matches = funding_df[
                    funding_df['fundingTime'].astype(str).str[:prefix_len] == settlement_prefix_broad]

                if not broad_matches.empty:
                    # Find the closest one within these broader matches
                    broad_matches['time_diff'] = abs(broad_matches['fundingTime'] - settlement_timestamp_ms)
                    closest_match = broad_matches.loc[broad_matches['time_diff'].idxmin()]
                    funding_rate = closest_match['fundingRate'] * 100
                    print(
                        f"Found funding rate for {symbol} using {prefix_len}-digit prefix matching (diff: {closest_match['time_diff'] / 1000:.1f}s)")
                    return funding_rate

            print(
                f"No funding rate data within reasonable time range for {symbol} (settlement: {settlement_timestamp_ms})")
            return None

        # Find the closest timestamp within tolerance
        closest_match = within_tolerance.loc[within_tolerance['time_diff'].idxmin()]
        funding_rate = closest_match['fundingRate'] * 100

        # Log if the difference is significant (more than 1 minute)
        if closest_match['time_diff'] > 1 * 60 * 1000:
            print(f"Warning: Large time difference for {symbol}: {closest_match['time_diff'] / 1000:.1f} seconds")

        return funding_rate

    except Exception as e:
        print(f"Error loading funding rate for {symbol}: {e}")
        return None


def find_severely_negative_funding_rates(funding_rate_dir, threshold=-0.003):
    """
    Find all severely negative funding rates from existing CSV files.
    Returns a list of funding events with timestamps.
    """
    negative_funding_events = []
    total_files_processed = 0

    print(f"Looking for funding rates <= {threshold * 100:.1f}%")
    print(f"Scanning directory: {funding_rate_dir}")

    if not os.path.exists(funding_rate_dir):
        print(f"ERROR: Directory does not exist: {funding_rate_dir}")
        return negative_funding_events

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
                for row in reader:
                    try:
                        funding_rate = float(row['fundingRate'])
                        if funding_rate <= threshold:
                            funding_time = int(row['fundingTime'])
                            funding_time_readable = row['fundingTimeReadable']

                            negative_funding_events.append({
                                'symbol': symbol,
                                'fundingRate': funding_rate,
                                'fundingTime': funding_time,
                                'fundingTimeReadable': funding_time_readable,
                                'markPrice': row['markPrice']
                            })

                            print(f"  ✅ Found: {symbol} rate={funding_rate * 100:.4f}% on {funding_time_readable}")
                    except (ValueError, KeyError) as e:
                        continue

        except Exception as e:
            print(f"  ❌ Error reading {csv_file}: {str(e)}")

    print(f"\n📈 Summary:")
    print(f"  Files processed: {total_files_processed}")
    print(f"  Negative funding events found: {len(negative_funding_events)}")

    return negative_funding_events


def find_corresponding_trading_data(symbol, funding_timestamp):
    """
    Find the corresponding trading data file for a specific symbol and timestamp.
    """
    symbol_dir = os.path.join(aggTrades_dir, symbol)
    if not os.path.exists(symbol_dir):
        return None

    # Look for files in the symbol directory that match the timestamp
    for file in os.listdir(symbol_dir):
        if file.endswith('_window.csv') and str(funding_timestamp) in file:
            return os.path.join(symbol_dir, file)

    return None


def analyze_price_drop_after_funding(funding_event, delay_ms=0):
    """
    Analyze price changes in the 10 seconds after a funding rate timestamp.
    Now calculates both max drop and max increase with configurable delay simulation.

    Args:
        funding_event: Dict containing funding rate event data
        delay_ms: Delay in milliseconds after funding timestamp to start analysis (default: 0)
    """
    symbol = funding_event['symbol']
    funding_timestamp = funding_event['fundingTime']
    funding_rate = funding_event['fundingRate']

    # Find corresponding trading data file
    trading_file = find_corresponding_trading_data(symbol, funding_timestamp)

    if not trading_file:
        print(f"No trading data found for {symbol} at {funding_timestamp}")
        return None

    try:
        df = pd.read_csv(trading_file)
        if delay_ms == 0:  # Only print for the first run to avoid spam
            print(f"Analyzing {symbol} funding rate {funding_rate * 100:.4f}% at {funding_event['fundingTimeReadable']}")
    except Exception as e:
        print(f"Error loading trading data for {symbol}: {e}")
        return None

    # Convert timestamps for analysis
    df['time'] = pd.to_datetime(df['transact_time'], unit='ms')
    funding_time = pd.to_datetime(funding_timestamp, unit='ms')

    # Define the 10-second window after funding timestamp [t, t+10000ms]
    window_start = funding_timestamp
    window_end = funding_timestamp + (time_window_seconds * 1000)

    # Filter trades within the 10-second window
    window_df = df[(df['transact_time'] >= window_start) & (df['transact_time'] <= window_end)]

    if window_df.empty:
        return None

    if len(window_df) <= 1:
        return None

    # Find max and min prices in the window
    max_price = window_df['price'].max()
    min_price = window_df['price'].min()

    # Get the first price in the window (price at funding time)
    first_price = window_df.iloc[0]['price']

    # DELAY ANALYSIS: Find price after specified delay
    delay_timestamp = funding_timestamp + delay_ms  # Add specified delay
    delay_trades = df[df['transact_time'] >= delay_timestamp]

    if delay_trades.empty:
        # If no trades after delay, use the last available price in window
        delay_price = first_price
        delay_available = False
    else:
        delay_price = delay_trades.iloc[0]['price']
        delay_available = True

    # Calculate original metrics (from funding timestamp)
    max_drop_percentage = (max_price - min_price) / max_price * 100
    max_increase_percentage = (max_price - first_price) / first_price * 100
    initial_drop_percentage = (first_price - min_price) / first_price * 100
    initial_increase_percentage = (max_price - first_price) / first_price * 100

    # Calculate delay-adjusted metrics (from delay_ms after funding timestamp)
    if delay_available:
        delay_drop_percentage = (delay_price - min_price) / delay_price * 100
        delay_increase_percentage = (max_price - delay_price) / delay_price * 100
        delay_impact = (delay_price - first_price) / first_price * 100  # How much price moved during delay
    else:
        delay_drop_percentage = initial_drop_percentage
        delay_increase_percentage = initial_increase_percentage
        delay_impact = 0

    # Calculate the absolute maximum movement
    max_absolute_movement = max(abs(initial_drop_percentage), abs(initial_increase_percentage))
    max_absolute_movement_delay = max(abs(delay_drop_percentage), abs(delay_increase_percentage))

    return {
        'symbol': symbol,
        'fundingRate': funding_rate,
        'fundingTime': funding_timestamp,
        'fundingTimeReadable': funding_event['fundingTimeReadable'],
        'settlement_time': funding_time,  # For plotting compatibility
        'delay_ms': delay_ms,
        'first_price': first_price,
        'max_price': max_price,
        'min_price': min_price,
        'delay_price': delay_price,
        'delay_available': delay_available,
        'max_drop_percentage': max_drop_percentage,
        'max_increase_percentage': max_increase_percentage,
        'initial_drop_percentage': initial_drop_percentage,
        'initial_increase_percentage': initial_increase_percentage,
        'delay_drop_percentage': delay_drop_percentage,
        'delay_increase_percentage': delay_increase_percentage,
        'delay_impact': delay_impact,
        'max_absolute_movement': max_absolute_movement,
        'max_absolute_movement_delay': max_absolute_movement_delay,
        'delta': max_drop_percentage,  # Keep max drop as delta for plotting compatibility
        'delta_delay': delay_drop_percentage,  # New metric for delay-adjusted analysis
        'fundingrate': funding_rate * 100,  # Convert to percentage for plotting
        'trades_in_window': len(window_df),
        'stock': symbol
    }


def analyze_price_change(price_file):
    """
    Analyze price change after settlement time for a given trading data file
    """
    # Extract settlement timestamp and symbol from filename
    filename = os.path.basename(price_file)
    parts = filename.split('_')
    symbol = parts[0]
    settlement_timestamp_ms = int(parts[-2])  # Keep full timestamp for time calculations

    try:
        df = pd.read_csv(price_file)
        print(f"Processing {symbol}...")
    except FileNotFoundError as e:
        print(f"Error loading data for {symbol}: {e}")
        return None

    # Convert timestamps for analysis
    df['time'] = pd.to_datetime(df['transact_time'], unit='ms')
    settlement_time = pd.to_datetime(settlement_timestamp_ms, unit='ms')

    # Find the first trade at or after settlement time
    post_settlement_df = df[df['transact_time'] >= settlement_timestamp_ms]

    # Skip if no data after settlement time
    if post_settlement_df.empty:
        print(f"No data after settlement time for {symbol}")
        return None

    # Get the first price after settlement (this is our maximum value)
    first_post_settlement_price = post_settlement_df.iloc[0]['price']
    max_price = post_settlement_df.iloc[0]['price']  # Maximum is the first price after settlement

    # Calculate end time for our window
    end_time_ms = settlement_timestamp_ms + (time_window_seconds * 1000)

    # Filter trades within our time window
    window_df = post_settlement_df[post_settlement_df['transact_time'] <= end_time_ms]

    if len(window_df) <= 1:
        print(f"Not enough data points after settlement for {symbol}")
        return None

    # Find minimum price within time window (after settlement)
    min_price = window_df['price'].min()

    # Calculate delta: (max_price - min_price) / max_price * 100
    # This gives us the percentage drop from the first price after settlement
    delta = (max_price - min_price) / max_price * 100

    # Get corresponding funding rate using full timestamp
    funding_rate = get_funding_rate(symbol, settlement_timestamp_ms)

    if funding_rate is None:
        print(f"Could not get funding rate for {symbol}")
        return None

    return {
        'symbol': symbol,
        'settlement_time': settlement_time,
        'initial_price': first_post_settlement_price,
        'max_price': max_price,
        'min_price': min_price,
        'delta': delta,
        'fundingrate': funding_rate,
        'stock': symbol
    }


def create_comprehensive_analysis_plots(results_df):
    """
    Create comprehensive analysis plots including scatter plots, histograms, and correlation analysis
    """
    print("\n--- Creating Comprehensive Analysis Plots ---")

    # Create a figure with multiple subplots (now 2x3 instead of 2x4)
    fig = plt.figure(figsize=(18, 12))

    # 1. Main scatter plot with trend line (Price Drop)
    plt.subplot(2, 3, 1)
    plt.scatter(results_df['fundingrate'], results_df['delta'], alpha=0.6, s=50, color='blue')

    # Add trend line
    z = np.polyfit(results_df['fundingrate'], results_df['delta'], 1)
    p = np.poly1d(z)
    plt.plot(results_df['fundingrate'], p(results_df['fundingrate']), "r--", alpha=0.8, linewidth=2)

    plt.xlabel('Funding Rate (%)')
    plt.ylabel('Max Price Drop (%)')
    plt.title('Funding Rate vs Max Price Drop\nwith Linear Trend')
    plt.grid(True, alpha=0.3)

    # Add correlation coefficient
    correlation = results_df['fundingrate'].corr(results_df['delta'])
    plt.text(0.05, 0.95, f'Correlation: {correlation:.4f}', transform=plt.gca().transAxes,
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    # 2. Scatter plot for Price Increase
    plt.subplot(2, 3, 2)
    plt.scatter(results_df['fundingrate'], results_df['max_increase_percentage'], alpha=0.6, s=50, color='green')

    # Add trend line for increase
    z_inc = np.polyfit(results_df['fundingrate'], results_df['max_increase_percentage'], 1)
    p_inc = np.poly1d(z_inc)
    plt.plot(results_df['fundingrate'], p_inc(results_df['fundingrate']), "r--", alpha=0.8, linewidth=2)

    plt.xlabel('Funding Rate (%)')
    plt.ylabel('Max Price Increase (%)')
    plt.title('Funding Rate vs Max Price Increase\nwith Linear Trend')
    plt.grid(True, alpha=0.3)

    # Add correlation coefficient for increase
    correlation_inc = results_df['fundingrate'].corr(results_df['max_increase_percentage'])
    plt.text(0.05, 0.95, f'Correlation: {correlation_inc:.4f}', transform=plt.gca().transAxes,
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    # 3. Funding Rate Distribution
    plt.subplot(2, 3, 3)
    plt.hist(results_df['fundingrate'], bins=50, alpha=0.7, color='orange', edgecolor='black')
    plt.axvline(results_df['fundingrate'].mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {results_df["fundingrate"].mean():.4f}')
    plt.axvline(results_df['fundingrate'].median(), color='blue', linestyle='--', linewidth=2, label=f'Median: {results_df["fundingrate"].median():.4f}')
    plt.xlabel('Funding Rate (%)')
    plt.ylabel('Frequency')
    plt.title('Distribution of Funding Rates')
    plt.legend()
    plt.grid(True, alpha=0.3)

    # 4. Hexbin plot for density visualization (Drop)
    plt.subplot(2, 3, 4)
    plt.hexbin(results_df['fundingrate'], results_df['delta'], gridsize=30, cmap='Reds')
    plt.colorbar(label='Drop Density')
    plt.xlabel('Funding Rate (%)')
    plt.ylabel('Max Price Drop (%)')
    plt.title('Price Drop Density Heatmap')

    # 5. Hexbin plot for density visualization (Increase)
    plt.subplot(2, 3, 5)
    plt.hexbin(results_df['fundingrate'], results_df['max_increase_percentage'], gridsize=30, cmap='Greens')
    plt.colorbar(label='Increase Density')
    plt.xlabel('Funding Rate (%)')
    plt.ylabel('Max Price Increase (%)')
    plt.title('Price Increase Density Heatmap')

    # 6. Box plot for different funding rate ranges
    plt.subplot(2, 3, 6)

    # Create funding rate categories with 0.5% intervals from 0 to -2.0%
    results_df['funding_category'] = pd.cut(results_df['fundingrate'],
                                          bins=[-float('inf'), -2.0, -1.5, -1.0, -0.5, 0],
                                          labels=['< -2.0%', '-2.0% to -1.5%', '-1.5% to -1.0%',
                                                 '-1.0% to -0.5%', '-0.5% to 0%'])

    # Create box plot for drops
    box_data = [results_df[results_df['funding_category'] == cat]['delta'].dropna()
                for cat in results_df['funding_category'].cat.categories]

    plt.boxplot(box_data, tick_labels=results_df['funding_category'].cat.categories)
    plt.xlabel('Funding Rate Category')
    plt.ylabel('Max Price Drop (%)')
    plt.title('Price Drop Distribution by\nFunding Rate Category')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)

    plt.tight_layout()

    # Save the comprehensive plot
    comprehensive_plot_filename = os.path.join(os.path.dirname(script_dir), 'comprehensive_analysis.png')
    plt.savefig(comprehensive_plot_filename, dpi=300, bbox_inches='tight')
    print(f"Comprehensive analysis plot saved to '{comprehensive_plot_filename}'")

    plt.close()


def create_delay_analysis_plots(delay_results, delay_summary, delay_intervals):
    """
    Create comprehensive delay analysis plots showing the impact of execution delays
    """
    print("\n--- Creating Delay Analysis Plots ---")

    # Create Analysis directory if it doesn't exist
    analysis_dir = os.path.join(os.path.dirname(script_dir), 'Analysis')
    os.makedirs(analysis_dir, exist_ok=True)

    # Create a comprehensive figure with multiple subplots
    fig = plt.figure(figsize=(20, 15))

    # 1. Trade Availability vs Delay
    plt.subplot(3, 3, 1)
    availability_percentages = [summary['trades_available'] / summary['valid_cases'] * 100
                               for summary in delay_summary]
    plt.plot(delay_intervals, availability_percentages, 'bo-', linewidth=2, markersize=6)
    plt.xlabel('Execution Delay (ms)')
    plt.ylabel('Trade Availability (%)')
    plt.title('Trade Availability vs Execution Delay')
    plt.grid(True, alpha=0.3)
    plt.ylim(0, 105)

    # Add annotations for key points
    for i, (delay, pct) in enumerate(zip(delay_intervals, availability_percentages)):
        if i % 2 == 0:  # Annotate every other point to avoid crowding
            plt.annotate(f'{pct:.1f}%', (delay, pct), textcoords="offset points",
                        xytext=(0,10), ha='center', fontsize=8)

    # 2. Average Drop Performance vs Delay
    plt.subplot(3, 3, 2)
    avg_drops = [summary['avg_drop'] for summary in delay_summary]
    plt.plot(delay_intervals, avg_drops, 'ro-', linewidth=2, markersize=6)
    plt.xlabel('Execution Delay (ms)')
    plt.ylabel('Average Price Drop (%)')
    plt.title('Average Price Drop vs Execution Delay')
    plt.grid(True, alpha=0.3)

    # 3. Average Slippage Impact vs Delay
    plt.subplot(3, 3, 3)
    avg_impacts = [summary['avg_impact'] for summary in delay_summary]
    colors = ['green' if x < 0 else 'red' for x in avg_impacts]
    plt.bar(delay_intervals, avg_impacts, color=colors, alpha=0.7, width=8)
    plt.xlabel('Execution Delay (ms)')
    plt.ylabel('Average Slippage Impact (%)')
    plt.title('Average Slippage Impact vs Execution Delay')
    plt.grid(True, alpha=0.3)
    plt.axhline(y=0, color='black', linestyle='-', alpha=0.3)

    # 4. Trades Lost vs Delay
    plt.subplot(3, 3, 4)
    trades_lost = [summary['valid_cases'] - summary['trades_available'] for summary in delay_summary]
    plt.bar(delay_intervals, trades_lost, color='orange', alpha=0.7, width=8)
    plt.xlabel('Execution Delay (ms)')
    plt.ylabel('Number of Trades Lost')
    plt.title('Trades Lost Due to Execution Delay')
    plt.grid(True, alpha=0.3)

    # Add value labels on bars
    for i, (delay, lost) in enumerate(zip(delay_intervals, trades_lost)):
        if lost > 0:
            plt.text(delay, lost + max(trades_lost) * 0.01, str(lost),
                    ha='center', va='bottom', fontsize=8)

    # 5. Performance Degradation vs Delay
    plt.subplot(3, 3, 5)
    baseline_drop = delay_summary[0]['avg_drop']
    drop_losses = [baseline_drop - summary['avg_drop'] for summary in delay_summary]
    plt.plot(delay_intervals, drop_losses, 'mo-', linewidth=2, markersize=6)
    plt.xlabel('Execution Delay (ms)')
    plt.ylabel('Drop Opportunity Loss (%)')
    plt.title('Performance Degradation vs Execution Delay')
    plt.grid(True, alpha=0.3)

    # 6. Delay Impact Distribution (Histogram)
    plt.subplot(3, 3, 6)
    # Combine all delay impacts for different delays
    all_impacts_0ms = []
    all_impacts_100ms = []

    if 0 in delay_results and delay_results[0]:
        df_0 = pd.DataFrame(delay_results[0])
        all_impacts_0ms = df_0[df_0['delay_available']]['delay_impact'].tolist() if 'delay_available' in df_0.columns else []

    if 100 in delay_results and delay_results[100]:
        df_100 = pd.DataFrame(delay_results[100])
        all_impacts_100ms = df_100[df_100['delay_available']]['delay_impact'].tolist() if 'delay_available' in df_100.columns else []

    plt.hist(all_impacts_0ms, bins=20, alpha=0.6, label='0ms delay', color='blue', density=True)
    plt.hist(all_impacts_100ms, bins=20, alpha=0.6, label='100ms delay', color='red', density=True)
    plt.xlabel('Slippage Impact (%)')
    plt.ylabel('Density')
    plt.title('Distribution of Slippage Impact')
    plt.legend()
    plt.grid(True, alpha=0.3)

    # 7. Correlation: Funding Rate vs Drop (Different Delays)
    plt.subplot(3, 3, 7)
    if 0 in delay_results and delay_results[0] and 100 in delay_results and delay_results[100]:
        df_0 = pd.DataFrame(delay_results[0])
        df_100 = pd.DataFrame(delay_results[100])

        plt.scatter(df_0['fundingrate'], df_0['delta'], alpha=0.6, s=30, color='blue', label='0ms delay')
        plt.scatter(df_100['fundingrate'], df_100['delay_drop_percentage'], alpha=0.6, s=30, color='red', label='100ms delay')

        plt.xlabel('Funding Rate (%)')
        plt.ylabel('Price Drop (%)')
        plt.title('Funding Rate vs Price Drop\n(Different Delays)')
        plt.legend()
        plt.grid(True, alpha=0.3)

    # 8. Cumulative Trade Loss
    plt.subplot(3, 3, 8)
    cumulative_loss_pct = [100 - pct for pct in availability_percentages]
    plt.fill_between(delay_intervals, cumulative_loss_pct, alpha=0.5, color='red')
    plt.plot(delay_intervals, cumulative_loss_pct, 'r-', linewidth=2)
    plt.xlabel('Execution Delay (ms)')
    plt.ylabel('Cumulative Trade Loss (%)')
    plt.title('Cumulative Trade Loss vs Execution Delay')
    plt.grid(True, alpha=0.3)

    plt.tight_layout()

    # Save the delay analysis plot
    delay_plot_filename = os.path.join(analysis_dir, 'delay_sensitivity_analysis.png')
    plt.savefig(delay_plot_filename, dpi=300, bbox_inches='tight')
    print(f"Delay sensitivity analysis plot saved to '{delay_plot_filename}'")
    plt.close()

    # Create a separate detailed comparison plot
    create_detailed_delay_comparison_plot(delay_results, analysis_dir)


def create_detailed_delay_comparison_plot(delay_results, analysis_dir):
    """
    Create detailed comparison plots for specific delay intervals
    """
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))

    # Compare available delays (0ms, 100ms, 200ms instead of hardcoded 50ms)
    available_delays = [delay for delay in [0, 100, 200] if delay in delay_results and delay_results[delay]]
    colors = ['blue', 'orange', 'red']
    labels = [f'{delay}ms' for delay in available_delays]

    # 1. Price Drop Comparison
    axes[0, 0].set_title('Price Drop Performance Comparison')
    for i, delay in enumerate(available_delays):
        if delay_results[delay]:
            df = pd.DataFrame(delay_results[delay])
            if delay == 0:
                drops = df['delta'].tolist()
            else:
                drops = df['delay_drop_percentage'].tolist()

            axes[0, 0].hist(drops, bins=20, alpha=0.6, label=labels[i],
                           color=colors[i], density=True)

    axes[0, 0].set_xlabel('Price Drop (%)')
    axes[0, 0].set_ylabel('Density')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)

    # 2. Slippage Impact Distribution
    axes[0, 1].set_title('Slippage Impact Distribution')
    for i, delay in enumerate([d for d in available_delays if d > 0]):  # Skip 0ms as it has no slippage
        if delay_results[delay]:
            df = pd.DataFrame(delay_results[delay])
            impacts = df[df['delay_available']]['delay_impact'].tolist()

            color_idx = available_delays.index(delay)
            axes[0, 1].hist(impacts, bins=20, alpha=0.6, label=f'{delay}ms',
                           color=colors[color_idx], density=True)

    axes[0, 1].set_xlabel('Slippage Impact (%)')
    axes[0, 1].set_ylabel('Density')
    axes[0, 1].legend()
    axes[0, 1].grid(True, alpha=0.3)
    axes[0, 1].axvline(x=0, color='black', linestyle='--', alpha=0.5)

    # 3. Box plot comparison of drops
    axes[1, 0].set_title('Price Drop Distribution by Delay')
    drop_data = []
    box_labels = []

    for i, delay in enumerate(available_delays):
        if delay_results[delay]:
            df = pd.DataFrame(delay_results[delay])
            if delay == 0:
                drops = df['delta'].tolist()
            else:
                drops = df['delay_drop_percentage'].tolist()
            drop_data.append(drops)
            box_labels.append(labels[i])

    if drop_data:
        box_plot = axes[1, 0].boxplot(drop_data, tick_labels=box_labels, patch_artist=True)
        for patch, color in zip(box_plot['boxes'], colors[:len(drop_data)]):
            patch.set_facecolor(color)
            patch.set_alpha(0.6)

    axes[1, 0].set_ylabel('Price Drop (%)')
    axes[1, 0].grid(True, alpha=0.3)

    # 4. Scatter plot: Funding Rate vs Performance for different delays
    axes[1, 1].set_title('Funding Rate vs Drop Performance\n(Different Delays)')

    for i, delay in enumerate(available_delays):
        if delay_results[delay]:
            df = pd.DataFrame(delay_results[delay])
            if delay == 0:
                y_values = df['delta']
            else:
                y_values = df['delay_drop_percentage']

            axes[1, 1].scatter(df['fundingrate'], y_values, alpha=0.6, s=20,
                              color=colors[i], label=labels[i])

    axes[1, 1].set_xlabel('Funding Rate (%)')
    axes[1, 1].set_ylabel('Price Drop (%)')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)

    plt.tight_layout()

    # Save the detailed comparison plot
    comparison_plot_filename = os.path.join(analysis_dir, 'delay_detailed_comparison.png')
    plt.savefig(comparison_plot_filename, dpi=300, bbox_inches='tight')
    print(f"Detailed delay comparison plot saved to '{comparison_plot_filename}'")
    plt.close()


def create_delay_summary_table_plot(delay_summary, analysis_dir):
    """
    Create a visual summary table of delay analysis results
    """
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.axis('tight')
    ax.axis('off')

    # Prepare data for the table
    headers = ['Delay (ms)', 'Valid Cases', 'Trades Available', 'Availability (%)',
               'Avg Drop (%)', 'Avg Impact (%)', 'Drop Loss (%)']

    table_data = []
    baseline_drop = delay_summary[0]['avg_drop']

    for summary in delay_summary:
        availability_pct = summary['trades_available'] / summary['valid_cases'] * 100
        drop_loss = baseline_drop - summary['avg_drop']

        row = [
            f"{summary['delay_ms']}",
            f"{summary['valid_cases']}",
            f"{summary['trades_available']}",
            f"{availability_pct:.1f}%",
            f"{summary['avg_drop']:.4f}%",
            f"{summary['avg_impact']:+.4f}%",
            f"{drop_loss:.4f}%"
        ]
        table_data.append(row)

    # Create the table
    table = ax.table(cellText=table_data, colLabels=headers, cellLoc='center', loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.2, 1.5)

    # Color code the table
    for i, row in enumerate(table_data):
        for j in range(len(headers)):
            cell = table[(i+1, j)]
            if j == 3:  # Availability %
                pct = float(row[j].replace('%', ''))
                if pct >= 90:
                    cell.set_facecolor('#90EE90')  # Light green
                elif pct >= 70:
                    cell.set_facecolor('#FFFFE0')  # Light yellow
                else:
                    cell.set_facecolor('#FFB6C1')  # Light red
            elif j == 5:  # Avg Impact %
                impact = float(row[j].replace('%', '').replace('+', ''))
                if abs(impact) <= 0.01:
                    cell.set_facecolor('#90EE90')  # Light green
                elif abs(impact) <= 0.05:
                    cell.set_facecolor('#FFFFE0')  # Light yellow
                else:
                    cell.set_facecolor('#FFB6C1')  # Light red

    plt.title('Delay Sensitivity Analysis Summary\nColor: Green=Good, Yellow=Moderate, Red=Poor',
              fontsize=14, fontweight='bold', pad=20)

    # Save the summary table plot
    table_plot_filename = os.path.join(analysis_dir, 'delay_summary_table.png')
    plt.savefig(table_plot_filename, dpi=300, bbox_inches='tight')
    print(f"Delay summary table saved to '{table_plot_filename}'")
    plt.close()


if __name__ == "__main__":
    """
    Main analysis function with delay sensitivity analysis:
    1. Find severely negative funding rates from historical data
    2. Map timestamps to find corresponding trading data
    3. Analyze max price drops with different execution delays (0ms to 100ms)
    """
    print("Starting analysis with delay sensitivity analysis...")
    print("=" * 60)

    # Step 1: Find severely negative funding rates from historical data
    print("\nStep 1: Finding severely negative funding rates...")
    threshold = -0.003  # -0.3%
    negative_funding_events = find_severely_negative_funding_rates(fundingRate_dir, threshold)

    if not negative_funding_events:
        print("No severely negative funding rates found!")
        exit(0)

    print(f"\nFound {len(negative_funding_events)} severely negative funding events!")

    # Step 2: Analyze price drops for each delay interval
    print(f"\nStep 2: Analyzing price drops with different execution delays...")

    # Define delay intervals from 0ms to 1000ms in 100ms steps
    delay_intervals = list(range(0, 2001, 200))  # [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
    delay_results = {}

    for delay_ms in delay_intervals:
        print(f"\n--- Analyzing with {delay_ms}ms execution delay ---")
        results_for_delay = []
        successful_count = 0

        for i, event in enumerate(negative_funding_events, 1):
            if delay_ms == 0:  # Only show progress for first run
                print(f"Processing event {i}/{len(negative_funding_events)}: {event['symbol']} at {event['fundingTimeReadable']}")

            analysis_result = analyze_price_drop_after_funding(event, delay_ms)
            if analysis_result:
                results_for_delay.append(analysis_result)
                successful_count += 1
                if delay_ms == 0:  # Only show details for first run
                    print(f"  ✅ Success: Max drop {analysis_result['max_drop_percentage']:.4f}% from funding rate {analysis_result['fundingRate'] * 100:.4f}%")
            else:
                if delay_ms == 0:  # Only show failures for first run
                    print(f"  ❌ Failed to analyze {event['symbol']}")

        delay_results[delay_ms] = results_for_delay
        print(f"Successfully analyzed {successful_count} out of {len(negative_funding_events)} events for {delay_ms}ms delay")

    # Step 3: Create comprehensive delay analysis
    print(f"\n" + "=" * 80)
    print("DELAY SENSITIVITY ANALYSIS RESULTS")
    print("=" * 80)

    # Create summary table
    print(f"\n{'Delay (ms)':<10} {'Valid Cases':<12} {'Avg Drop (%)':<12} {'Avg Increase (%)':<15} {'Avg Impact (%)':<15}")
    print("-" * 65)

    delay_summary = []

    for delay_ms in delay_intervals:
        if delay_results[delay_ms]:
            df = pd.DataFrame(delay_results[delay_ms])
            avg_drop = df['delay_drop_percentage'].mean()
            avg_increase = df['delay_increase_percentage'].mean()
            avg_impact = df['delay_impact'].mean()
            valid_cases = len(df)

            delay_summary.append({
                'delay_ms': delay_ms,
                'valid_cases': valid_cases,
                'avg_drop': avg_drop,
                'avg_increase': avg_increase,
                'avg_impact': avg_impact,
                'trades_available': df['delay_available'].sum()
            })

            print(f"{delay_ms:<10} {valid_cases:<12} {avg_drop:<12.4f} {avg_increase:<15.4f} {avg_impact:<15.4f}")

    # Analysis of delay impact on strategy performance
    print(f"\n--- Strategy Performance Analysis ---")
    baseline_drop = delay_summary[0]['avg_drop']  # 0ms delay as baseline
    baseline_increase = delay_summary[0]['avg_increase']

    print(f"Baseline performance (0ms delay):")
    print(f"  Average drop: {baseline_drop:.4f}%")
    print(f"  Average increase: {baseline_increase:.4f}%")

    print(f"\nPerformance degradation by delay:")
    for summary in delay_summary[1:]:  # Skip 0ms
        drop_loss = baseline_drop - summary['avg_drop']
        increase_loss = baseline_increase - summary['avg_increase']
        trade_availability = summary['trades_available'] / summary['valid_cases'] * 100

        print(f"  {summary['delay_ms']}ms delay:")
        print(f"    Drop opportunity loss: {drop_loss:.4f}%")
        print(f"    Increase opportunity loss: {increase_loss:.4f}%")
        print(f"    Trade availability: {trade_availability:.1f}%")
        print(f"    Average slippage impact: {summary['avg_impact']:+.4f}%")

    # Find optimal delay threshold
    print(f"\n--- Optimal Execution Timing ---")
    min_impact_delay = min(delay_summary[1:], key=lambda x: abs(x['avg_impact']))
    print(f"Delay with minimal price impact: {min_impact_delay['delay_ms']}ms")
    print(f"  Average impact: {min_impact_delay['avg_impact']:+.4f}%")
    print(f"  Drop performance: {min_impact_delay['avg_drop']:.4f}%")
    print(f"  Trade availability: {min_impact_delay['trades_available']/min_impact_delay['valid_cases']*100:.1f}%")

    # Create delay analysis plots
    if delay_summary:
        create_delay_analysis_plots(delay_results, delay_summary, delay_intervals)
        create_delay_summary_table_plot(delay_summary, os.path.join(os.path.dirname(script_dir), 'Analysis'))

    # Save baseline results (0ms delay) for plotting
    if delay_results[0]:
        results_df = pd.DataFrame(delay_results[0])
        results_df.to_csv(output_file, index=False)
        print(f"\nBaseline results (0ms delay) saved to {output_file}")
        create_comprehensive_analysis_plots(results_df)

        # Detailed statistics for baseline (0ms delay)
        print(f"\n--- Summary Statistics (Baseline - 0ms delay) ---")
        print(f"Number of data points: {len(results_df)}")
        print(f"Funding rate range: {results_df['fundingrate'].min():.4f}% to {results_df['fundingrate'].max():.4f}%")
        print(f"Max price drop range: {results_df['delta'].min():.4f}% to {results_df['delta'].max():.4f}%")
        print(f"Max price increase range: {results_df['max_increase_percentage'].min():.4f}% to {results_df['max_increase_percentage'].max():.4f}%")
        print(f"Average max price drop: {results_df['delta'].mean():.4f}%")
        print(f"Average max price increase: {results_df['max_increase_percentage'].mean():.4f}%")
        print(f"Average initial drop: {results_df['initial_drop_percentage'].mean():.4f}%")
        print(f"Average initial increase: {results_df['initial_increase_percentage'].mean():.4f}%")
        print(f"Average max absolute movement: {results_df['max_absolute_movement'].mean():.4f}%")
        print(f"Average trades per 10s window: {results_df['trades_in_window'].mean():.1f}")

        # DELAY ANALYSIS - Calculate trade availability and lost opportunities
        print(f"\n--- Trade Availability and Lost Opportunities ---")

        # Compare all delay intervals for trade loss analysis
        for delay_ms in delay_intervals:
            if delay_results[delay_ms]:
                delay_df = pd.DataFrame(delay_results[delay_ms])
                trades_available = delay_df['delay_available'].sum()
                total_events = len(delay_df)
                availability_pct = trades_available / total_events * 100
                trades_lost = total_events - trades_available

                print(f"  {delay_ms}ms delay:")
                print(f"    Trades available: {trades_available}/{total_events} ({availability_pct:.1f}%)")
                print(f"    Trades lost due to late entry: {trades_lost} ({100-availability_pct:.1f}%)")

                if trades_available > 0:
                    avg_slippage_impact = delay_df[delay_df['delay_available']]['delay_impact'].mean()
                    print(f"    Average slippage impact (when trades available): {avg_slippage_impact:+.4f}%")

        # Analyze cases where price never dropped below funding rate price
        no_drop_cases = results_df[results_df['initial_drop_percentage'] <= 0]
        only_increase_cases = results_df[results_df['min_price'] >= results_df['first_price']]

        print(f"\n--- Price Movement Patterns ---")
        print(f"Cases where price never dropped below funding rate price: {len(no_drop_cases)} ({len(no_drop_cases)/len(results_df)*100:.1f}%)")
        print(f"Cases where price only increased (min_price >= first_price): {len(only_increase_cases)} ({len(only_increase_cases)/len(results_df)*100:.1f}%)")

        if len(no_drop_cases) > 0:
            print(f"Average funding rate for no-drop cases: {no_drop_cases['fundingrate'].mean():.4f}%")
            print(f"Average price increase for no-drop cases: {no_drop_cases['max_increase_percentage'].mean():.4f}%")

            print(f"\nTop 5 no-drop cases with highest increases:")
            top_no_drop = no_drop_cases.nlargest(5, 'max_increase_percentage')
            for idx, row in top_no_drop.iterrows():
                print(f"  {row['symbol']}: {row['fundingrate']:.4f}% funding → +{row['max_increase_percentage']:.4f}% increase")

        # Analyze cases where price dropped significantly
        significant_drop_cases = results_df[results_df['initial_drop_percentage'] > 1.0]  # More than 1% drop
        print(f"Cases with >1% drop from funding price: {len(significant_drop_cases)} ({len(significant_drop_cases)/len(results_df)*100:.1f}%)")

        if len(significant_drop_cases) > 0:
            print(f"Average funding rate for significant drop cases: {significant_drop_cases['fundingrate'].mean():.4f}%")
            print(f"Average drop for significant drop cases: {significant_drop_cases['initial_drop_percentage'].mean():.4f}%")

        # Simple linear regression statistics
        model = LinearRegression()
        X = results_df[['fundingrate']]
        y = results_df['delta']
        model.fit(X, y)

        correlation = results_df['fundingrate'].corr(results_df['delta'])
        r_squared = model.score(X, y)

        print(f"\n--- Regression Analysis ---")
        print(f"Correlation (funding rate vs max drop): {correlation:.4f}")
        print(f"R-squared: {r_squared:.4f}")
        print(f"Linear equation: max_drop = {model.coef_[0]:.6f} * funding_rate + {model.intercept_:.6f}")

        # Additional insights
        print(f"\n--- Key Insights ---")
        most_negative_rate = results_df.loc[results_df['fundingrate'].idxmin()]
        biggest_drop = results_df.loc[results_df['delta'].idxmax()]
        biggest_increase = results_df.loc[results_df['max_increase_percentage'].idxmax()]
        biggest_absolute_move = results_df.loc[results_df['max_absolute_movement'].idxmax()]

        print(f"Most negative funding rate: {most_negative_rate['fundingrate']:.4f}% ({most_negative_rate['symbol']}) → {most_negative_rate['delta']:.4f}% drop")
        print(f"  Time: {most_negative_rate['fundingTimeReadable']}")
        print(f"Biggest price drop: {biggest_drop['delta']:.4f}% ({biggest_drop['symbol']}) from {biggest_drop['fundingrate']:.4f}% funding rate")
        print(f"  Time: {biggest_drop['fundingTimeReadable']}")
        print(f"Biggest price increase: {biggest_increase['max_increase_percentage']:.4f}% ({biggest_increase['symbol']}) from {biggest_increase['fundingrate']:.4f}% funding rate")
        print(f"  Time: {biggest_increase['fundingTimeReadable']}")
        print(f"Biggest absolute movement: {biggest_absolute_move['max_absolute_movement']:.4f}% ({biggest_absolute_move['symbol']}) from {biggest_absolute_move['fundingrate']:.4f}% funding rate")
        print(f"  Time: {biggest_absolute_move['fundingTimeReadable']}")
    else:
        print("No valid results to analyze")
