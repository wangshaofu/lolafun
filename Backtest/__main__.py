import pandas as pd
import numpy as np
import os
import sys
import matplotlib.pyplot as plt
from datetime import datetime
import csv

# Add the parent directory to the path to import from DA
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DA.analyzer import find_severely_negative_funding_rates, find_corresponding_trading_data

class FundingRateBacktester:
    def __init__(self, initial_capital=10000, delay_ms=0, stop_loss_pct=1.0, leverage=1, entry_fee_pct=0.05, exit_fee_pct=0.02, compound=False):
        """
        Initialize the backtester

        Args:
            initial_capital: Starting capital in USD
            delay_ms: Server delay in milliseconds
            stop_loss_pct: Stop loss percentage (1.0 = 1%)
            leverage: Trading leverage (1 = no leverage)
            entry_fee_pct: Entry fee percentage (0.05 = 0.05%)
            exit_fee_pct: Exit fee percentage (0.02 = 0.02%)
            compound: Whether to compound returns (True) or use fixed position size (False)
        """
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.delay_ms = delay_ms
        self.stop_loss_pct = stop_loss_pct / 100  # Convert to decimal
        self.leverage = leverage
        self.entry_fee_pct = entry_fee_pct / 100  # Convert to decimal (0.05% = 0.0005)
        self.exit_fee_pct = exit_fee_pct / 100    # Convert to decimal (0.02% = 0.0002)
        self.compound = compound  # New parameter to control compounding

        # Trading statistics
        self.trades = []
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_pnl = 0
        self.total_fees_paid = 0  # Track total fees

        # Directories (same as analyzer)
        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.aggTrades_dir = os.path.join(script_dir, 'DA', 'Negative Funding AggTrades')
        self.fundingRate_dir = os.path.join(script_dir, 'DA', 'Funding Rate History')

    def simulate_trade(self, funding_event):
        """
        Simulate a single trade based on the funding rate strategy

        Strategy:
        - Enter SHORT position when funding rate is negative
        - Take profit = -1 * funding_rate (opposite direction)
        - Stop loss = 1% against position
        - Apply server delay
        """
        symbol = funding_event['symbol']
        funding_timestamp = funding_event['fundingTime']
        funding_rate = funding_event['fundingRate']  # This is already as decimal (e.g., -0.003 for -0.3%)

        # Find trading data
        trading_file = find_corresponding_trading_data(symbol, funding_timestamp)
        if not trading_file:
            return None

        try:
            df = pd.read_csv(trading_file)
        except Exception as e:
            return None

        # Apply server delay to entry
        entry_timestamp = funding_timestamp + self.delay_ms
        entry_trades = df[df['transact_time'] >= entry_timestamp]

        if entry_trades.empty:
            return None

        entry_price = entry_trades.iloc[0]['price']
        entry_time = entry_trades.iloc[0]['transact_time']

        # Calculate target prices
        # Since funding rate is negative, we go SHORT
        # Take profit = entry_price * (1 + funding_rate) - we profit when price drops
        take_profit_pct = -funding_rate  # Convert negative funding to positive take profit
        take_profit_price = entry_price * (1 - take_profit_pct)  # SHORT profits when price drops

        # Stop loss = entry_price * (1 + stop_loss_pct) - we lose when price rises
        stop_loss_price = entry_price * (1 + self.stop_loss_pct)

        # Find exit point
        future_trades = df[df['transact_time'] > entry_time]

        trade_result = {
            'symbol': symbol,
            'entry_time': entry_time,
            'entry_price': entry_price,
            'funding_rate': funding_rate,
            'funding_rate_pct': funding_rate * 100,
            'take_profit_price': take_profit_price,
            'stop_loss_price': stop_loss_price,
            'take_profit_pct': take_profit_pct * 100,
            'delay_ms': self.delay_ms,
            'position_size': 0,
            'pnl': 0,
            'pnl_pct': 0,
            'exit_reason': 'NO_EXIT',
            'exit_price': entry_price,
            'exit_time': entry_time,
            'trade_duration_ms': 0
        }

        # Check each subsequent trade for exit conditions
        for _, trade in future_trades.iterrows():
            current_price = trade['price']
            current_time = trade['transact_time']

            # Check take profit (price dropped enough for SHORT position)
            if current_price <= take_profit_price:
                trade_result['exit_reason'] = 'TAKE_PROFIT'
                trade_result['exit_price'] = current_price
                trade_result['exit_time'] = current_time
                break

            # Check stop loss (price rose too much against SHORT position)
            if current_price >= stop_loss_price:
                trade_result['exit_reason'] = 'STOP_LOSS'
                trade_result['exit_price'] = current_price
                trade_result['exit_time'] = current_time
                break

        # Calculate P&L for SHORT position
        if trade_result['exit_reason'] != 'NO_EXIT':
            # Position size based on compounding setting
            if self.compound:
                # Use current capital (compound returns)
                position_size = self.current_capital * self.leverage
            else:
                # Use fixed initial capital (no compounding)
                position_size = self.initial_capital * self.leverage

            # Calculate fees
            entry_fee = position_size * self.entry_fee_pct
            exit_fee = position_size * self.exit_fee_pct
            total_fees = entry_fee + exit_fee

            # SHORT P&L = (entry_price - exit_price) / entry_price
            pnl_pct_before_fees = (entry_price - trade_result['exit_price']) / entry_price
            pnl_amount_before_fees = position_size * pnl_pct_before_fees

            # Net P&L after deducting fees
            pnl_amount_after_fees = pnl_amount_before_fees - total_fees

            trade_result['position_size'] = position_size
            trade_result['pnl_before_fees'] = pnl_amount_before_fees
            trade_result['entry_fee'] = entry_fee
            trade_result['exit_fee'] = exit_fee
            trade_result['total_fees'] = total_fees
            trade_result['pnl'] = pnl_amount_after_fees  # Net P&L after fees
            trade_result['pnl_pct'] = (pnl_amount_after_fees / position_size) * 100
            trade_result['trade_duration_ms'] = trade_result['exit_time'] - trade_result['entry_time']

            # Update capital (always track total, but position size depends on compound setting)
            if self.compound:
                self.current_capital += pnl_amount_after_fees
            else:
                self.current_capital = self.initial_capital + self.total_pnl + pnl_amount_after_fees

            self.total_pnl += pnl_amount_after_fees
            self.total_fees_paid += total_fees

            # Update statistics
            if pnl_amount_after_fees > 0:
                self.winning_trades += 1
            else:
                self.losing_trades += 1

        self.total_trades += 1
        self.trades.append(trade_result)

        return trade_result

    def run_backtest(self, threshold=-0.003):
        """
        Run the complete backtest

        Args:
            threshold: Funding rate threshold (default -0.3%)
        """
        print("🚀 Starting Funding Rate Strategy Backtest")
        print("=" * 60)
        print(f"Initial Capital: ${self.initial_capital:,.2f}")
        print(f"Server Delay: {self.delay_ms}ms")
        print(f"Stop Loss: {self.stop_loss_pct * 100:.1f}%")
        print(f"Leverage: {self.leverage}x")
        print(f"Strategy: SHORT when funding rate <= {threshold * 100:.1f}%")
        print(f"Take Profit: -1 * funding_rate")
        print("=" * 60)

        # Find negative funding events
        negative_funding_events = find_severely_negative_funding_rates(self.fundingRate_dir, threshold)

        if not negative_funding_events:
            print("No negative funding events found!")
            return

        print(f"\nFound {len(negative_funding_events)} negative funding events")
        print("Starting trade simulation...\n")

        successful_trades = 0

        for i, event in enumerate(negative_funding_events, 1):
            if i % 50 == 0 or i <= 10:  # Show progress for first 10 and every 50 trades
                print(f"Processing trade {i}/{len(negative_funding_events)}: {event['symbol']} "
                      f"(Rate: {event['fundingRate'] * 100:.4f}%)")

            trade_result = self.simulate_trade(event)

            if trade_result and trade_result['exit_reason'] != 'NO_EXIT':
                successful_trades += 1
                if i <= 10:  # Show details for first 10 trades
                    pnl_status = "✅ PROFIT" if trade_result['pnl'] > 0 else "❌ LOSS"
                    print(f"  {pnl_status}: {trade_result['exit_reason']} - "
                          f"P&L: ${trade_result['pnl']:.2f} ({trade_result['pnl_pct']:.2f}%)")

        print(f"\nBacktest completed! Processed {successful_trades} successful trades out of {len(negative_funding_events)} events")
        self.generate_report()

    def generate_report(self):
        """Generate comprehensive backtest report"""
        if not self.trades:
            print("No trades to analyze!")
            return

        # Filter successful trades (those that exited)
        successful_trades = [t for t in self.trades if t['exit_reason'] != 'NO_EXIT']

        if not successful_trades:
            print("No successful trades completed!")
            return

        df = pd.DataFrame(successful_trades)

        # Calculate metrics
        total_return = (self.current_capital - self.initial_capital) / self.initial_capital * 100
        win_rate = (self.winning_trades / len(successful_trades)) * 100 if successful_trades else 0

        avg_win = df[df['pnl'] > 0]['pnl'].mean() if self.winning_trades > 0 else 0
        avg_loss = df[df['pnl'] < 0]['pnl'].mean() if self.losing_trades > 0 else 0

        profit_factor = abs(avg_win * self.winning_trades / (avg_loss * self.losing_trades)) if self.losing_trades > 0 and avg_loss != 0 else float('inf')

        avg_duration = df['trade_duration_ms'].mean() / 1000  # Convert to seconds

        print("\n" + "=" * 60)
        print("📊 BACKTEST RESULTS")
        print("=" * 60)

        print(f"\n💰 PERFORMANCE SUMMARY:")
        print(f"  Initial Capital:     ${self.initial_capital:,.2f}")
        print(f"  Final Capital:       ${self.current_capital:,.2f}")
        print(f"  Total P&L:           ${self.total_pnl:,.2f}")
        print(f"  Total Fees Paid:     ${self.total_fees_paid:,.2f}")
        print(f"  Total Return:        {total_return:.2f}%")
        print(f"  Return w/o Fees:     {((self.total_pnl + self.total_fees_paid) / self.initial_capital * 100):.2f}%")

        print(f"\n📈 TRADE STATISTICS:")
        print(f"  Total Opportunities: {len(self.trades)}")
        print(f"  Successful Trades:   {len(successful_trades)}")
        print(f"  Winning Trades:      {self.winning_trades}")
        print(f"  Losing Trades:       {self.losing_trades}")
        print(f"  Win Rate:            {win_rate:.1f}%")

        print(f"\n💵 PROFIT ANALYSIS:")
        print(f"  Average Win:         ${avg_win:.2f}")
        print(f"  Average Loss:        ${avg_loss:.2f}")
        print(f"  Profit Factor:       {profit_factor:.2f}")
        print(f"  Average Duration:    {avg_duration:.1f} seconds")

        print(f"\n💸 FEE ANALYSIS:")
        avg_fees_per_trade = df['total_fees'].mean() if 'total_fees' in df.columns else 0
        fee_to_pnl_ratio = (self.total_fees_paid / abs(self.total_pnl)) * 100 if self.total_pnl != 0 else 0
        print(f"  Entry Fee Rate:      {self.entry_fee_pct * 100:.3f}%")
        print(f"  Exit Fee Rate:       {self.exit_fee_pct * 100:.3f}%")
        print(f"  Total Fees Paid:     ${self.total_fees_paid:,.2f}")
        print(f"  Average Fees/Trade:  ${avg_fees_per_trade:.2f}")
        print(f"  Fees as % of P&L:    {fee_to_pnl_ratio:.1f}%")

        # Exit reason breakdown
        print(f"\n🚪 EXIT REASONS:")
        exit_reasons = df['exit_reason'].value_counts()
        for reason, count in exit_reasons.items():
            pct = (count / len(successful_trades)) * 100
            print(f"  {reason.replace('_', ' ').title()}: {count} ({pct:.1f}%)")

        # Best and worst trades
        if len(successful_trades) > 0:
            best_trade = df.loc[df['pnl'].idxmax()]
            worst_trade = df.loc[df['pnl'].idxmin()]

            print(f"\n🏆 BEST TRADE:")
            print(f"  Symbol: {best_trade['symbol']}")
            print(f"  P&L: ${best_trade['pnl']:.2f} ({best_trade['pnl_pct']:.2f}%)")
            print(f"  Funding Rate: {best_trade['funding_rate_pct']:.4f}%")
            print(f"  Duration: {best_trade['trade_duration_ms'] / 1000:.1f}s")

            print(f"\n📉 WORST TRADE:")
            print(f"  Symbol: {worst_trade['symbol']}")
            print(f"  P&L: ${worst_trade['pnl']:.2f} ({worst_trade['pnl_pct']:.2f}%)")
            print(f"  Funding Rate: {worst_trade['funding_rate_pct']:.4f}%")
            print(f"  Duration: {worst_trade['trade_duration_ms'] / 1000:.1f}s")

        # Save detailed results
        self.save_results(df)
        self.create_plots(df)

    def save_results(self, df):
        """Save detailed results to CSV"""
        output_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                   f'backtest_results_delay_{self.delay_ms}ms.csv')
        df.to_csv(output_file, index=False)
        print(f"\n💾 Detailed results saved to: {output_file}")

    def create_plots(self, df):
        """Create visualization plots"""
        if df.empty:
            return

        fig, axes = plt.subplots(2, 2, figsize=(15, 10))

        # 1. Cumulative P&L
        df_sorted = df.sort_values('entry_time')
        cumulative_pnl = df_sorted['pnl'].cumsum()
        cumulative_capital = self.initial_capital + cumulative_pnl

        axes[0, 0].plot(range(len(cumulative_capital)), cumulative_capital, 'b-', linewidth=2)
        axes[0, 0].axhline(y=self.initial_capital, color='r', linestyle='--', alpha=0.7, label='Initial Capital')
        axes[0, 0].set_title('Cumulative Capital Over Time')
        axes[0, 0].set_xlabel('Trade Number')
        axes[0, 0].set_ylabel('Capital ($)')
        axes[0, 0].grid(True, alpha=0.3)
        axes[0, 0].legend()

        # 2. P&L Distribution
        axes[0, 1].hist(df['pnl'], bins=30, alpha=0.7, color='green', edgecolor='black')
        axes[0, 1].axvline(x=0, color='red', linestyle='--', alpha=0.7)
        axes[0, 1].set_title('P&L Distribution')
        axes[0, 1].set_xlabel('P&L ($)')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].grid(True, alpha=0.3)

        # 3. Funding Rate vs P&L
        axes[1, 0].scatter(df['funding_rate_pct'], df['pnl_pct'], alpha=0.6, s=30)
        axes[1, 0].axhline(y=0, color='red', linestyle='--', alpha=0.7)
        axes[1, 0].set_title('Funding Rate vs P&L')
        axes[1, 0].set_xlabel('Funding Rate (%)')
        axes[1, 0].set_ylabel('P&L (%)')
        axes[1, 0].grid(True, alpha=0.3)

        # 4. Trade Duration Distribution
        durations_seconds = df['trade_duration_ms'] / 1000
        axes[1, 1].hist(durations_seconds, bins=30, alpha=0.7, color='orange', edgecolor='black')
        axes[1, 1].set_title('Trade Duration Distribution')
        axes[1, 1].set_xlabel('Duration (seconds)')
        axes[1, 1].set_ylabel('Frequency')
        axes[1, 1].grid(True, alpha=0.3)

        plt.tight_layout()

        # Save plot
        plot_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                f'backtest_analysis_delay_{self.delay_ms}ms.png')
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        print(f"📊 Analysis plots saved to: {plot_file}")
        plt.close()


def run_delay_sensitivity_analysis():
    """Run backtest with different delay values to see impact"""
    print("\n🔬 DELAY SENSITIVITY ANALYSIS")
    print("=" * 60)

    delays = [0, 100, 200, 500, 2000, 5000]  # milliseconds
    results = []

    for delay in delays:
        print(f"\n--- Testing {delay}ms delay ---")
        backtester = FundingRateBacktester(
            initial_capital=10000,
            delay_ms=delay,
            stop_loss_pct=1.0,
            leverage=1
        )

        # Run quietly for delay analysis
        negative_funding_events = find_severely_negative_funding_rates(backtester.fundingRate_dir, -0.003)

        if not negative_funding_events:
            continue

        successful_trades = 0
        for event in negative_funding_events:
            trade_result = backtester.simulate_trade(event)
            if trade_result and trade_result['exit_reason'] != 'NO_EXIT':
                successful_trades += 1

        # Calculate results
        total_return = (backtester.current_capital - backtester.initial_capital) / backtester.initial_capital * 100
        win_rate = (backtester.winning_trades / successful_trades * 100) if successful_trades > 0 else 0

        results.append({
            'delay_ms': delay,
            'total_trades': successful_trades,
            'total_return': total_return,
            'final_capital': backtester.current_capital,
            'total_pnl': backtester.total_pnl,
            'win_rate': win_rate,
            'winning_trades': backtester.winning_trades,
            'losing_trades': backtester.losing_trades
        })

        print(f"  Return: {total_return:.2f}% | Win Rate: {win_rate:.1f}% | Trades: {successful_trades}")

    # Create delay analysis report
    print(f"\n📋 DELAY ANALYSIS SUMMARY:")
    print(f"{'Delay (ms)':<10} {'Trades':<8} {'Return (%)':<12} {'Win Rate (%)':<12} {'Final Capital':<15}")
    print("-" * 65)

    for result in results:
        print(f"{result['delay_ms']:<10} {result['total_trades']:<8} "
              f"{result['total_return']:<12.2f} {result['win_rate']:<12.1f} ${result['final_capital']:<15,.2f}")

    return results


def optimize_stop_loss(delay_ms=100, initial_capital=10000, leverage=1, entry_fee_pct=0.05, exit_fee_pct=0.02,
                     compound=False, threshold=-0.003, stop_loss_range=(0.5, 3.0), step_size=0.1):
    """
    Find the optimal stop loss percentage for a given delay

    Args:
        delay_ms: Server delay in milliseconds
        initial_capital: Starting capital
        leverage: Trading leverage
        entry_fee_pct: Entry fee percentage
        exit_fee_pct: Exit fee percentage
        compound: Whether to compound returns
        threshold: Funding rate threshold
        stop_loss_range: Tuple of (min_stop_loss, max_stop_loss) percentages
        step_size: Step size for stop loss testing

    Returns:
        Dictionary with optimization results
    """
    print(f"\n🔍 STOP LOSS OPTIMIZATION for {delay_ms}ms delay")
    print("=" * 60)
    print(f"Testing stop loss range: {stop_loss_range[0]:.1f}% to {stop_loss_range[1]:.1f}% (step: {step_size:.1f}%)")

    # Generate stop loss percentages to test
    stop_loss_percentages = []
    current = stop_loss_range[0]
    while current <= stop_loss_range[1]:
        stop_loss_percentages.append(current)
        current += step_size

    results = []
    best_return = -float('inf')
    best_stop_loss = None

    for stop_loss_pct in stop_loss_percentages:
        print(f"\nTesting {stop_loss_pct:.1f}% stop loss...")

        # Create backtester with current stop loss
        backtester = FundingRateBacktester(
            initial_capital=initial_capital,
            delay_ms=delay_ms,
            stop_loss_pct=stop_loss_pct,
            leverage=leverage,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            compound=compound
        )

        # Get negative funding events
        negative_funding_events = find_severely_negative_funding_rates(backtester.fundingRate_dir, threshold)

        if not negative_funding_events:
            continue

        # Run trades quietly
        successful_trades = 0
        total_trades = 0
        winning_trades = 0
        losing_trades = 0

        for event in negative_funding_events:
            trade_result = backtester.simulate_trade(event)
            if trade_result and trade_result['exit_reason'] != 'NO_EXIT':
                successful_trades += 1
                if trade_result['pnl'] > 0:
                    winning_trades += 1
                else:
                    losing_trades += 1
            total_trades += 1

        # Calculate metrics
        if successful_trades > 0:
            total_return = (backtester.current_capital - backtester.initial_capital) / backtester.initial_capital * 100
            win_rate = (winning_trades / successful_trades) * 100
            avg_trade_pnl = backtester.total_pnl / successful_trades if successful_trades > 0 else 0

            # Calculate Sharpe-like ratio (return per unit of risk)
            successful_trade_data = [t for t in backtester.trades if t['exit_reason'] != 'NO_EXIT']
            if len(successful_trade_data) > 1:
                trade_returns = [t['pnl_pct'] for t in successful_trade_data]
                volatility = np.std(trade_returns) if len(trade_returns) > 1 else 0
                risk_adjusted_return = total_return / volatility if volatility > 0 else total_return
            else:
                risk_adjusted_return = total_return

            # Count exit reasons
            exit_reasons = {}
            for trade in successful_trade_data:
                reason = trade['exit_reason']
                exit_reasons[reason] = exit_reasons.get(reason, 0) + 1

            stop_loss_exits = exit_reasons.get('STOP_LOSS', 0)
            take_profit_exits = exit_reasons.get('TAKE_PROFIT', 0)
            stop_loss_rate = (stop_loss_exits / successful_trades) * 100 if successful_trades > 0 else 0

            result = {
                'stop_loss_pct': stop_loss_pct,
                'total_return': total_return,
                'total_pnl': backtester.total_pnl,
                'final_capital': backtester.current_capital,
                'successful_trades': successful_trades,
                'win_rate': win_rate,
                'avg_trade_pnl': avg_trade_pnl,
                'risk_adjusted_return': risk_adjusted_return,
                'total_fees': backtester.total_fees_paid,
                'stop_loss_exits': stop_loss_exits,
                'take_profit_exits': take_profit_exits,
                'stop_loss_rate': stop_loss_rate,
                'volatility': np.std([t['pnl_pct'] for t in successful_trade_data]) if len(successful_trade_data) > 1 else 0
            }

            results.append(result)

            # Track best performing stop loss
            if total_return > best_return:
                best_return = total_return
                best_stop_loss = stop_loss_pct

            print(f"  Return: {total_return:.2f}% | Win Rate: {win_rate:.1f}% | "
                  f"Trades: {successful_trades} | SL Rate: {stop_loss_rate:.1f}%")

    if not results:
        print("No valid results found!")
        return None

    # Create comprehensive analysis
    df = pd.DataFrame(results)

    # Find optimal stop loss by different criteria
    best_return_sl = df.loc[df['total_return'].idxmax()]
    best_risk_adj_sl = df.loc[df['risk_adjusted_return'].idxmax()]
    best_win_rate_sl = df.loc[df['win_rate'].idxmax()]

    print(f"\n📊 OPTIMIZATION RESULTS:")
    print("=" * 60)

    print(f"\n🏆 BEST BY TOTAL RETURN:")
    print(f"  Stop Loss: {best_return_sl['stop_loss_pct']:.1f}%")
    print(f"  Total Return: {best_return_sl['total_return']:.2f}%")
    print(f"  Win Rate: {best_return_sl['win_rate']:.1f}%")
    print(f"  Stop Loss Rate: {best_return_sl['stop_loss_rate']:.1f}%")

    print(f"\n📈 BEST BY RISK-ADJUSTED RETURN:")
    print(f"  Stop Loss: {best_risk_adj_sl['stop_loss_pct']:.1f}%")
    print(f"  Risk-Adjusted Return: {best_risk_adj_sl['risk_adjusted_return']:.2f}")
    print(f"  Total Return: {best_risk_adj_sl['total_return']:.2f}%")
    print(f"  Volatility: {best_risk_adj_sl['volatility']:.2f}%")

    print(f"\n🎯 BEST BY WIN RATE:")
    print(f"  Stop Loss: {best_win_rate_sl['stop_loss_pct']:.1f}%")
    print(f"  Win Rate: {best_win_rate_sl['win_rate']:.1f}%")
    print(f"  Total Return: {best_win_rate_sl['total_return']:.2f}%")

    # Create detailed table
    print(f"\n📋 DETAILED RESULTS:")
    print(f"{'Stop Loss':<10} {'Return (%)':<12} {'Win Rate':<10} {'Trades':<8} {'SL Rate':<8} {'Risk-Adj':<10}")
    print("-" * 70)

    for _, row in df.iterrows():
        print(f"{row['stop_loss_pct']:<10.1f} {row['total_return']:<12.2f} "
              f"{row['win_rate']:<10.1f} {row['successful_trades']:<8} "
              f"{row['stop_loss_rate']:<8.1f} {row['risk_adjusted_return']:<10.2f}")

    # Create visualization
    create_stop_loss_optimization_plots(df, delay_ms)

    # Save results
    save_stop_loss_optimization_results(df, delay_ms)

    return {
        'results': df,
        'best_return': best_return_sl,
        'best_risk_adjusted': best_risk_adj_sl,
        'best_win_rate': best_win_rate_sl,
        'delay_ms': delay_ms
    }


def create_stop_loss_optimization_plots(df, delay_ms):
    """Create plots for stop loss optimization analysis"""
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    # 1. Stop Loss vs Total Return
    axes[0, 0].plot(df['stop_loss_pct'], df['total_return'], 'bo-', linewidth=2, markersize=6)
    axes[0, 0].set_xlabel('Stop Loss (%)')
    axes[0, 0].set_ylabel('Total Return (%)')
    axes[0, 0].set_title(f'Stop Loss vs Total Return (Delay: {delay_ms}ms)')
    axes[0, 0].grid(True, alpha=0.3)

    # Mark the best point
    best_idx = df['total_return'].idxmax()
    best_row = df.loc[best_idx]
    axes[0, 0].scatter(best_row['stop_loss_pct'], best_row['total_return'],
                      color='red', s=100, zorder=5)
    axes[0, 0].annotate(f'Best: {best_row["stop_loss_pct"]:.1f}%',
                       xy=(best_row['stop_loss_pct'], best_row['total_return']),
                       xytext=(10, 10), textcoords='offset points',
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7))

    # 2. Stop Loss vs Win Rate
    axes[0, 1].plot(df['stop_loss_pct'], df['win_rate'], 'go-', linewidth=2, markersize=6)
    axes[0, 1].set_xlabel('Stop Loss (%)')
    axes[0, 1].set_ylabel('Win Rate (%)')
    axes[0, 1].set_title(f'Stop Loss vs Win Rate (Delay: {delay_ms}ms)')
    axes[0, 1].grid(True, alpha=0.3)

    # 3. Stop Loss vs Risk-Adjusted Return
    axes[1, 0].plot(df['stop_loss_pct'], df['risk_adjusted_return'], 'mo-', linewidth=2, markersize=6)
    axes[1, 0].set_xlabel('Stop Loss (%)')
    axes[1, 0].set_ylabel('Risk-Adjusted Return')
    axes[1, 0].set_title(f'Stop Loss vs Risk-Adjusted Return (Delay: {delay_ms}ms)')
    axes[1, 0].grid(True, alpha=0.3)

    # 4. Stop Loss Rate vs Total Return (Scatter)
    scatter = axes[1, 1].scatter(df['stop_loss_rate'], df['total_return'],
                                c=df['stop_loss_pct'], cmap='viridis', s=60, alpha=0.7)
    axes[1, 1].set_xlabel('Stop Loss Rate (%)')
    axes[1, 1].set_ylabel('Total Return (%)')
    axes[1, 1].set_title(f'Stop Loss Rate vs Return (Delay: {delay_ms}ms)')
    axes[1, 1].grid(True, alpha=0.3)

    # Add colorbar
    cbar = plt.colorbar(scatter, ax=axes[1, 1])
    cbar.set_label('Stop Loss %')

    plt.tight_layout()

    # Save plot
    plot_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            f'stop_loss_optimization_delay_{delay_ms}ms.png')
    plt.savefig(plot_file, dpi=300, bbox_inches='tight')
    print(f"\n📊 Stop loss optimization plots saved to: {plot_file}")
    plt.close()


def save_stop_loss_optimization_results(df, delay_ms):
    """Save stop loss optimization results to CSV"""
    output_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                              f'stop_loss_optimization_delay_{delay_ms}ms.csv')
    df.to_csv(output_file, index=False)
    print(f"💾 Stop loss optimization results saved to: {output_file}")


if __name__ == "__main__":
    # Run single backtest with specific parameters
    backtester = FundingRateBacktester(
        initial_capital=1000,    # $10,000 starting capital
        delay_ms=2000,            # 100ms server delay
        stop_loss_pct=1,       # 1% stop loss
        leverage=1               # No leverage
    )

    # Run the backtest
    backtester.run_backtest(threshold=-0.003)  # -0.3% funding rate threshold

    # Run delay sensitivity analysis
    # print("\n" + "=" * 80)
    # run_delay_sensitivity_analysis()

    # Run stop loss optimization
    print("\n" + "=" * 80)
    optimize_stop_loss(delay_ms=2000, initial_capital=10000, leverage=1,
                       entry_fee_pct=0.05, exit_fee_pct=0.02, compound=False,
                       threshold=-0.003, stop_loss_range=(0.5, 3.0), step_size=0.1)
