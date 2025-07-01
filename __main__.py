import configparser
import time
import traceback
import logging
from datetime import datetime, timedelta
from binance.um_futures import UMFutures

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename="funding_rate_bot.log",  # Log file name.
    filemode="a"  # Append mode.
)
logger = logging.getLogger(__name__)


def load_config(path='config.ini'):
    config = configparser.ConfigParser()
    config.read(path)
    return config


def get_available_funds(client, base_currency):
    try:
        account_info = client.get_account()
        for balance in account_info["balances"]:
            if balance["asset"] == base_currency:
                funds = float(balance["free"])
                logger.info(f"Available {base_currency}: {funds}")
                return funds
    except Exception as e:
        logger.error(f"Error fetching account balance for {base_currency}: {e}")
    return 0.0


def get_futures_account_balance(client):
    """Get futures account balance information."""
    try:
        account_info = client.account()
        total_wallet_balance = float(account_info.get('totalWalletBalance', 0))
        total_unrealized_pnl = float(account_info.get('totalUnrealizedPnL', 0))
        total_margin_balance = float(account_info.get('totalMarginBalance', 0))
        available_balance = float(account_info.get('availableBalance', 0))

        logger.info(f"Futures Account Balance:")
        logger.info(f"  Total Wallet Balance: {total_wallet_balance} USDT")
        logger.info(f"  Total Unrealized PnL: {total_unrealized_pnl} USDT")
        logger.info(f"  Total Margin Balance: {total_margin_balance} USDT")
        logger.info(f"  Available Balance: {available_balance} USDT")

        return {
            'totalWalletBalance': total_wallet_balance,
            'totalUnrealizedPnL': total_unrealized_pnl,
            'totalMarginBalance': total_margin_balance,
            'availableBalance': available_balance
        }
    except Exception as e:
        logger.error(f"Error fetching futures account balance: {e}")
        return None


def get_all_funding_rates(client):
    """Get all funding rates and sort by negative rates first."""
    try:
        # Get mark price and funding rate data for all symbols (don't pass symbol parameter)
        funding_data = client.mark_price()

        # Convert to list and extract funding rates
        funding_rates = []
        for item in funding_data:
            try:
                funding_rate = float(item['lastFundingRate'])
                funding_rates.append({
                    'symbol': item['symbol'],
                    'markPrice': float(item['markPrice']),
                    'indexPrice': float(item['indexPrice']),
                    'lastFundingRate': funding_rate,
                    'lastFundingRatePercent': funding_rate * 100,
                    'interestRate': float(item['interestRate']),
                    'nextFundingTime': item['nextFundingTime'],
                    'nextFundingTimeReadable': datetime.utcfromtimestamp(item['nextFundingTime'] // 1000).strftime('%Y-%m-%d %H:%M:%S'),
                    'time': item['time']
                })
            except (ValueError, KeyError) as e:
                logger.warning(f"Error processing funding data for {item.get('symbol', 'unknown')}: {e}")
                continue

        # Sort by funding rate (negative first)
        funding_rates.sort(key=lambda x: x['lastFundingRate'])

        logger.info(f"Retrieved funding rates for {len(funding_rates)} symbols")
        logger.info(f"Most negative funding rate: {funding_rates[0]['lastFundingRate']:.6f} ({funding_rates[0]['symbol']})")
        logger.info(f"Most positive funding rate: {funding_rates[-1]['lastFundingRate']:.6f} ({funding_rates[-1]['symbol']})")

        return funding_rates

    except Exception as e:
        logger.error(f"Error fetching funding rates: {e}")
        return []


def display_negative_funding_rates(funding_rates, threshold=-0.001):
    """Display funding rates below the threshold (default -0.1%)."""
    negative_rates = [rate for rate in funding_rates if rate['lastFundingRate'] <= threshold]

    if negative_rates:
        print(f"\n📉 Found {len(negative_rates)} symbols with funding rates <= {threshold * 100:.3f}%:")
        print("-" * 80)
        print(f"{'Symbol':<20} {'Funding Rate':<15} {'Percentage':<12} {'Mark Price':<15} {'Next Funding'}")
        print("-" * 80)

        for rate in negative_rates:
            print(f"{rate['symbol']:<20} {rate['lastFundingRate']:<15.6f} {rate['lastFundingRatePercent']:<12.4f}% ${rate['markPrice']:<14.4f} {rate['nextFundingTimeReadable']}")
    else:
        print(f"\n✅ No symbols found with funding rates <= {threshold * 100:.3f}%")


def display_top_funding_rates(funding_rates, top_n=10):
    """Display top N most negative and positive funding rates."""
    print(f"\n🔴 Top {top_n} Most NEGATIVE Funding Rates:")
    print("-" * 70)
    print(f"{'Symbol':<20} {'Funding Rate':<15} {'Percentage':<12} {'Mark Price'}")
    print("-" * 70)

    for rate in funding_rates[:top_n]:
        print(f"{rate['symbol']:<20} {rate['lastFundingRate']:<15.6f} {rate['lastFundingRatePercent']:<12.4f}% ${rate['markPrice']:.4f}")

    print(f"\n🟢 Top {top_n} Most POSITIVE Funding Rates:")
    print("-" * 70)
    print(f"{'Symbol':<20} {'Funding Rate':<15} {'Percentage':<12} {'Mark Price'}")
    print("-" * 70)

    for rate in funding_rates[-top_n:]:
        print(f"{rate['symbol']:<20} {rate['lastFundingRate']:<15.6f} {rate['lastFundingRatePercent']:<12.4f}% ${rate['markPrice']:.4f}")


def main():
    """Main function to run the funding rate analysis."""
    try:
        # Load configuration
        config = load_config()

        # Initialize Binance Futures client
        client = UMFutures(
            key=config['ACCOUNT']['APIKey'],
            secret=config['ACCOUNT']['APISecret']
        )

        print("=" * 60)
        print("BINANCE FUTURES FUNDING RATE ANALYZER")
        print("=" * 60)

        # Get futures account balance
        print("\n📊 Fetching futures account balance...")
        balance_info = get_futures_account_balance(client)

        if balance_info:
            print(f"\n💰 Account Summary:")
            print(f"  Available Balance: ${balance_info['availableBalance']:,.2f} USDT")
            print(f"  Total Margin Balance: ${balance_info['totalMarginBalance']:,.2f} USDT")
            print(f"  Unrealized PnL: ${balance_info['totalUnrealizedPnL']:,.2f} USDT")

        # Get all funding rates
        print("\n📈 Fetching current funding rates for all symbols...")
        funding_rates = get_all_funding_rates(client)

        if not funding_rates:
            print("❌ Failed to retrieve funding rates")
            return

        # Display analysis
        display_top_funding_rates(funding_rates, top_n=10)
        display_negative_funding_rates(funding_rates, threshold=-0.003)  # -0.3%

        # Save current funding rates summary
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"current_funding_rates_{timestamp}.csv"

        import csv
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['symbol', 'lastFundingRate', 'lastFundingRatePercent', 'markPrice', 'indexPrice',
                         'interestRate', 'nextFundingTime', 'nextFundingTimeReadable']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for rate in funding_rates:
                writer.writerow({
                    'symbol': rate['symbol'],
                    'lastFundingRate': rate['lastFundingRate'],
                    'lastFundingRatePercent': rate['lastFundingRatePercent'],
                    'markPrice': rate['markPrice'],
                    'indexPrice': rate['indexPrice'],
                    'interestRate': rate['interestRate'],
                    'nextFundingTime': rate['nextFundingTime'],
                    'nextFundingTimeReadable': rate['nextFundingTimeReadable']
                })

        print(f"\n💾 Saved current funding rates to: {filename}")

        # Display next funding time
        if funding_rates:
            next_funding = funding_rates[0]['nextFundingTimeReadable']
            print(f"\n⏰ Next funding time: {next_funding}")

    except Exception as e:
        logger.error(f"Error in main function: {e}")
        print(f"❌ Error: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
