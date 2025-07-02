#!/usr/bin/env python3
"""
Refactored Live Trading Bot for Negative Funding Rate Strategy

This modular bot:
1. Monitors funding rates in real-time
2. Detects severely negative funding rates (≤ -0.3%)
3. Schedules short positions to be placed at funding settlement time
4. Places orders precisely when funding settlement occurs (no delays)
5. Uses proper symbol precision for all orders
6. Manages positions with real-time monitoring
"""

import asyncio
import logging
import configparser
from datetime import datetime
from binance.um_futures import UMFutures
from typing import Dict, List

# Import our refactored modules
from utils.ntp_sync import NTPTimeSync
from market.data_stream import MarketDataStream
from trading.position import TradingPosition
from trading.precision_manager import SymbolPrecisionManager
from trading.websocket_client import WebSocketTradingClient
from trading.funding_analyzer import FundingRateAnalyzer

# Setup comprehensive logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("live_trading_bot.log", mode="a"),
        logging.StreamHandler()  # Also log to console
    ]
)
logger = logging.getLogger(__name__)


class ScheduledTrade:
    """Represents a scheduled trade to be executed at funding settlement time"""
    def __init__(self, symbol: str, funding_rate: float, settlement_time_ms: int, current_price: float):
        self.symbol = symbol
        self.funding_rate = funding_rate
        self.settlement_time_ms = settlement_time_ms
        self.current_price = current_price
        self.pre_settlement_price = None  # Price 500ms before settlement
        self.is_executed = False
        self.ntp_sync_done = False  # Track if NTP sync was done before settlement

    def should_execute(self, current_time_ms: int) -> bool:
        """Check if this trade should be executed now"""
        # Execute when we reach the settlement time (with small buffer for precision)
        return current_time_ms >= self.settlement_time_ms and not self.is_executed
    def should_capture_pre_settlement_price(self, current_time_ms: int) -> bool:
        """Check if we should capture the price 100ms before settlement"""
        # Capture price when we're 100ms before settlement time (with 10ms tolerance)
        target_time = self.settlement_time_ms - 500
        return (current_time_ms >= target_time and self.pre_settlement_price is None)

    def should_force_ntp_sync(self, current_time_ms: int) -> bool:
        """Check if we should force NTP sync (10 seconds before settlement)"""
        sync_time = self.settlement_time_ms - 10000  # 10 seconds before
        return (current_time_ms >= sync_time and not self.ntp_sync_done)


class LiveTradingBot:
    """Main trading bot class - refactored and modular"""
    def __init__(self, config_path='config.ini'):
        # Load configuration
        self.config = self.load_config(config_path)
        # Initialize Binance client with standard API key/secret for REST API
        self.client = UMFutures(
            key=self.config['ACCOUNT']['APIKey'],  # Use ACCOUNT section for REST API
            secret=self.config['ACCOUNT']['APISecret']
        )
        # Initialize core components
        self.ntp_sync = NTPTimeSync()
        self.precision_manager = SymbolPrecisionManager(self.client)
        # Initialize WebSocket client with Ed25519 authentication
        self.websocket_client = WebSocketTradingClient(
            self.config['FUTURE_ACCOUNT']['APIKey'],  # API key from config
            'private_key.pem',  # Ed25519 private key file for WebSocket orders
            self.precision_manager,
            self.ntp_sync
        )
        self.funding_analyzer = FundingRateAnalyzer(self.client)
        # Trading state
        self.market_streams: Dict[str, MarketDataStream] = {}
        self.active_positions: List[TradingPosition] = []
        self.scheduled_trades: List[ScheduledTrade] = []  # List of scheduled trades
        # Trading parameters
        self.position_size_usd = 100.0  # $100 per trade
        self.max_concurrent_positions = 20  # Allow up to 20 concurrent positions
        self.max_symbols_per_scan = 20  # Process top 10 negative funding rates per scan
        # Initialize NTP sync
        logger.info("🕐 Synchronizing with NTP servers for precise timing...")
        self.ntp_sync.sync_time()
        logger.info("✅ NTP synchronization completed")

    def load_config(self, path: str) -> configparser.ConfigParser:
        """Load configuration file"""
        config = configparser.ConfigParser()
        config.read(path)
        return config

    async def initialize(self) -> bool:
        """Initialize all components"""
        try:
            logger.info("🔧 Initializing trading bot components...")
            # Initialize precision manager
            if not await self.precision_manager.initialize():
                logger.error("❌ Failed to initialize precision manager")
                return False
            # Initialize WebSocket connection early for faster order execution
            if not await self.websocket_client.initialize_connection():
                logger.error("❌ Failed to initialize WebSocket trading connection")
                return False
            logger.info("✅ All components initialized successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize bot: {e}")
            return False

    async def setup_market_stream(self, symbol: str) -> bool:
        """Setup market data stream for a symbol"""
        if symbol in self.market_streams:
            return True
        stream = MarketDataStream(symbol)
        if await stream.connect():
            self.market_streams[symbol] = stream
            # Start the stream in background
            asyncio.create_task(stream.start_stream(self.ntp_sync))
            # Wait a moment for initial data
            await asyncio.sleep(0.5)
            return True
        return False

    async def calculate_position_quantity(self, symbol: str, current_price: float) -> float:
        """Calculate position quantity based on position size and current price"""
        try:
            # Calculate base quantity
            base_quantity = self.position_size_usd / current_price
            # Round to proper precision
            rounded_quantity = self.precision_manager.round_quantity(symbol, base_quantity)
            # Validate minimum notional
            if not self.precision_manager.validate_order_size(symbol, rounded_quantity, current_price):
                logger.warning(f"Order size {rounded_quantity} * {current_price} below minimum notional for {symbol}")
                return 0.0
            return rounded_quantity
        except Exception as e:
            logger.error(f"Error calculating position quantity for {symbol}: {e}")
            return 0.0

    async def scan_funding_opportunities(self):
        """Scan for new funding rate opportunities and schedule them for settlement time"""
        try:
            # Get negative funding rates
            funding_rates = await self.funding_analyzer.get_negative_funding_rates()
            # Clean up scheduled trades that are no longer viable
            await self.cleanup_stale_scheduled_trades(funding_rates if funding_rates else [])
            if not funding_rates:
                return
            logger.info(f"📊 Found {len(funding_rates)} symbols with negative funding rates")
            current_time = self.ntp_sync.get_ntp_time_ms()
            scheduled_count = 0
            # Process top symbols (limit to avoid overloading)
            for rate_data in funding_rates:
                if scheduled_count >= self.max_symbols_per_scan:
                    break
                if len(self.active_positions) + len(self.scheduled_trades) >= self.max_concurrent_positions:
                    logger.info(f"⚠️ Maximum concurrent positions/scheduled trades ({self.max_concurrent_positions}) reached")
                    break
                symbol = rate_data['symbol']
                funding_rate = rate_data['lastFundingRate']
                current_price = rate_data['markPrice']
                settlement_time = rate_data['nextFundingTime']
                # Check if we already have a position or scheduled trade for this symbol
                if any(pos.symbol == symbol and pos.is_active for pos in self.active_positions):
                    continue
                if any(trade.symbol == symbol and not trade.is_executed for trade in self.scheduled_trades):
                    continue
                # Check if settlement time is in the future (with reasonable buffer)
                time_until_settlement = settlement_time - current_time
                if time_until_settlement <= 1000:  # Less than 1 second away or in the past
                    logger.info(f"⏰ Funding settlement for {symbol} is too close or in the past, skipping")
                    continue
                # Setup market stream for this symbol
                if not await self.setup_market_stream(symbol):
                    logger.warning(f"Failed to setup market stream for {symbol}")
                    continue
                # Get more accurate price from stream
                if symbol in self.market_streams:
                    stream_price = self.market_streams[symbol].get_mid_price()
                    if stream_price > 0:
                        current_price = stream_price
                # Schedule the trade
                scheduled_trade = ScheduledTrade(symbol, funding_rate, settlement_time, current_price)
                self.scheduled_trades.append(scheduled_trade)
                settlement_datetime = datetime.fromtimestamp(settlement_time / 1000)
                logger.info(f"📅 SCHEDULED TRADE - {symbol}")
                logger.info(f"   Funding Rate: {funding_rate * 100:.3f}%")
                logger.info(f"   Settlement Time: {settlement_datetime}")
                logger.info(f"   Time Until Settlement: {time_until_settlement / 1000:.1f} seconds")
                scheduled_count += 1
        except Exception as e:
            logger.error(f"Error scanning funding opportunities: {e}")

    async def execute_scheduled_trade(self, scheduled_trade: ScheduledTrade) -> bool:
        """Execute a scheduled trade immediately without any delays"""
        try:
            symbol = scheduled_trade.symbol
            funding_rate = scheduled_trade.funding_rate
            # Get the most current price from market stream for execution
            current_price = scheduled_trade.current_price
            if symbol in self.market_streams:
                stream_price = self.market_streams[symbol].get_mid_price()
                if stream_price > 0:
                    current_price = stream_price
            # Calculate position quantity based on current execution price
            quantity = await self.calculate_position_quantity(symbol, current_price)
            if quantity <= 0:
                logger.warning(f"Cannot execute scheduled trade for {symbol}: invalid quantity")
                return False
            # Use pre-settlement price for stop loss and take profit calculations if available
            price_for_sl_tp = scheduled_trade.pre_settlement_price if scheduled_trade.pre_settlement_price else current_price
            # Calculate trade levels using the pre-settlement price
            stop_loss_price, take_profit_price = self.funding_analyzer.calculate_trade_levels(
                funding_rate, price_for_sl_tp
            )
            logger.info(f"⚡ EXECUTING SCHEDULED TRADE AT FUNDING SETTLEMENT - {symbol}")
            if scheduled_trade.pre_settlement_price:
                logger.info(f"   Using pre-settlement price (${price_for_sl_tp:.6f}) for SL/TP calculations")
            else:
                logger.info(f"   Using current price (${price_for_sl_tp:.6f}) for SL/TP calculations (pre-settlement price not captured)")
            # Place market short order immediately
            order_result = await self.websocket_client.place_market_order(symbol, "SELL", quantity)
            if not order_result:
                logger.error(f"Failed to place scheduled short order for {symbol}")
                return False
            # Use actual fill price from order response (accounting for slippage)
            actual_entry_price = order_result.get("actualFillPrice", current_price)
            actual_quantity = order_result.get("actualQuantity", quantity)
            # Log slippage information
            if actual_entry_price != current_price:
                slippage = ((actual_entry_price - current_price) / current_price) * 100
                logger.info(f"📊 SLIPPAGE DETECTED - {symbol}")
                logger.info(f"   Expected Price: ${current_price:.6f}")
                logger.info(f"   Actual Fill Price: ${actual_entry_price:.6f}")
                logger.info(f"   Slippage: {slippage:.4f}%")
            # Create position object with ACTUAL fill price
            current_time = self.ntp_sync.get_ntp_time_ms()
            position = TradingPosition(
                symbol=symbol,
                entry_price=actual_entry_price,  # Use actual fill price, not estimated price
                position_size=self.position_size_usd,
                stop_loss=stop_loss_price,
                take_profit=take_profit_price,
                entry_time=current_time
            )
            self.active_positions.append(position)
            # Place stop loss and take profit orders immediately using actual quantity
            await self.websocket_client.place_take_profit_order(symbol, actual_quantity, take_profit_price)
            await self.websocket_client.place_stop_loss_order(symbol, actual_quantity, stop_loss_price)
            logger.info(f"🎯 FUNDING SETTLEMENT SHORT POSITION OPENED - {symbol}")
            logger.info(f"   Entry Price: ${actual_entry_price:.6f} (ACTUAL FILL)")
            logger.info(f"   Quantity: {self.precision_manager.format_quantity(symbol, actual_quantity)} (ACTUAL FILL)")
            logger.info(f"   Stop Loss: ${self.precision_manager.format_price(symbol, stop_loss_price)} (based on ${price_for_sl_tp:.6f})")
            logger.info(f"   Take Profit: ${self.precision_manager.format_price(symbol, take_profit_price)} (based on ${price_for_sl_tp:.6f})")
            logger.info(f"   Funding Rate: {funding_rate * 100:.3f}%")
            # Mark trade as executed
            scheduled_trade.is_executed = True
            return True
        except Exception as e:
            logger.error(f"Error executing scheduled trade for {scheduled_trade.symbol}: {e}")
            return False

    async def monitor_scheduled_trades(self):
        """Monitor scheduled trades and execute them at the right time"""
        current_time = int(self.ntp_sync.get_ntp_time_ms())
        for scheduled_trade in self.scheduled_trades[:]:  # Create copy to avoid modification during iteration
            # Check if we should force NTP sync (10 seconds before settlement)
            if scheduled_trade.should_force_ntp_sync(current_time):
                logger.info(f"🕐 FORCING NTP SYNC 10 seconds before settlement for {scheduled_trade.symbol}")
                success = self.ntp_sync.force_sync_before_settlement(scheduled_trade.settlement_time_ms)
                if success:
                    scheduled_trade.ntp_sync_done = True
                    logger.info(f"✅ Pre-settlement NTP sync completed for {scheduled_trade.symbol}")
                else:
                    logger.warning(f"⚠️ Pre-settlement NTP sync failed for {scheduled_trade.symbol}")
            # Check if we should capture pre-settlement price (500ms before settlement)
            elif scheduled_trade.should_capture_pre_settlement_price(current_time):
                symbol = scheduled_trade.symbol
                if symbol in self.market_streams:
                    stream_price = self.market_streams[symbol].get_mid_price()
                    if stream_price > 0:
                        scheduled_trade.pre_settlement_price = stream_price
                        logger.info(f"📈 CAPTURED PRE-SETTLEMENT PRICE for {symbol}: ${stream_price:.6f}")
                        logger.info(f"   This price will be used for Stop Loss and Take Profit calculations")
                    else:
                        logger.warning(f"⚠️ Failed to capture pre-settlement price for {symbol} - using current price")
                        scheduled_trade.pre_settlement_price = scheduled_trade.current_price
                else:
                    logger.warning(f"⚠️ No market stream for {symbol} - using current price for SL/TP")
                    scheduled_trade.pre_settlement_price = scheduled_trade.current_price
            # Check if we should execute the trade
            elif scheduled_trade.should_execute(current_time):
                logger.info(f"🕒 Funding settlement time reached for {scheduled_trade.symbol}")
                success = await self.execute_scheduled_trade(scheduled_trade)
                if success:
                    logger.info(f"✅ Successfully executed scheduled trade for {scheduled_trade.symbol}")
                else:
                    logger.error(f"❌ Failed to execute scheduled trade for {scheduled_trade.symbol}")
                self.scheduled_trades.remove(scheduled_trade)

    async def execute_trading_strategy(self):
        """Main trading strategy execution loop"""
        logger.info("🚀 Starting Refactored Live Funding Rate Trading Bot")
        logger.info("=" * 60)
        logger.info(f"📊 Position Size: ${self.position_size_usd:.2f}")
        logger.info(f"🎯 Funding Threshold: {self.funding_analyzer.funding_threshold*100:.2f}%")
        logger.info(f"🛑 Stop Loss: {self.funding_analyzer.stop_loss_pct:.1f}%")
        logger.info(f"📈 Max Concurrent Positions: {self.max_concurrent_positions}")
        logger.info("⏰ Orders will be placed exactly at funding settlement time")
        scan_counter = 0
        try:
            while True:
                # Monitor existing positions
                await self.monitor_positions()
                # Monitor scheduled trades and execute when time comes (check frequently for precision)
                await self.monitor_scheduled_trades()
                if scan_counter % 300 == 0:
                    await self.scan_funding_opportunities()
                if scan_counter % 300 == 0:
                    active_count = len([pos for pos in self.active_positions if pos.is_active])
                    scheduled_count = len([trade for trade in self.scheduled_trades if not trade.is_executed])
                    logger.info(f"📊 Active positions: {active_count}, Scheduled trades: {scheduled_count}")
                    # Log detailed information about scheduled trades
                    if self.scheduled_trades:
                        logger.info("📅 SCHEDULED TRADES DETAIL:")
                        for i, trade in enumerate(self.scheduled_trades, 1):
                            if not trade.is_executed:
                                settlement_datetime = datetime.fromtimestamp(trade.settlement_time_ms / 1000)
                                time_until_settlement = (trade.settlement_time_ms - self.ntp_sync.get_ntp_time_ms()) / 1000
                                # Get real-time current price from market stream
                                current_price = trade.current_price  # fallback to original price
                                if trade.symbol in self.market_streams:
                                    stream_price = self.market_streams[trade.symbol].get_mid_price()
                                    if stream_price > 0:
                                        current_price = stream_price
                                        # Update the trade's current price for future reference
                                        trade.current_price = current_price
                                logger.info(f"   {i}. {trade.symbol}")
                                logger.info(f"      📈 Funding Rate: {trade.funding_rate * 100:.4f}%")
                                logger.info(f"      💰 Current Price: ${current_price:.6f}")
                                logger.info(f"      ⏰ Settlement: {settlement_datetime.strftime('%H:%M:%S')}")
                                logger.info(f"      ⏳ Time Until: {time_until_settlement:.1f}s")
                                if trade.pre_settlement_price:
                                    logger.info(f"      📊 Pre-settlement Price: ${trade.pre_settlement_price:.6f}")
                                if trade.ntp_sync_done:
                                    logger.info(f"      🕐 NTP Sync: ✅ Done")
                    # Log detailed information about active positions
                    if self.active_positions:
                        active_positions = [pos for pos in self.active_positions if pos.is_active]
                        if active_positions:
                            logger.info("💼 ACTIVE POSITIONS DETAIL:")
                            for i, pos in enumerate(active_positions, 1):
                                current_price = pos.current_price if hasattr(pos, 'current_price') else 0
                                if pos.symbol in self.market_streams:
                                    stream_price = self.market_streams[pos.symbol].get_mid_price()
                                    if stream_price > 0:
                                        current_price = stream_price
                                pnl = 0
                                if current_price > 0:
                                    pnl = ((pos.entry_price - current_price) / pos.entry_price) * pos.position_size
                                position_age = (self.ntp_sync.get_ntp_time_ms() - pos.entry_time) / 1000 / 60  # minutes
                                logger.info(f"   {i}. {pos.symbol}")
                                logger.info(f"      📊 Entry: ${pos.entry_price:.6f} | Current: ${current_price:.6f}")
                                logger.info(f"      💰 Position Size: ${pos.position_size:.2f}")
                                logger.info(f"      📈 P&L: ${pnl:.2f} ({(pnl/pos.position_size)*100:.2f}%)")
                                logger.info(f"      🛡️ Stop Loss: ${pos.stop_loss:.6f}")
                                logger.info(f"      🎯 Take Profit: ${pos.take_profit:.6f}")
                                logger.info(f"      ⏰ Age: {position_age:.1f} minutes")
                scan_counter += 1
                # Dynamic sleep based on upcoming scheduled trades for precise timing
                sleep_duration = await self.calculate_optimal_sleep_duration()
                await asyncio.sleep(sleep_duration)
        except KeyboardInterrupt:
            logger.info("🛑 Bot stopped by user")
        except Exception as e:
            logger.error(f"❌ Unexpected error in trading loop: {e}")
            raise
        finally:
            await self.cleanup()

    async def calculate_optimal_sleep_duration(self) -> float:
        """Calculate optimal sleep duration based on upcoming scheduled trades"""
        if not self.scheduled_trades:
            return 1.0  # Default 1 second if no trades scheduled
        current_time = self.ntp_sync.get_ntp_time_ms()
        # Find the next critical event time
        next_critical_time = None
        for trade in self.scheduled_trades:
            if trade.is_executed:
                continue
            # Check for various critical times
            critical_times = []
            # NTP sync time (10 seconds before settlement)
            ntp_sync_time = trade.settlement_time_ms - 10000
            if not trade.ntp_sync_done and ntp_sync_time > current_time:
                critical_times.append(ntp_sync_time)
            # Pre-settlement price capture time (100ms before settlement)
            pre_settlement_time = trade.settlement_time_ms - 100
            if trade.pre_settlement_price is None and pre_settlement_time > current_time:
                critical_times.append(pre_settlement_time)
            # Settlement execution time
            if trade.settlement_time_ms > current_time:
                critical_times.append(trade.settlement_time_ms)
            # Find the earliest critical time
            for critical_time in critical_times:
                if next_critical_time is None or critical_time < next_critical_time:
                    next_critical_time = critical_time
        if next_critical_time is None:
            return 1.0  # Default 1 second
        time_until_critical = (next_critical_time - current_time) / 1000.0  # Convert to seconds
        # If critical event is very soon (within 5 seconds), use high-frequency checking
        if time_until_critical <= 5.0:
            return 0  # No sleep - check immediately for maximum precision
        else:
            return 1.0   # Check every 1 second for distant events

    async def cleanup(self):
        """Clean up resources"""
        logger.info("🧹 Cleaning up resources...")
        # Close WebSocket connections
        await self.websocket_client.close()
        for stream in self.market_streams.values():
            await stream.close()
        logger.info("✅ Cleanup completed")

    async def cleanup_stale_scheduled_trades(self, current_funding_rates: List[Dict]):
        """Remove scheduled trades that are no longer viable (e.g., funding rate no longer negative enough)"""
        try:
            for trade in self.scheduled_trades[:]:  # Create copy to avoid modification during iteration
                # If trade is executed, skip it
                if trade.is_executed:
                    continue
                # Check if the symbol still has a sufficiently negative funding rate
                symbol_funding_data = next((rate for rate in current_funding_rates if rate['symbol'] == trade.symbol), None)
                current_funding_rate = symbol_funding_data['lastFundingRate']
                if current_funding_rate > self.funding_analyzer.funding_threshold:
                    removal_reason = f"funding rate improved to {current_funding_rate * 100:.3f}% (above {self.funding_analyzer.funding_threshold * 100:.1f}% threshold)"
                    logger.info(f"🗑️ Removing stale scheduled trade for {trade.symbol} - {removal_reason}")
                    self.scheduled_trades.remove(trade)
                    continue
        except Exception as e:
            logger.error(f"Error cleaning up stale scheduled trades: {e}")


async def main():
    """Main entry point"""
    bot = LiveTradingBot()
    # Initialize bot
    if not await bot.initialize():
        logger.error("❌ Failed to initialize bot")
        return
    # Start trading
    await bot.execute_trading_strategy()


if __name__ == "__main__":
    asyncio.run(main())
