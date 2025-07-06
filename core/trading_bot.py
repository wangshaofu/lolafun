"""
Trading Bot Core Components

Main bot class and core trading logic separated from the monolithic file.
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Optional

from core.config import TradingConfig
from core.scheduled_trade import ScheduledTrade
from services.market_data_service import MarketDataService
from services.trading_service import TradingService
from services.timing_service import TimingService
from utils.ntp_sync import NTPTimeSync

logger = logging.getLogger(__name__)


class LiveTradingBot:
    """Main trading bot orchestrator with cleaner separation of concerns"""

    def __init__(self, config_path: str = 'config.ini'):
        # Load configuration
        self.config = TradingConfig(config_path)

        # Initialize services
        self.timing_service = TimingService()
        self.market_data_service = MarketDataService()
        self.trading_service = TradingService(
            self.config,
            self.timing_service.ntp_sync,
            self.market_data_service.precision_manager
        )

        # Trading state
        self.scheduled_trades: List[ScheduledTrade] = []
        self.is_running = False

        # Trading parameters from config
        params = self.config.get_trading_params()
        self.position_size_usd = params['position_size_usd']
        self.scan_interval_ms = params['scan_interval_ms']

        logger.info(f"🤖 Trading bot initialized with ${self.position_size_usd} position size")

    async def initialize(self) -> bool:
        """Initialize all bot services and connections"""
        logger.info("🔧 Initializing trading bot services...")

        try:
            # Initialize timing service (NTP sync)
            if not await self.timing_service.initialize():
                logger.error("❌ Failed to initialize timing service")
                return False

            # Initialize market data service
            if not await self.market_data_service.initialize():
                logger.error("❌ Failed to initialize market data service")
                return False

            # Initialize trading service
            if not await self.trading_service.initialize():
                logger.error("❌ Failed to initialize trading service")
                return False

            logger.info("✅ All services initialized successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Critical error during initialization: {e}")
            return False

    async def cleanup(self):
        """Clean up all services and connections"""
        logger.info("🧹 Cleaning up trading bot...")

        self.is_running = False

        # Cleanup services in reverse order
        await self.trading_service.cleanup()
        await self.market_data_service.cleanup()
        await self.timing_service.cleanup()

        logger.info("✅ Cleanup completed")

    async def scan_for_opportunities(self) -> int:
        """Scan for new funding rate opportunities"""
        logger.debug("🔍 Scanning for funding opportunities...")

        # Get negative funding rates from market data service
        opportunities = await self.market_data_service.get_negative_funding_opportunities()

        if not opportunities:
            logger.info("🔍 No negative funding opportunities found")
            return 0

        scheduled_count = 0
        current_time = self.timing_service.get_current_time_ms()

        for opportunity in opportunities:
            symbol = opportunity['symbol']

            # Check if already scheduled
            if self._is_already_scheduled(symbol):
                continue

            # Create and schedule trade
            scheduled_trade = ScheduledTrade.from_opportunity(
                opportunity,
                self.position_size_usd,
                self.config.get_trading_params()  # Pass trading config for timing parameters
            )
            self.scheduled_trades.append(scheduled_trade)

            # Log scheduling details
            self._log_scheduled_trade(scheduled_trade, current_time)
            scheduled_count += 1

        if scheduled_count > 0:
            logger.info(f"✅ Scheduled {scheduled_count} new trades")

        return scheduled_count

    async def monitor_scheduled_trades(self):
        """Monitor and execute scheduled trades"""
        if not self.scheduled_trades:
            return

        current_time = self.timing_service.get_current_time_ms()

        for trade in self.scheduled_trades[:]:  # Copy list to allow safe removal
            try:
                # Update trade with current market data
                await self._update_trade_data(trade)

                # Check if trade should be removed (funding rate improved)
                if await self._should_remove_trade(trade):
                    self.scheduled_trades.remove(trade)
                    continue

                # Handle trade timing events
                await self._handle_trade_timing(trade, current_time)

                # Execute trade if it's time
                if trade.should_execute(current_time):
                    success = await self.trading_service.execute_trade(trade)
                    if success:
                        self.scheduled_trades.remove(trade)
                        logger.info(f"✅ Trade executed and removed: {trade.symbol}")

            except Exception as e:
                logger.error(f"❌ Error monitoring trade {trade.symbol}: {e}")

    async def run(self):
        """Main trading loop"""
        logger.info("🚀 Starting trading bot main loop")
        self.is_running = True

        last_scan_time = 0

        while self.is_running:
            try:
                current_time = self.timing_service.get_current_time_ms()

                # Monitor scheduled trades (high frequency)
                await self.monitor_scheduled_trades()

                # Scan for new opportunities (lower frequency)
                if current_time - last_scan_time >= self.scan_interval_ms:
                    await self.scan_for_opportunities()
                    last_scan_time = current_time

                # Clean up completed trades
                self._cleanup_completed_trades()


            except Exception as e:
                logger.error(f"❌ Error in main trading loop: {e}")
                await asyncio.sleep(1)  # Longer delay on error

    def _is_already_scheduled(self, symbol: str) -> bool:
        """Check if symbol is already scheduled for trading"""
        return any(trade.symbol == symbol and not trade.is_executed for trade in self.scheduled_trades)

    async def _update_trade_data(self, trade: ScheduledTrade):
        """Update trade with current market data"""
        current_data = await self.market_data_service.get_symbol_data(trade.symbol)
        if current_data:
            trade.update_market_data(current_data)

    async def _should_remove_trade(self, trade: ScheduledTrade) -> bool:
        """Check if trade should be removed due to improved funding rate"""
        threshold = self.config.get_trading_params()['funding_threshold']
        if trade.funding_rate > threshold:
            logger.info(f"🗑️ Removing {trade.symbol}: Funding rate improved to {trade.funding_rate*100:.3f}%")
            return True
        return False

    async def _handle_trade_timing(self, trade: ScheduledTrade, current_time: int):
        """Handle timing-based events for trades"""
        # NTP sync before settlement
        if trade.should_force_ntp_sync(current_time):
            await self.timing_service.force_sync_for_settlement(trade.settlement_time_ms)
            trade.ntp_sync_done = True

        # Capture pre-settlement price
        if trade.should_capture_pre_settlement_price(current_time):
            price = await self.market_data_service.capture_precise_price(trade.symbol)
            if price:
                trade.pre_settlement_price = price
                logger.info(f"📈 Pre-settlement price captured for {trade.symbol}: ${price:.6f}")

    def _log_scheduled_trade(self, trade: ScheduledTrade, current_time: int):
        """Log details of a newly scheduled trade"""
        settlement_datetime = datetime.fromtimestamp(trade.settlement_time_ms / 1000)
        time_to_settlement = (trade.settlement_time_ms - current_time) // 1000

        logger.info(f"📅 SCHEDULED: {trade.symbol}")
        logger.info(f"    💰 Rate: {trade.funding_rate * 100:.4f}% | Price: ${trade.current_price:.6f}")
        logger.info(f"    📊 Position: {trade.quantity:.4f} {trade.symbol} (${trade.position_value:.2f})")
        logger.info(f"    ⏰ Settlement: {settlement_datetime.strftime('%H:%M:%S')} ({time_to_settlement}s)")

    def _cleanup_completed_trades(self):
        """Remove completed trades from the list"""
        initial_count = len(self.scheduled_trades)
        self.scheduled_trades = [t for t in self.scheduled_trades if not t.is_executed]
        removed_count = initial_count - len(self.scheduled_trades)

        if removed_count > 0:
            logger.debug(f"🧹 Cleaned up {removed_count} completed trades")
