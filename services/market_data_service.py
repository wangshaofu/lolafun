"""
Market Data Service

Handles all market data operations including funding rates, prices, and WebSocket streams.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from trading.funding_analyzer import FundingRateAnalyzer
from trading.book_ticker_stream import BookTickerStream
from trading.websocket_precision_manager import WebSocketPrecisionManager

logger = logging.getLogger(__name__)


class MarketDataService:
    """Service for managing market data streams and analysis"""

    def __init__(self):
        self.precision_manager = WebSocketPrecisionManager()
        self.funding_analyzer = FundingRateAnalyzer(None)  # No REST client needed
        self.book_ticker_stream = BookTickerStream()

        # Connect precision manager to funding stream
        self.funding_analyzer.funding_stream.set_precision_manager(self.precision_manager)
        logger.info("🔗 Connected WebSocket precision manager to funding stream")

    async def initialize(self) -> bool:
        """Initialize market data streams"""
        try:
            logger.info("📡 Starting market data streams...")

            # Start funding rate stream
            await self.funding_analyzer.start_stream()

            # Start book ticker stream
            asyncio.create_task(self.book_ticker_stream.start_stream())

            # Wait for streams to populate data
            if not await self._wait_for_data_population():
                return False

            # Subscribe to symbols with negative funding rates
            await self._subscribe_to_relevant_symbols()

            logger.info("✅ Market data service initialized successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize market data service: {e}")
            return False

    async def cleanup(self):
        """Clean up market data streams"""
        logger.info("🧹 Cleaning up market data service...")

        try:
            await self.funding_analyzer.stop_stream()
            logger.info("✅ Funding stream stopped")
        except Exception as e:
            logger.error(f"Error stopping funding stream: {e}")

        try:
            await self.book_ticker_stream.stop_stream()
            logger.info("✅ Book ticker stream stopped")
        except Exception as e:
            logger.error(f"Error stopping book ticker stream: {e}")

    async def get_negative_funding_opportunities(self) -> List[Dict[str, Any]]:
        """Get symbols with negative funding rates that meet trading criteria"""
        try:
            # DEBUG: Print connection status and data freshness
            logger.info("🔍 DEBUG - Checking funding opportunities...")
            logger.info(f"📊 Funding stream status: Running={self.funding_analyzer.funding_stream.is_running}, Data count={len(self.funding_analyzer.funding_stream.funding_data)}")
            logger.info(f"📊 Book ticker status: Running={self.book_ticker_stream.is_running}, Data count={len(self.book_ticker_stream.ticker_data)}")

            # Check data freshness for SOONUSDT specifically
            soonusdt_data = self.funding_analyzer.funding_stream.get_funding_data('SOONUSDT')
            if soonusdt_data:
                last_update = soonusdt_data.get('updateTimestamp', 0)
                current_time = datetime.now().timestamp()
                age_seconds = current_time - last_update
                logger.info(f"🔍 SOONUSDT data age: {age_seconds:.1f} seconds (last update: {soonusdt_data.get('lastUpdate', 'unknown')})")

            # Get negative funding rates from the funding stream (correct method)
            opportunities = self.funding_analyzer.funding_stream.get_negative_funding_rates()

            # DEBUG: Print detailed opportunity data
            logger.info(f"🔍 Found {len(opportunities)} negative funding opportunities")
            for opp in opportunities:
                symbol = opp['symbol']
                rate = opp['lastFundingRate']
                mark_price = opp['markPrice']
                data_age = datetime.now().timestamp() - opp.get('updateTimestamp', 0)

                # Check if we have book ticker data for this symbol
                book_ticker = self.book_ticker_stream.get_ticker_data(symbol)
                if book_ticker:
                    mid_price = self.book_ticker_stream.get_mid_price(symbol)
                    logger.info(f"📊 {symbol}: Funding={rate*100:.4f}%, Mark=${mark_price:.6f}, BookTicker Mid=${mid_price:.6f}, Age={data_age:.1f}s")
                else:
                    logger.info(f"📊 {symbol}: Funding={rate*100:.4f}%, Mark=${mark_price:.6f}, BookTicker=NO DATA, Age={data_age:.1f}s")

            return opportunities

        except Exception as e:
            logger.error(f"❌ Error getting negative funding opportunities: {e}")
            return []

    async def get_symbol_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current market data for a specific symbol"""
        try:
            return self.funding_analyzer.funding_stream.get_funding_data(symbol)
        except Exception as e:
            logger.error(f"❌ Error getting data for {symbol}: {e}")
            return None

    async def capture_precise_price(self, symbol: str) -> Optional[float]:
        """Capture precise price using book ticker stream"""
        try:
            # Subscribe to book ticker if not already subscribed
            if symbol not in self.book_ticker_stream.get_subscribed_symbols():
                logger.info(f"📊 Subscribing to {symbol} book ticker for precise price")
                await self.book_ticker_stream.subscribe_symbol(symbol)

            # Get mid price (most accurate for execution)
            mid_price = self.book_ticker_stream.get_mid_price(symbol)

            if mid_price:
                ticker_data = self.book_ticker_stream.get_ticker_data(symbol)
                spread = self.book_ticker_stream.get_spread(symbol)
                logger.debug(f"📈 Precise price for {symbol}: Mid=${mid_price:.6f} "
                           f"(Bid=${ticker_data['bestBidPrice']:.6f}, "
                           f"Ask=${ticker_data['bestAskPrice']:.6f}, Spread=${spread:.6f})")
                return mid_price
            else:
                # Fallback to funding stream mark price
                logger.warning(f"⚠️ No book ticker data for {symbol}, using mark price fallback")
                symbol_data = await self.get_symbol_data(symbol)
                return symbol_data.get('markPrice') if symbol_data else None

        except Exception as e:
            logger.error(f"❌ Error capturing precise price for {symbol}: {e}")
            return None

    def get_latest_tick_time(self, symbol: str) -> Optional[int]:
        """Get the event time of the latest book ticker update for a symbol"""
        try:
            ticker_data = self.book_ticker_stream.get_ticker_data(symbol)
            if ticker_data:
                return ticker_data.get('eventTime')
            return None
        except Exception as e:
            logger.error(f"❌ Error getting latest tick time for {symbol}: {e}")
            return None

    async def unsubscribe_symbol_ticker(self, symbol: str):
        """Unsubscribe from book ticker for a symbol"""
        try:
            if symbol in self.book_ticker_stream.get_subscribed_symbols():
                await self.book_ticker_stream.unsubscribe_symbol(symbol)
                logger.info(f"📊 Unsubscribed from {symbol} book ticker")
        except Exception as e:
            logger.error(f"❌ Error unsubscribing from {symbol}: {e}")

    def get_funding_threshold(self) -> float:
        """Get the funding rate threshold for trading decisions"""
        return self.funding_analyzer.funding_threshold

    async def _wait_for_data_population(self, timeout_seconds: int = 10) -> bool:
        """Wait for funding stream to populate with data"""
        logger.info("⏳ Waiting for funding stream to populate data...")

        for i in range(timeout_seconds):
            await asyncio.sleep(1)
            data_count = len(self.funding_analyzer.funding_stream.get_all_funding_data())
            logger.info(f"📊 Funding stream data check {i+1}/{timeout_seconds}: {data_count} symbols")

            if data_count > 0:
                logger.info(f"✅ Funding stream populated with {data_count} symbols")
                return True

        logger.error("❌ Funding stream failed to populate data within timeout")
        return False

    async def _subscribe_to_relevant_symbols(self):
        """Subscribe to symbols that have negative funding rates or are of interest"""
        try:
            # Wait for book ticker stream to connect fully
            logger.info("⏳ Waiting for book ticker stream to establish connection...")
            max_wait = 15  # seconds

            # Wait for the websocket to be initialized (instead of checking closed attribute)
            for i in range(max_wait):
                if self.book_ticker_stream.is_running and self.book_ticker_stream.websocket is not None:
                    logger.info("✅ Book ticker stream connection verified")
                    break
                logger.info(f"⏳ Waiting for book ticker connection... {i+1}/{max_wait}s")
                await asyncio.sleep(1)
            else:
                logger.warning("⚠️ Book ticker connection not fully established after timeout")
                # Add a safety delay to allow more time for connection
                await asyncio.sleep(3)

            # Force a manual reconnect if needed
            if self.book_ticker_stream.websocket is None:
                logger.info("🔄 Attempting to manually connect book ticker stream")
                await self.book_ticker_stream.connect()
                await asyncio.sleep(2)  # Give it time to establish

            # Get symbols with negative funding rates
            opportunities = self.funding_analyzer.funding_stream.get_negative_funding_rates()

            # Subscribe to each symbol's book ticker
            subscription_success = False
            for opp in opportunities:
                symbol = opp['symbol']
                logger.info(f"📊 Auto-subscribing to {symbol} book ticker")
                success = await self.book_ticker_stream.subscribe_symbol(symbol)
                subscription_success = subscription_success or success

            # Specifically ensure SOONUSDT is subscribed (since it appears in your logs)
            if 'SOONUSDT' not in self.book_ticker_stream.get_subscribed_symbols():
                logger.info(f"📊 Auto-subscribing to SOONUSDT book ticker")
                await self.book_ticker_stream.subscribe_symbol('SOONUSDT')

            logger.info(f"✅ Subscribed to {len(self.book_ticker_stream.get_subscribed_symbols())} book tickers")

            if not subscription_success and len(opportunities) > 0:
                logger.warning("⚠️ No successful subscriptions despite attempts. Check websocket connection.")
        except Exception as e:
            logger.error(f"❌ Error subscribing to relevant symbols: {e}", exc_info=True)
