"""
Funding Rate Analyzer

Handles funding rate data fetching and analysis for trading decisions.
Now uses WebSocket Mark Price Stream for real-time updates.
"""

import asyncio
import logging
from typing import Dict, List, Tuple, Optional
from binance.um_futures import UMFutures
from trading.funding_stream import FundingRateStream

logger = logging.getLogger(__name__)


class FundingRateAnalyzer:
    """Analyzes funding rates and calculates trading levels using real-time WebSocket data"""

    def __init__(self, client: UMFutures = None, update_speed: str = "1000ms"):
        # Note: client is now optional since we use pure WebSocket
        self.client = client  # Keep for backward compatibility, but not used
        # Trading parameters
        self.funding_threshold = -0.001  # -0.3%
        self.stop_loss_pct = 1.0  # 1% stop loss
        # Regression model parameters (from backtesting)
        self.lower_bound_intercept = -0.136717
        self.lower_bound_slope = -1.088807

        # Initialize WebSocket funding stream
        self.funding_stream = FundingRateStream(update_speed)
        self.stream_task = None
        self.is_running = False

    async def start_stream(self):
        """Start the funding rate WebSocket stream"""
        if not self.is_running:
            logger.info("🚀 Starting Mark Price Stream for real-time funding rates")
            self.is_running = True

            # Start the stream task in the background
            self.stream_task = asyncio.create_task(self.funding_stream.start_stream())

            # Wait a moment for the connection to establish
            await asyncio.sleep(1)

            # Verify the connection is working by checking if we can connect
            connection_attempts = 0
            max_attempts = 10

            while connection_attempts < max_attempts:
                if (self.funding_stream.is_running and
                    self.funding_stream.websocket and
                    hasattr(self.funding_stream.websocket, 'close_code') and
                    self.funding_stream.websocket.close_code is None):
                    logger.info("✅ Mark Price Stream connection verified")
                    break

                logger.info(f"⏳ Waiting for Mark Price Stream connection... {connection_attempts + 1}/{max_attempts}")
                await asyncio.sleep(1)
                connection_attempts += 1
            else:
                logger.error("❌ Failed to establish Mark Price Stream connection")
                self.is_running = False
                if self.stream_task:
                    self.stream_task.cancel()
                    self.stream_task = None
                raise ConnectionError("Failed to connect to Mark Price Stream")

            logger.info("🚀 Mark Price Stream started successfully")

    async def stop_stream(self):
        """Stop the funding rate WebSocket stream"""
        if self.is_running:
            self.is_running = False
            await self.funding_stream.stop_stream()
            if self.stream_task:
                self.stream_task.cancel()
                try:
                    await self.stream_task
                except asyncio.CancelledError:
                    pass
            logger.info("⏹️ Stopped Mark Price Stream")

    async def get_negative_funding_rates(self) -> List[Dict]:
        """Get current funding rates for symbols with negative rates from WebSocket stream"""
        try:
            # Get data from WebSocket stream (real-time)
            funding_stream_data = self.funding_stream.get_all_funding_data()
            logger.debug(f"📊 Fetching negative funding rates from {len(funding_stream_data)} cached symbols")
            funding_rates = self.funding_stream.get_negative_funding_rates()

            if funding_rates:
                logger.debug(f"📊 Found {len(funding_rates)} symbols with funding rate ≤ {self.funding_threshold*100:.1f}%")
                for rate in funding_rates[:5]:  # Log first 5 for debugging
                    logger.debug(f"  📉 {rate['symbol']}: {rate['lastFundingRate']*100:.4f}% (${rate['markPrice']:.6f})")
            else:
                logger.debug("📊 No symbols found with negative funding rates above threshold")

            return funding_rates

        except Exception as e:
            logger.error(f"Error fetching funding rates: {e}")
            return []

    def get_funding_data(self, symbol: str) -> Optional[Dict]:
        """Get real-time funding data for a specific symbol"""
        return self.funding_stream.get_funding_data(symbol)

    def add_funding_callback(self, callback):
        """Add callback for real-time funding rate updates"""
        self.funding_stream.add_callback(callback)

    def remove_funding_callback(self, callback):
        """Remove callback for funding rate updates"""
        self.funding_stream.remove_callback(callback)

    def calculate_trade_levels(self, funding_rate: float, current_price: float) -> Tuple[float, float]:
        """
        Calculate stop loss and take profit levels based on regression model

        Args:
            funding_rate: The negative funding rate (e.g., -0.003 for -0.3%)
            current_price: Current market price

        Returns:
            (stop_loss_price, take_profit_price)
        """
        # Convert funding rate to percentage for regression model
        funding_rate_pct = funding_rate * 100
        # Calculate expected price drop using regression model
        # Lower_Bound = -0.136717 + (-1.088807) × Funding_Rate
        expected_drop_pct = self.lower_bound_intercept + (self.lower_bound_slope * funding_rate_pct)
        # Take profit: Use full expected drop (already at 95% confidence interval)
        take_profit_drop_pct = abs(expected_drop_pct)
        take_profit_price = current_price * (1 - take_profit_drop_pct / 100)
        # Stop loss: Fixed 1% above entry price
        stop_loss_price = current_price * (1 + self.stop_loss_pct / 100)
        return stop_loss_price, take_profit_price
