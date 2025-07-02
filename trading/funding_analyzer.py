"""
Funding Rate Analyzer

Handles funding rate data fetching and analysis for trading decisions.
"""

import asyncio
import logging
from typing import Dict, List, Tuple
from binance.um_futures import UMFutures

logger = logging.getLogger(__name__)


class FundingRateAnalyzer:
    """Analyzes funding rates and calculates trading levels"""
    def __init__(self, client: UMFutures):
        self.client = client
        # Trading parameters
        self.funding_threshold = -0.003  # -0.3%
        self.stop_loss_pct = 1.0  # 1% stop loss
        # Regression model parameters (from backtesting)
        self.lower_bound_intercept = -0.136717
        self.lower_bound_slope = -1.088807

    async def get_negative_funding_rates(self) -> List[Dict]:
        """Get current funding rates for symbols with negative rates"""
        try:
            # Use asyncio to run sync API call
            loop = asyncio.get_event_loop()
            funding_data = await loop.run_in_executor(None, self.client.mark_price)
            funding_rates = []
            for item in funding_data:
                try:
                    funding_rate = float(item['lastFundingRate'])
                    if funding_rate <= self.funding_threshold:
                        funding_rates.append({
                            'symbol': item['symbol'],
                            'lastFundingRate': funding_rate,
                            'nextFundingTime': item['nextFundingTime'],
                            'markPrice': float(item['markPrice'])
                        })
                except (ValueError, KeyError):
                    continue
            # Sort by most negative funding rate
            funding_rates.sort(key=lambda x: x['lastFundingRate'])
            return funding_rates
        except Exception as e:
            logger.error(f"Error fetching funding rates: {e}")
            return []

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

