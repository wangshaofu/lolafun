"""
Trading Position Management

Handles individual trading positions with risk management and exit conditions.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class TradingPosition:
    """Represents an active trading position with risk management"""
    def __init__(self, symbol: str, entry_price: float, position_size: float,
                 stop_loss: float, take_profit: float, entry_time: float):
        self.symbol = symbol
        self.entry_price = entry_price
        self.position_size = position_size
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.entry_time = entry_time
        # Position status
        self.is_active = True
        self.exit_price = None
        self.exit_time = None
        self.exit_reason = None
        self.realized_pnl = 0.0
        # Risk management
        self.max_age_hours = 8.0  # Close position after 8 hours (funding period)

    def get_unrealized_pnl(self, current_price: float) -> float:
        """Calculate unrealized P&L for SHORT position"""
        if not self.is_active:
            return self.realized_pnl

        return ((self.entry_price - current_price) / self.entry_price) * self.position_size
