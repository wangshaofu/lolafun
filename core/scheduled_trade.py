"""
Scheduled Trade Entity

Represents a trade scheduled for execution at funding settlement time.
"""

from datetime import datetime
from typing import Dict, Any, Optional


class ScheduledTrade:
    """Represents a scheduled trade with all timing and execution logic"""

    def __init__(
        self,
        symbol: str,
        funding_rate: float,
        settlement_time_ms: int,
        current_price: float,
        quantity: float,
        position_value: float,
        trading_config: Dict[str, Any] = None
    ):
        self.symbol = symbol
        self.funding_rate = funding_rate
        self.settlement_time_ms = settlement_time_ms
        self.current_price = current_price
        self.quantity = quantity
        self.position_value = position_value

        # Execution state
        self.pre_settlement_price: Optional[float] = None
        self.post_settlement_price: Optional[float] = None
        self.is_executed = False
        self.is_closed = False
        self.ntp_sync_done = False
        self.entry_time_ms: Optional[int] = None

        # Timing parameters from config (with fallbacks)
        if trading_config:
            self.execution_delay_ms = trading_config.get('execution_delay_ms', 0)
            self.price_capture_offset_ms = trading_config.get('price_capture_offset_ms', 500)
            self.ntp_sync_offset_ms = trading_config.get('ntp_sync_offset_ms', 10000)
        else:
            # Default values if no config provided
            self.execution_delay_ms = 0
            self.price_capture_offset_ms = 500
            self.ntp_sync_offset_ms = 10000

        # Tracking
        self.last_update_log_time = 0

    @classmethod
    def from_opportunity(
        cls,
        opportunity: Dict[str, Any],
        position_size_usd: float,
        trading_config: Dict[str, Any] = None
    ) -> 'ScheduledTrade':
        """Create a ScheduledTrade from a funding opportunity"""
        symbol = opportunity['symbol']
        funding_rate = opportunity['lastFundingRate']
        settlement_time_ms = opportunity['nextFundingTime']
        current_price = opportunity['markPrice']

        # Calculate position details
        quantity = position_size_usd / current_price
        # Note: quantity rounding will be handled by precision manager in trading service
        position_value = quantity * current_price

        return cls(
            symbol=symbol,
            funding_rate=funding_rate,
            settlement_time_ms=settlement_time_ms,
            current_price=current_price,
            quantity=quantity,
            position_value=position_value,
            trading_config=trading_config
        )

    def update_market_data(self, market_data: Dict[str, Any]):
        """Update trade with current market data"""
        old_funding_rate = self.funding_rate
        old_price = self.current_price

        self.funding_rate = market_data.get('lastFundingRate', self.funding_rate)
        self.current_price = market_data.get('markPrice', self.current_price)

        # Recalculate position value with new price
        self.position_value = self.quantity * self.current_price

        return {
            'funding_changed': abs(self.funding_rate - old_funding_rate) > 0.001,
            'price_changed': abs(self.current_price - old_price) > (old_price * 0.001)
        }

    def should_execute(self, current_time_ms: int) -> bool:
        """Check if trade should be executed now"""
        execution_time = self.settlement_time_ms + self.execution_delay_ms
        return current_time_ms >= execution_time and not self.is_executed

    def should_capture_pre_settlement_price(self, current_time_ms: int) -> bool:
        """Check if we should capture pre-settlement price"""
        target_time = self.settlement_time_ms - self.price_capture_offset_ms
        return current_time_ms >= target_time and self.pre_settlement_price is None

    def should_force_ntp_sync(self, current_time_ms: int) -> bool:
        """Check if we should force NTP sync"""
        sync_time = self.settlement_time_ms - self.ntp_sync_offset_ms
        return current_time_ms >= sync_time and not self.ntp_sync_done

    def mark_executed(self, entry_time_ms: int, actual_price: float, actual_quantity: float):
        """Mark trade as executed with actual execution details"""
        self.is_executed = True
        self.entry_time_ms = entry_time_ms
        self.post_settlement_price = actual_price
        self.quantity = actual_quantity  # Update with actual filled quantity

    def get_time_to_settlement(self, current_time_ms: int) -> int:
        """Get seconds until settlement"""
        return max(0, (self.settlement_time_ms - current_time_ms) // 1000)

    def get_settlement_datetime(self) -> datetime:
        """Get settlement time as datetime object"""
        return datetime.fromtimestamp(self.settlement_time_ms / 1000)

    def should_log_update(self, current_time_ms: int, significant_change: bool = False) -> bool:
        """Check if we should log an update for this trade"""
        time_since_last_log = current_time_ms - self.last_update_log_time
        time_based_update = time_since_last_log >= 30000  # 30 seconds

        should_log = significant_change or time_based_update

        if should_log:
            self.last_update_log_time = current_time_ms

        return should_log

    def __str__(self) -> str:
        """String representation of the scheduled trade"""
        settlement_time = self.get_settlement_datetime().strftime('%H:%M:%S')
        status = "EXECUTED" if self.is_executed else "SCHEDULED"
        return (f"{status} {self.symbol}: {self.funding_rate*100:.4f}% "
                f"@ ${self.current_price:.6f} (Settlement: {settlement_time})")

    def __repr__(self) -> str:
        return f"ScheduledTrade({self.symbol}, {self.funding_rate:.6f}, {self.settlement_time_ms})"
