import unittest
from core.scheduled_trade import ScheduledTrade

class TestScheduledTradeExecution(unittest.TestCase):
    def test_should_execute_with_market_time(self):
        settlement_time = 1000
        trade = ScheduledTrade(
            symbol="BTCUSDT",
            funding_rate=-0.01,
            settlement_time_ms=settlement_time,
            current_price=50000,
            quantity=0.1,
            position_value=5000
        )
        
        # Case 1: Market time is before settlement -> Should NOT execute
        self.assertFalse(trade.should_execute(current_time_ms=2000, market_time_ms=999))
        
        # Case 2: Market time is AT settlement -> Should execute
        self.assertTrue(trade.should_execute(current_time_ms=2000, market_time_ms=1000))
        
        # Case 3: Market time is AFTER settlement -> Should execute
        self.assertTrue(trade.should_execute(current_time_ms=2000, market_time_ms=1001))
        
    def test_should_execute_fallback(self):
        settlement_time = 1000
        trade = ScheduledTrade(
            symbol="BTCUSDT",
            funding_rate=-0.01,
            settlement_time_ms=settlement_time,
            current_price=50000,
            quantity=0.1,
            position_value=5000,
            trading_config={'execution_delay_ms': 100}
        )
        
        # Case 1: System time is before delay -> Should NOT execute
        self.assertFalse(trade.should_execute(current_time_ms=1099))
        
        # Case 2: System time is after delay -> Should execute
        self.assertTrue(trade.should_execute(current_time_ms=1100))

if __name__ == '__main__':
    unittest.main()
