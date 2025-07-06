"""
Trading Bot Performance Monitor

Provides monitoring and statistics for the refactored trading bot.
"""

import logging
from datetime import datetime
from typing import Dict, List, Any
from core.scheduled_trade import ScheduledTrade

logger = logging.getLogger(__name__)


class TradingBotMonitor:
    """Monitors trading bot performance and provides statistics"""

    def __init__(self):
        self.start_time = datetime.now()
        self.total_opportunities_found = 0
        self.total_trades_scheduled = 0
        self.total_trades_executed = 0
        self.total_trades_removed = 0
        self.executed_trades: List[Dict] = []

    def log_opportunity_scan(self, opportunities_count: int):
        """Log results of opportunity scanning"""
        self.total_opportunities_found += opportunities_count
        if opportunities_count > 0:
            logger.info(f"📊 Total opportunities found today: {self.total_opportunities_found}")

    def log_trade_scheduled(self, trade: ScheduledTrade):
        """Log when a trade is scheduled"""
        self.total_trades_scheduled += 1
        logger.info(f"📅 Total trades scheduled today: {self.total_trades_scheduled}")

    def log_trade_executed(self, trade: ScheduledTrade, execution_details: Dict):
        """Log when a trade is executed"""
        self.total_trades_executed += 1

        execution_record = {
            'symbol': trade.symbol,
            'funding_rate': trade.funding_rate,
            'entry_price': execution_details.get('entry_price'),
            'quantity': execution_details.get('quantity'),
            'execution_time': datetime.now(),
            'settlement_time': trade.get_settlement_datetime()
        }

        self.executed_trades.append(execution_record)

        logger.info(f"✅ Total trades executed today: {self.total_trades_executed}")

    def log_trade_removed(self, trade: ScheduledTrade, reason: str):
        """Log when a trade is removed from schedule"""
        self.total_trades_removed += 1
        logger.info(f"🗑️ Trade removed: {trade.symbol} - {reason}")
        logger.info(f"📊 Total trades removed today: {self.total_trades_removed}")

    def get_daily_summary(self) -> Dict[str, Any]:
        """Get summary of today's trading activity"""
        uptime = datetime.now() - self.start_time

        return {
            'uptime_hours': uptime.total_seconds() / 3600,
            'opportunities_found': self.total_opportunities_found,
            'trades_scheduled': self.total_trades_scheduled,
            'trades_executed': self.total_trades_executed,
            'trades_removed': self.total_trades_removed,
            'success_rate': (self.total_trades_executed / max(1, self.total_trades_scheduled)) * 100,
            'executed_symbols': [trade['symbol'] for trade in self.executed_trades]
        }

    def log_daily_summary(self):
        """Log daily summary statistics"""
        summary = self.get_daily_summary()

        logger.info("📊 === DAILY TRADING SUMMARY ===")
        logger.info(f"⏰ Uptime: {summary['uptime_hours']:.1f} hours")
        logger.info(f"🔍 Opportunities Found: {summary['opportunities_found']}")
        logger.info(f"📅 Trades Scheduled: {summary['trades_scheduled']}")
        logger.info(f"✅ Trades Executed: {summary['trades_executed']}")
        logger.info(f"🗑️ Trades Removed: {summary['trades_removed']}")
        logger.info(f"📈 Success Rate: {summary['success_rate']:.1f}%")
        if summary['executed_symbols']:
            logger.info(f"💰 Executed Symbols: {', '.join(summary['executed_symbols'])}")
        logger.info("=" * 35)
