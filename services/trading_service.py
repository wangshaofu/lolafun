"""
Trading Service

Handles all trading operations including order execution and position management.
"""

import asyncio
import logging
from typing import Dict, Optional

from core.config import TradingConfig
from core.scheduled_trade import ScheduledTrade
from trading.websocket_client import WebSocketTradingClient
from trading.funding_analyzer import FundingRateAnalyzer
from utils.ntp_sync import NTPTimeSync

logger = logging.getLogger(__name__)


class TradingService:
    """Service for handling trade execution and position management"""

    def __init__(self, config: TradingConfig, ntp_sync: NTPTimeSync, precision_manager):
        self.config = config
        self.ntp_sync = ntp_sync
        self.precision_manager = precision_manager

        # Initialize WebSocket trading client
        self.websocket_client = WebSocketTradingClient(
            config.api_key,
            config.private_key_path,
            precision_manager,
            ntp_sync
        )

        # Initialize funding analyzer for trade level calculations
        self.funding_analyzer = FundingRateAnalyzer()

        # Active positions tracking
        self.active_positions = []

    async def initialize(self) -> bool:
        """Initialize trading service and WebSocket connection"""
        try:
            logger.info("🔌 Initializing WebSocket trading connection...")

            if not await self.websocket_client.initialize_connection():
                logger.error("❌ Failed to initialize WebSocket trading connection")
                return False

            logger.info("✅ Trading service initialized successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize trading service: {e}")
            return False

    async def cleanup(self):
        """Clean up trading service resources"""
        logger.info("🧹 Cleaning up trading service...")
        # WebSocket client cleanup would go here
        logger.info("✅ Trading service cleanup completed")

    async def execute_trade(self, scheduled_trade: ScheduledTrade) -> bool:
        """Execute a scheduled trade with minimal latency"""
        try:
            symbol = scheduled_trade.symbol
            funding_rate = scheduled_trade.funding_rate

            # Use pre-settlement price for execution
            execution_price = scheduled_trade.pre_settlement_price
            if not execution_price:
                logger.error(f"❌ No pre-settlement price available for {symbol}")
                return False

            # Calculate precise position quantity
            quantity = await self._calculate_position_quantity(symbol, execution_price)

            # Calculate stop loss and take profit levels
            stop_loss_price, take_profit_price = self.funding_analyzer.calculate_trade_levels(
                funding_rate, execution_price
            )

            # Log execution details
            ntp_time = self.ntp_sync.get_ntp_time_ms()
            logger.info(f"⚡ EXECUTING {symbol} at NTP time: {ntp_time}")
            logger.info(f"    💰 Funding Rate: {funding_rate * 100:.4f}%")
            logger.info(f"    📊 Quantity: {quantity:.4f} @ ${execution_price:.6f}")
            logger.info(f"    🛡️ Stop Loss: ${stop_loss_price:.6f}")
            logger.info(f"    🎯 Take Profit: ${take_profit_price:.6f}")

            # Execute market order
            order_result = await self.websocket_client.place_market_order(symbol, "SELL", quantity)

            if not order_result:
                logger.error(f"❌ Market order failed for {symbol}")
                return False

            # Get actual execution details
            actual_entry_price = order_result.get("actualFillPrice", execution_price)
            actual_quantity = order_result.get("actualQuantity", quantity)

            # Place stop loss and take profit orders
            await self._place_risk_management_orders(
                symbol, actual_quantity, stop_loss_price, take_profit_price
            )

            # Track position
            position = self._create_position_record(
                symbol, actual_entry_price, actual_quantity,
                stop_loss_price, take_profit_price
            )
            self.active_positions.append(position)

            # Mark trade as executed
            scheduled_trade.mark_executed(
                self.ntp_sync.get_ntp_time_ms(),
                actual_entry_price,
                actual_quantity
            )

            logger.info(f"✅ {symbol} executed successfully - Entry: ${actual_entry_price:.6f}")
            return True

        except Exception as e:
            logger.error(f"❌ Execution error for {scheduled_trade.symbol}: {e}")
            return False

    async def _calculate_position_quantity(self, symbol: str, price: float) -> float:
        """Calculate position quantity with proper precision"""
        trading_params = self.config.get_trading_params()
        position_size_usd = trading_params['position_size_usd']

        base_quantity = position_size_usd / price
        rounded_quantity = self.precision_manager.round_quantity(symbol, base_quantity)

        return rounded_quantity

    async def _place_risk_management_orders(
        self,
        symbol: str,
        quantity: float,
        stop_loss_price: float,
        take_profit_price: float
    ):
        """Place stop loss and take profit orders"""
        try:
            # Place stop loss order
            sl_result = await self.websocket_client.place_stop_loss_order(
                symbol, quantity, stop_loss_price
            )
            if sl_result:
                logger.info(f"🛡️ Stop loss placed for {symbol} at ${stop_loss_price:.6f}")
            else:
                logger.warning(f"⚠️ Failed to place stop loss for {symbol}")

            # Place take profit order
            tp_result = await self.websocket_client.place_take_profit_order(
                symbol, quantity, take_profit_price
            )
            if tp_result:
                logger.info(f"🎯 Take profit placed for {symbol} at ${take_profit_price:.6f}")
            else:
                logger.warning(f"⚠️ Failed to place take profit for {symbol}")

        except Exception as e:
            logger.error(f"❌ Error placing risk management orders for {symbol}: {e}")

    def _create_position_record(
        self,
        symbol: str,
        entry_price: float,
        quantity: float,
        stop_loss: float,
        take_profit: float
    ) -> Dict:
        """Create a position record for tracking"""
        return {
            'symbol': symbol,
            'entry_price': entry_price,
            'quantity': quantity,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'entry_time': self.ntp_sync.get_ntp_time_ms(),
            'status': 'active'
        }

    def get_active_positions_count(self) -> int:
        """Get number of active positions"""
        return len(self.active_positions)

    def get_position_summary(self) -> Dict:
        """Get summary of active positions"""
        if not self.active_positions:
            return {'count': 0, 'total_value': 0.0}

        total_value = sum(pos['entry_price'] * pos['quantity'] for pos in self.active_positions)

        return {
            'count': len(self.active_positions),
            'total_value': total_value,
            'symbols': [pos['symbol'] for pos in self.active_positions]
        }
