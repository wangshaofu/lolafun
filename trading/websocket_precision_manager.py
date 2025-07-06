"""
WebSocket-based Symbol Precision Manager

Gets symbol precision data from the WebSocket mark price stream instead of REST API.
This avoids rate limits and IP bans while providing real-time precision data.
"""

import logging
from typing import Dict, Optional
from decimal import Decimal, ROUND_DOWN

logger = logging.getLogger(__name__)


class WebSocketPrecisionManager:
    """Manages symbol precision data from WebSocket mark price stream"""

    def __init__(self):
        # Cache for symbol precision data extracted from WebSocket stream
        self.precision_cache: Dict[str, Dict] = {}

        # Default precision values (conservative estimates)
        self.default_precision = {
            'quantity_precision': 3,  # Most futures symbols use 3 decimal places
            'price_precision': 2,     # Most futures symbols use 2 decimal places
            'min_qty': 0.001,        # Conservative minimum quantity
            'step_size': 0.001,      # Conservative step size
            'tick_size': 0.01,       # Conservative tick size
            'min_notional': 5.0      # Conservative minimum notional
        }

    def extract_precision_from_websocket(self, symbol: str, mark_price: float):
        """
        Extract precision data from WebSocket mark price data

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            mark_price: Current mark price from WebSocket
        """
        if symbol in self.precision_cache:
            return  # Already have precision data for this symbol

        # Extract price precision from the mark price format
        price_str = f"{mark_price:.8f}".rstrip('0').rstrip('.')
        if '.' in price_str:
            price_precision = len(price_str.split('.')[1])
        else:
            price_precision = 0

        # Apply intelligent defaults based on symbol and price
        if mark_price > 1000:  # High-value symbols like BTC
            quantity_precision = 3
            min_qty = 0.001
            step_size = 0.001
        elif mark_price > 100:  # Medium-value symbols like ETH
            quantity_precision = 2
            min_qty = 0.01
            step_size = 0.01
        elif mark_price > 1:    # Regular symbols
            quantity_precision = 1
            min_qty = 0.1
            step_size = 0.1
        else:                   # Small-value symbols (meme coins, etc.)
            quantity_precision = 0
            min_qty = 1
            step_size = 1

        # Store precision data
        self.precision_cache[symbol] = {
            'quantity_precision': quantity_precision,
            'price_precision': max(price_precision, 2),  # Minimum 2 for prices
            'min_qty': min_qty,
            'step_size': step_size,
            'tick_size': 10 ** (-max(price_precision, 2)),
            'min_notional': 5.0
        }

        logger.debug(f"📊 Extracted precision for {symbol}: "
                    f"qty_prec={quantity_precision}, price_prec={price_precision}, "
                    f"min_qty={min_qty}, step_size={step_size}")

    def get_symbol_precision(self, symbol: str) -> Dict:
        """Get precision data for a symbol, using defaults if not available"""
        if symbol in self.precision_cache:
            return self.precision_cache[symbol]
        else:
            logger.warning(f"⚠️ No precision data for {symbol}, using defaults")
            return self.default_precision.copy()

    def round_quantity(self, symbol: str, quantity: float) -> float:
        """Round quantity to the correct precision for the symbol"""
        precision_data = self.get_symbol_precision(symbol)
        step_size = precision_data['step_size']
        min_qty = precision_data['min_qty']

        # Round down to the nearest step size
        if step_size > 0:
            rounded_qty = float(Decimal(str(quantity)).quantize(
                Decimal(str(step_size)), rounding=ROUND_DOWN
            ))
        else:
            rounded_qty = quantity

        # Ensure minimum quantity
        if rounded_qty < min_qty:
            rounded_qty = min_qty

        return rounded_qty

    def round_price(self, symbol: str, price: float) -> float:
        """Round price to the correct precision for the symbol"""
        precision_data = self.get_symbol_precision(symbol)
        tick_size = precision_data['tick_size']

        if tick_size > 0:
            rounded_price = float(Decimal(str(price)).quantize(
                Decimal(str(tick_size)), rounding=ROUND_DOWN
            ))
        else:
            rounded_price = price

        return rounded_price

    def validate_order(self, symbol: str, quantity: float, price: float) -> tuple[bool, str]:
        """
        Validate order parameters

        Returns:
            (is_valid, error_message)
        """
        precision_data = self.get_symbol_precision(symbol)

        # Check minimum quantity
        if quantity < precision_data['min_qty']:
            return False, f"Quantity {quantity} below minimum {precision_data['min_qty']}"

        # Check minimum notional
        notional = quantity * price
        if notional < precision_data['min_notional']:
            return False, f"Notional {notional:.2f} below minimum {precision_data['min_notional']}"

        return True, ""

    def get_cache_status(self) -> Dict:
        """Get current cache status for debugging"""
        return {
            'symbols_cached': len(self.precision_cache),
            'symbols': list(self.precision_cache.keys())
        }
