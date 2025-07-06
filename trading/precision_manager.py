"""
Symbol Precision Manager

Handles exchange symbol precision data for proper price and quantity formatting.
"""

import asyncio
import logging
from typing import Dict, Optional
from binance.um_futures import UMFutures

logger = logging.getLogger(__name__)


class SymbolPrecisionManager:
    """Manages symbol precision data from exchange info"""

    def __init__(self, client: UMFutures):
        self.client = client
        self.exchange_info = None
        self.precision_cache: Dict[str, Dict] = {}

    async def initialize(self) -> bool:
        """Fetch and cache exchange info for symbol precision data"""
        try:
            if self.exchange_info is None:
                logger.info("📡 Fetching exchange info for symbol precision data...")
                loop = asyncio.get_event_loop()
                self.exchange_info = await loop.run_in_executor(None, self.client.exchange_info)

                # Cache symbol precision data for faster lookup
                for symbol_info in self.exchange_info.get('symbols', []):
                    symbol = symbol_info['symbol']

                    # Initialize default values
                    quantity_precision = 0
                    price_precision = 2
                    min_qty = 1
                    step_size = 1
                    tick_size = 0.01
                    min_notional = 5.0

                    # Extract precision data from filters
                    for filter_info in symbol_info.get('filters', []):
                        if filter_info['filterType'] == 'LOT_SIZE':
                            step_size = float(filter_info['stepSize'])
                            min_qty = float(filter_info['minQty'])
                            # Calculate precision from step size
                            step_str = filter_info['stepSize'].rstrip('0').rstrip('.')
                            if '.' in step_str:
                                quantity_precision = len(step_str.split('.')[1])

                        elif filter_info['filterType'] == 'PRICE_FILTER':
                            tick_size = float(filter_info['tickSize'])
                            # Calculate price precision from tick size
                            tick_str = filter_info['tickSize'].rstrip('0').rstrip('.')
                            if '.' in tick_str:
                                price_precision = len(tick_str.split('.')[1])

                        elif filter_info['filterType'] == 'MIN_NOTIONAL':
                            min_notional = float(filter_info['notional'])

                    self.precision_cache[symbol] = {
                        'quantityPrecision': quantity_precision,
                        'pricePrecision': price_precision,
                        'stepSize': step_size,
                        'tickSize': tick_size,
                        'minQty': min_qty,
                        'minNotional': min_notional
                    }

                logger.info(f"✅ Exchange info cached for {len(self.precision_cache)} symbols")
                return True

        except Exception as e:
            logger.error(f"❌ Failed to fetch exchange info: {e}")
            return False

        return True

    def get_symbol_precision(self, symbol: str) -> Dict:
        """Get precision data for a symbol"""
        symbol_upper = symbol.upper()
        return self.precision_cache.get(symbol_upper, {
            'quantityPrecision': 3,  # Default fallback
            'pricePrecision': 2,
            'stepSize': 0.001,
            'tickSize': 0.01,
            'minQty': 0.001,
            'minNotional': 5.0
        })

    def round_quantity(self, symbol: str, quantity: float) -> float:
        """Round quantity to proper precision based on exchange rules"""
        precision_data = self.get_symbol_precision(symbol)
        step_size = precision_data['stepSize']
        min_qty = precision_data['minQty']
        precision = precision_data['quantityPrecision']

        # Round to step size
        if step_size > 0:
            rounded_qty = round(quantity / step_size) * step_size
        else:
            rounded_qty = quantity

        # Ensure minimum quantity
        rounded_qty = max(rounded_qty, min_qty)

        # Round to proper decimal places
        return round(rounded_qty, precision)

    def round_price(self, symbol: str, price: float) -> float:
        """Round price to proper precision based on exchange rules"""
        precision_data = self.get_symbol_precision(symbol)
        tick_size = precision_data['tickSize']
        precision = precision_data['pricePrecision']

        # Round to tick size
        if tick_size > 0:
            rounded_price = round(price / tick_size) * tick_size
        else:
            rounded_price = price

        # Round to proper decimal places
        return round(rounded_price, precision)

    def format_quantity(self, symbol: str, quantity: float) -> str:
        """Format quantity as string with proper precision"""
        rounded_qty = self.round_quantity(symbol, quantity)
        precision = self.get_symbol_precision(symbol)['quantityPrecision']
        return f"{rounded_qty:.{precision}f}"

    def format_price(self, symbol: str, price: float) -> str:
        """Format price as string with proper precision"""
        rounded_price = self.round_price(symbol, price)
        precision = self.get_symbol_precision(symbol)['pricePrecision']
        return f"{rounded_price:.{precision}f}"