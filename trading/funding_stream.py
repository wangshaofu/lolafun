"""
Real-time Funding Rate Stream Manager

Handles real-time funding rate updates via WebSocket for negative funding rate detection.
Uses individual symbol mark price streams for better reliability.
"""

import asyncio
import json
import logging
import time
import websockets
from typing import Dict, List, Optional, Callable
from datetime import datetime

logger = logging.getLogger(__name__)


class FundingRateStream:
    """Manages real-time funding rate data via WebSocket for trading decisions"""

    def __init__(self, update_speed: str = "1000ms"):
        """Initialize funding rate stream

        Args:
            update_speed: Update frequency for mark price stream (1000ms or 3000ms)
        """
        self.base_uri = "wss://fstream.binance.com/stream"
        self.update_speed = update_speed

        # WebSocket connection management
        self.websocket = None
        self.is_running = False

        # Real-time funding data
        self.funding_data: Dict[str, Dict] = {}  # symbol -> latest funding data
        self.callbacks: List[Callable] = []

        # Trading parameters
        self.funding_threshold = -0.001  # -0.3%

        # Connection management
        self.connection_start_time = None
        self.stream_task = None

        # Precision manager
        self.precision_manager = None

        # Popular symbols to monitor for funding rates
        self.symbols_to_monitor = [
            'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT',
            'ADAUSDT', 'DOGEUSDT', 'AVAXUSDT', 'DOTUSDT', 'MATICUSDT',
            'LINKUSDT', 'LTCUSDT', 'UNIUSDT', 'ATOMUSDT', 'FILUSDT',
            'TRXUSDT', 'ETCUSDT', 'XLMUSDT', 'BCHUSDT', 'NEARUSDT',
            'SOONUSDT', 'ALGOUSDT', 'VETUSDT', 'ICPUSDT', 'FTMUSDT'
        ]

    def set_precision_manager(self, precision_manager):
        """Set the precision manager for the funding stream"""
        self.precision_manager = precision_manager
        logger.info("🔗 Precision manager connected to funding stream")

    async def start_stream(self):
        """Start the mark price stream for popular symbols"""
        try:
            logger.info("🚀 Starting Mark Price Stream for funding rates...")

            # Create stream names for WebSocket subscription
            stream_names = [f"{symbol.lower()}@markPrice" for symbol in self.symbols_to_monitor]

            # Connect to WebSocket with stream multiplexing
            self.websocket = await websockets.connect(self.base_uri)
            self.is_running = True
            self.connection_start_time = asyncio.get_event_loop().time()

            # Subscribe to mark price streams
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": stream_names,
                "id": 1
            }

            await self.websocket.send(json.dumps(subscribe_msg))
            logger.info(f"📊 Subscribed to {len(stream_names)} mark price streams")

            # Start listening for messages
            async for message in self.websocket:
                if not self.is_running:
                    break
                await self._handle_message(message)

        except websockets.exceptions.ConnectionClosed:
            logger.warning("🔌 WebSocket connection closed")
        except Exception as e:
            logger.error(f"❌ Error in funding stream: {e}", exc_info=True)
        finally:
            self.is_running = False
            if self.websocket:
                await self.websocket.close()

    async def stop_stream(self):
        """Stop the funding rate stream"""
        self.is_running = False
        if self.websocket:
            await self.websocket.close()
            self.websocket = None
        logger.info("⏹️ Stopped funding rate stream")

    async def _handle_message(self, message: str):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)

            # Handle subscription confirmation
            if 'id' in data and 'result' in data:
                if data['result'] is None:
                    logger.info("✅ Mark price stream subscription confirmed")
                else:
                    logger.warning(f"⚠️ Mark price subscription error: {data}")
                return

            # Handle stream data
            if 'stream' in data and 'data' in data:
                stream_data = data['data']

                if stream_data.get('e') == 'markPriceUpdate':
                    symbol = stream_data['s']
                    funding_rate = float(stream_data['r'])
                    mark_price = float(stream_data['p'])
                    # Binance fields:
                    #   E: event time (ms), T: next funding time (ms)
                    event_time = int(stream_data.get('E', int(time.time() * 1000)))
                    next_funding_time = int(stream_data.get('T')) if 'T' in stream_data else None

                    # Store funding data
                    self.funding_data[symbol] = {
                        'symbol': symbol,
                        'markPrice': mark_price,
                        'lastFundingRate': funding_rate,
                        'eventTime': event_time,
                        'nextFundingTime': next_funding_time,
                        'updateTimestamp': time.time(),
                    }

                    # Allow a precision manager to learn from mark prices
                    if self.precision_manager is not None:
                        try:
                            self.precision_manager.extract_precision_from_websocket(symbol, mark_price)
                        except Exception:
                            pass

                    # Track if this is a negative funding rate
                    if funding_rate <= self.funding_threshold:
                        # Call callbacks for updated negative funding rates
                        if self.callbacks:
                            for callback in self.callbacks:
                                try:
                                    await callback([symbol])
                                except Exception as e:
                                    logger.error(f"Error in funding rate callback: {e}")

                    # Log periodic updates to show it's working
                    if len(self.funding_data) <= 50 or len(self.funding_data) % 25 == 0:
                        logger.info(f"📊 Updated {symbol}: {funding_rate*100:.4f}% (Total: {len(self.funding_data)} symbols)")

                        # Log negative funding rates
                        negative_count = len(self.get_negative_funding_rates())
                        if negative_count > 0:
                            logger.info(f"📉 Currently {negative_count} symbols with negative funding rates")

        except Exception as e:
            logger.error(f"Error handling funding stream message: {e}", exc_info=True)
            logger.debug(f"Raw message: {message[:1000]}...")

    def get_funding_data(self, symbol: str) -> Optional[Dict]:
        """Get funding data for a specific symbol"""
        return self.funding_data.get(symbol)

    def get_all_funding_data(self) -> Dict[str, Dict]:
        """Get all funding data"""
        return self.funding_data.copy()

    def get_negative_funding_rates(self) -> List[Dict]:
        """Get all symbols with negative funding rates above threshold"""
        negative_rates = []
        for symbol, data in self.funding_data.items():
            funding_rate = data.get('lastFundingRate', 0)
            if funding_rate <= self.funding_threshold:
                negative_rates.append(data)

        # Sort by funding rate (most negative first)
        negative_rates.sort(key=lambda x: x['lastFundingRate'])
        return negative_rates

    def add_callback(self, callback: Callable):
        """Add callback for funding rate updates"""
        if callback not in self.callbacks:
            self.callbacks.append(callback)

    def remove_callback(self, callback: Callable):
        """Remove callback for funding rate updates"""
        if callback in self.callbacks:
            self.callbacks.remove(callback)

    @property
    def symbol_count(self) -> int:
        """Get number of symbols being tracked"""
        return len(self.funding_data)

    @property
    def negative_rate_count(self) -> int:
        """Get number of symbols with negative funding rates"""
        return len(self.get_negative_funding_rates())
