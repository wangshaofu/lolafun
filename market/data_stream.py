"""
Market Data Stream Module

Handles real-time market data streaming via WebSocket connections.
"""

import asyncio
import json
import logging
import websockets
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from utils.ntp_sync import NTPTimeSync

logger = logging.getLogger(__name__)


class MarketDataStream:
    """Real-time market data stream via WebSocket"""

    def __init__(self, symbol: str):
        self.symbol = symbol.lower()
        self.websocket = None
        self.best_bid = 0.0
        self.best_ask = 0.0
        self.last_update = 0
        self.uri = f"wss://fstream.binance.com/ws/{self.symbol}@bookTicker"
        self.running = False
        self.connection_start_time = None
        self.connection_max_age_hours = 23.5  # Reconnect before 24-hour limit
        self._update_count = 0

    async def connect(self):
        """Establish WebSocket connection"""
        try:
            logger.info(f"🔌 Connecting to market data stream for {self.symbol.upper()}")
            self.websocket = await websockets.connect(self.uri)
            self.connection_start_time = asyncio.get_event_loop().time()
            logger.info(f"✅ WebSocket connected for {self.symbol.upper()}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect WebSocket for {self.symbol}: {e}")
            return False

    async def disconnect(self):
        """Disconnect from WebSocket"""
        if self.websocket:
            try:
                await self.websocket.close()
                logger.info(f"🔌 Disconnected from {self.symbol.upper()} stream")
            except Exception as e:
                logger.error(f"Error disconnecting {self.symbol}: {e}")
            finally:
                self.websocket = None

    async def start_stream(self, ntp_sync: 'NTPTimeSync'):
        """Start receiving market data with proper connection management"""
        logger.info(f"📡 Starting market data stream for {self.symbol.upper()}")
        self.running = True
        last_ping_time = asyncio.get_event_loop().time()

        while self.running:
            try:
                # Check 24-hour connection limit
                if self.connection_start_time:
                    current_time = asyncio.get_event_loop().time()
                    if current_time - self.connection_start_time > (self.connection_max_age_hours * 3600):
                        logger.info(f"🔄 {self.symbol.upper()} connection approaching 24-hour limit, reconnecting...")
                        await self.disconnect()
                        self.connection_start_time = None

                # Ensure connection
                if not self.websocket or self.websocket.closed:
                    if not await self.connect():
                        logger.warning(f"⚠️ Failed to connect {self.symbol.upper()}, retrying in 5 seconds...")
                        await asyncio.sleep(5)
                        continue

                # Listen for messages
                logger.info(f"📊 {self.symbol.upper()} stream listening for messages...")

                async for message in self.websocket:
                    if not self.running:
                        break

                    try:
                        recv_time = ntp_sync.get_ntp_time_ms()
                        data = json.loads(message)

                        # Update price data
                        self.best_bid = float(data.get("b", 0))
                        self.best_ask = float(data.get("a", 0))
                        self.last_update = recv_time
                        self._update_count += 1

                        # Log first few updates to confirm stream is working
                        if self._update_count <= 3:
                            logger.info(f"✅ {self.symbol.upper()} stream active - Update #{self._update_count}: Bid ${self.best_bid}, Ask ${self.best_ask}")
                        elif self._update_count % 1000 == 0:
                            logger.info(f"📊 {self.symbol.upper()} stream healthy - {self._update_count} updates received")

                        # Send periodic ping to maintain connection
                        current_time = asyncio.get_event_loop().time()
                        if current_time - last_ping_time > 120:  # Every 2 minutes
                            try:
                                await self.websocket.ping()
                                logger.debug(f"📧 Sent ping to {self.symbol.upper()} stream")
                                last_ping_time = current_time
                            except Exception as ping_error:
                                logger.warning(f"⚠️ Failed to send ping to {self.symbol.upper()}: {ping_error}")

                    except json.JSONDecodeError as e:
                        logger.warning(f"⚠️ Invalid JSON from {self.symbol.upper()} stream: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"❌ Error processing {self.symbol.upper()} message: {e}")
                        continue

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"🔄 {self.symbol.upper()} connection closed: {e}, reconnecting...")
                self.websocket = None
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"❌ Critical error in {self.symbol.upper()} stream: {e}")
                self.websocket = None
                await asyncio.sleep(5)

        # Clean shutdown
        await self.disconnect()
        logger.info(f"🛑 {self.symbol.upper()} stream stopped")

    def get_mid_price(self) -> float:
        """Get current mid price"""
        if self.best_bid > 0 and self.best_ask > 0:
            return (self.best_bid + self.best_ask) / 2
        return 0.0

    def get_spread_pct(self) -> float:
        """Get bid-ask spread as percentage"""
        if self.best_bid > 0 and self.best_ask > 0:
            return ((self.best_ask - self.best_bid) / self.get_mid_price()) * 100
        return 0.0

    def is_data_fresh(self, max_age_ms: int = 5000) -> bool:
        """Check if the last update is within the specified time window"""
        if self.last_update == 0:
            return False
        current_time = asyncio.get_event_loop().time() * 1000  # Convert to ms
        return (current_time - self.last_update) < max_age_ms

    def get_status(self) -> dict:
        """Get current stream status for debugging"""
        return {
            'symbol': self.symbol.upper(),
            'running': self.running,
            'connected': self.websocket is not None and not self.websocket.closed if self.websocket else False,
            'best_bid': self.best_bid,
            'best_ask': self.best_ask,
            'mid_price': self.get_mid_price(),
            'spread_pct': self.get_spread_pct(),
            'last_update': self.last_update,
            'update_count': self._update_count,
            'data_fresh': self.is_data_fresh()
        }

    async def close(self):
        """Close WebSocket connection"""
        self.running = False
        await self.disconnect()
