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

    async def connect(self):
        """Establish WebSocket connection"""
        try:
            logger.info(f"Connecting to market data stream for {self.symbol.upper()}")
            self.websocket = await websockets.connect(self.uri)
            self.running = True
            logger.info(f"✅ WebSocket connected for {self.symbol.upper()}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect WebSocket for {self.symbol}: {e}")
            return False

    async def start_stream(self, ntp_sync: 'NTPTimeSync'):
        """Start receiving market data"""
        if not self.websocket:
            if not await self.connect():
                return

        try:
            while self.running:
                msg = await self.websocket.recv()
                recv_time = ntp_sync.get_ntp_time_ms()

                data = json.loads(msg)
                self.best_bid = float(data.get("b", 0))
                self.best_ask = float(data.get("a", 0))
                self.last_update = recv_time

                # Log only the first update to confirm connection, then silent updates
                if hasattr(self, '_update_count'):
                    self._update_count += 1
                else:
                    self._update_count = 1

                if self._update_count == 1:
                    logger.info(f"✅ {self.symbol.upper()} stream active - Initial: Bid {self.best_bid}, Ask {self.best_ask}")

        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"WebSocket connection closed for {self.symbol}")
            self.running = False
        except Exception as e:
            logger.error(f"Error in market data stream for {self.symbol}: {e}")
            self.running = False

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

    async def close(self):
        """Close WebSocket connection"""
        self.running = False
        if self.websocket:
            await self.websocket.close()
            logger.info(f"WebSocket closed for {self.symbol.upper()}")
