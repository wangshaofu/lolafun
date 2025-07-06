"""
Real-time Book Ticker Stream Manager

Handles real-time bid/ask price updates for precise pre-settlement price capture.
"""

import asyncio
import json
import logging
import websockets
from typing import Dict, Optional, Set
from datetime import datetime

logger = logging.getLogger(__name__)


class BookTickerStream:
    """Manages real-time book ticker data via WebSocket for precise price capture"""

    def __init__(self):
        """Initialize book ticker stream"""
        self.base_uri = "wss://fstream.binance.com/ws"

        # WebSocket connection management
        self.websocket = None
        self.is_running = False

        # Real-time price data
        self.ticker_data: Dict[str, Dict] = {}  # symbol -> latest ticker data
        self.subscribed_symbols: Set[str] = set()

        # Connection management
        self.connection_start_time = None

    async def connect(self):
        """Establish WebSocket connection"""
        try:
            logger.info("🔌 Connecting to Book Ticker Stream...")
            self.websocket = await websockets.connect(self.base_uri)
            self.is_running = True
            self.connection_start_time = asyncio.get_event_loop().time()
            logger.info("✅ Connected to Book Ticker Stream")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Book Ticker Stream: {e}")
            return False

    async def disconnect(self):
        """Disconnect from WebSocket"""
        self.is_running = False
        if self.websocket:
            await self.websocket.close()
            self.websocket = None
            logger.info("🔌 Disconnected from Book Ticker Stream")

    async def subscribe_symbol(self, symbol: str) -> bool:
        """Subscribe to book ticker updates for a specific symbol"""
        # More robust connection check
        connection_issue = False
        if self.websocket is None:
            connection_issue = True
            logger.warning(f"⚠️ Cannot subscribe to {symbol}: WebSocket not connected (None)")

        # If we have a connection issue, try reconnecting once
        if connection_issue:
            logger.info(f"🔄 Attempting to reconnect before subscribing to {symbol}")
            if not await self.connect():
                logger.error(f"❌ Cannot subscribe to {symbol}: WebSocket reconnection failed")
                return False
            # Wait for the connection to stabilize
            await asyncio.sleep(1)

        try:
            # Convert to lowercase for Binance API
            symbol_lower = symbol.lower()
            stream_name = f"{symbol_lower}@bookTicker"

            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [stream_name],
                "id": len(self.subscribed_symbols) + 1
            }

            # Add detailed logging
            logger.info(f"🔍 DEBUG: Sending subscription for {symbol} with message: {json.dumps(subscribe_msg)}")
            logger.info(f"🔍 DEBUG: WebSocket type: {type(self.websocket).__name__}")

            await self.websocket.send(json.dumps(subscribe_msg))
            self.subscribed_symbols.add(symbol)
            logger.info(f"📊 Subscribed to {symbol} book ticker")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to subscribe to {symbol}: {e}", exc_info=True)
            return False

    async def unsubscribe_symbol(self, symbol: str):
        """Unsubscribe from book ticker updates for a specific symbol"""
        if not self.websocket or self.websocket.closed:
            return False

        try:
            symbol_lower = symbol.lower()
            stream_name = f"{symbol_lower}@bookTicker"

            unsubscribe_msg = {
                "method": "UNSUBSCRIBE",
                "params": [stream_name],
                "id": len(self.subscribed_symbols) + 1000
            }

            await self.websocket.send(json.dumps(unsubscribe_msg))
            self.subscribed_symbols.discard(symbol)

            # Remove from ticker data
            if symbol in self.ticker_data:
                del self.ticker_data[symbol]

            logger.info(f"📊 Unsubscribed from {symbol} book ticker")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to unsubscribe from {symbol}: {e}")
            return False

    async def _handle_message(self, message: str):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)

            # DEBUG: Print all received data with more details
            logger.info(f"🔍 BOOK TICKER DEBUG - Received message: {message[:2000]}...")

            # Handle subscription confirmation
            if 'id' in data and 'result' in data:
                if data['result'] is None:
                    logger.info(f"✅ Book ticker subscription confirmed: {data.get('id')}")
                else:
                    logger.warning(f"⚠️ Book ticker subscription error: {data}")
                return

            # Handle book ticker updates - Binance futures format
            # For futures, data might have a different structure
            # Check for different possible data formats

            # Standard book ticker format
            if data.get('e') == 'bookTicker':
                symbol = data['s']

                # DEBUG: Print each price update
                logger.info(f"📊 BOOK TICKER UPDATE - {symbol}: Bid ${data['b']}, Ask ${data['a']}")

                # Store real-time ticker data
                self.ticker_data[symbol] = {
                    'symbol': symbol,
                    'bestBidPrice': float(data['b']),
                    'bestBidQty': float(data['B']),
                    'bestAskPrice': float(data['a']),
                    'bestAskQty': float(data['A']),
                    'eventTime': int(data['E']),
                    'transactionTime': int(data['T']),
                    'updateId': int(data['u'])
                }

                logger.info(f"📊 {symbol}: Bid ${data['b']}, Ask ${data['a']} (Mid: ${self.get_mid_price(symbol):.6f})")

            # Alternative format without event type field
            elif 's' in data and 'b' in data and 'a' in data:
                # This is likely a book ticker but in a different format
                symbol = data['s']

                # Log the raw format for debugging
                logger.info(f"🔍 Alternative format book ticker data: {data}")

                # Determine if this is uppercase or lowercase format
                bid_price = data.get('b', data.get('B', '0'))
                ask_price = data.get('a', data.get('A', '0'))
                bid_qty = data.get('B', data.get('q', '0'))
                ask_qty = data.get('A', data.get('Q', '0'))

                # Store normalized data
                self.ticker_data[symbol.upper()] = {
                    'symbol': symbol.upper(),
                    'bestBidPrice': float(bid_price),
                    'bestBidQty': float(bid_qty),
                    'bestAskPrice': float(ask_price),
                    'bestAskQty': float(ask_qty),
                    'eventTime': int(data.get('E', datetime.now().timestamp() * 1000)),
                    'transactionTime': int(data.get('T', datetime.now().timestamp() * 1000)),
                    'updateId': int(data.get('u', 0))
                }

                logger.info(f"📊 {symbol.upper()}: Bid ${bid_price}, Ask ${ask_price} (Mid: ${(float(bid_price) + float(ask_price))/2:.6f})")

            else:
                logger.info(f"🔍 Unrecognized message format: {data}")

        except Exception as e:
            logger.error(f"Error handling book ticker message: {e}", exc_info=True)
            logger.debug(f"Problematic message: {message[:200]}...")

    def get_ticker_data(self, symbol: str) -> Optional[Dict]:
        """Get real-time ticker data for a specific symbol"""
        return self.ticker_data.get(symbol)

    def get_mid_price(self, symbol: str) -> Optional[float]:
        """Get the mid price (average of bid and ask) for a symbol"""
        ticker = self.ticker_data.get(symbol)
        if ticker:
            return (ticker['bestBidPrice'] + ticker['bestAskPrice']) / 2
        return None

    def get_spread(self, symbol: str) -> Optional[float]:
        """Get the bid-ask spread for a symbol"""
        ticker = self.ticker_data.get(symbol)
        if ticker:
            return ticker['bestAskPrice'] - ticker['bestBidPrice']
        return None

    async def start_stream(self):
        """Start the book ticker stream with proper connection management"""
        logger.info("🔌 Starting Book Ticker Stream...")
        self.is_running = True

        while self.is_running:
            try:
                # Check 24-hour connection limit
                if self.connection_start_time:
                    current_time = asyncio.get_event_loop().time()
                    if current_time - self.connection_start_time > 84600:  # 23.5 hours
                        logger.info("🔄 Book Ticker connection approaching 24-hour limit, reconnecting...")
                        if self.websocket:
                            await self.websocket.close()
                            self.websocket = None
                        self.connection_start_time = None

                if not self.websocket or self.websocket.closed:
                    if not await self.connect():
                        logger.warning("⚠️ Failed to connect Book Ticker, retrying in 5 seconds...")
                        await asyncio.sleep(5)
                        continue

                await self._listen_for_messages()

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"🔄 Book Ticker connection closed: {e}, reconnecting...")
                self.websocket = None
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"❌ Error in Book Ticker Stream: {e}")
                await asyncio.sleep(5)

        await self.disconnect()

    async def _listen_for_messages(self):
        """Listen for messages and process them"""
        # Add a small delay to avoid race conditions with other streams
        await asyncio.sleep(1)
        logger.info("📡 Book Ticker listening for messages...")
        while self.is_running:
            try:
                async for message in self.websocket:
                    await self._handle_message(message)
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"🔄 Book Ticker connection closed during message listen: {e}, reconnecting...")
                self.websocket = None
                await asyncio.sleep(1)
                break  # Exit to the outer loop to reconnect
            except Exception as e:
                logger.error(f"❌ Unexpected error in message listener: {e}")
                self.websocket = None
                await asyncio.sleep(5)
                break # Exit to the outer loop to reconnect

    async def stop_stream(self):
        """Stop the book ticker stream"""
        self.is_running = False
        await self.disconnect()

    def get_subscribed_symbols(self) -> Set[str]:
        """Get list of currently subscribed symbols"""
        return self.subscribed_symbols.copy()

    def get_data_status(self) -> Dict:
        """Get current stream status for debugging"""
        return {
            'is_running': self.is_running,
            'subscribed_symbols': len(self.subscribed_symbols),
            'active_tickers': len(self.ticker_data),
            'symbols': list(self.subscribed_symbols)
        }
