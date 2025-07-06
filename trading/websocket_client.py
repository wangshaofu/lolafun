"""
WebSocket Trading Client

Handles WebSocket-based trading operations with proper Ed25519 authentication and order management.
"""

import asyncio
import base64
import json
import logging
import websockets
import uuid
from typing import Optional
from urllib.parse import urlencode
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from trading.precision_manager import SymbolPrecisionManager
from utils.ntp_sync import NTPTimeSync

logger = logging.getLogger(__name__)


class WebSocketTradingClient:
    """WebSocket-based trading client for Binance Futures with Ed25519 authentication"""
    def __init__(self, api_key: str, private_key_path: str, precision_manager: SymbolPrecisionManager, ntp_sync: NTPTimeSync):
        self.api_key = api_key
        self.private_key_path = private_key_path
        self.precision_manager = precision_manager
        self.ntp_sync = ntp_sync
        self.private_key = None
        # Load the Ed25519 private key
        self._load_private_key()
        # WebSocket connection management
        self.websocket = None
        self.ws_lock = asyncio.Lock()
        self.is_connected = False
        self.connection_task = None
        self.connection_start_time = None  # Track when connection was established
        self.connection_max_age_hours = 23.5  # Reconnect before 24-hour limit
        # Fixed WebSocket URL - use the correct endpoint
        self.ws_uri = "wss://ws-fapi.binance.com/ws-fapi/v1"

    def _load_private_key(self):
        """Load the Ed25519 private key from PEM file"""
        try:
            with open(self.private_key_path, 'rb') as f:
                self.private_key = load_pem_private_key(data=f.read(), password=b'binance')
            logger.info(f"Successfully loaded Ed25519 private key from {self.private_key_path}")
            # Log the key type for debugging
            logger.info(f"Private key type: {type(self.private_key)}")
            logger.info(f"Private key algorithm: {self.private_key.__class__.__name__}")

        except Exception as e:
            logger.error(f"Failed to load private key from {self.private_key_path}: {e}")
            raise

    def generate_signature(self, params: dict) -> str:
        """Generate Ed25519 signature for Binance API"""
        try:
            # Sort parameters by key
            sorted_params = sorted(params.items())
            # Create query string (payload)
            payload = urlencode(sorted_params)
            # Generate Ed25519 signature
            signature_bytes = self.private_key.sign(payload.encode('ASCII'))
            signature = base64.b64encode(signature_bytes).decode('ASCII')
            logger.debug(f"Generated Ed25519 signature for payload: {payload}")
            return signature
        except Exception as e:
            logger.error(f"Failed to generate Ed25519 signature: {e}")
            raise

    async def ensure_connection(self) -> bool:
        """Ensure WebSocket connection is established and ready"""
        async with self.ws_lock:
            connection_needed = False
            if self.websocket is None:
                connection_needed = True
            else:
                # Check if websocket is closed using proper methods
                try:
                    if hasattr(self.websocket, 'close_code') and self.websocket.close_code is not None:
                        connection_needed = True
                    elif hasattr(self.websocket, 'state') and str(self.websocket.state) in ['CLOSED', 'CLOSING']:
                        connection_needed = True
                except Exception:
                    connection_needed = True
            if connection_needed:
                try:
                    logger.info("🔌 Establishing WebSocket trading connection...")
                    self.websocket = await websockets.connect(self.ws_uri)
                    logger.info("✅ WebSocket trading connection established")
                    self.connection_start_time = asyncio.get_event_loop().time()  # Reset connection start time
                    return True
                except Exception as e:
                    logger.error(f"❌ Failed to establish WebSocket connection: {e}")
                    self.websocket = None
                    return False
            return True

    async def send_order_request(self, order_request: dict) -> Optional[dict]:
        """Send order request via WebSocket with retry logic"""
        if not await self.ensure_connection():
            return None
        async with self.ws_lock:
            try:
                # Send order request
                await self.websocket.send(json.dumps(order_request))
                # Wait for response with timeout - handle ping/pong during wait
                while True:
                    try:
                        response = await asyncio.wait_for(self.websocket.recv(), timeout=10.0)
                        # Check if this is a ping frame that we need to respond to
                        if hasattr(self.websocket, 'opcode') and self.websocket.opcode == 0x9:  # Ping frame
                            # Respond with pong immediately
                            await self.websocket.pong(response)
                            logger.debug("Responded to server ping with pong")
                            continue  # Wait for actual response
                        # This is the actual response
                        return json.loads(response)
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("🔄 WebSocket disconnected during order request, reconnecting...")
                        self.websocket = None
                        # Retry once with new connection
                        if await self.ensure_connection():
                            await self.websocket.send(json.dumps(order_request))
                            continue  # Try to receive response again
                        else:
                            return None
                    except asyncio.TimeoutError:
                        logger.error("⏰ Order response timeout")
                        return None
            except Exception as e:
                logger.error(f"❌ Error sending order: {e}")
                return None

    async def place_market_order(self, symbol: str, side: str, quantity: float) -> Optional[dict]:
        """Place a market order and return order details including actual fill price"""
        try:
            current_time = int(self.ntp_sync.get_ntp_time_ms())
            # Format quantity with proper precision
            formatted_quantity = self.precision_manager.format_quantity(symbol, quantity)
            params = {
                "apiKey": self.api_key,
                "symbol": symbol.upper(),
                "side": side.upper(),
                "type": "MARKET",
                "quantity": formatted_quantity,
                "positionSide": "LONG" if side.upper() == "BUY" else "SHORT",  # Use LONG/SHORT for Hedge mode
                "timestamp": current_time,
                "recvWindow": 5000
            }
            params["signature"] = self.generate_signature(params)
            request_id = str(uuid.uuid4())
            order_request = {
                "id": request_id,
                "method": "order.place",
                "params": params
            }
            response_data = await self.send_order_request(order_request)
            if response_data and response_data.get("status") == 200:
                result = response_data.get("result", {})
                # Extract actual fill information from the response
                actual_fill_price = float(result.get("avgPrice", 0)) if result.get("avgPrice") else 0
                actual_quantity = float(result.get("executedQty", 0)) if result.get("executedQty") else 0
                logger.info(f"✅ Market {side} order placed: {formatted_quantity} {symbol}")
                logger.info(f"   📊 Actual fill price: ${actual_fill_price:.6f}")
                logger.info(f"   📊 Actual quantity: {actual_quantity:.6f}")
                # Return enhanced result with actual fill data
                enhanced_result = result.copy()
                enhanced_result["actualFillPrice"] = actual_fill_price
                enhanced_result["actualQuantity"] = actual_quantity
                return enhanced_result
            else:
                logger.error(f"❌ Market order failed: {response_data}")
                return None
        except Exception as e:
            logger.error(f"Error placing market order: {e}")
            return None

    async def place_stop_loss_order(self, symbol: str, quantity: float, stop_price: float) -> Optional[dict]:
        """Place a stop loss order"""
        try:
            current_time = int(self.ntp_sync.get_ntp_time_ms())
            # Format with proper precision
            formatted_quantity = self.precision_manager.format_quantity(symbol, quantity)
            formatted_stop_price = self.precision_manager.format_price(symbol, stop_price)
            params = {
                "apiKey": self.api_key,
                "symbol": symbol.upper(),
                "side": "BUY",  # BUY to close SHORT position
                "type": "STOP_MARKET",
                "quantity": formatted_quantity,
                "stopPrice": formatted_stop_price,
                "positionSide": "SHORT",  # SHORT position in Hedge mode
                "timestamp": current_time,
                "recvWindow": 5000
            }
            params["signature"] = self.generate_signature(params)
            request_id = str(uuid.uuid4())
            order_request = {
                "id": request_id,
                "method": "order.place",
                "params": params
            }
            response_data = await self.send_order_request(order_request)
            if response_data and response_data.get("status") == 200:
                logger.info(f"🛡️ Stop loss order placed at {formatted_stop_price}")
                return response_data.get("result")
            else:
                logger.error(f"❌ Stop loss order failed: {response_data}")
                return None
        except Exception as e:
            logger.error(f"Error placing stop loss order: {e}")
            return None

    async def place_take_profit_order(self, symbol: str, quantity: float, take_profit_price: float) -> Optional[dict]:
        """Place a take profit order"""
        try:
            current_time = int(self.ntp_sync.get_ntp_time_ms())
            # Format with proper precision
            formatted_quantity = self.precision_manager.format_quantity(symbol, quantity)
            formatted_tp_price = self.precision_manager.format_price(symbol, take_profit_price)
            params = {
                "apiKey": self.api_key,
                "symbol": symbol.upper(),
                "side": "BUY",  # BUY to close SHORT position
                "type": "TAKE_PROFIT_MARKET",
                "quantity": formatted_quantity,
                "stopPrice": formatted_tp_price,
                "positionSide": "SHORT",  # SHORT position in Hedge mode
                "timestamp": current_time,
                "recvWindow": 5000
            }
            params["signature"] = self.generate_signature(params)
            request_id = str(uuid.uuid4())
            order_request = {
                "id": request_id,
                "method": "order.place",
                "params": params
            }
            response_data = await self.send_order_request(order_request)
            if response_data and response_data.get("status") == 200:
                logger.info(f"🎯 Take profit order placed at {formatted_tp_price}")
                return response_data.get("result")
            else:
                logger.error(f"❌ Take profit order failed: {response_data}")
                return None
        except Exception as e:
            logger.error(f"Error placing take profit order: {e}")
            return None

    async def initialize_connection(self) -> bool:
        """Initialize WebSocket connection early for faster order execution"""
        try:
            logger.info("🔌 Initializing WebSocket trading connection early...")
            # Establish initial connection
            success = await self.ensure_connection()
            if success:
                # Start background task to maintain connection
                self.connection_task = asyncio.create_task(self._maintain_connection())
                logger.info("✅ WebSocket trading connection initialized and maintenance started")
                return True
            else:
                logger.error("❌ Failed to initialize WebSocket connection")
                return False
        except Exception as e:
            logger.error(f"❌ Error initializing WebSocket connection: {e}")
            return False

    async def _maintain_connection(self):
        """Background task to maintain WebSocket connection with 24-hour limit handling"""
        while True:
            try:
                # Check if connection is None or closed using proper websockets library methods
                connection_lost = False
                if self.websocket is None:
                    connection_lost = True
                else:
                    # Check if websocket is closed by checking its state
                    try:
                        if hasattr(self.websocket, 'close_code') and self.websocket.close_code is not None:
                            connection_lost = True
                        elif hasattr(self.websocket, 'state') and str(self.websocket.state) in ['CLOSED', 'CLOSING']:
                            connection_lost = True
                    except Exception:
                        connection_lost = True
                # Check if connection is approaching 24-hour limit
                if self.connection_start_time:
                    connection_age_hours = (asyncio.get_event_loop().time() - self.connection_start_time) / 3600
                    if connection_age_hours >= self.connection_max_age_hours:
                        logger.info(f"🕐 WebSocket connection approaching 24-hour limit ({connection_age_hours:.1f}h), reconnecting...")
                        connection_lost = True
                if connection_lost:
                    logger.warning("🔄 WebSocket connection lost or expired, reconnecting...")
                    await self.ensure_connection()
                # Send ping every 30 seconds to keep connection alive
                if self.websocket:
                    try:
                        # Check connection is still valid before pinging
                        if hasattr(self.websocket, 'close_code') and self.websocket.close_code is None:
                            await self.websocket.ping()
                        elif hasattr(self.websocket, 'state') and str(self.websocket.state) == 'OPEN':
                            await self.websocket.ping()
                    except Exception as e:
                        logger.warning(f"WebSocket ping failed: {e}")

                await asyncio.sleep(30)  # Check every 30 seconds

            except asyncio.CancelledError:
                logger.info("WebSocket maintenance task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in WebSocket maintenance: {e}")
                await asyncio.sleep(5)  # Wait before retrying

    async def close(self):
        """Close WebSocket connection and cleanup resources"""
        try:
            # Cancel the maintenance task
            if self.connection_task and not self.connection_task.done():
                self.connection_task.cancel()
                try:
                    await self.connection_task
                except asyncio.CancelledError:
                    pass
            # Close the WebSocket connection
            async with self.ws_lock:
                if self.websocket:
                    await self.websocket.close()
                    self.websocket = None
                    logger.info("WebSocket trading connection closed")
        except Exception as e:
            logger.error(f"Error closing WebSocket connection: {e}")
