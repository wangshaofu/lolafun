"""
Simple WebSocket Test for BTCUSDT Mark Price and Book Ticker

This test file connects to Binance Futures WebSocket streams to verify:
1. Mark Price stream for funding rate data
2. Book Ticker stream for bid/ask prices
"""

import asyncio
import json
import logging
import websockets
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WebSocketTester:
    def __init__(self):
        self.mark_price_ws = None
        self.book_ticker_ws = None
        self.is_running = False

        # WebSocket URLs
        self.mark_price_uri = "wss://fstream.binance.com/ws/soonusdt@markPrice"
        self.book_ticker_uri = "wss://fstream.binance.com/ws/soonusdt@bookTicker"

    async def connect_mark_price(self):
        """Connect to BTCUSDT mark price stream"""
        try:
            logger.info(f"🔌 Connecting to Mark Price Stream: {self.mark_price_uri}")
            self.mark_price_ws = await websockets.connect(self.mark_price_uri)
            logger.info("✅ Connected to BTCUSDT Mark Price Stream")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Mark Price Stream: {e}")
            return False

    async def connect_book_ticker(self):
        """Connect to BTCUSDT book ticker stream"""
        try:
            logger.info(f"🔌 Connecting to Book Ticker Stream: {self.book_ticker_uri}")
            self.book_ticker_ws = await websockets.connect(self.book_ticker_uri)
            logger.info("✅ Connected to BTCUSDT Book Ticker Stream")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Book Ticker Stream: {e}")
            return False

    async def listen_mark_price(self):
        """Listen to mark price messages"""
        message_count = 0
        try:
            while self.is_running and self.mark_price_ws:
                try:
                    message = await asyncio.wait_for(
                        self.mark_price_ws.recv(),
                        timeout=10.0
                    )

                    data = json.loads(message)
                    message_count += 1

                    if data.get('e') == 'markPriceUpdate':
                        symbol = data['s']
                        funding_rate = float(data['r']) * 100  # Convert to percentage
                        mark_price = float(data['p'])
                        next_funding_time = data['T']

                        # Convert timestamp to readable time
                        funding_time = datetime.fromtimestamp(next_funding_time / 1000)

                        logger.info(f"📊 MARK PRICE #{message_count}: {symbol}")
                        logger.info(f"   💰 Funding Rate: {funding_rate:.4f}%")
                        logger.info(f"   💵 Mark Price: ${mark_price:.2f}")
                        logger.info(f"   ⏰ Next Funding: {funding_time.strftime('%H:%M:%S')}")
                        logger.info(f"   🕐 Received: {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")

                except asyncio.TimeoutError:
                    logger.warning("⚠️ Mark Price: No message received in 10 seconds")
                    continue
                except websockets.exceptions.ConnectionClosed:
                    logger.warning("🔄 Mark Price: Connection closed")
                    break
                except Exception as e:
                    logger.error(f"❌ Mark Price error: {e}")
                    break

        except Exception as e:
            logger.error(f"❌ Mark Price listen error: {e}")
        finally:
            logger.info(f"🛑 Mark Price listener stopped after {message_count} messages")

    async def listen_book_ticker(self):
        """Listen to book ticker messages"""
        message_count = 0
        try:
            while self.is_running and self.book_ticker_ws:
                try:
                    message = await asyncio.wait_for(
                        self.book_ticker_ws.recv(),
                        timeout=10.0
                    )

                    data = json.loads(message)
                    message_count += 1

                    if data.get('e') == 'bookTicker':
                        symbol = data['s']
                        bid_price = float(data['b'])
                        bid_qty = float(data['B'])
                        ask_price = float(data['a'])
                        ask_qty = float(data['A'])

                        spread = ask_price - bid_price
                        spread_pct = (spread / bid_price) * 100

                        logger.info(f"📈 BOOK TICKER #{message_count}: {symbol}")
                        logger.info(f"   💚 Bid: ${bid_price:.2f} (Qty: {bid_qty:.4f})")
                        logger.info(f"   ❤️ Ask: ${ask_price:.2f} (Qty: {ask_qty:.4f})")
                        logger.info(f"   📏 Spread: ${spread:.2f} ({spread_pct:.4f}%)")
                        logger.info(f"   🕐 Received: {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")

                except asyncio.TimeoutError:
                    logger.warning("⚠️ Book Ticker: No message received in 10 seconds")
                    continue
                except websockets.exceptions.ConnectionClosed:
                    logger.warning("🔄 Book Ticker: Connection closed")
                    break
                except Exception as e:
                    logger.error(f"❌ Book Ticker error: {e}")
                    break

        except Exception as e:
            logger.error(f"❌ Book Ticker listen error: {e}")
        finally:
            logger.info(f"🛑 Book Ticker listener stopped after {message_count} messages")

    async def start_test(self):
        """Start the WebSocket test"""
        logger.info("🚀 Starting BTCUSDT WebSocket Test")
        logger.info("📊 Will test both Mark Price and Book Ticker streams")
        logger.info("⏹️ Press Ctrl+C to stop")

        self.is_running = True

        # Connect to both streams
        mark_price_connected = await self.connect_mark_price()
        book_ticker_connected = await self.connect_book_ticker()

        if not mark_price_connected and not book_ticker_connected:
            logger.error("❌ Failed to connect to any streams")
            return

        # Start listening tasks
        tasks = []

        if mark_price_connected:
            tasks.append(asyncio.create_task(self.listen_mark_price()))

        if book_ticker_connected:
            tasks.append(asyncio.create_task(self.listen_book_ticker()))

        try:
            # Run both listeners concurrently
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("🛑 Tasks cancelled")
        except Exception as e:
            logger.error(f"❌ Error running tasks: {e}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Clean up WebSocket connections"""
        logger.info("🧹 Cleaning up connections...")
        self.is_running = False

        if self.mark_price_ws:
            try:
                await self.mark_price_ws.close()
                logger.info("✅ Mark Price connection closed")
            except:
                pass

        if self.book_ticker_ws:
            try:
                await self.book_ticker_ws.close()
                logger.info("✅ Book Ticker connection closed")
            except:
                pass


async def main():
    """Main test function"""
    tester = WebSocketTester()

    try:
        await tester.start_test()
    except KeyboardInterrupt:
        logger.info("🛑 Received Ctrl+C, stopping test...")
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
    finally:
        await tester.cleanup()
        logger.info("✅ Test completed")


if __name__ == "__main__":
    asyncio.run(main())
