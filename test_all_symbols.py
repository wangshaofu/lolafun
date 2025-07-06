"""
Test the all-symbols mark price stream that the main bot uses
"""

import asyncio
import json
import logging
import websockets
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_all_symbols_stream():
    """Test the all-symbols mark price stream"""
    uri = "wss://fstream.binance.com/ws/!markPrice@arr@1s"
    logger.info(f"🔌 Connecting to all-symbols stream: {uri}")

    try:
        async with websockets.connect(uri) as websocket:
            logger.info("✅ Connected to all-symbols mark price stream")

            message_count = 0
            while message_count < 5:  # Just get first 5 messages
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                    data = json.loads(message)
                    message_count += 1

                    logger.info(f"📥 Message #{message_count} received")
                    logger.info(f"   Type: {type(data)}")

                    if isinstance(data, list):
                        logger.info(f"   Array length: {len(data)}")
                        if len(data) > 0:
                            first_item = data[0]
                            logger.info(f"   First item type: {first_item.get('e', 'unknown')}")
                            if first_item.get('s'):
                                logger.info(f"   First symbol: {first_item['s']}")
                    else:
                        logger.info(f"   Data: {data}")

                except asyncio.TimeoutError:
                    logger.warning("⚠️ Timeout waiting for message")
                    break

    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_all_symbols_stream())
