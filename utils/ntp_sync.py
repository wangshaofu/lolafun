"""
NTP Time Synchronization Utility

Provides accurate time synchronization with NTP servers for precise trading operations.
"""

import time
import logging
from datetime import datetime
import ntplib
from typing import Tuple

logger = logging.getLogger(__name__)


class NTPTimeSync:
    """NTP time synchronization utility"""
    def __init__(self, server="162.159.200.1"):
        self.server = server
        self.offset_ms = 0
        self.last_sync_time = 0
        self.sync_interval = 300  # Re-sync every 5 minutes
        self.backup_servers = ["162.159.200.123", "129.250.35.250", "133.130.121.141", "pool.ntp.org", "time.nist.gov"]
        self.max_retries = 3

    def sync_time(self) -> Tuple[float, float]:
        """Synchronize with NTP server with fallback servers"""
        servers_to_try = [self.server] + self.backup_servers
        for server in servers_to_try:
            for attempt in range(self.max_retries):
                try:
                    client = ntplib.NTPClient()
                    client.request_timeout = 5
                    t0 = time.time() * 1000
                    resp = client.request(server, version=3)
                    t1 = time.time() * 1000
                    server_time_ms = resp.tx_time * 1000
                    local_mid_ms = (t0 + t1) / 2
                    self.offset_ms = local_mid_ms - server_time_ms
                    delay_ms = resp.delay * 1000
                    self.last_sync_time = time.time()

                    # Enhanced logging with actual NTP time
                    ntp_time = datetime.fromtimestamp(server_time_ms / 1000)
                    local_time = datetime.fromtimestamp(local_mid_ms / 1000)
                    logger.info(f"NTP sync successful with {server}")
                    logger.info(f"  Server time: {ntp_time.strftime('%H:%M:%S.%f')[:-3]}")
                    logger.info(f"  Local time:  {local_time.strftime('%H:%M:%S.%f')[:-3]}")
                    logger.info(f"  Offset: {self.offset_ms:.2f}ms, Delay: {delay_ms:.2f}ms")

                    return self.offset_ms, delay_ms
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        logger.warning(f"NTP sync attempt {attempt + 1} failed with {server}: {e}")
                        time.sleep(1)
                    continue

        logger.error("❌ All NTP sync attempts failed")
        return self.offset_ms, 0

    def get_ntp_time_ms(self) -> int:
        """Get current NTP-synchronized time in milliseconds"""
        return int((time.time() * 1000) - self.offset_ms)

    def force_sync_before_settlement(self, settlement_time_ms: int) -> bool:
        """Force NTP sync before settlement with enhanced logging"""
        try:
            current_ntp_time = self.get_ntp_time_ms()
            time_until_settlement = (settlement_time_ms - current_ntp_time) / 1000

            logger.info(f"🕐 Forcing NTP sync ({time_until_settlement:.1f}s until settlement)")

            offset, delay = self.sync_time()

            # Log the time difference after sync
            new_ntp_time = self.get_ntp_time_ms()
            time_difference = new_ntp_time - current_ntp_time

            logger.info(f"✅ Pre-settlement NTP sync completed")
            logger.info(f"  Time adjustment: {time_difference:.2f}ms")
            logger.info(f"  New time until settlement: {(settlement_time_ms - new_ntp_time) / 1000:.3f}s")

            return True
        except Exception as e:
            logger.error(f"❌ Pre-settlement NTP sync failed: {e}")
            return False

    def should_resync(self) -> bool:
        """Check if we should resync based on time elapsed"""
        return (time.time() - self.last_sync_time) > self.sync_interval

