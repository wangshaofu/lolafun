"""
NTP Time Synchronization Utility

Provides accurate time synchronization with NTP servers for precise trading operations.
"""

import time
import logging
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
        """
        Synchronize with NTP server with fallback servers
        Returns (offset_ms, delay_ms)
        """
        servers_to_try = [self.server] + self.backup_servers
        for server in servers_to_try:
            for attempt in range(self.max_retries):
                try:
                    client = ntplib.NTPClient()
                    client.request_timeout = 5  # 5 second timeout
                    t0 = time.time() * 1000
                    resp = client.request(server, version=3)
                    t1 = time.time() * 1000
                    server_time_ms = resp.tx_time * 1000
                    local_mid_ms = (t0 + t1) / 2
                    self.offset_ms = local_mid_ms - server_time_ms
                    delay_ms = resp.delay * 1000
                    self.last_sync_time = time.time()
                    logger.info(f"NTP sync successful with {server}: offset={self.offset_ms:.2f}ms, delay={delay_ms:.2f}ms")
                    return self.offset_ms, delay_ms
                except Exception as e:
                    logger.warning(f"NTP sync attempt {attempt + 1} failed with {server}: {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep(1)  # Wait 1 second before retry
                    continue

        # If all servers fail, use system time (no offset)
        logger.error("All NTP servers failed. Using system time without offset.")
        self.offset_ms = 0
        self.last_sync_time = time.time()
        return 0, 0

    def get_ntp_time_ms(self) -> float:
        """Get current NTP-synchronized time in milliseconds"""
        # Re-sync if needed
        if time.time() - self.last_sync_time > self.sync_interval:
            self.sync_time()
        return (time.time() * 1000) - self.offset_ms

    def force_sync_before_settlement(self, next_funding_time: int) -> bool:
        """
        Force NTP synchronization exactly 10 seconds before settlement
        Returns True if sync was successful
        """
        current_time = self.get_ntp_time_ms()
        sync_time = next_funding_time - 10000  # 10 seconds before settlement
        if current_time >= sync_time - 1000 and current_time <= sync_time + 1000:  # Within 1 second window
            logger.info(f"🕐 FORCING NTP SYNC 10 seconds before settlement...")
            offset_ms, delay_ms = self.sync_time()
            logger.info(f"✅ Pre-settlement NTP sync completed: offset={offset_ms:.2f}ms, delay={delay_ms:.2f}ms")
            return True
        return False
