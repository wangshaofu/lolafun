"""
Timing Service

Handles NTP synchronization and timing-related operations for precise trade execution.
"""

import logging
from datetime import datetime
from utils.ntp_sync import NTPTimeSync

logger = logging.getLogger(__name__)


class TimingService:
    """Service for managing timing and NTP synchronization"""

    def __init__(self):
        self.ntp_sync = NTPTimeSync()

    async def initialize(self) -> bool:
        """Initialize timing service with NTP synchronization"""
        try:
            logger.info("🕐 Initializing timing service with NTP sync...")

            # Perform initial NTP synchronization
            self.ntp_sync.sync_time()

            # Log synchronized time
            ntp_time_ms = self.ntp_sync.get_ntp_time_ms()
            ntp_time = datetime.fromtimestamp(ntp_time_ms / 1000)
            logger.info(f"✅ NTP sync completed - Time: {ntp_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")

            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize timing service: {e}")
            return False

    async def cleanup(self):
        """Clean up timing service resources"""
        logger.info("🧹 Timing service cleanup completed")

    def get_current_time_ms(self) -> int:
        """Get current NTP-synchronized time in milliseconds"""
        return self.ntp_sync.get_ntp_time_ms()

    def get_current_datetime(self) -> datetime:
        """Get current NTP-synchronized time as datetime object"""
        return datetime.fromtimestamp(self.get_current_time_ms() / 1000)

    async def force_sync_for_settlement(self, settlement_time_ms: int) -> bool:
        """Force NTP sync before a critical settlement time"""
        try:
            current_time = self.get_current_time_ms()
            settlement_datetime = datetime.fromtimestamp(settlement_time_ms / 1000)

            logger.info(f"🕐 Forcing NTP sync before settlement at {settlement_datetime.strftime('%H:%M:%S')}")

            # Log time before sync
            time_before = self.get_current_datetime()
            logger.info(f"🕐 Time before sync: {time_before.strftime('%H:%M:%S.%f')[:-3]}")

            # Perform forced sync
            success = self.ntp_sync.force_sync_before_settlement(settlement_time_ms)

            if success:
                # Log time after sync
                time_after = self.get_current_datetime()
                logger.info(f"✅ NTP sync completed - Updated time: {time_after.strftime('%H:%M:%S.%f')[:-3]}")
                return True
            else:
                logger.warning("⚠️ NTP sync failed during forced sync")
                return False

        except Exception as e:
            logger.error(f"❌ Error during forced NTP sync: {e}")
            return False

    def calculate_time_to_event(self, event_time_ms: int) -> int:
        """Calculate seconds until an event"""
        current_time = self.get_current_time_ms()
        return max(0, (event_time_ms - current_time) // 1000)

    def is_time_for_event(self, event_time_ms: int, offset_ms: int = 0) -> bool:
        """Check if it's time for an event (with optional offset)"""
        current_time = self.get_current_time_ms()
        target_time = event_time_ms + offset_ms
        return current_time >= target_time
