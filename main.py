#!/usr/bin/env python3
"""
Refactored Live Trading Bot for Negative Funding Rate Strategy

This is the new main entry point using the refactored architecture with
better separation of concerns and improved maintainability.

Core functionality:
1. Monitors funding rates for severely negative rates (≤ -0.3%)
2. Schedules short positions at funding settlement time
3. Uses WebSocket for fast order execution
4. NTP sync 10 seconds before settlement

Architecture:
- Core: Main bot logic and entities (TradingBot, ScheduledTrade, Config)
- Services: Business logic services (MarketData, Trading, Timing)
- Trading: WebSocket clients and market data streams
- Utils: Utility functions (NTP sync, etc.)
"""

import asyncio
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

# Add the project root to Python path for imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.trading_bot import LiveTradingBot
from core.config import TradingConfig


def setup_logging(config: TradingConfig):
    """Setup logging based on configuration"""
    log_config = config.get_logging_config()

    # Create logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_config["level"].upper()))

    # Clear existing handlers
    logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(log_config["log_format"])

    handlers = []

    # Console handler
    if log_config["enable_console_log"]:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        handlers.append(console_handler)

    # File handler with rotation
    if log_config["enable_file_log"]:
        # Ensure logs directory exists
        log_file = log_config["log_file"]
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=log_config["max_log_size_mb"] * 1024 * 1024,  # Convert MB to bytes
            backupCount=log_config["backup_count"],
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    # Add handlers to logger
    for handler in handlers:
        logger.addHandler(handler)

    logger.info(
        f"✅ Logging configured - Level: {log_config['level']}, File: {log_config['log_file']}"
    )


async def main():
    """Main entry point for the refactored trading bot"""
    logger = None  # Initialize logger variable

    try:
        # Load configuration first
        config = TradingConfig()

        # Setup logging based on config
        setup_logging(config)

        # Ensure required directories exist
        # config.ensure_directories()

        logger = logging.getLogger(__name__)
        logger.info("🚀 Starting Refactored Live Trading Bot")
        logger.info(f"📁 Working directory: {Path.cwd()}")

        # Log configuration summary
        all_config = config.get_all_config()
        logger.info(f"⚙️ Configuration loaded with {len(all_config)} sections")

        # Create bot instance
        bot = LiveTradingBot()

        # Initialize all bot services
        if not await bot.initialize():
            logger.error("❌ Failed to initialize trading bot")
            return 1

        logger.info("✅ Bot initialization completed successfully")

        # Start the main trading loop
        await bot.run()

    except KeyboardInterrupt:
        if logger:
            logger.info("🛑 Received shutdown signal (Ctrl+C)")
        else:
            print("🛑 Received shutdown signal (Ctrl+C)")
    except Exception as e:
        if logger:
            logger.error(f"❌ Critical error in trading bot: {e}", exc_info=True)
        else:
            print(f"❌ Critical error in trading bot: {e}")
        return 1
    finally:
        # Clean up all resources
        if logger:
            logger.info("🧹 Shutting down trading bot...")
        else:
            print("🧹 Shutting down trading bot...")

        if "bot" in locals():
            await bot.cleanup()

        if logger:
            logger.info("✅ Shutdown completed")
        else:
            print("✅ Shutdown completed")

    return 0


if __name__ == "__main__":
    # Run the bot and exit with appropriate code
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
