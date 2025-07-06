"""
Configuration Management

Centralized configuration loading and validation for the trading bot.
"""

import configparser
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class TradingConfig:
    """Handles configuration loading and validation"""

    def __init__(self, config_path: str = 'config.ini'):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self._validate_config()

    def _load_config(self) -> configparser.ConfigParser:
        """Load configuration from file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        config = configparser.ConfigParser()
        config.read(self.config_path)
        logger.info(f"✅ Configuration loaded from {self.config_path}")
        return config

    def _validate_config(self):
        """Validate required configuration sections and keys"""
        required_sections = ['FUTURE_ACCOUNT']
        required_keys = {
            'FUTURE_ACCOUNT': ['APIKey']
        }

        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required configuration section: {section}")

            for key in required_keys[section]:
                if key not in self.config[section]:
                    raise ValueError(f"Missing required configuration key: {section}.{key}")

        logger.info("✅ Configuration validation passed")

    @property
    def api_key(self) -> str:
        """Get API key from configuration"""
        return self.config['FUTURE_ACCOUNT']['APIKey']

    @property
    def private_key_path(self) -> str:
        """Get private key path"""
        return self.config.get('FUTURE_ACCOUNT', 'PrivateKeyPath', fallback='private_key.pem')

    def get_trading_params(self) -> Dict[str, Any]:
        """Get trading parameters with defaults"""
        return {
            'position_size_usd': float(self.config.get('TRADING', 'PositionSizeUSD', fallback='50.0')),
            'funding_threshold': float(self.config.get('TRADING', 'FundingThreshold', fallback='-0.003')),
            'stop_loss_pct': float(self.config.get('TRADING', 'StopLossPct', fallback='1.0')),
            'scan_interval_ms': int(self.config.get('TRADING', 'ScanIntervalMs', fallback='30000')),
            'execution_delay_ms': int(self.config.get('TRADING', 'ExecutionDelayMs', fallback='100')),
            'price_capture_offset_ms': int(self.config.get('TRADING', 'PriceCaptureOffsetMs', fallback='500')),
            'ntp_sync_offset_ms': int(self.config.get('TRADING', 'NTPSyncOffsetMs', fallback='10000'))
        }

    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return {
            'level': self.config.get('LOGGING', 'LogLevel', fallback='INFO'),
            'log_file': Path(self.config.get('LOGGING', 'LogFile', fallback='live_trading_bot.log')),
            'max_log_size_mb': int(self.config.get('LOGGING', 'MaxLogSizeMB', fallback='10')),
            'backup_count': int(self.config.get('LOGGING', 'BackupCount', fallback='5')),
            'log_format': self.config.get('LOGGING', 'LogFormat',
                                        fallback='%(asctime)s - %(levelname)s - %(message)s', raw=True),
            'enable_console_log': self.config.getboolean('LOGGING', 'EnableConsoleLog', fallback=True),
            'enable_file_log': self.config.getboolean('LOGGING', 'EnableFileLog', fallback=True)
        }

    def get_paths_config(self) -> Dict[str, Path]:
        """Get path configurations"""
        return {
            'data_dir': Path(self.config.get('PATHS', 'DataDirectory', fallback='./data')),
            'output_dir': Path(self.config.get('PATHS', 'OutputDirectory', fallback='./Output')),
            'backtest_dir': Path(self.config.get('PATHS', 'BacktestDirectory', fallback='./Backtest')),
            'logs_dir': Path(self.config.get('PATHS', 'LogsDirectory', fallback='./logs')),
            'temp_dir': Path(self.config.get('PATHS', 'TempDirectory', fallback='./temp'))
        }

    def get_websocket_config(self) -> Dict[str, Any]:
        """Get WebSocket configuration"""
        return {
            'funding_update_speed': self.config.get('WEBSOCKET', 'FundingUpdateSpeed', fallback='1000ms'),
            'max_book_ticker_subscriptions': int(self.config.get('WEBSOCKET', 'MaxBookTickerSubscriptions', fallback='10')),
            'reconnect_delay_ms': int(self.config.get('WEBSOCKET', 'ReconnectDelayMs', fallback='5000')),
            'ping_interval_ms': int(self.config.get('WEBSOCKET', 'PingIntervalMs', fallback='30000')),
            'connection_timeout_ms': int(self.config.get('WEBSOCKET', 'ConnectionTimeoutMs', fallback='10000'))
        }

    def get_risk_management_config(self) -> Dict[str, Any]:
        """Get risk management configuration"""
        return {
            'max_concurrent_trades': int(self.config.get('RISK', 'MaxConcurrentTrades', fallback='5')),
            'max_position_size_pct': float(self.config.get('RISK', 'MaxPositionSizePct', fallback='10.0')),
            'daily_loss_limit_usd': float(self.config.get('RISK', 'DailyLossLimitUSD', fallback='500.0')),
            'enable_emergency_stop': self.config.getboolean('RISK', 'EnableEmergencyStop', fallback=True),
            'emergency_stop_loss_pct': float(self.config.get('RISK', 'EmergencyStopLossPct', fallback='5.0'))
        }

    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring and alerting configuration"""
        return {
            'enable_performance_monitoring': self.config.getboolean('MONITORING', 'EnablePerformanceMonitoring', fallback=True),
            'daily_summary_time': self.config.get('MONITORING', 'DailySummaryTime', fallback='23:59'),
            'log_update_interval_ms': int(self.config.get('MONITORING', 'LogUpdateIntervalMs', fallback='30000')),
            'enable_trade_alerts': self.config.getboolean('MONITORING', 'EnableTradeAlerts', fallback=True)
        }

    def get_ntp_config(self) -> Dict[str, Any]:
        """Get NTP synchronization configuration"""
        return {
            'ntp_servers': self._parse_list(self.config.get('NTP', 'NTPServers',
                fallback='pool.ntp.org,time.google.com,time.cloudflare.com')),
            'sync_interval_minutes': int(self.config.get('NTP', 'SyncIntervalMinutes', fallback='60')),
            'max_time_drift_ms': int(self.config.get('NTP', 'MaxTimeDriftMs', fallback='100')),
            'sync_timeout_seconds': int(self.config.get('NTP', 'SyncTimeoutSeconds', fallback='10'))
        }

    def get_exchange_config(self) -> Dict[str, Any]:
        """Get exchange-specific configuration"""
        return {
            'base_url': self.config.get('EXCHANGE', 'BaseURL', fallback='https://fapi.binance.com'),
            'websocket_url': self.config.get('EXCHANGE', 'WebSocketURL', fallback='wss://fstream.binance.com'),
            'rate_limit_requests_per_minute': int(self.config.get('EXCHANGE', 'RateLimitRequestsPerMinute', fallback='1200')),
            'testnet_mode': self.config.getboolean('EXCHANGE', 'TestnetMode', fallback=False)
        }

    def _parse_list(self, value: str, delimiter: str = ',') -> list:
        """Parse comma-separated string into list"""
        return [item.strip() for item in value.split(delimiter) if item.strip()]

    def ensure_directories(self):
        """Create necessary directories if they don't exist"""
        paths = self.get_paths_config()
        for path_name, path in paths.items():
            try:
                path.mkdir(parents=True, exist_ok=True)
                logger.debug(f"✅ Directory ensured: {path_name} -> {path}")
            except Exception as e:
                logger.error(f"❌ Failed to create directory {path_name} ({path}): {e}")

    def get_all_config(self) -> Dict[str, Dict[str, Any]]:
        """Get all configuration sections as a dictionary"""
        return {
            'trading': self.get_trading_params(),
            'logging': self.get_logging_config(),
            'paths': {k: str(v) for k, v in self.get_paths_config().items()},
            'websocket': self.get_websocket_config(),
            'risk': self.get_risk_management_config(),
            'monitoring': self.get_monitoring_config(),
            'ntp': self.get_ntp_config(),
            'exchange': self.get_exchange_config()
        }
