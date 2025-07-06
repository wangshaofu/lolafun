# 🚀 Crypto Futures Funding Rate Trading Bot

A professional-grade automated trading bot that exploits negative funding rate opportunities in cryptocurrency futures markets with precise timing and risk management.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Trading Strategy](#trading-strategy)
- [Risk Management](#risk-management)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## 🎯 Overview

This bot automatically monitors funding rates across cryptocurrency futures and executes short positions when funding rates drop below a configurable threshold (default: -0.3%). The strategy aims to capture funding payments while managing risk through automated stop-loss and take-profit orders.

### Key Statistics
- **Precision Timing**: Executes trades within 100ms of funding settlement
- **Risk Managed**: Automatic stop-loss and take-profit orders
- **Real-time Data**: WebSocket-based market data feeds
- **NTP Synchronized**: Millisecond-accurate timing
- **Highly Configurable**: 30+ configuration parameters

## ✨ Features

### 🎛️ **Advanced Trading**
- Real-time funding rate monitoring via WebSocket
- Precise execution timing around funding settlement
- Automatic position sizing with configurable limits
- Intelligent price capture using book ticker streams
- Risk management with stop-loss and take-profit orders

### ⚙️ **Professional Architecture**
- Service-oriented design with clear separation of concerns
- Comprehensive configuration system (30+ parameters)
- Robust error handling and automatic recovery
- Professional logging with rotation and multiple levels
- Modular components for easy testing and maintenance

### 🛡️ **Risk Controls**
- Daily loss limits with automatic trading halt
- Maximum concurrent position limits
- Emergency stop functionality
- Position sizing controls
- Real-time risk monitoring

### 📊 **Monitoring & Analytics**
- Real-time performance tracking
- Daily trading summaries
- Comprehensive logging system
- Trade execution alerts
- WebSocket connection monitoring

## 🏗️ Architecture

The bot follows a clean service-oriented architecture:

```
├── main.py                 # Entry point with enhanced logging
├── core/                   # Core business logic
│   ├── config.py          # Comprehensive configuration system
│   ├── trading_bot.py     # Main orchestrator
│   ├── scheduled_trade.py # Trade entity with timing logic
│   └── monitor.py         # Performance monitoring
├── services/              # Business logic services  
│   ├── market_data_service.py  # WebSocket streams & data
│   ├── trading_service.py      # Order execution & positions
│   └── timing_service.py       # NTP sync & timing
├── trading/               # Market interface components
│   ├── funding_analyzer.py     # Funding rate analysis
│   ├── websocket_client.py     # Trading WebSocket client
│   ├── book_ticker_stream.py   # Real-time price feeds
│   └── precision_manager.py    # Order precision handling
└── utils/                 # Utility functions
    └── ntp_sync.py        # Network time synchronization
```

### Service Responsibilities

- **TradingBot**: Main orchestrator coordinating all services
- **MarketDataService**: Real-time market data and analysis
- **TradingService**: Order execution and position management
- **TimingService**: NTP synchronization and precise timing
- **ScheduledTrade**: Self-contained trade entity with timing logic

## 🚀 Installation

### Prerequisites
- Python 3.9+
- Binance Futures API account with Ed25519 key pair
- Stable internet connection for WebSocket streams

### Setup Steps

1. **Clone and Setup Environment**
   ```bash
   git clone <repository-url>
   cd "Crypto Future Funding Rate"
   python -m venv trading_bot_venv
   trading_bot_venv\Scripts\activate  # Windows
   # source trading_bot_venv/bin/activate  # Linux/Mac
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure API Access**
   ```bash
   # Copy configuration template
   copy config.ini.example config.ini
   
   # Edit config.ini with your API credentials
   notepad config.ini
   ```

4. **Setup API Key**
   - Place your Ed25519 private key as `private_key.pem`
   - Update `APIKey` in config.ini with your Binance API key

## ⚙️ Configuration

The bot uses a comprehensive configuration system with 8 major sections:

### Basic Setup (config.ini)
```ini
[FUTURE_ACCOUNT]
APIKey = your_binance_api_key_here
PrivateKeyPath = private_key.pem

[TRADING]
PositionSizeUSD = 50.0
FundingThreshold = -0.003
StopLossPct = 1.0
ScanIntervalMs = 30000
```

### Advanced Configuration Sections

| Section | Description | Key Parameters |
|---------|-------------|----------------|
| **TRADING** | Core trading parameters | Position size, thresholds, timing |
| **LOGGING** | Log levels, rotation, format | Level, file size, backup count |
| **PATHS** | Directory locations | Data, logs, output, temp dirs |
| **WEBSOCKET** | Connection settings | Update speed, timeouts, limits |
| **RISK** | Risk management | Daily limits, max positions |
| **MONITORING** | Performance tracking | Alerts, summaries, intervals |
| **NTP** | Time synchronization | Servers, sync frequency |
| **EXCHANGE** | API endpoints | URLs, rate limits, testnet |

### Complete Configuration Reference
See `config.ini.example` for all 30+ configurable parameters with detailed explanations.

## 🎮 Usage

### Starting the Bot
```bash
# Activate virtual environment
trading_bot_venv\Scripts\activate

# Run the bot
python main.py
```

### Expected Startup Output
```
✅ Configuration loaded from config.ini
✅ Configuration validation passed  
✅ Logging configured - Level: INFO, File: live_trading_bot.log
🚀 Starting Refactored Live Trading Bot
📁 Working directory: D:\Programming\Crypto Future Funding Rate
⚙️ Configuration loaded with 8 sections
🤖 Trading bot initialized with $50.0 position size
🔧 Initializing trading bot services...
🕐 Initializing timing service with NTP sync...
✅ NTP sync completed - Time: 2025-07-07 14:23:45.123
📡 Starting market data streams...
✅ Market data service initialized successfully
🔌 Initializing WebSocket trading connection...
✅ Trading service initialized successfully
✅ All services initialized successfully
✅ Bot initialization completed successfully
🚀 Starting trading bot main loop
```

### Stopping the Bot
- **Graceful shutdown**: Press `Ctrl+C`
- **Emergency stop**: Close terminal window

## 📈 Trading Strategy

### Strategy Overview
1. **Monitor** funding rates across all USDT futures pairs
2. **Identify** opportunities when funding rate ≤ -0.3% (configurable)
3. **Schedule** trades for optimal execution timing
4. **Execute** short positions 100ms after funding settlement
5. **Manage** risk with automatic stop-loss and take-profit

### Execution Timeline
```
Settlement Time - 10s:  🕐 NTP sync triggered
Settlement Time - 500ms: 📈 Pre-settlement price captured  
Settlement Time + 100ms: ⚡ Trade executed
Immediately after:       🛡️ Stop-loss & take-profit placed
```

### Position Management
- **Entry**: Market sell order 100ms after funding settlement
- **Risk Management**: Automatic 1% stop-loss and calculated take-profit
- **Exit**: Positions close automatically via risk management orders

## 🛡️ Risk Management

### Built-in Safety Features

| Risk Control | Default Setting | Description |
|--------------|----------------|-------------|
| **Daily Loss Limit** | $500 USD | Bot stops trading after daily losses exceed limit |
| **Position Size Limit** | 10% of capital | Maximum single position size |
| **Concurrent Trades** | 5 positions | Maximum simultaneous open positions |
| **Stop Loss** | 1% | Automatic stop-loss on all positions |
| **Emergency Stop** | 5% total loss | Kill-switch for all positions |

### Risk Monitoring
- Real-time position tracking
- Daily P&L monitoring  
- Automatic trading halt on risk limit breach
- Emergency stop functionality for extreme losses

## 📊 Monitoring

### Real-time Logs
- **INFO**: Trade executions, scheduling, performance
- **DEBUG**: Detailed WebSocket activity, timing data
- **ERROR**: Connection issues, execution failures
- **WARNING**: Risk limit approaches, data inconsistencies

### Log Files
- **live_trading_bot.log**: Main application log with rotation
- **Rotation**: Automatic at 10MB with 5 backup files
- **Custom Format**: Configurable timestamp and message format

### Performance Tracking
```
📊 === DAILY TRADING SUMMARY ===
⏰ Uptime: 8.5 hours
🔍 Opportunities Found: 23
📅 Trades Scheduled: 8  
✅ Trades Executed: 6
🗑️ Trades Removed: 2
📈 Success Rate: 75.0%
💰 Executed Symbols: ETHUSDT, BTCUSDT, ADAUSDT
=====================================
```

## 🔧 Troubleshooting

### Common Issues

#### Configuration Errors
```bash
# Error: Missing API key
❌ Missing required configuration key: FUTURE_ACCOUNT.APIKey

# Solution: Add API key to config.ini
[FUTURE_ACCOUNT]
APIKey = your_actual_api_key_here
```

#### WebSocket Connection Issues
```bash
# Error: WebSocket connection failed
❌ Failed to initialize WebSocket trading connection

# Solutions:
# 1. Check internet connection
# 2. Verify API key permissions (Futures trading enabled)
# 3. Check if API key is not IP-restricted
# 4. Ensure private key file exists and is valid
```

#### NTP Synchronization Problems
```bash
# Error: NTP sync failed
⚠️ NTP sync failed during forced sync

# Solutions:
# 1. Check firewall settings (allow NTP traffic)
# 2. Try different NTP servers in config
# 3. Check system time is roughly correct
```

### Log Analysis
- **Check log files** in the configured logs directory
- **Increase log level** to DEBUG for detailed information
- **Monitor WebSocket connections** for stability issues
- **Review trading performance** via daily summaries

### Performance Optimization
- **Reduce scan interval** for faster opportunity detection
- **Adjust execution timing** based on network latency
- **Optimize position sizing** based on market volatility
- **Fine-tune risk parameters** based on trading performance

## 🔄 Migration from Legacy Bot

If upgrading from the old monolithic bot:

1. **Backup** your existing configuration and logs
2. **Copy** your `config.ini` to the new structure
3. **Update** any custom settings using `config.ini.example`
4. **Test** with small position sizes initially
5. **Monitor** performance and adjust parameters as needed

## 📝 Contributing

### Development Setup
```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
python -m pytest tests/

# Format code
black . --line-length 100
```

### Code Style
- Follow PEP 8 with 100-character line limit
- Use type hints for all function parameters
- Add comprehensive docstrings for all classes and methods
- Maintain service separation and clean architecture

### Submitting Changes
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request with detailed description

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## ⚠️ Disclaimer

This software is for educational and research purposes only. Cryptocurrency trading involves substantial risk of loss. Past performance does not guarantee future results. Always trade responsibly and never risk more than you can afford to lose.

## 📞 Support

For support, bug reports, or feature requests:
- Open an issue on GitHub
- Check the troubleshooting section above
- Review the configuration documentation

---

**Made with ❤️ for the crypto trading community**
