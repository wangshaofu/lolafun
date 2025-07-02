# Crypto Futures Funding Rate Trading Bot

A sophisticated automated trading system that monitors Binance USD-M Futures funding rates and executes high-frequency trading strategies based on negative funding rate arbitrage opportunities.

## 🚀 Overview

This project implements a real-time trading bot that:
- Monitors funding rates across all Binance futures pairs
- Identifies severely negative funding rates (≤ -0.3%) 
- Automatically schedules and executes short positions at funding settlement times
- Uses Ed25519 WebSocket authentication for ultra-low latency order execution
- Implements NTP time synchronization for precise timing
- Includes comprehensive data acquisition and backtesting capabilities

**⚡ Key Innovation**: Orders are placed exactly at funding settlement time (not before) to maximize capture of negative funding payments while minimizing market exposure.

## 📊 Features

### Live Trading Bot
- **Real-time Funding Rate Monitoring**: Continuous scanning of all Binance futures pairs
- **Precision Timing**: NTP-synchronized execution at exact funding settlement times
- **WebSocket Trading**: Ed25519 authenticated WebSocket orders for minimal latency
- **Smart Position Management**: Automated stop-loss and take-profit orders
- **Risk Management**: Configurable position sizes and maximum concurrent positions
- **Market Data Streaming**: Real-time bid/ask price feeds for optimal execution

### Data Analysis & Research
- **Historical Data Collection**: Download funding rate history for all trading pairs
- **Negative Funding Analysis**: Identify and analyze extreme funding events
- **Volume Correlation**: Analyze trading volume patterns around funding events
- **Aggregate Trade Data**: Detailed trade-by-trade data around negative funding
- **Backtesting Framework**: Test strategies against historical data

### Advanced Features
- **Slippage Monitoring**: Track actual vs. expected fill prices
- **Pre-settlement Price Capture**: Use price 500ms before settlement for SL/TP calculations
- **Regression-based Profit Targets**: ML-derived profit targets based on funding rates
- **Connection Management**: Auto-reconnection and 24-hour connection cycling

## 🏗️ Project Structure

```
├── live_trading_bot_refactored.py    # Main trading bot (Ed25519 WebSocket)
├── test_ed25519_order.py            # Ed25519 authentication testing
├── config.ini                       # API configuration
├── requirements.txt                  # Python dependencies
├── private_key.pem                   # Ed25519 private key for WebSocket auth
├──
├── trading/                          # Trading engine modules
│   ├── websocket_client.py           # Ed25519 WebSocket trading client
│   ├── funding_analyzer.py           # Funding rate analysis & ML models
│   ├── position.py                   # Position management
│   └── precision_manager.py          # Exchange precision handling
├──
├── market/                           # Market data modules
│   └── data_stream.py               # Real-time price streaming
├──
├── utils/                           # Utility modules  
│   └── ntp_sync.py                  # NTP time synchronization
├──
├── DA/                              # Data Acquisition & Analysis
│   ├── analyzer.py                   # Real-time funding rate analyzer
│   ├── fetch_funding_rate.py         # Historical funding rate collector
│   ├── fetch_historic_trading_volume.py  # Volume analysis
│   ├── download_negative_funding_aggtrades.py  # Trade data downloader
│   ├── Funding Rate History/         # Historical funding rate data
│   ├── Trading Volume History/       # Volume analysis results
│   └── Negative Funding AggTrades/   # Detailed trade data
├──
├── Backtest/                        # Backtesting framework
├── Latency/                         # Latency testing tools
└── Output/                          # Analysis results & logs
```

## 🛠️ Installation

### Prerequisites
- Python 3.8+ (3.11+ recommended)
- Binance Futures API account with trading permissions
- Ed25519 private key for WebSocket authentication
- Windows/Linux/macOS

### Step 1: Clone & Setup Environment
```bash
git clone <repository-url>
cd "Crypto Future Funding Rate"

# Create virtual environment
python -m venv trading_bot_venv

# Activate virtual environment
# Windows:
trading_bot_venv\Scripts\activate
# Linux/macOS:
source trading_bot_venv/bin/activate
```

### Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 3: Configuration Setup

#### API Configuration
Create `config.ini` with your API credentials:
```ini
[ACCOUNT]
APIKey = your_standard_api_key_here
APISecret = your_standard_api_secret_here

[FUTURE_ACCOUNT]  
APIKey = your_ed25519_api_key_here
# Ed25519 private key stored in private_key.pem file
```

#### Ed25519 Key Setup
1. Generate Ed25519 API key in Binance (required for WebSocket trading)
2. Save the private key as `private_key.pem` in the root directory
3. Ensure the key file has proper permissions (read-only for user)

**⚠️ Security**: Never commit `config.ini` or `private_key.pem` to version control!

## 🚀 Usage

### Live Trading Bot

Start the main trading bot:
```bash
python live_trading_bot_refactored.py
```

The bot will:
1. Initialize NTP time synchronization
2. Establish WebSocket connections
3. Scan for negative funding opportunities  
4. Schedule trades for funding settlement times
5. Execute orders with precise timing
6. Monitor positions until exit

#### Trading Parameters
```python
POSITION_SIZE_USD = 100.0           # $100 per trade
FUNDING_THRESHOLD = -0.003          # -0.3% funding rate threshold
STOP_LOSS_PCT = 1.0                # 1% stop loss
MAX_CONCURRENT_POSITIONS = 20       # Maximum active positions
```

### Data Analysis Tools

#### Real-time Funding Analysis
```bash
cd DA
python analyzer.py
```

#### Historical Data Collection  
```bash
cd DA
python fetch_funding_rate.py        # Download all historical funding rates
python fetch_historic_trading_volume.py  # Analyze negative funding events
python download_negative_funding_aggtrades.py  # Get detailed trade data
```

#### Ed25519 Authentication Test
```bash
python test_ed25519_order.py        # Test WebSocket authentication
```

## 📈 Trading Strategy

### Core Concept
The bot exploits negative funding rate arbitrage by:

1. **Detection**: Continuously monitor funding rates for values ≤ -0.3%
2. **Scheduling**: Schedule short positions to open exactly at funding settlement time
3. **Execution**: Place market short orders at settlement (receive funding payment)
4. **Exit**: Close positions quickly using ML-derived profit targets and stop losses

### Timing Precision
- **NTP Synchronization**: Sub-millisecond time accuracy
- **Pre-settlement Analysis**: Capture price 500ms before settlement for calculations
- **Zero Delay Execution**: Orders placed at exact settlement time (not before)

### Risk Management
- **Position Sizing**: Fixed USD amount per trade ($100 default)
- **Stop Loss**: 1% stop loss on all positions
- **Take Profit**: ML regression model determines profit targets based on funding rate severity
- **Concurrent Limits**: Maximum 20 simultaneous positions

### Machine Learning Integration
The bot uses regression analysis to predict price movements:
```python
# Regression model: Expected_Drop = -0.136717 + (-1.088807) × Funding_Rate_Pct
take_profit_drop_pct = abs(-0.136717 + (-1.088807 * funding_rate_pct))
```

## 📊 Performance Monitoring

### Real-time Logging
The bot provides comprehensive logging:
- Position entries/exits with P&L
- Slippage analysis (expected vs actual fill prices)
- Funding settlement timing accuracy
- WebSocket connection status
- NTP synchronization status

### Key Metrics Tracked
- **Execution Latency**: Time from settlement to order fill
- **Slippage**: Price difference between expected and actual fills
- **Position Duration**: Typical holding time (target: seconds to minutes)
- **Success Rate**: Percentage of profitable trades
- **Funding Capture**: Amount of funding payments received

## ⚠️ Risk Considerations

### Market Risks
- **Price Movement**: Short positions exposed to adverse price moves
- **Liquidity**: Large negative funding events may coincide with low liquidity
- **Slippage**: Market orders may fill at worse prices during volatile periods

### Technical Risks  
- **Timing Precision**: Incorrect settlement timing could miss funding payments
- **Connection Issues**: WebSocket disconnections during critical moments
- **API Limits**: Rate limiting could prevent order execution

### Mitigation Strategies
- Fixed stop losses limit downside risk
- Position sizing controls maximum exposure
- Connection monitoring and auto-reconnection
- Comprehensive error handling and logging

## 🧪 Testing & Validation

### Ed25519 Authentication Test
```bash
python test_ed25519_order.py
```
This tests WebSocket authentication without actual trading.

### Paper Trading Mode
Modify trading parameters to test with minimal position sizes:
```python
POSITION_SIZE_USD = 5.0  # Minimum for testing
```

### Backtesting
Historical analysis tools in `DA/` directory allow strategy validation against past data.

## 📚 Technical Documentation

### WebSocket Trading Flow
1. Establish authenticated WebSocket connection
2. Monitor market data streams for price updates
3. Calculate position sizes using exchange precision rules
4. Execute market orders with proper formatting
5. Place protective stop-loss and take-profit orders
6. Monitor position until exit

### NTP Time Synchronization
- Primary NTP server: 162.159.200.1 (Cloudflare)
- Backup servers: Multiple fallbacks for reliability  
- Re-sync every 5 minutes or before critical events
- Sub-millisecond accuracy for precise timing

### Error Handling
- Comprehensive exception handling for all operations
- Automatic reconnection for dropped connections
- Graceful degradation when services unavailable
- Detailed error logging for debugging

## 🔧 Configuration Options

### Trading Parameters
```python
# Position sizing
POSITION_SIZE_USD = 100.0

# Funding rate threshold
FUNDING_THRESHOLD = -0.003  # -0.3%

# Risk management
STOP_LOSS_PCT = 1.0        # 1%
MAX_CONCURRENT_POSITIONS = 20

# Timing
NTP_SYNC_INTERVAL = 300    # 5 minutes
PRE_SETTLEMENT_CAPTURE = 500  # 500ms before settlement
```

### WebSocket Settings
```python
# Connection management
CONNECTION_MAX_AGE_HOURS = 23.5  # Reconnect before 24h limit
PING_INTERVAL = 30               # Keep-alive ping every 30s
TIMEOUT_SECONDS = 10             # Order response timeout
```

## 📝 Development Status

- ✅ **Live Trading Bot**: Fully operational with Ed25519 WebSocket trading
- ✅ **Data Acquisition**: Complete historical data collection suite
- ✅ **Real-time Analysis**: Funding rate monitoring and alerting
- ✅ **Risk Management**: Stop-loss, take-profit, position sizing
- ✅ **Precision Timing**: NTP synchronization and settlement execution
- 🔄 **Backtesting**: Enhanced backtesting framework in development
- 📋 **Portfolio Management**: Multi-strategy coordination planned
- 📋 **Web Interface**: Real-time dashboard planned

## 📄 License

This project is for educational and research purposes. Use at your own risk. 

## ⚠️ Disclaimer

**This software is for educational purposes only. Cryptocurrency trading involves substantial risk of loss. Past performance does not guarantee future results. Only trade with capital you can afford to lose.**

- No guarantee of profitability
- Market conditions can change rapidly
- Technical failures may result in losses
- Regulatory changes may affect operations
- Always test thoroughly before live trading

## 🤝 Contributing

This is a research project. Contributions welcome for:
- Performance optimizations
- Additional risk management features  
- Enhanced backtesting capabilities
- Documentation improvements

## 📞 Support

For issues or questions:
1. Check the logs in `live_trading_bot.log`
2. Review configuration in `config.ini`
3. Test Ed25519 authentication with `test_ed25519_order.py`
4. Verify WebSocket connectivity and NTP synchronization

---

**Built with precision, deployed with caution. Trade responsibly.** ⚡
