# Crypto Futures Funding Rate Trading Bot

An automated trading system that monitors Binance USD-M Futures funding rates and executes short positions on negative funding rate arbitrage opportunities.

## 🚀 Overview

This bot:
- Monitors funding rates across all Binance futures pairs in real-time
- Identifies severely negative funding rates
- Executes short positions exactly at funding settlement times
- Uses Ed25519 WebSocket authentication for ultra-low latency
- Implements NTP time synchronization for precise timing
- Includes comprehensive data acquisition and analysis tools

**⚡ Key Strategy**: Short positions are opened exactly at funding settlement time to capture negative funding payments while the price typically drops afterward.

## 📊 Core Features

- **Real-time Funding Monitoring**: Continuous scanning of all futures pairs
- **Precision Timing**: NTP-synchronized execution at exact settlement times
- **WebSocket Trading**: Ed25519 authenticated orders for minimal latency
- **Smart Risk Management**: Automated stop-loss and ML-based take-profit
- **Historical Analysis**: Complete data collection and backtesting suite
- **Slippage Monitoring**: Track execution quality and timing accuracy

## 🏗️ Project Structure

```
├── live_trading_bot_refactored.py    # Main trading bot
├── config.ini                       # API configuration
├── requirements.txt                  # Dependencies
├── private_key.pem                   # Ed25519 private key
├──
├── trading/                          # Trading engine
│   ├── websocket_client.py           # WebSocket trading client
│   ├── funding_analyzer.py           # ML analysis & rate detection
│   ├── position.py                   # Position management
│   └── precision_manager.py          # Exchange precision handling
├──
├── market/data_stream.py             # Real-time price streaming
├── utils/ntp_sync.py                 # NTP time synchronization
├──
└── DA/                              # Data Acquisition & Analysis
    ├── analyzer.py                   # Real-time funding analyzer
    ├── fetch_funding_rate.py         # Historical data collector
    └── Funding Rate History/         # Historical datasets
```

## 🛠️ Installation

### Prerequisites
- Python 3.8+ (3.11+ recommended)
- Binance Futures API account with trading permissions
- Ed25519 private key for WebSocket authentication

### Setup
```bash
# Clone and navigate
cd "Crypto Future Funding Rate"

# Create virtual environment
python -m venv trading_bot_venv
trading_bot_venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### Configuration
Create `config.ini`:
```ini
[ACCOUNT]
APIKey = your_standard_api_key
APISecret = your_standard_api_secret

[FUTURE_ACCOUNT]  
APIKey = your_ed25519_api_key
# Ed25519 private key stored in private_key.pem
```

**⚠️ Security**: Never commit `config.ini` or `private_key.pem` to version control!

## 🚀 Usage

### Start Trading Bot
```bash
python live_trading_bot_refactored.py
```

### Test Authentication
```bash
python test_ed25519_order.py
```

### Data Analysis
```bash
cd DA
python analyzer.py                    # Real-time monitoring
python fetch_funding_rate.py         # Historical data collection
```

## 📈 Trading Strategy

### How It Works
1. **Detection**: Monitor funding rates for values ≤ -0.3%
2. **Scheduling**: Schedule short positions for exact settlement time
3. **Execution**: Place market short orders at settlement (receive funding payment)
4. **Exit**: Close positions using ML-derived profit targets and 1% stop loss

### Why It Works
- **Price typically drops** after negative funding settlement due to:
  - Bearish sentiment indicated by negative funding
  - Long position closures to avoid funding fees
  - Selling pressure drives price down


## 📊 Performance Monitoring

The bot tracks:
- **Execution Latency**: Settlement to order fill time
- **Slippage**: Expected vs actual fill prices  
- **Success Rate**: Percentage of profitable trades

## 🔧 Technical Details

### WebSocket Trading
- Ed25519 authenticated WebSocket orders
- Connection cycling every 23.5 hours
- Automatic reconnection handling
- Real-time bid/ask price streams

## ⚠️ Risk Considerations

**Market Risks**
- Short positions exposed to adverse price moves
- Slippage during volatile periods
- Low liquidity during extreme events

**Technical Risks**
- Timing precision critical for success
- WebSocket disconnections during execution
- API rate limiting

**Mitigation**
- Fixed stop losses limit downside
- Position sizing controls exposure
- Comprehensive error handling
- Real-time monitoring and alerts

## 📝 Configuration Options

```python
POSITION_SIZE_USD = 100.0           # $100 per trade
FUNDING_THRESHOLD = -0.003          # -0.3% threshold
STOP_LOSS_PCT = 1.0                # 1% stop loss
MAX_CONCURRENT_POSITIONS = 20       # Max positions
NTP_SYNC_INTERVAL = 300            # 5 minute NTP sync
```

## ⚠️ Disclaimer

**Educational purposes only. Cryptocurrency trading involves substantial risk of loss. Only trade with capital you can afford to lose.**

- No guarantee of profitability
- Market conditions change rapidly
- Technical failures may result in losses
- Always test thoroughly before live trading
