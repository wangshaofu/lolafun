# Crypto Future Funding Rate Analyzer

This repository is a comprehensive tool for analyzing Binance USD-M Futures funding rates and trading volumes. The project includes data acquisition scripts, historical analysis tools, and real-time funding rate monitoring capabilities.

**Note:** The long-term goal is to migrate this prototype to a dedicated low-latency trading platform.

## Features

- **Real-time Funding Rate Analysis**: Monitor current funding rates across all Binance futures pairs
- **Historical Data Collection**: Fetch and store historical funding rates for all trading pairs
- **Negative Funding Detection**: Identify and analyze severely negative funding rate events (≤ -0.3%)
- **Trading Volume Analysis**: Correlate trading volume with funding rate events
- **Aggregate Trade Data**: Download detailed trade data around negative funding events
- **Account Balance Integration**: View your futures account balance alongside analysis
- **Data Export**: Save all analysis results to CSV files for further research

## Project Structure

```
├── __main__.py                 # Main entry point for real-time analysis
├── config.ini                  # API configuration (create from template)
├── requirements.txt            # Python dependencies
├── DA/                         # Data Acquisition scripts
│   ├── fetch_funding_rate.py   # Historical funding rate collection
│   ├── fetch_historic_trading_volume.py  # Volume analysis for negative events
│   ├── download_negative_funding_aggtrades.py  # Detailed trade data
│   └── binance-public-data-master/  # Binance data utilities
└── Backtest/                   # Future backtesting modules (TBD)
```

## Requirements

- Python 3.8+ (recommended)
- Binance Futures API account with API keys
- Sufficient storage space for historical data (can be several GB)

## Installation

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd "Crypto Future Funding Rate"
```

### 2. Create Virtual Environment (Recommended)
```bash
# Windows
python -m venv trading_bot_venv
trading_bot_venv\Scripts\activate

# Linux/macOS
python -m venv trading_bot_venv
source trading_bot_venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure API Keys
Create a `config.ini` file in the root directory:
```ini
[ACCOUNT]
APIKey = your_binance_api_key_here
APISecret = your_binance_api_secret_here
```

**⚠️ Security Note**: Never commit your `config.ini` file to version control. It's already included in `.gitignore`.

## Usage

### Real-time Funding Rate Analysis
```bash
python __main__.py
```
This will:
- Display your futures account balance
- Show current funding rates for all pairs
- Highlight negative funding rates
- Save results to timestamped CSV files

### Historical Data Collection
```bash
cd DA
python fetch_funding_rate.py
```
Downloads historical funding rates for all Binance futures pairs since 2022.

### Analyze Negative Funding Events
```bash
cd DA
python fetch_historic_trading_volume.py
```
Finds historical negative funding events and fetches corresponding trading volume data.

### Download Detailed Trade Data
```bash
cd DA
python download_negative_funding_aggtrades.py
```
Downloads aggregate trade data around severely negative funding rate events.

## Output Files

- `DA/Funding Rate History/` - Historical funding rates by symbol
- `DA/Trading Volume History/` - Volume analysis results
- `DA/Negative Funding AggTrades/` - Detailed trade data

## Key Features Explained

### Negative Funding Rate Detection
The system identifies funding rates ≤ -0.3% (configurable threshold) which indicate:
- High demand for short positions
- Potential market stress or price manipulation
- Arbitrage opportunities

### Trading Volume Correlation
For each negative funding event, the system fetches:
- 4-hour trading volume data immediately before the funding rate
- Historical context to understand market conditions
- Volume patterns that may predict funding rate extremes

### Aggregate Trade Analysis
Downloads detailed trade-by-trade data in ±10 second windows around negative funding events to analyze:
- Price action leading to extreme funding rates
- Trading behavior and market microstructure
- Potential manipulation patterns

## Status

- ✅ **Data Acquisition**: Complete and functional
- ✅ **Real-time Monitoring**: Active funding rate analysis
- ✅ **Historical Analysis**: Full historical data collection
- 🔄 **Backtesting**: In development
- 📋 **Live Trading**: Planned for future releases

## Performance Notes

- Historical data collection can take several hours for all symbols
- Large datasets may require several GB of storage
- Rate limiting is implemented to comply with Binance API limits
- Use SSD storage for better performance with large CSV files

## Troubleshooting

### Common Issues

1. **API Errors**: Ensure your API keys have futures trading permissions
2. **Rate Limiting**: The scripts include delays to prevent API rate limits
3. **Storage Space**: Monitor disk space when downloading historical data
4. **Memory Usage**: Large datasets may require 8GB+ RAM for processing