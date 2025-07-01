# low_latency_funding_rate_speculation_bot

This repository is a prototype for a low-latency funding rate speculation bot focused on Binance USD-M Futures. The project currently includes basic data acquisition (DA) scripts for collecting and analyzing funding rates and trading volumes. Backtesting features are planned (TBD).

**Note:** The long-term goal is to migrate this prototype to a dedicated low-latency trading platform.

## Features

- Data acquisition (DA) for funding rates and trading volumes
- Scripts for identifying negative funding rate events
- Prototype structure for future backtesting and live trading modules

## Status

- Prototype / research phase
- Backtest and full low-latency integration are planned

## Requirements

- Python 3.7+
- binance-futures-connector
- pandas

## Usage

1. Configure your Binance API keys in `config.ini`.
2. Run DA scripts in the `DA/` directory to collect and analyze data.

## Disclaimer

This project is for research and prototyping only. Use at your own risk.