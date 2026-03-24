# Commodities Data Generator

High-fidelity synthetic commodities market data generator for QuestDB. Produces realistic order books, trades, and daily settlements for 25 commodity instruments across energy, power, metals, and agriculture.

All prices are anchored to real market data via Yahoo Finance (19 symbols) and the EIA API (4 power hubs). Deferred contracts (CL12, NG12) track their front month with a drifting basis offset.

## Instruments (25 symbols)

### Energy (9)

| Symbol | Name | Exchange | Unit | Source |
|--------|------|----------|------|--------|
| CL | WTI Crude Oil | NYMEX | bbl | Yahoo Finance |
| BZ | Brent Crude Oil | ICE | bbl | Yahoo Finance |
| NG | Natural Gas Henry Hub | NYMEX | MMBtu | Yahoo Finance |
| TTF | Dutch TTF Gas | ICE | MWh | Yahoo Finance |
| RB | RBOB Gasoline | NYMEX | gal | Yahoo Finance |
| HO | Heating Oil | NYMEX | gal | Yahoo Finance |
| JKM | LNG Japan-Korea | ICE | MMBtu | Yahoo Finance |
| CL12 | WTI Crude 12-Month | NYMEX | bbl | Derived from CL |
| NG12 | Nat Gas 12-Month | NYMEX | MMBtu | Derived from NG |

### Power (4)

| Symbol | Name | Exchange | Unit | Source |
|--------|------|----------|------|--------|
| PJM | PJM Western Hub | ICE | MWh | EIA API (PA industrial) |
| ERCT | ERCOT North Texas | ICE | MWh | EIA API (TX industrial) |
| CISO | CAISO South (SP15) | ICE | MWh | EIA API (CA industrial) |
| NBPL | New England (NEPOOL) | ICE | MWh | EIA API (MA industrial) |

### Metals (5)

| Symbol | Name | Exchange | Unit | Source |
|--------|------|----------|------|--------|
| GC | Gold | COMEX | oz | Yahoo Finance |
| SI | Silver | COMEX | oz | Yahoo Finance |
| HG | Copper | COMEX | lb | Yahoo Finance |
| PL | Platinum | NYMEX | oz | Yahoo Finance |
| PA | Palladium | NYMEX | oz | Yahoo Finance |

### Agriculture (7)

| Symbol | Name | Exchange | Unit | Source |
|--------|------|----------|------|--------|
| ZC | Corn | CBOT | bu | Yahoo Finance |
| ZS | Soybeans | CBOT | bu | Yahoo Finance |
| ZW | Wheat | CBOT | bu | Yahoo Finance |
| KC | Coffee | ICE | lb | Yahoo Finance |
| CC | Cocoa | ICE | MT | Yahoo Finance |
| SB | Sugar | ICE | lb | Yahoo Finance |
| CT | Cotton | ICE | lb | Yahoo Finance |

## Price Anchoring

All generated prices are anchored to real market data so they remain realistic even during volatile periods.

### Yahoo Finance (19 symbols)

Energy, metals, and agriculture symbols fetch the latest daily close from Yahoo Finance at startup. In real-time mode, brackets refresh every 5 minutes (configurable via `--yahoo_refresh_secs`). Prices are constrained to a +/-1% bracket around the latest close.

If Yahoo is unreachable, the generator falls back to wide static brackets that cover historical extremes (e.g. CL: $30-$150).

### EIA API (4 power symbols)

Power hub prices are anchored to the EIA's monthly industrial retail electricity prices per state (the `electricity/retail-sales` endpoint). This provides actual $/kWh prices which are converted to $/MWh:

- **PJM** - Pennsylvania industrial rate
- **ERCOT** - Texas industrial rate
- **CAISO** - California industrial rate
- **NEPOOL** - Massachusetts industrial rate

These prices update monthly and reflect real market conditions including crisis-driven spikes.

**An EIA API key is required** for power hub price anchoring. The key is free - register at:

https://www.eia.gov/opendata/register.php

Pass it via `--eia_api_key YOUR_KEY`. Without a key, power symbols fall back to static brackets.

### Deferred Contracts (2 symbols)

CL12 and NG12 track their front-month contracts (CL, NG) with a slow-drifting basis offset that simulates contango/backwardation dynamics. The basis drifts by up to +/-0.002 per second, clamped to +/-$2.

## Table Schemas

### `commodities_market_data`

Full depth order books with array-based bids/asks.

```sql
CREATE TABLE commodities_market_data (
    timestamp TIMESTAMP_NS,
    symbol SYMBOL,
    exchange SYMBOL,
    commodity_class SYMBOL,
    bids DOUBLE[][],       -- bids[1]=prices, bids[2]=sizes
    asks DOUBLE[][],       -- asks[1]=prices, asks[2]=sizes
    best_bid DOUBLE,
    best_ask DOUBLE
) timestamp(timestamp) PARTITION BY HOUR;
```

### `commodities_trades`

Individual trade events with price, size, and side.

```sql
CREATE TABLE commodities_trades (
    timestamp TIMESTAMP_NS,
    symbol SYMBOL,
    exchange SYMBOL,
    commodity_class SYMBOL,
    price DOUBLE,
    size LONG,
    side SYMBOL           -- 'B' or 'S'
) timestamp(timestamp) PARTITION BY HOUR;
```

### `commodities_settlements`

Daily settlement prices with open interest and volume (~23 rows/day, one per non-deferred symbol).

```sql
CREATE TABLE commodities_settlements (
    timestamp TIMESTAMP_NS,
    symbol SYMBOL,
    settlement_price DOUBLE,
    open_interest LONG,
    volume LONG
) timestamp(timestamp) PARTITION BY DAY;
```

## Materialized Views

Continuous (real-time):
- `commodities_bbo_1s` - last best bid/ask + spread per second
- `commodities_trades_ohlcv_1s` - OHLC + volume per second

Timed rollups:
- `commodities_bbo_1m` - REFRESH EVERY 1m
- `commodities_bbo_1h` - REFRESH EVERY 10m
- `commodities_trades_ohlcv_1m` - REFRESH EVERY 1m
- `commodities_trades_ohlcv_15m` - REFRESH EVERY 1m

## Event Rates

Default rates (per second, before scale_factor):
- Market data: 15-40 events/s
- Trades: 5-20 events/s

Events are rank-weighted: CL/NG/GC (rank 1) get the most activity, deferred months (rank 10) the least.

| Scale Factor | MD/s | Trades/s | Daily (23h active) |
|-------------|------|----------|-------------------|
| 1 (default) | ~30 | ~15 | ~2.5M MD + ~1.2M trades |
| 100 | ~3,000 | ~1,500 | ~250M MD + ~120M trades |

## Per-Class Volatility

Each commodity class has its own volatility profile controlling price evolution:

| Class | Drift (ticks) | Shock Prob | Shock Mult |
|-------|--------------|------------|------------|
| Energy | 3.0 | 0.5% | 20x |
| Power | 3.0 | 1.5% | 40x |
| Metals | 2.0 | 0.3% | 15x |
| Ags | 2.5 | 0.4% | 18x |

## Power Spike Model

Power symbols (PJM, ERCT, CISO, NBPL) have a spike/revert state machine:
- 2% chance per second to enter a spike (5-30 second duration)
- During spike: 10x normal drift, 50% shock probability
- After spike: price reverts toward pre-spike level over ~5 seconds

This produces realistic power market dynamics where prices can briefly spike to multiples of normal levels before reverting.

## Session Pacing

CME Globex hours (Sunday 5pm CT - Friday 4pm CT):
- **Active**: normal event rates
- **Settlement** (2:25-2:30 PM CT): elevated rates, daily settlement emission
- **Maintenance** (4:00-5:00 PM CT daily): no events
- **Off** (weekends, Friday after 4pm CT): no events

Settlements are emitted once per simulated day (23 non-deferred symbols) regardless of session pacing mode.

Override with `--offsession_trades=trickle|full` for demos.

## Usage

### Real-time mode

```bash
python commodities_data_generator.py \
  --host 127.0.0.1 \
  --mode real-time \
  --processes 1 \
  --scale_factor 1 \
  --eia_api_key YOUR_KEY
```

### Backfill (faster-than-life)

```bash
python commodities_data_generator.py \
  --host 127.0.0.1 \
  --mode faster-than-life \
  --processes 6 \
  --scale_factor 50 \
  --total_market_data_events 100_000_000 \
  --start_ts "2025-11-11T00:00:00.000000Z" \
  --end_ts "2025-11-11T14:00:00.000000Z" \
  --eia_api_key YOUR_KEY
```

### Stress test

```bash
python commodities_data_generator.py \
  --host 127.0.0.1 \
  --mode faster-than-life \
  --processes 8 \
  --scale_factor 100 \
  --total_market_data_events 500_000_000
```

## Command-Line Arguments

### Connection
| Argument | Default | Description |
|----------|---------|-------------|
| `--host` | 127.0.0.1 | QuestDB host |
| `--pg_port` | 8812 | PG wire port |
| `--user` | admin | PG user |
| `--password` | quest | PG password |
| `--token` | None | ILP auth token |
| `--token_x` | None | ILP token X |
| `--token_y` | None | ILP token Y |
| `--ilp_user` | admin | ILP username |
| `--protocol` | http | http or tcp |

### Mode & Rates
| Argument | Default | Description |
|----------|---------|-------------|
| `--mode` | required | real-time or faster-than-life |
| `--market_data_min_eps` | 15 | Min market data events per second |
| `--market_data_max_eps` | 40 | Max market data events per second |
| `--trades_min_eps` | 5 | Min trade events per second |
| `--trades_max_eps` | 20 | Max trade events per second |
| `--scale_factor` | 1 | Multiplier for all event rates |
| `--total_market_data_events` | 1,000,000 | Target events (faster-than-life) |

### Time Window
| Argument | Default | Description |
|----------|---------|-------------|
| `--start_ts` | now | Start timestamp (faster-than-life only) |
| `--end_ts` | None | End timestamp |
| `--chunk_seconds` | 900 | Precompute chunk size |

### Generation
| Argument | Default | Description |
|----------|---------|-------------|
| `--processes` | 1 | Worker process count |
| `--min_levels` | 20 | Min order book depth |
| `--max_levels` | 20 | Max order book depth |
| `--create_views` | true | Create materialized views |
| `--short_ttl` | false | Enable short TTLs on views |
| `--prefix` | "" | Table name prefix |

### Session
| Argument | Default | Description |
|----------|---------|-------------|
| `--session_pacing` | true | Enable session-aware pacing |
| `--offsession_trades` | none | none, trickle, or full |
| `--session_tz` | America/Chicago | Session timezone |

### Data Sources
| Argument | Default | Description |
|----------|---------|-------------|
| `--yahoo_refresh_secs` | 300 | Yahoo refresh interval (real-time) |
| `--eia_api_key` | None | EIA API key ([register here](https://www.eia.gov/opendata/register.php)) |

## Grafana Dashboard

A ready-to-import Grafana dashboard is included at `Commodities-Orderbook-Realtime-Demo.json`. It uses the QuestDB datasource plugin (`questdb-questdb-datasource`) and connects via PG wire on port 8812. On import, Grafana will prompt you to select your QuestDB datasource.

![Commodities Dashboard](assets/grafana-dashboard.png)

### Panels

**Term Structure & Cross-Commodity** (top row, no symbol filter)

| Panel | Description |
|-------|-------------|
| Contango / Backwardation | Front month minus deferred month mid-price for CL vs CL12 (WTI Crude) and NG vs NG12 (Natural Gas). Positive values indicate backwardation (near-term premium), negative indicates contango. Uses ASOF JOIN on `commodities_bbo_1s`. |
| Live Movers | All 25 commodities ranked by most volatile in the last 10 seconds. Shows current mid-price and percentage change over 10s, 1m, and 5m horizons with color-coded gauge bars (green = up, red = down). Sorted by `abs(chg_10s)`. |

**Market Depth** (Plotly order book visualization)

| Panel | Description |
|-------|-------------|
| Market Depth | Interactive order book for the selected symbol. Green area = cumulative bid volume, red area = cumulative ask volume, white dotted line = mid-price, yellow dotted lines = top volume walls per segment. Uses `bids[][]` and `asks[][]` arrays with `array_cum_sum()`. |

**Prices and Spread** (per-symbol row)

| Panel | Description |
|-------|-------------|
| Spread and Volume | Rolling table of the last 6 seconds showing average bid-ask spread and aggregated bid/ask top-of-book volumes. Uses SAMPLE BY 1s on `commodities_market_data`. |
| Trade Execution vs Orderbook | Pairs each trade with the most recent order book snapshot via ASOF JOIN. Shows trade price, BBO at the time, and slippage in basis points relative to mid-price. Positive slippage = adverse execution. |
| OHLC - Bids - 1s | Candlestick chart of the best bid aggregated into 1-second bars with volume, plus a cumulative VWAP overlay line (yellow). |

**Indicators** (per-symbol row)

| Panel | Description |
|-------|-------------|
| VWAP / RSI / Bollinger | Multi-indicator candlestick chart using 15-minute OHLC bars from `commodities_trades_ohlcv_15m`. Overlays cumulative VWAP (from `commodities_trades_ohlcv_1m`), RSI-12h (from `commodities_bbo_1h`), and Bollinger Bands (20-period SMA +/- 2 standard deviations). |
| Bid vs Ask Volume + BBO | Aggregated bid and ask top-of-book volumes over 30-second intervals (bar-like), with best bid, best ask, and mid-price from `commodities_bbo_1s` overlaid. |

### Importing

1. In Grafana, go to Dashboards > Import
2. Upload `Commodities-Orderbook-Realtime-Demo.json`
3. Select your QuestDB datasource when prompted
4. The dashboard auto-refreshes at 250ms-1s and uses a symbol dropdown (defaults to CL)

Requires the [Plotly panel plugin](https://grafana.com/grafana/plugins/ae3e-plotly-panel/) for the Market Depth chart.

## Dependencies

```
questdb[dataframe]==4.0.0
psycopg[binary]==3.2.6
numpy==2.0.2
yfinance==0.2.65
requests==2.32.3
```
