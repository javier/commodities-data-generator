# Energy trading desk: real-time analytics demo on QuestDB

A synthetic but realistic energy trading desk (oil, gas, LNG, power, carbon) built to
show seven kinds of real-time analytics in SQL on QuestDB, against the desk's own
trading activity and live market data:

1. Intraday PnL
2. Intraday exposure
3. Volatility analytics
4. Forward curve analytics
5. Spread analytics
6. Historical valuation reconstruction
7. Pricing model validation

Prices are anchored to live front-month levels from Yahoo Finance, the curves follow a
two-factor model with seasonality and the Samuelson effect, trade sizes are exchange
lots, and the booking log is bitemporal. A planted storyline on the demo day gives every
act something to find. Everything is generic: no real desk, no real names.

| File | What it is |
|---|---|
| `energy_trading_data_generator.py` | The generator. Creates every table and view in code, backfills history with several workers, then streams in real time |
| `energy_demo_queries.sql` | The query pack: seven acts, one cell per analytic, with presenter notes |
| `check.py` | Smoke test: runs every cell and prints `ok` with row count and timing, or `FAIL` |
| `energy_backfill.sh`, `energy_realtime.sh` | Local runners (QuestDB OSS on `127.0.0.1:9000`) |
| `energy_enterprise_backfill.sh`, `energy_enterprise_realtime.sh` | Cluster runners (QWP over TLS, token auth, VPC-internal endpoints) |
| `requirements.txt` | `questdb[dataframe]>=5.0`, numpy, pandas, yfinance |

Tested end to end on QuestDB 10.0.1 (the `questdb/questdb:10.0.1` image) and on a
10.0.2 snapshot, both OSS, with Python 3.12. Transport is QWP
(WebSocket) for both SQL and ingestion through a single `questdb.connect()` handle; no
PGWire, no ILP/HTTP.

## Quick start

```bash
pip install -r requirements.txt
./energy_backfill.sh          # five days of history ending now, storyline on today
python check.py               # every cell should print ok
./energy_realtime.sh          # resumes from the backfill state, catches up, streams
```

Open the Web Console and paste cells from `energy_demo_queries.sql`, or load the whole
file. Cell `0a_planted_events` is the presenter's cheat sheet; keep it open.

The backfill rewrites the `@name := '...'` lines in the query pack (demo day, planted-event
timestamps, front-month symbols) so the cells point at the data it just loaded. In a
notebook you can define them once as notebook variables and delete the `DECLARE` lines.

### Timing the session

Planted events sit at fixed UTC times on the demo day (see below). If the session runs
before 15:00 UTC, the events at 13:20, 14:05, 14:10 and 14:30 have not happened yet:
the real-time generator still emits them at their wall-clock time during the session,
because the storyline is a function of the clock, not of the backfill. The reconstruction
cells (`6a` to `6c`) use `@asof` = 12:00 and `@t2` = 15:00 by default; before 15:00 the
"cancellations" and "amendments" columns of `6c` simply fill in as the afternoon unfolds.

To plant the story on a later day (backfill the days before the session, story on the
session day): `DEMO_DAY=2026-10-21 END_TS=2026-10-21T06:00:00Z ./energy_backfill.sh`,
then start real-time on the day.

## Running it

### Backfill: `--mode faster-than-life`

The parent process simulates the whole factor path per minute, plans every fill, booking
and snapshot the desk makes (so positions are continuous however the window is sharded),
writes the reference rows, then hands each worker a contiguous run of hour-aligned slices.
Concurrent writes land in different `quotes` partitions, so out-of-order merging stays
cheap. Every worker generates every table for its slice; rising-edge rows (settlements,
EOD snapshots, planted corrections) are pure functions of the timestamp, so they fire
exactly once. `--chunk_seconds` (default 900) bounds memory per send. More than three
workers mostly buys O3 merge work, not speed.

```
--start_ts / --end_ts   window, UTC ISO; default demo day minus 4 days at 00:00, to now
--demo_day              YYYY-MM-DD the storyline is planted on; default the end day
--scale_factor          multiplies every tick and fill rate; floats allowed
--processes             workers; default 3
--incremental true      skip what the database already holds (resume an interrupted run)
--tables a,b,c          create and populate only these base tables
--create_views / --create_live_view / --create_plain_views   default true
--parquet_encodings false   plain column types for a server that rejects PARQUET(...)
--enterprise true       with --short_ttl: STORAGE POLICY on tables instead of TTL
--short_ttl true        retention clauses (off for demo data in the past)
--static_anchors true   skip Yahoo, use FALLBACK_BRACKETS
--winter_repricing_pct  size of the planted cold-snap repricing at the front (default 8)
--seed                  everything is deterministic given the seed and the window
```

Volumes at `--scale_factor 1` for 4 October 00:00 to 7 October 15:50 UTC (a Sunday and
three weekdays): quotes 182M, curve_marks 1.3M, model_prices 760k, iv_marks 190k,
fills 15k, trade_events 17k, settlements 490, position_snapshots 335, plus the
materialized views (quotes_1m 930k, quotes_5m 240k). A full weekday is roughly 50M quotes
(low thousands of ticks a second in European hours, 10% at night); the load took about
90 seconds on a laptop with three workers. Fill counts scale with the same factor. The
Enterprise runner defaults to 0.5 for the gp3 volume.

### Real-time: `--mode real-time`

Single process, 250 ms slices (`--realtime_slice_ms`), timestamps two seconds ahead of
the wall clock. Started with `--incremental true` after a backfill it loads
`.energy_state.pkl` (RNG states, the factor path tail, the desk's positions and targets,
planted rows not yet due), catches up the gap since the last backfilled timestamp faster
than life, then paces itself. No price jump, no position jump at the boundary. Without a
state file it rebuilds from the database: last mid per contract (levels), last snapshot
plus today's fills and non-cancelled bookings (positions), last settlements. Anchors are
refreshed from Yahoo every `--yahoo_refresh_secs` (default 300) and the front months are
pulled gently towards them (two-hour half-life). `--end_ts` stops it; so does Ctrl-C,
which saves the state file.

### Schema conventions

- Every table is WAL with a designated timestamp. `instruments` and `limits` are the only
  tables that carry the epoch as their timestamp (small lookup tables); everything else,
  `demo_events` included, carries event time.
- `DEDUP UPSERT KEYS` on every table's natural key: re-running a backfill over the same
  window is an upsert, reference rows are rewritten harmlessly.
- Per-column `PARQUET(...)` encodings: `delta_binary_packed` for timestamps,
  `rle_dictionary` for symbols (with `bloom_filter` on the high-cardinality ones),
  `default` for numerics, `zstd(4)` throughout. Same on OSS and Enterprise; only storage
  policies are Enterprise-specific and sit behind `--enterprise`.
- Nothing is updated in place. A corrected mark is a new row with the same `ts` and
  `version + 1`; a trade amendment or cancel is a new row in the booking log.
- Partitioning: HOUR for `quotes`, DAY for minute-level and trade tables, MONTH and YEAR
  for the slow ones. Retention (`--short_ttl`) is placed after `PARTITION BY` and before
  `WAL`, the only placement the parser accepts, and never on the lookup tables.
- `quotes` and `fills` use `TIMESTAMP_NS`; everything else `TIMESTAMP`. Do not subtract
  the two in SQL (`datediff` is unit-safe, raw `-` is not).
- A table that exists with a different schema is a hard error, not something the
  generator works around.

## The market

| Curve | Complex | Unit | Ccy | Contracts | Exchange / CCP | Anchor |
|---|---|---|---|---|---|---|
| BRENT | OIL | bbl | USD | 24 months | ICE / ICE_CLEAR_EU | `BZ=F` |
| WTI | OIL | bbl | USD | 24 months | CME / CME_CLEARING | `CL=F` (Brent minus the live spread) |
| GASOIL | OIL | t | USD | 18 months | ICE | `HO=F` x 42 as the crack over Brent, x 7.45 |
| TTF | GAS | MWh | EUR | 38 months, 8 quarters, 4 seasons, 3 cals | ICE | `TTF=F` |
| NBP | GAS | therm (pence, `px_factor` 0.01) | GBP | same strip | ICE | TTF converted at live EURGBP plus a basis |
| JKM | LNG | MMBtu | USD | 12 months | ICE | `JKM=F` (TTF in $/MMBtu plus the live Asia premium) |
| UKPWR | POWER | MWh | GBP | 38 months, 8 quarters, 4 seasons, 3 cals | EEX / ECC | NBP / 50% + UKA x 0.2 / 50% + clean spark margin |
| EUA | CARBON | tCO2 | EUR | Dec-26/27/28 | ICE | static bracket (no reliable ticker) |
| UKA | CARBON | tCO2 | GBP | Dec-26/27/28 | ICE | EUA x EURGBP minus the live discount |
| FX | | | | EURUSD, GBPUSD, EURGBP | FX_FEED | `EURUSD=X`, `GBPUSD=X` |

Gas and power months run far enough to cover the last listed cal so every quarter,
season and cal decomposes into listed months; strips are derived from their months
(days-weighted for gas, hours-weighted for power, DST-aware: 743 hours in March, 745 in
October), which is why the strip consistency check holds by construction and only the
planted bad mark breaks it. Delivery periods are stored as calendar-month boundaries at
00:00 UTC; lot sizes are 1,000 bbl, 100 t, 1 MW x hours (TTF, UKPWR), 1,000 therms/day x
days (NBP), 10,000 MMBtu, 1,000 tCO2. Brent keeps its real expiry rule (last business day
of the second month before delivery) so the front month matches `BZ=F`; WTI and gasoil
expire on the last business day of the month before delivery, gas and power two business
days before delivery, carbon on the last Monday of December. Nothing expires inside a
demo window.

Two-factor model per base curve (Brent, TTF, EUA): a slow random walk `L(t)` (daily vol
0.8% oil, 1.5% gas, 1.2% carbon) and a fast mean-reverting factor `S(t)` (two-hour
half-life) damped by `exp(-kappa * tau)`, with kappa such that the 12-month contract
carries about half the front month's vol. Front-month realised vol lands near 30% Brent,
60% TTF, 65% UK power, 35% carbon. Brent and WTI are backwardated (minus 2% per year),
carbon in contango (plus 4%), gas and power seasonal (jittered 2% at startup). Derived
curves add mean-reverting spreads whose centres come from the live anchors. Within a
minute, the short factors follow a Brownian bridge keyed on (seed, minute, factor), so
the parent and every worker compute the same path without sharing it.

Anchors are fetched once at startup and refreshed in real time. If a fetch fails the
generator falls back to `FALLBACK_BRACKETS` (dated 2026-10-07 in the file; update them on
the day) and logs it. EUA and UKA always come from the static values: check the current
levels and set them before a session.

Tick rates per contract follow liquidity: front months tick many times a second, months
4 to 12 every few seconds, back months and strips every 10 to 60 seconds, FX several
times a second. European hours (07:00 to 17:00 UTC) run at full rate, 17:00 to 22:00 at
40% (70% for oil), nights and weekends at 10% but never zero. Spreads are one tick on
front oil, two to four ticks on front gas and power, widening with tenor and off-hours.

Other market tables, all pure functions of the clock: `curve_marks` every minute for
every contract (`MARKET` from the quotes, `INTERP` from the model when a contract has
not ticked in five minutes or during the feed outage); `settlements` at 16:30 UTC for
gas, power and carbon and 19:30 UTC for oil, weekdays only; `iv_marks` every five
minutes for the front twelve months of Brent, TTF and UK power at five delta buckets
(crude put skew, gas and power call skew); `model_prices` every minute for the first 24
gas and power months, `champion_v1` (seasonal table calibrated once) and
`challenger_v2` (refits hourly), both with 10 bps of noise and an `inputs_ts` lineage
column; `da_prices` for UK day-ahead, 24 hours published at 12:45 the day before.

## The desk

Books `CRUDE`, `PRODUCTS`, `LNG`, `EU_GAS`, `UK_POWER`, `CARBON`, traders `trader_01` to
`trader_24` (four or five per book), trade support `ops_01` to `ops_06`. Position limits
per book and curve in `limits`, in delivery units.

`fills` holds exchange executions only (ICE, EEX, CME by the contract's exchange): sizes
log-normal around five lots with a tail to fifty, strips one to ten lots, a price at the
touch 60% of the time (`passive = false`) or resting at the touch 40%. Each book's net
position per curve follows a slow mean-reverting target. `CRUDE` trades with foresight (it
picks the side of the next 15-minute move 60% of the time), so its markouts rise with the
horizon; `PRODUCTS` always crosses the spread and has no foresight, so its markouts sit
flat and negative; the other books are coin flips.

`trade_events` is the booking log: every fill is mirrored by STP within 50 to 500 ms with
the CCP as counterparty; broker and bilateral deals (about 10% of the fill count, strips
and back months, 10 to 100 lots capped at 5% of the book's limit) exist only here, booked
by trade support after a delay (broker median 20 minutes, bilateral median two hours with
a tail to the next morning). Counterparties are an invented pool of 40 (`BANK_`,
`UTILITY_`, `TRADER_`, `PRODUCER_`, `INDUSTRIAL_`): banks and trading houses are mildly
informed, utilities and industrials uninformed, producers neutral. Background amendments
(version 2: `PX_CORRECTION`, `QTY_CORRECTION`, `BOOK_TRANSFER`) and cancels are rare on
STP-mirrored exchange fills (0.3% and 0.1%) and common on hand-booked voice deals (5%
and 1%).

`position_snapshots` is written at 00:00 UTC each day: the net position per book and
contract from fills, deals and bookings as known at that moment, at the last settlement.
It anchors intraday PnL (the `ledger` view rewrites it as a pseudo-fill at settlement)
and the reconstruction.

## The planted storyline (demo day, UTC)

| Time | What | Shows up in |
|---|---|---|
| day minus 2, 01:00 to 05:00 | Windy night: UK day-ahead hours clear negative | `3e` |
| 07:40 to 08:10, unwound by 11:30 | CRUDE buys the Brent front month to about 3M bbl against a 2M limit | `2a`, `2b` |
| 09:10 deal, 14:30 booking | UK_POWER sells 100 MW of a power season by voice, booked 5h20m late | `6a`, `6b`, `6c`, `6d` |
| 09:15 to 09:45 | Manual mark on the first TTF quarter 2.50 EUR above its months (trader_11), corrected as version 2 at 09:45 | `4d` |
| 10:00 to 10:03 | Quotes outage on EEX-routed contracts; marks fall back to INTERP, model inputs go stale | `7d`, `4a` |
| 10:05, amended 13:20 | LNG fill booked with 10x the quantity by STP | `6b`, `6c` |
| 10:30, cancelled 14:05 | EU_GAS broker trade on the TTF quarter booked twice | `6b`, `6c` |
| 11:00 | Cold-snap forecast: Nov to Feb gas and power prices step up 8% at the front over 20 minutes (`--winter_repricing_pct`), damped along the curve; the champion model never adapts, the challenger refits on the hour | `7a`, `7b` |
| 14:10 to 14:30 | Bad ATM vol mark on the fourth TTF month (minus 8 points) breaks calendar no-arbitrage, corrected as version 2 | `7c` |

All of them are listed in `demo_events` (cell `0a`).

## The query pack, act by act

Every cell runs on its own. `DECLARE @name := ...` at the top of a cell holds the demo
day, the planted timestamps and the front-month symbols; the backfill rewrites them.
Chart cells return `ts` first and numeric columns after, so they drop into Grafana.

1. **Intraday PnL.** `1a` PnL by book in USD: the opening book revalued from settlement
   against today's trading, one GROUP BY over the `ledger` view. `1b` the PnL curve with
   drawdown on a 5-minute grid from the `positions_live` live view, and
   `1b_twin_window_function`, the same cell as window functions over `fills` (live views
   are beta in 10.0). `1c` markouts by desk with `HORIZON JOIN` at 0, 1s, 10s, 1m, 5m, 15m
   on raw ticks. `1d` markouts by counterparty type over the booking log, from deal
   time, at 1m to 4h against the 1-minute bars (voice deals are timed to the minute).
2. **Exposure.** `2a` net position against limits, now versus intraday peak (the breach).
   `2b` the breach chart. `2c` the delivery-month ladder with strips spread over their
   months, `PIVOT`ed by curve.
3. **Volatility.** `3a` realised vol by tenor (the Samuelson effect). `3b` rolling
   realised, Parkinson and EWMA (`avg(x, 'alpha', 0.06) OVER`) on the front month.
   `3c` the implied term structure with risk reversal and butterfly. `3d` implied minus
   realised. `3e` why negative power prices break log returns.
4. **Forward curves.** `4a` live marks versus the last settlement. `4b` curve shape.
   `4c` strip consistency now, `4d` the same check over every minute of the day as the
   marks were published (finds the manual mark). `4e` the curve as it stood at `@asof`.
5. **Spreads.** `5a` JKM minus TTF in $/MMBtu, `5b` gasoil crack and Brent-WTI, `5c` the
   UK clean spark spread with a z-score, as one-point-a-minute series from the 1-minute
   bars; `5a_ticks_half_hour` and `5c_ticks_half_hour` are the same spreads on raw ticks
   over 30 minutes with `ASOF JOIN ... TOLERANCE` on every leg (the "legs tick at
   different times" point, with each leg's tick time in the output); `5d` the forward
   clean spark curve; `5e` leg correlation and the hedge ratio.
6. **Reconstruction.** `6a` the book as known at `@asof` versus as restated, `6b` the
   trades that changed, `6c` PnL forensics between `@t1` and `@t2` whose five columns add
   up to the total, `6d` booking latency by channel. The as-known and as-restated views
   differ by one line, `WHERE booked_ts <= @asof`.
7. **Model validation.** `7a` champion versus challenger (bias, RMSE, share inside the
   quote, by season, before and after the repricing: the champion's winter bias after
   11:00 is about -500 bps, the challenger's about -90 in its one lagging hour and zero
   after), `7b` hourly drift, `7c` calendar-arbitrage violations on the vol surface as
   published and `7c_as_corrected` on the latest version, `7d` how much of the grading
   had a fresh quote to grade against, minute by minute around the outage.

### QuestDB notes from building this

- `ASOF JOIN ... TOLERANCE` over several legs: write each leg as the table with a filter,
  `(energy_quotes WHERE symbol = @x) a ASOF JOIN (energy_quotes WHERE ...) b TOLERANCE 30s`.
  The same join over CTEs takes a slow path and times out on tens of millions of rows.
- `HORIZON JOIN` wants a plain table on the right. For a derived left side (markouts from
  deal time rather than booking time) wrap it: `FROM (t TIMESTAMP(ts)) AS t`. Put the
  time filter on the left side inside the parentheses; a `WHERE` after the join runs
  after it and the join reads every quote in the table (2.4 s instead of 0.4 s here).
- Series cells read the 1-minute bars (`quotes_1m`), not ticks: a day-long spread or a
  model-grading pass over 180M quotes takes seconds on ticks and milliseconds on bars,
  and the bar's last quote is the ASOF match with a one-minute tolerance.
- The first run of a cell after a load or a restart is slower (cold pages); the timings
  below are warm.

### Query timings on the scale 1 dataset

Measured with `check.py` on a laptop (QuestDB 10.0.2, 182M quotes, four days), warm:

| Cell | ms | Cell | ms |
|---|---|---|---|
| `1a_pnl_by_book` | 195 | `5a_lng_arb_jkm_ttf` | 15 |
| `1b_pnl_curve_and_drawdown` | 239 | `5a_ticks_half_hour` (18k rows) | 110 |
| `1b_twin_window_function` | 201 | `5b_gasoil_crack_and_brent_wti` | 5 |
| `1c_markouts_by_desk` | 422 | `5c_clean_spark_zscore` | 15 |
| `1d_markouts_by_counterparty` | 13 | `5c_ticks_half_hour` (54k rows) | 222 |
| `2a`, `2b`, `2c` | 2 to 7 | `5d`, `5e` | 6 to 9 |
| `3a` to `3e` | 1 to 16 | `6a`, `6b`, `6c`, `6d` | 3 to 102 |
| `4a`, `4b`, `4c`, `4e` | 2 to 4 | `7a`, `7b` | 112, 89 |
| `4d_consistency_history` | 148 | `7c`, `7c_as_corrected`, `7d` | 12 to 31 |

Everything is under half a second warm; the first run after a load is a few seconds on
the tick-level cells (`1c`, the two `_ticks_` cells) while the quote pages come in. On
the cluster expect the same shape with slower cold runs on a gp3 volume.
- `LATEST ON` applies a `WHERE` at the same level before picking the row; status filters
  go in an outer query, or a cancelled trade's earlier NEW row comes back.
- `PIVOT` after a `WITH` only works as `SELECT * FROM cte PIVOT (...)`.
- `::decimal(p,s)` truncates, so `round()` first.
- TTL must be an integer multiple of the partition unit and sits before `WAL`.
- The Python client sends UUID columns from strings, drops nothing silently except that a
  symbol column that is entirely null must be dropped from the frame before sending.

## Simplifications to be upfront about

- Price dynamics are synthetic. Levels are anchored to real closes, relationships (crack,
  Brent-WTI, Asia premium, UKA discount) to the real ones on the day, but the paths are
  simulated and the market diffuses around the clock, so vol is annualised on calendar
  time (365 x 288 five-minute bars).
- UK power is routed through EEX so the EEX outage story has contracts to hit; in the
  real market it trades on ICE and bilaterally too.
- Marks are the curve builder's fair value quantized to the tick; the model prices add
  10 bps of noise on top of the same fair value, which is what makes "inside the quote"
  a meaningful grade.
- PnL marks at mid and converts at the latest FX tick. Realised versus unrealised PnL
  needs a lot-matching convention and is left to the position engine.
- The database stores and checks model inputs and outputs. It does not replace the
  pricing library or the curve bootstrapper.
