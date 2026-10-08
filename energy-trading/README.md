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
| `check.py` | Smoke test: runs every cell and prints `ok` with row count and timing, or `FAIL`; `--dump` and `--compare` check a change against a saved run |
| `energy_backfill.sh`, `energy_realtime.sh` | Local runners (QuestDB OSS on `127.0.0.1:9000`) |
| `energy_enterprise_backfill.sh`, `energy_enterprise_realtime.sh` | Cluster runners (QWP over TLS, token auth, VPC-internal endpoints) |
| `requirements.txt` | `questdb[dataframe]>=5.0`, numpy, pandas, yfinance |
| `grafana/` | The Energy Trading Desk dashboard for the live session, its README and a screenshot |

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

Volumes at `--scale_factor 1` for 4 October 00:00 to 8 October 12:00 UTC (a Sunday,
three full weekdays and a morning): quotes 255M (27M of them on EEX), curve_marks 1.6M,
model_prices 930k, iv_marks 233k, fills 19k, trade_events 21k, settlements 729,
position_snapshots 729, plus the materialized views (quotes_1m 1.15M, quotes_5m 297k). A
full weekday is about 71M quotes, 7.5M of them on EEX (low thousands of ticks a second
in European hours, 10% at night); the load took about two minutes on a laptop with three
workers. Fill counts scale with the same factor. The
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

- Every table is WAL with a designated timestamp. `instruments`, `listings` and `limits`
  are the only tables that carry the epoch as their timestamp (small lookup tables, with
  `ts` as the last column); everything else, `demo_events` included, carries event time.
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
  generator works around. The listings change reshaped `instruments` (the exchange
  columns moved to `listings`), `limits` (`ts` last), `curve_marks` and
  `position_snapshots` (a `venue` column) and `quotes_1m` (primary venues only): an
  instance loaded before it needs a clean reload. Drop every `energy_` view, live view,
  materialized view and table, then run the backfill.

## The market

| Curve | Complex | Unit | Ccy | Contracts | Venues (MIC), product code, primary first | Anchor |
|---|---|---|---|---|---|---|
| BRENT | OIL | bbl | USD | 24 months | ICE Futures Europe (IFEU), `BRN` | `BZ=F` |
| WTI | OIL | bbl | USD | 24 months | CME NYMEX (XNYM), `CL` | `CL=F` (Brent minus the live spread) |
| GASOIL | OIL | t | USD | 18 months | ICE Futures Europe, `ULS` (Low Sulphur Gasoil) | `HO=F` x 42 as the crack over Brent, x 7.45 |
| TTF | GAS | MWh | EUR | 38 months, 8 quarters, 4 seasons, 3 cals | ICE Endex (NDEX), `TFM`; EEX (XEEE), `G3BM`/`G3BQ`/`G3BS`/`G3BY` | `TTF=F` |
| NBP | GAS | therm (pence, `px_factor` 0.01) | GBP | same strip | ICE Futures Europe, `GWM` | TTF converted at live EURGBP plus a basis |
| JKM | LNG | MMBtu | USD | 12 months | ICE Futures Europe, `JKM` | `JKM=F` (TTF in $/MMBtu plus the live Asia premium) |
| UKPWR | POWER | MWh | GBP | 38 months, 8 quarters, 4 seasons, 3 cals | ICE Futures Europe, `UBL` (UK Base Electricity, Gregorian); EEX, `FUBM`/`FUBQ`/`FUBS`/`FUBY` | NBP / 50% + UKA x 0.2 / 50% + clean spark margin |
| EUA | CARBON | tCO2 | EUR | Dec-26/27/28 | ICE Endex, `ECF`; EEX, `FEUA` | static bracket (no reliable ticker) |
| UKA | CARBON | tCO2 | GBP | Dec-26/27/28 | ICE Futures Europe, `UKA` | EUA x EURGBP minus the live discount |
| FX | | | | EURUSD, GBPUSD, EURGBP | FX_FEED | `EURUSD=X`, `GBPUSD=X` |

Venues and product codes are taken from the exchanges' own code lists (ICE's product
code file, EEX's short-code list, checked 2026-10-08). The `exchange` column of
`listings` reads `ICE`, `ICE_ENDEX`, `EEX` or `CME`; all ICE contracts clear at ICE Clear
Europe (`ICE_CLEAR_EU`), EEX contracts at European Commodity Clearing (`ECC`), WTI at
`CME_CLEARING`. EEX lot and tick sizes match the ICE contracts they compete with.

Gas and power months run far enough to cover the last listed cal so every quarter,
season and cal decomposes into listed months; strips are derived from their months
(days-weighted for gas, hours-weighted for power, DST-aware: 743 hours in March, 745 in
October), which is why the strip consistency check holds by construction and only the
planted bad mark breaks it. Delivery periods are stored as calendar-month boundaries at
00:00 UTC; lot sizes are 1,000 bbl, 100 t, 1 MW x hours (TTF, UKPWR), 1,000 therms/day x
days (NBP), 10,000 MMBtu, 1,000 tCO2; JKM ticks in tenths of a cent. Expiries follow each
exchange's rule (weekends only, no holiday calendar beyond the year end): Brent the last
business day of the second month before delivery (a day earlier when that is the day
before Christmas or New Year); WTI three business days before the 25th of the prior
month; gasoil two business days before the 14th of the delivery month, so its front
month trades into its own month; JKM the 15th of the prior month; gas and power two
business days before delivery; EUA and UKA Decembers the penultimate Monday of December
(the exchange's last-Monday rule always rolls back a week in December because of the
Christmas and New Year bank holidays).

## Instruments and listings

An instrument is the thing the desk carries risk in: TTF January 2027 has one fair value
and one position, wherever it was traded. A listing is that instrument on one venue,
with its own exchange code, contract name, clearing house, lot and tick. Risk and PnL
aggregate by instrument; margin aggregates by listing, because each clearing house calls
margin on what is open with it. A long on ICE Endex and a short on EEX in the same month
are flat for risk and two open positions for margin.

`instruments` holds one row per contract (curve, delivery period, units, `term_code`,
`month_code`); `listings` one row per contract and venue (`exchange`, `mic`,
`exchange_code`, `exchange_physical_code`, `exchange_symbol`, `ccp`, `lot_size`,
`tick_size`, `is_primary`, `liquidity_share`). The `energy_instrument_master` view joins
them with the tenors. Three curves are dual-listed: TTF and EUA on ICE Endex (primary)
and EEX, UK power on ICE Futures Europe (primary) and EEX. Every other curve has one
listing. `liquidity_share` sums to 1 per instrument: 80/20 on months and carbon
Decembers, 70/30 on strips, where a second venue picks up more of the business.

The secondary venue's book is thinner and slightly behind: 40% of the primary's tick
rate, half its displayed size, a spread 1.5 to 2 times wider (drawn per listing), quotes
50 to 300 ms behind fair value, and a slow cross-venue basis of about a third of a tick.
On weekdays in European hours the front months of the dual-listed curves also get
planted divergences, about one every three hours, 2 to 4 ticks for 5 to 30 seconds
(`5f_divergence_episodes` finds them).

Routing. Each screen order picks a venue at random, weighted by `liquidity_share`,
doubled on a venue where the book holds the opposite side (closing where you are open
releases margin) and 1.5 times on the venue showing the better touch for the order's side.
A fill routed to the secondary venue is capped at its displayed size; the remainder fills
on the primary 1 microsecond later as a second fill with the same `order_id`, which is
how an order splits across venues. Every fill on the secondary venue is preceded, one
nanosecond earlier, by a quote on that venue showing the book the router saw, so an
`ASOF JOIN` from any EEX fill to EEX quotes finds the price it hit and a displayed size
at least as large as the fill. While a venue's feed is down the router skips it. On the
scale 1 dataset 19% of month fills, 22% of carbon December fills and 26% of strip fills
on the dual-listed curves went to EEX, and 3% of orders split. Broker and bilateral
deals are not exchange trades and carry the venue `OTC` in positions.

Listings change where the desk trades, not what it trades. Checked against the generator
before listings, same seed, window and anchors (scale 0.2, 4 to 7 October): the same
3,369 orders with the same ids, times and quantities (109 of them now split into two
fills), the same instrument positions in every snapshot, and a desk PnL for the demo day
of 4,676,598 USD against 4,678,374 before. The difference, -1,776 USD, is the extra spread
paid on the 64 fills routed to EEX that day (-1,800 USD); the remaining 24 USD is FX
conversion, because the extra EEX quote streams moved the FX feed's last tick (17 USD of
it on the opening book). FX ticks now draw from their own random stream, so a future
change to the quote streams leaves every USD conversion exactly where it was.

| Book (demo day PnL, USD) | Before listings | After listings | Difference | Extra spread on EEX fills |
|---|---|---|---|---|
| CARBON | -621,695 | -621,933 | -238 | -235 |
| EU_GAS | 6,228,471 | 6,227,516 | -955 | -980 |
| LNG | -450,574 | -450,632 | -58 | -57 |
| UK_POWER | 459,492 | 458,967 | -525 | -528 |
| CRUDE | -37,320 | -37,320 | 0 | 0 |
| PRODUCTS | -900,000 | -900,000 | 0 | 0 |
| Desk | 4,678,374 | 4,676,598 | -1,776 | -1,800 |

CRUDE and PRODUCTS trade only single-listed curves, so routing never touches them.

The planted feed outage silences EEX, not the primary. Marks keep coming from ICE, so
`curve_marks` stays `MARKET` on the dual-listed curves and carries the `venue` it was
taken from (the best fresh quote; null on `INTERP` marks).

## Symbology and tenor

Every contract has a desk name in `instruments` and an exchange name per listing in
`listings`:

| Name | Example (TTF January 2027) | What it is |
|---|---|---|
| `symbol` | `TTF_Jan-27` | The desk's readable id, how a trader says it. Every cell joins on this. |
| `exchange_symbol`, ICE Endex | `TFM FMF0027` | The primary exchange's own contract name. |
| `exchange_symbol`, EEX | `G3BM 2027-01` | The same contract on the secondary venue. |

ICE builds a futures name as the logical product code (left-justified to four
characters, so gasoil is `ULS FMV0026`), `F` for futures, the term letter (`M` month, `Q`
quarter, `S` season, `Y` calendar year), the month code of the first delivery month, `00`
for the whole period and the two-digit year; a season adds a `.` switch and its last
delivery month (`GWM FSV0027.H0028` is NBP winter 2027). Carbon Decembers are months with
month code `Z` (`ECF FMZ0026`). ICE publishes two codes per product: the logical code
used in contract names (`ULS` for Low Sulphur Gasoil) and the physical code used on the
clearing side (`G`). `exchange_code` holds the logical one, `exchange_physical_code` the
physical one (equal to the logical code on most ICE products and on CME, null on EEX). CME uses root,
month code and two-digit year (`CLF27`). EEX identifies a contract by product code (one
per granularity: `G3BM` TTF month, `G3BQ` quarter, `G3BS` season, `G3BY` year) plus
expiry year and month as separate fields; `exchange_symbol` joins them for display with
the first delivery month. `term_code` and `month_code` stay in `instruments`.

| Month | F | G | H | J | K | M | N | Q | U | V | X | Z |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| | Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |


**Tenor.** The delivery period in the symbol (`Jan-27`) is the absolute tenor. What a
vol analyst or a risk report means by tenor is the relative one: the position on the
curve from today, M1 (front month), M2, ..., Q1, S1, Y1, and Z1 for the first carbon
December. It rolls (Jan-27 is M3 today and M2 next month), so it is computed at query
time by the `energy_tenors_asof` view: contracts unexpired at `@asof` numbered per curve
and granularity in delivery order, with `tenor`, `tenor_n` (the number, for sorting and
bucketing) and `months_to_delivery`. `@asof` defaults to `now()` (`energy_tenors` is that
default); cells about a past instant (`4e`, `6a`, `6b`) set it, so a reconstructed book
reads in the tenors the desk saw then.

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
not ticked in five minutes on any of its venues, with the `venue` the mark was taken
from: the best fresh quote, so the primary while EEX is down); `settlements` at 16:30 UTC for
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

`fills` holds exchange executions only, with the `venue` they were routed to (`ICE`,
`ICE_ENDEX`, `EEX` or `CME`) and an `order_id` shared by the two fills of a split order
(see Instruments and listings): sizes
log-normal around five lots with a tail to fifty, strips one to ten lots, a price at the
touch 60% of the time (`passive = false`) or resting at the touch 40%. Each book's net
position per curve follows a slow mean-reverting target. `CRUDE` trades with foresight (it
picks the side of the next 15-minute move 60% of the time), so its markouts rise with the
horizon; `PRODUCTS` always crosses the spread and has no foresight, so its markouts sit
flat and negative; the other books are coin flips.

`trade_events` is the booking log: every fill is mirrored by STP within 50 to 500 ms with
the venue's CCP as counterparty; broker and bilateral deals (about 10% of the fill count, strips
and back months, 10 to 100 lots capped at 5% of the book's limit) exist only here, booked
by trade support after a delay (broker median 20 minutes, bilateral median two hours with
a tail to the next morning). Counterparties are an invented pool of 40 (`BANK_`,
`UTILITY_`, `TRADER_`, `PRODUCER_`, `INDUSTRIAL_`): banks and trading houses are mildly
informed, utilities and industrials uninformed, producers neutral. Background amendments
(version 2: `PX_CORRECTION`, `QTY_CORRECTION`, `BOOK_TRANSFER`) and cancels are rare on
STP-mirrored exchange fills (0.3% and 0.1%) and common on hand-booked voice deals (5%
and 1%).

`position_snapshots` is written at 00:00 UTC each day: the net position per book,
contract and venue from fills, deals and bookings as known at that moment, at the last
settlement. Broker and bilateral deals sit under the venue `OTC`; summing over venues
gives the instrument position.
It anchors intraday PnL (the `ledger` view rewrites it as a pseudo-fill at settlement)
and the reconstruction.

## The planted storyline (demo day, UTC)

| Time | What | Shows up in |
|---|---|---|
| day minus 2, 01:00 to 05:00 | Windy night: UK day-ahead hours clear negative | `3e` |
| 07:40 to 08:10, unwound by 11:30 | CRUDE buys the Brent front month to about 3M bbl against a 2M limit | `2a`, `2b` |
| 09:10 deal, 14:30 booking | UK_POWER sells 100 MW of a power season by voice, booked 5h20m late | `6a`, `6b`, `6c`, `6d` |
| 09:15 to 09:45 | Manual mark on the first TTF quarter 2.50 EUR above its months (trader_11), corrected as version 2 at 09:45 | `4d` |
| 10:00 to 10:03 | The EEX quote feed (TTF, EUA, UK power) goes silent; ICE keeps ticking, marks stay `MARKET` from the primary venue, grading carries on | `7d` |
| 07:00 to 17:00, every weekday | About three times a day per dual-listed front month, EEX sits 2 to 4 ticks off the primary for 5 to 30 seconds (five on TTF Nov-26 on the demo day) | `5f` |
| 10:05, amended 13:20 | LNG fill booked with 10x the quantity by STP | `6b`, `6c` |
| 10:30, cancelled 14:05 | EU_GAS broker trade on the TTF quarter booked twice | `6b`, `6c` |
| 11:00 | Cold-snap forecast: Nov to Feb gas and power prices step up 8% at the front over 20 minutes (`--winter_repricing_pct`), damped along the curve; the champion model never adapts, the challenger refits on the hour | `7a`, `7b` |
| 14:10 to 14:30 | Bad ATM vol mark on the fourth TTF month (minus 8 points) breaks calendar no-arbitrage, corrected as version 2 | `7c` |

All of them are listed in `demo_events` (cell `0a`).

## Views

The definitions the cells share live in plain views, created by the generator with the
tables (`--create_plain_views`, `CREATE OR REPLACE`, so a re-run updates a definition in
place). A view stores nothing: its query is inlined into the query that references it,
so filters and joins are optimised across it. The parameterised ones declare their
variables `OVERRIDABLE`:

| View | Parameter, default | Returns |
|---|---|---|
| `energy_marks_asof` | `@asof`, `now()` | latest mark per contract in the day up to `@asof`: `price, version, source, venue, ts` |
| `energy_fx_asof` | `@asof`, `now()` | latest `usd` mid per FX pair in the hour up to `@asof` |
| `energy_usd_factor_asof` | `@asof`, `now()` | per contract, `px_factor` times its currency's USD rate: what one unit of price is worth in USD |
| `energy_tenors_asof` | `@asof`, `now()` | relative tenor of every contract unexpired at `@asof`, with the primary exchange symbol |
| `energy_book_asof` | `@asof`, `now()` | deals done on the day of `@asof` (from 00:00), as known at `@asof`: latest version per deal booked by then, cancelled ones dropped |
| `energy_book_restated` | `@asof`, `now()` | the same deals as the booking log says now |
| `energy_mid_1m_day` | `@day`, today's 00:00 | one day of 1-minute bars: `close, last_bid, last_ask, ticks` |
| `energy_positions_running_day` | `@day`, today's 00:00 | running position and cash per book and contract over the day's fills, and the running position per venue |
| `energy_model_graded_day` | `@day`, today's 00:00 | every model price of the day against the last quote of the preceding minute: `err_bps`, season, tenor |
| `energy_strip_gaps_day` | `@day`, today's 00:00 | per minute and strip, as published: strip mark, the weighted average of its months, the gap |

Without parameters: `energy_ledger` (opening book as a pseudo-fill plus today's fills),
`energy_tenors` and `energy_curve_marks_latest` and `energy_trade_events_latest` (the
`_asof` and `book_restated` views at their defaults), `energy_instrument_master` (one
row per listing).

A cell overrides a parameter with its own leading `DECLARE`, and every view it touches
follows, nested ones included:

```sql
DECLARE @asof := '2026-10-07T12:00:00Z'
SELECT b.book, sum(b.qty * (m.price - b.px) * u.factor) AS mtm_usd
FROM energy_book_asof b
JOIN energy_marks_asof m ON (symbol)
JOIN energy_usd_factor_asof u ON (symbol)
GROUP BY b.book;
```

Cells about the demo day as a whole set `@asof` to its last instant,
`dateadd('d', 1, @demo_day::timestamp) - 1`, and `@day := @demo_day`. `@day` is the
day's start (`timestamp_floor('d', now())` by default; a `'YYYY-MM-DD'` string works),
so a view can also bound a joined table relative to it: the grading view reads the bars
from the minute before the day. Views take times,
never instruments: one statement holds one value per variable, so a spread reads the
same day view once per leg with `WHERE symbol = ...`, and that filter is pushed down.
`6c` compares two instants, `@t1` and `@t2`, so it keeps its two sets of marks and
bookings inline; the mechanics are the point of that cell anyway.

The reconstruction rests on two definitions. `SHOW CREATE VIEW energy_book_asof` and
`SHOW CREATE VIEW energy_book_restated` differ by one line:

```sql
WHERE booked_ts <= @asof
```

`0e_definitions` lists every view with its status.

## The query pack, act by act

Every cell runs on its own. `DECLARE @name := ...` at the top of a cell holds the demo
day, the planted timestamps and the front-month symbols; the backfill rewrites them.
Chart cells return `ts` first and numeric columns after, so they drop into Grafana.
Every cell returns the contract it is about: contract-grain cells carry `symbol` (plus
`tenor` and `exchange_symbol` in acts 3, 4, 5 and 7), cells pinned to one contract by a
`DECLARE` return it as a constant column, and book-level cells have a `_by_symbol`
drill-down sibling right below them.

0. **Orientation.** `0a` the planted events, `0b` volumes, `0c_instrument_master` every
   unexpired listing with its desk symbol, exchange symbol, venue, clearing house, tenor,
   delivery and lot, from the `energy_instrument_master` view. `0d_listing_lookup` the
   other way round: paste an exchange symbol (`G3BM 2026-11`) and get the instrument
   behind it and that venue's latest quote. `0e_definitions` the views every cell stands
   on.
1. **Intraday PnL.** `1a` PnL by book in USD: the opening book revalued from settlement
   against today's trading, one GROUP BY over the `ledger` view; `1a_pnl_by_book_by_symbol`
   the same per contract. `1b` the PnL curve with drawdown on a 5-minute grid from the
   `positions_live` live view, `1b_twin_window_function` the same cell as window
   functions over `fills` (live views are beta in 10.0), `1b_pnl_curve_by_symbol` one
   series per contract. `1c` markouts by desk with `HORIZON JOIN` at 0, 1s, 10s, 1m, 5m,
   15m on raw ticks, each fill against its own venue's quotes. `1d` markouts by
   counterparty type over the booking log, from deal time, at 1m to 4h against the
   1-minute bars (voice deals are timed to the minute). `1e_markouts_by_venue` the `1c`
   markouts split by venue (EEX fills start further from mid: the wider spread).
2. **Exposure.** `2a` net position against limits, now versus intraday peak (the breach),
   `2a_limits_by_symbol` which contracts make it up. `2b` the breach chart. `2c` the
   delivery-month ladder with strips spread over their months, `PIVOT`ed by curve.
   `2d_exposure_by_tenor` the risk-report view: front, M2 to M6, back and strips.
   `2e_position_by_instrument_vs_venue` the instrument position next to its pieces per
   venue (live view `positions_live_by_venue`, with `2e_twin_window_function` as the
   window-function twin): flat for risk can still be open at two clearing houses.
   `2f_venue_share_of_fills` where each book's fills went, per instrument.
3. **Volatility.** `3a` realised vol of every contract on every curve by relative tenor
   (the Samuelson effect: M1 about twice M12). `3b` rolling realised, Parkinson and EWMA
   (`avg(x, 'alpha', 0.06) OVER`) on the front month. `3c` the implied term structure
   with risk reversal and butterfly. `3d` implied minus realised. `3e` why negative power
   prices break log returns.
4. **Forward curves.** `4a` live marks versus the last settlement. `4b` curve shape, with
   M1, M2 and M12 taken from the tenors view. `4c` strip consistency now, `4d` the same
   check over every minute of the day as the marks were published (finds the manual
   mark). `4e` the curve as it stood at `@asof`, in the tenors of that moment.
5. **Spreads.** `5a` JKM minus TTF in $/MMBtu, `5b` gasoil crack and Brent-WTI, `5c` the
   UK clean spark spread with a z-score, as one-point-a-minute series from the 1-minute
   bars; `5a_ticks_half_hour` and `5c_ticks_half_hour` are the same spreads on raw ticks
   over 30 minutes with `ASOF JOIN ... TOLERANCE` on every leg (the "legs tick at
   different times" point, with each leg's tick time in the output); `5d` the forward
   clean spark curve by power tenor; `5e` leg correlation and the hedge ratio.
   `5f_cross_venue_spread` the TTF front month on ICE Endex minus EEX, second by second
   through European hours, in ticks; `5f_divergence_episodes` the minutes where the two
   venues sat 1.75 ticks or more apart for at least three seconds (on the demo day it
   finds the five planted TTF episodes and nothing else).
6. **Reconstruction.** `6a` the day's trades as known at `@asof` versus as restated (and
   `6a_by_symbol`), `6b` the trades that changed, `6c` PnL forensics between `@t1` and
   `@t2` whose five columns add up to the total (and `6c_by_symbol`), `6d` booking latency
   by channel (and `6d_by_symbol`). `6e_margin_by_ccp` gross and net notional per book
   and clearing house (ICE Clear Europe, ECC, CME; voice deals as `BILATERAL`). The
   as-known and as-restated views, `energy_book_asof` and `energy_book_restated`, differ
   by one line, `WHERE booked_ts <= @asof`. Late voice bookings are normal desk life (broker median 20
   minutes, bilateral two hours), so `6b` lists the three planted cases alongside every
   ordinary voice deal done before `@asof` and booked after it.
7. **Model validation.** `7a` champion versus challenger (bias, RMSE, share inside the
   quote, by season, before and after the repricing: the champion's winter bias after
   11:00 is about -500 bps, the challenger's about -90 in its one lagging hour and zero
   after); `7a_by_tenor` the same by relative tenor (the error sits on M1 to M4, the
   winter months, and fades along them); `7a_by_symbol` per contract. `7b` hourly drift.
   `7c` calendar-arbitrage violations on the vol surface as published and
   `7c_as_corrected` on the latest version. `7d` the EEX outage minute by minute on the
   dual-listed front months: primary and EEX tick counts, the mark's source and venue,
   and whether the model was graded. EEX goes to zero for three minutes; marks and
   grading carry on from the primary.

### QuestDB notes from building this

- `ASOF JOIN ... TOLERANCE` over several legs: write each leg as the table with a filter,
  `(energy_quotes WHERE symbol = @x) a ASOF JOIN (energy_quotes WHERE ...) b TOLERANCE 30s`.
  The same join over CTEs takes a slow path and times out on tens of millions of rows.
- `HORIZON JOIN`: give both sides as the table with a time filter inside the
  parentheses, `FROM (energy_fills WHERE ts IN @demo_day) t HORIZON JOIN (energy_quotes
  WHERE ts >= ... AND ts < ...) q`. A `WHERE` after the join runs after it and the join
  reads every quote in the table (2.4 s instead of 0.4 s here). For a derived left side
  (markouts from deal time rather than booking time) wrap it: `FROM (t TIMESTAMP(ts)) AS t`.
- Every query carries a time bound, except lookups on the reference tables
  (`instruments`, `listings`, `limits`, whose rows carry the epoch). The `_asof` views
  look back a day for marks and an hour for FX; the booking views take the deals done on
  the day of `@asof` (bounded on `trade_ts`; their `booked_ts >= 00:00` only starts the
  scan at the day, since a booking never precedes its deal); the `_day` views take one
  day.
- Series cells read the 1-minute bars (`quotes_1m`), not ticks: a day-long spread or a
  model-grading pass over 250M quotes takes seconds on ticks and milliseconds on bars,
  and the bar's last quote is the ASOF match with a one-minute tolerance. The bars are
  built from primary-venue quotes only (a `WHERE source IN (...)` in the view), so a bar
  is one venue's price, not a blend of two books with different spreads.
- Parameterised views (checked on 10.0.1 and 10.0.2 before building on them): a caller's
  `DECLARE` reaches a view nested inside another view, one `DECLARE` serves every view
  joined in the query, a cell can compute a variable from another (`@asof :=
  dateadd('d', 1, @demo_day::timestamp) - 1`), and overriding a variable not declared `OVERRIDABLE` is an error
  (`variable is not overridable`), which keeps fixed definitions fixed. `EXPLAIN` of a
  view and of its inline query give the same plan, and `ASOF JOIN` over view legs with a
  `WHERE symbol = ...` runs as fast as over the table. Dropping the tables marks the views
  `invalid`; recreating the tables makes them `valid` again with no other step.
- `EXPLAIN` goes before `DECLARE`: `EXPLAIN DECLARE @asof := ... SELECT * FROM view`.
- `LATEST ON ... PARTITION BY symbol` with the symbols named (`WHERE symbol IN ('EURUSD',
  'GBPUSD', 'EURGBP')`) stops at each one's last row; a filter on another column
  (`curve = 'FX'`) scans back through every row until it has them. That is why
  `energy_fx_asof` names its pairs and needs no time window.
- Join on columns, not expressions: `energy_model_graded_day` computes the bar time
  (`dateadd('m', -1, ts) AS bar_ts`) in a subquery and joins on it, which keeps the join a
  hash join.
- `HAVING` is not supported: filter an aggregating CTE in the outer `WHERE`.
- `IN (SELECT ... FROM cte)` is rejected (the CTE is looked up as a table); filter
  directly or join the CTE.
- The first run of a cell after a load or a restart is slower (cold pages); the timings
  in the next section are warm.
- `LATEST ON` applies a `WHERE` at the same level before picking the row; status filters
  go in an outer query, or a cancelled trade's earlier NEW row comes back.
- `PIVOT` after a `WITH` only works as `SELECT * FROM cte PIVOT (...)`.
- `::decimal(p,s)` truncates, so `round()` first.
- TTL must be an integer multiple of the partition unit and sits before `WAL`.
- The Python client sends UUID columns from strings, drops nothing silently except that a
  symbol column that is entirely null must be dropped from the frame before sending.

### Query timings on the scale 1 dataset

Measured with `check.py` on a laptop (QuestDB 10.0.2, 4 October to midday on 8
October, about 255M quotes), warm:

| Cell | ms | Cell | ms |
|---|---|---|---|
| `0a` to `0e` | 1 to 13 | `5a_lng_arb_jkm_ttf` | 4 |
| `1a_pnl_by_book`, `_by_symbol` | 4 to 7 | `5a_ticks_half_hour` (18k rows) | 114 |
| `1b` (all three) | 40 to 64 | `5b`, `5c_clean_spark_zscore` | 5 |
| `1c_markouts_by_desk` | 1,350 | `5c_ticks_half_hour` (54k rows) | 271 |
| `1d_markouts_by_counterparty` | 16 | `5d`, `5e` | 8 to 9 |
| `1e_markouts_by_venue` | 1,310 | `5f_cross_venue_spread` (36k rows) | 125 |
| `2a` to `2f` | 2 to 6 | `5f_divergence_episodes` | 83 |
| `3a` to `3e` | 2 to 44 | `6a`, `6b` and siblings | 5 to 7 |
| `4a`, `4b`, `4c`, `4e` | 2 to 4 | `6c` and `6c_by_symbol` | 49 |
| `4d_consistency_history` | 238 | `6d`, `6e` | 2 |
| | | `7a` and siblings, `7b` | 61 to 111 |
| | | `7c`, `7c_as_corrected`, `7d` | 19 to 40 |

Everything is under 1.5 seconds warm. The slowest are the two markout cells, which join
every fill to its own venue's raw ticks at six horizons. The PnL and margin cells take a
few milliseconds: their FX rates come from `energy_fx_asof`, whose `LATEST ON` names the
three pairs and stops at each one's last tick. The first run after a load or a restart
is slower on the tick-level cells while the quote pages come in (here `1c` took 8 to 10
s), so run `check.py` once before the session. On the cluster expect the same shape with
slower cold runs on a gp3 volume.

## Dashboard

[`grafana/energy_desk_dashboard.json`](grafana/energy_desk_dashboard.json) is one Grafana
dashboard on the same views: intraday PnL with drawdown, the blotter and limit
utilisation, the curve's evolution and its shape against settlement, the book as known
versus as restated, and a PnL explain. Every panel is as of the right edge of the time
range, so the same dashboard is the live desk at "Last 6 hours" and the reconstruction
when the range ends in the past. Import steps, the plugin macros and the two demo moves
are in the [dashboard README](grafana/README.md).

![Energy Trading Desk dashboard](grafana/screenshot.png)

## Simplifications to be upfront about

- Price dynamics are synthetic. Levels are anchored to real closes, relationships (crack,
  Brent-WTI, Asia premium, UKA discount) to the real ones on the day, but the paths are
  simulated and the market diffuses around the clock, so vol is annualised on calendar
  time (365 x 288 five-minute bars).
- The feed outage takes down a whole venue's feed (EEX: TTF, EUA and UK power) at once;
  a real outage can be narrower or wider.
- Routing is a weighted draw, not a smart order router: no queue position, no fees, no
  latency between venues.
- The vol service publishes an arbitrage-free surface (total variance non-decreasing in
  expiry), as a production vol service does; real raw surfaces carry small violations
  from noise. The planted bad mark is the one that escaped.
- Marks are the curve builder's fair value quantized to the tick; the model prices add
  10 bps of noise on top of the same fair value, which is what makes "inside the quote"
  a meaningful grade.
- PnL marks at mid and converts at the latest FX tick. Realised versus unrealised PnL
  needs a lot-matching convention and is left to the position engine.
- The database stores and checks model inputs and outputs. It does not replace the
  pricing library or the curve bootstrapper.
