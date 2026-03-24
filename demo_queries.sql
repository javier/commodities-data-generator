-- Commodities Demo SQL Script

-- Intro
show tables;

select * from commodities_market_data;
select * from commodities_trades;
select * from commodities_settlements;

select * from commodities_market_data where timestamp in today();
select * from commodities_trades where timestamp in today();


-- Array Basics: extracting best bid/ask and deeper levels
select timestamp, symbol, exchange, commodity_class,
       bids[1][1] as bid_price,
       bids[2][1] as bid_size,
       asks[1][1] as ask_price,
       asks[2][1] as ask_size
from commodities_market_data
where symbol = 'CL' and timestamp in today()
limit -20;

-- Deeper levels
select timestamp, symbol,
       bids[1][1] as bid_L1,
       bids[2][1] as bid_size_L1,
       bids[1][5] as bid_L5,
       bids[2][5] as bid_size_L5,
       array_sum(bids[2]) as total_bid_volume
from commodities_market_data
where symbol = 'GC' and timestamp in today()
limit -20;


-- LATEST BY
select * from commodities_market_data latest by symbol;

select * from commodities_market_data latest by symbol, exchange;

select * from commodities_trades latest by symbol;


-- SAMPLE BY: 1s candles on best_bid
select timestamp, symbol,
       first(best_bid) as open,
       max(best_bid)   as high,
       min(best_bid)   as low,
       last(best_bid)  as close,
       sum(bids[2][1]) as volume
from commodities_market_data
where symbol = 'CL' and timestamp in today()
sample by 1s;

-- 15-minute candles
select timestamp, symbol,
       first(best_bid) as open,
       max(best_bid)   as high,
       min(best_bid)   as low,
       last(best_bid)  as close,
       sum(bids[2][1]) as volume
from commodities_market_data
where symbol = 'CL' and timestamp in today()
sample by 15m;

-- 1-day candles across all symbols
select timestamp, symbol,
       first(best_bid) as open,
       max(best_bid)   as high,
       min(best_bid)   as low,
       last(best_bid)  as close
from commodities_market_data
sample by 1d;


-- Materialized Views

-- BBO 1s (continuous)
select * from commodities_bbo_1s
where timestamp in today()
order by timestamp desc, symbol asc
limit 50;

-- Trades OHLCV 1s (continuous)
select * from commodities_trades_ohlcv_1s
where symbol = 'CL' and timestamp in today();

-- Cascading views
select * from commodities_bbo_1m
where timestamp in today()
order by timestamp desc, symbol asc;

select * from commodities_trades_ohlcv_1m
where symbol = 'CL' and timestamp in today();


-- Spread analysis by commodity class
select timestamp, commodity_class,
       avg(best_ask - best_bid) as avg_spread,
       min(best_ask - best_bid) as min_spread,
       max(best_ask - best_bid) as max_spread
from commodities_market_data
where timestamp in today()
sample by 1m;

-- Spread per symbol
select timestamp, symbol,
       best_ask - best_bid as spread
from commodities_market_data
where symbol in ('CL', 'GC', 'ZC', 'PJM')
and timestamp in today();


-- ASOF JOIN: trades with market data
select * from commodities_trades
asof join commodities_market_data on (symbol)
where commodities_trades.symbol = 'CL'
and commodities_trades.timestamp in today();

-- Trades joined with BBO
select *
from commodities_trades_ohlcv_1s as t
asof join commodities_bbo_1s as n on t.symbol = n.symbol;


-- Volume within price threshold using insertion_point
DECLARE
    @prices := asks[1],
    @volumes := asks[2],
    @best_price := @prices[1],
    @multiplier := 1.01,
    @target_price := @multiplier * @best_price,
    @relevant_volume_levels := @volumes[1:insertion_point(@prices, @target_price)]
SELECT timestamp, symbol, asks,
     @relevant_volume_levels as volume_levels,
     array_sum(@relevant_volume_levels) as total_volume
FROM commodities_market_data
where timestamp in today() and symbol = 'CL';

-- Same without DECLARE
SELECT asks,
     asks[2, 1:insertion_point(asks[1], 1.01 * asks[1, 1])] volume_levels,
     array_sum(asks[2, 1:insertion_point(asks[1], 1.01 * asks[1, 1])]) total_volume
FROM commodities_market_data
where timestamp in today() and symbol = 'CL' limit -100;

-- What price level will a buy order for the given volume reach?
WITH
    q1 AS (
    SELECT timestamp, symbol, asks,
        array_cum_sum(asks[2]) cum_volumes
    FROM commodities_market_data
    where symbol = 'CL' and timestamp in today()),
    q2 AS (
    SELECT timestamp, symbol,
        asks, cum_volumes,
        insertion_point(cum_volumes, 50, true) target_level
        FROM q1)
SELECT timestamp, symbol,
    cum_volumes, target_level, asks[1, target_level] price
FROM q2;


-- Anomaly detection (especially interesting for power spikes)
DECLARE
    @l1_ask := best_ask,
    @low_threshold := 2,
    @medium_threshold := 3.3,
    @high_threshold := 4
WITH s AS (
    SELECT avg(@l1_ask) as avg_ask, stddev(@l1_ask) as stddev_ask
    FROM commodities_market_data
    where symbol IN ('PJM')
    and timestamp >= dateadd('h', -1, now())
), combined AS (
SELECT timestamp, @l1_ask as l1_ask, avg_ask, stddev_ask, abs(l1_ask - avg_ask) as delta
FROM commodities_market_data m CROSS JOIN s
where m.symbol IN ('PJM')
and m.timestamp >= dateadd('h', -1, now())
)
SELECT timestamp, l1_ask, avg_ask, delta,
    CASE
        WHEN delta >= stddev_ask * @high_threshold THEN 'High'
        WHEN delta >= stddev_ask * @medium_threshold THEN 'Medium'
        WHEN delta >= stddev_ask * @low_threshold THEN 'Low'
        ELSE '-'
    END AS anomaly
 from combined where delta >= stddev_ask * @low_threshold;

-- Power spike detection across all power hubs
DECLARE
    @l1_ask := best_ask,
    @threshold := 2.5
WITH s AS (
    SELECT symbol, avg(@l1_ask) as avg_ask, stddev(@l1_ask) as stddev_ask
    FROM commodities_market_data
    where commodity_class = 'power'
    and timestamp >= dateadd('h', -2, now())
), combined AS (
SELECT m.timestamp, m.symbol, @l1_ask as l1_ask, avg_ask, stddev_ask,
       abs(l1_ask - avg_ask) as delta
FROM commodities_market_data m
JOIN s ON m.symbol = s.symbol
where m.commodity_class = 'power'
and m.timestamp >= dateadd('h', -2, now())
)
SELECT timestamp, symbol, l1_ask, avg_ask, delta
from combined where delta >= stddev_ask * @threshold;


-- WINDOW JOIN: min ask / max bid in 10s window around each trade
SELECT
    t.symbol,
    t.timestamp,
    t.side,
    t.price,
    min(m.best_ask) AS min_ask,
    max(m.best_bid) AS max_bid
FROM commodities_trades t
WINDOW JOIN commodities_market_data m
    ON (symbol)
    RANGE BETWEEN 10 seconds PRECEDING AND 10 seconds FOLLOWING
    EXCLUDE PREVAILING
WHERE t.timestamp in today()
AND t.symbol = 'CL';


-- HORIZON JOIN: markout analysis at fixed offsets
SELECT
    t.symbol,
    h.offset / 1000000000 AS horizon_sec,
    count() AS n,
    avg(((m.best_bid + m.best_ask) / 2 - t.price) / t.price * 10000) AS avg_markout_bps,
    sum(t.size) AS total_volume
FROM commodities_trades t
HORIZON JOIN commodities_market_data m ON (symbol)
    LIST (-10s, 0, 1s, 5s, 10s, 1m, 5m) AS h
WHERE t.side = 'B'
    AND t.timestamp in today()
GROUP BY t.symbol, horizon_sec
ORDER BY t.symbol, horizon_sec;


-- HORIZON JOIN: range with step
SELECT
    t.symbol,
    h.offset / 1000000000 AS horizon_sec,
    count() AS n,
    avg(((m.best_bid + m.best_ask) / 2 - t.price) / t.price * 10000) AS avg_markout_bps,
    sum(((m.best_bid + m.best_ask) / 2 - t.price) * t.size) AS total_pnl
FROM commodities_trades t
HORIZON JOIN commodities_market_data m ON (symbol)
    RANGE FROM 0s TO 10m STEP 30s AS h
WHERE t.side = 'B'
  AND t.timestamp in today()
GROUP BY t.symbol, horizon_sec
ORDER BY t.symbol, horizon_sec;


-- Settlement joins: ASOF JOIN trades with daily settlements
select t.*, s.settlement_price, s.open_interest
from commodities_trades t
asof join commodities_settlements s on (symbol)
where t.symbol = 'CL' and t.timestamp in today();

-- Settlements overview
select * from commodities_settlements
order by timestamp desc;


-- Cross-class analytics: compare volatility across sectors
select timestamp, commodity_class,
       count() as events,
       avg(best_ask - best_bid) as avg_spread,
       stddev(best_bid) as price_stddev
from commodities_market_data
where timestamp in today()
sample by 1h;


-- Deferred vs front month comparison
select timestamp, symbol,
       last(best_bid) as bid,
       last(best_ask) as ask,
       last((best_bid + best_ask) / 2) as mid
from commodities_market_data
where symbol in ('CL', 'CL12', 'NG', 'NG12')
and timestamp in today()
sample by 1m;


-- DECLARE with overridable variables
CREATE OR REPLACE VIEW single_commodity AS (
DECLARE
  OVERRIDABLE @sym := 'CL',
  OVERRIDABLE @range := '$today'
select * from commodities_market_data
where timestamp in @range and symbol = @sym
);

SELECT * from single_commodity;

DECLARE
  OVERRIDABLE @sym := 'GC',
  OVERRIDABLE @range := '$now-10m..$now'
SELECT * from single_commodity;


-- Partitions and parquet
table_partitions('commodities_market_data');
table_partitions('commodities_trades');

with parts as (
    select name, last(isParquet)
    from table_partitions('commodities_trades')
), totals as (
    select timestamp, count(), avg(price) as avg_price
    from commodities_trades
    where symbol = 'CL'
    sample by 1h
)
select * from totals join parts
on to_str(timestamp, 'yyyy-MM-ddTHH') = parts.name;


-- Array dimensions
select timestamp, symbol,
    bids[1] as bprices, bids[2] as bsizes, array_count(bids[1]) as bid_levels,
    asks[1] as aprices, asks[2] as asizes, array_count(asks[1]) as ask_levels
from commodities_market_data latest by symbol;


--------------------------------------------
-- demo end --
--------------------------------------------

--wal_tables();
--materialized_views();
--select view_name, base_table_name, view_status, last_refresh_start_timestamp,last_refresh_finish_timestamp,refresh_base_table_txn, base_table_txn from materialized_views() order by view_status;
--select * from (table_storage()) order by tableName;
--(show parameters) where value_source <> 'default';
