tables();

--show tables;
-- dataset intro
select * from commodities_market_data order by timestamp desc;
select * from commodities_trades order by timestamp desc;
select * from commodities_settlements order by timestamp desc;

select * from commodities_market_data where timestamp in '$yesterday';
select * from commodities_trades where timestamp in '$yesterday';
select * from commodities_settlements where timestamp in '$yesterday';


select * from commodities_market_data
where symbol in 'CL' and timestamp in '$today'
limit -10;

select * from commodities_market_data
where symbol in 'CL'
and  timestamp IN '$yesterday#XNYS' limit 1;

select first(timestamp), last(timestamp) from commodities_trades
WHERE timestamp IN '$yesterday#XNYS';


select timestamp, symbol,
    bids[1,1] as bprice, bids[2,1] as bvolume,
    asks[1,1] as aprice, asks[2,1] as avolume,
    bids[1,-1] as bprice2, bids[2,20] as bvolume2,
    asks[1,-1] as aprice2, asks[2,20] as avolume2,
    array_sum(bids[2]) as total_volume
from commodities_market_data where timestamp  in '$today' limit -20;


-- latest on. SQL extensions

select * from commodities_market_data latest by symbol;

select * from commodities_market_data latest by symbol, exchange;

select * from commodities_market_data latest by symbol, exchange
where timestamp < '2026-03-02';


-- parquet
table_partitions('commodities_market_data');
table_partitions('commodities_trades');
table_partitions('commodities_settlements');


select timestamp, count(), symbol,
    avg(best_bid) as bprice, avg(best_ask) as aprice
from commodities_market_data sample by 1d;

with parts as (
    select name, last(isParquet) from table_partitions('commodities_trades')
), totals as (
select timestamp, count(),
    avg(price) as avg_price
from commodities_trades
WHERE symbol = 'CL' sample by 1h
)
select * from totals join parts ON to_str(timestamp, 'yyyy-MM-ddTHH') = name;


read_parquet('trades.parquet');

select timestamp, count() from (select * from (read_parquet('trades.parquet') order by timestamp) timestamp(timestamp) )
sample by 1d;


-- 15 minutes candles
  select timestamp, symbol,
            first(best_bid) as open,
            max(best_bid) as high,
            min(best_bid) as low,
            last(best_bid) as close,
            avg(best_bid) as avgr,
            sum(bids[2][1]) as volume
        from commodities_market_data
        where timestamp  in '$today'
        and   symbol = 'CL'
        sample by 15m;

-- mat views

CREATE MATERIALIZED VIEW IF NOT EXISTS 'commodities_market_data_ohlc_1m'
    WITH BASE 'commodities_market_data' REFRESH IMMEDIATE AS (

            SELECT timestamp, symbol,
                first(best_bid) AS open,
                max(best_bid) AS high,
                min(best_bid) AS low,
                last(best_bid) AS close,
                SUM(bids[2][1]) AS total_volume
            FROM commodities_market_data
            SAMPLE BY 1m

) PARTITION BY HOUR TTL 2 DAYS
OWNED BY 'admin';



 select * from commodities_market_data_ohlc_1m where
symbol = 'CL' AND timestamp in '$today';

select * from commodities_bbo_1s
where timestamp in '$today'
order by timestamp desc, symbol asc ;

select * from commodities_market_data_ohlc_1m
where timestamp in '$today'
order by timestamp desc, symbol asc ;



-- TTL and cascading
CREATE MATERIALIZED VIEW IF NOT EXISTS 'commodities_bbo_1s'
    WITH BASE 'commodities_market_data' REFRESH IMMEDIATE AS (
            SELECT timestamp, symbol,
                last(best_bid) AS best_bid,
                last(best_ask) AS best_ask
            FROM commodities_market_data
            SAMPLE BY 1s
) PARTITION BY HOUR TTL 3 DAYS
OWNED BY 'admin';


CREATE MATERIALIZED VIEW IF NOT EXISTS 'commodities_bbo_1m'
WITH BASE 'commodities_bbo_1s' REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(best_bid) AS max_bid,
                min(best_ask) AS min_ask
            FROM commodities_bbo_1s
            SAMPLE BY 1m
) PARTITION BY DAY TTL 7 DAYS
OWNED BY 'admin';

select * from commodities_bbo_1m
where timestamp in '$today'
order by timestamp desc, symbol asc ;

CREATE MATERIALIZED VIEW IF NOT EXISTS 'commodities_bbo_1h'
WITH BASE 'commodities_bbo_1m' REFRESH EVERY 10m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(max_bid) AS max_bid,
                min(min_ask) AS min_ask
            FROM  commodities_bbo_1m
            SAMPLE BY 1h
) PARTITION BY MONTH TTL 1 MONTH
OWNED BY 'admin';


-- create view with overridable variables
CREATE OR REPLACE VIEW single_commodity AS (
DECLARE
  OVERRIDABLE @sym := 'CL' ,
  OVERRIDABLE @range := '$today'
select * from commodities_market_data where timestamp in @range and symbol = @sym
);



SELECT * from single_commodity;



DECLARE
  OVERRIDABLE @sym := 'GC' ,
  OVERRIDABLE @range := '$now-10m..$now'
  SELECT * from single_commodity;



-- Array dimensions
select timestamp, symbol,
    bids[1] as bprices, bids[2] as bsizes, array_count(bids[1]) as bid_levels,
    asks[1] as aprices, asks[2] as asizes, array_count(asks[1]) as ask_levels
from commodities_market_data latest by symbol;


-- spread
SELECT timestamp, symbol,
    best_ask - best_bid
FROM commodities_market_data
where symbol IN ('CL', 'GC')
and timestamp in '$today';

-- spread from deepest levels
SELECT timestamp, symbol,
    asks[-1][1] - bids[-1][1]
FROM commodities_market_data
where symbol IN ('CL', 'GC')
and timestamp in '$today';

-- moving averages
select timestamp, symbol, best_bid as l1_bid_price,
avg(best_bid) over (partition by symbol order by timestamp) as moving_l1_bid_price,

bids[2,1] as l1_bid_volume, sum(bids[2,1]) over (partition by symbol order by timestamp) as moving_l1_bid_volume,
 sum(bids[2,1]) over (order by timestamp) as moving_l1_total_bid_volume
from commodities_market_data
where timestamp in '$today'
and symbol='CL'
;


-- basic anomaly detection. Ask further from average than a given multiple of stddev
DECLARE
    @l1_ask := best_ask,
    @low_threshold := 1.9,
    @medium_threshold := 2.0,
    @high_threshold := 2.1
WITH s AS (
    SELECT avg(@l1_ask) as avg_ask, stddev(@l1_ask ) as stddev_ask
    FROM commodities_market_data
    where symbol IN ('CL')
    and timestamp IN '$now-1h..$now'
), combined AS (
SELECT timestamp, @l1_ask as l1_ask, avg_ask, stddev_ask, abs(l1_ask - avg_ask) as delta
FROM commodities_market_data m CROSS JOIN s
where m.symbol IN ('CL')
and m.timestamp IN '$now-1h..$now'
)
SELECT timestamp, l1_ask, avg_ask, delta,
    CASE
        WHEN delta >= stddev_ask * @high_threshold THEN 'High'
        WHEN delta >= stddev_ask * @medium_threshold THEN 'Medium'
        ELSE 'Low'
    END AS anomaly
 from combined where delta >= stddev_ask * @low_threshold;



-- Volume is available within 1% of the best price?
-- How much volume I can capture at a cheap price because of a relatively flat orderbook
DECLARE
    @prices := asks[1],
    @volumes := asks[2],
    @best_price := @prices[1],
    @multiplier := 1.01,
    @target_price := @multiplier *  @best_price,
    @relevant_volume_levels := @volumes[1:insertion_point(@prices, @target_price)]
SELECT timestamp, asks,
     @relevant_volume_levels as volume_levels,
     array_sum(@relevant_volume_levels) as total_volume
    FROM commodities_market_data where timestamp in '$today' AND symbol = 'CL';

-- Equivalent query without declare. Volume is available within 1% of the best price?
SELECT asks,
     asks[2, 1:insertion_point(asks[1], 1.01 * asks[1, 1])] volume_levels,
     array_sum(asks[2, 1:insertion_point(asks[1], 1.01 * asks[1, 1])]) total_volume
    FROM commodities_market_data where timestamp in '$today' AND symbol = 'CL' ;

-- What price level will a buy order for the given volume reach?
WITH
    q1 AS (
    SELECT timestamp, symbol, asks,
        array_cum_sum(asks[2]) cum_volumes
    FROM commodities_market_data
    where symbol = 'CL' and timestamp in '$today'),
    q2 AS (
    SELECT timestamp, symbol,
        asks, cum_volumes,
        insertion_point(cum_volumes, 50, true) target_level
        FROM q1 )
SELECT timestamp, symbol,
    cum_volumes, target_level, asks[1, target_level] price
FROM q2;


-- ASOF JOIN: trades with market data
select * from commodities_trades asof join commodities_market_data on symbol
where commodities_trades.symbol = 'CL' and commodities_trades.timestamp in '$today';

-- Use ASOF JOIN to pair each trade with the most recent order book snapshot, then calculate slippage in basis points
SELECT
    t.timestamp,
    t.symbol,
    t.exchange,
    t.side,
    t.price,
    t.size,
    m.best_bid,
    m.best_ask,
    (m.best_bid + m.best_ask) / 2 AS mid,
    (m.best_ask - m.best_bid) AS spread,
    CASE t.side
        WHEN 'B'  THEN (t.price - (m.best_bid + m.best_ask) / 2)
                         / ((m.best_bid + m.best_ask) / 2) * 10000
        WHEN 'S' THEN ((m.best_bid + m.best_ask) / 2 - t.price)
                         / ((m.best_bid + m.best_ask) / 2) * 10000
    END AS slippage_bps,
    CASE t.side
        WHEN 'B'  THEN (t.price - m.best_ask) / m.best_ask * 10000
        WHEN 'S' THEN (m.best_bid - t.price) / m.best_bid * 10000
    END AS slippage_vs_tob_bps
FROM commodities_trades t
ASOF JOIN commodities_market_data m ON (symbol)
WHERE t.timestamp IN '$yesterday'
ORDER BY t.timestamp;

/* Find the minimum ask and maximum bid in
the 10 seconds before and after each trade
*/
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
WHERE t.timestamp IN '$yesterday';

-- fixed horizons
SELECT
    t.symbol,
    h.offset / 1000000000 AS horizon_sec,
    count() AS n,
    avg(((m.best_bid + m.best_ask) / 2 - t.price) / t.price * 10000) AS avg_markout_bps,
    sum(t.size) AS total_volume
FROM commodities_trades t
HORIZON JOIN commodities_market_data m ON (symbol)
    LIST (-10s,0, 1s, 5s, 10s,
           1m, 5m) AS h
WHERE t.side = 'B'
    AND t.timestamp IN '$yesterday'
GROUP BY t.symbol, horizon_sec
ORDER BY t.symbol, horizon_sec;



-- horizon at 30s for 10 minutes
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
  AND t.timestamp IN '$yesterday'
GROUP BY t.symbol, horizon_sec
ORDER BY t.symbol, horizon_sec;


--------------------------------------------------
-- Execution quality: who is picking us off
--------------------------------------------------

-- Counterparty scorecard. Every fill paired with the mid 60s later in ONE
-- HORIZON JOIN pass, signed from the counterparty's side: positive means the
-- market moved their way after they traded and we were adversely selected.
SELECT
    t.counterparty,
    count()     AS fills,
    sum(t.size) AS lots,
    avg(CASE t.side
            WHEN 'B' THEN ((m.best_bid + m.best_ask) / 2 - t.price) / t.price * 10000
            WHEN 'S' THEN (t.price - (m.best_bid + m.best_ask) / 2) / t.price * 10000
        END) AS markout_60s_bps
FROM commodities_trades t
HORIZON JOIN commodities_market_data m ON (symbol) LIST (60s) AS h
WHERE t.timestamp IN '$yesterday'
GROUP BY t.counterparty
ORDER BY markout_60s_bps DESC;


-- The markout CURVE is the part that convinces. Informed flow starts negative
-- (it crosses the spread to get filled), turns positive as the information
-- plays out, peaks near the information horizon, then decays.
SELECT
    h.offset / 1000000000 AS horizon_s,
    t.counterparty,
    avg(CASE t.side
            WHEN 'B' THEN ((m.best_bid + m.best_ask) / 2 - t.price) / t.price * 10000
            WHEN 'S' THEN (t.price - (m.best_bid + m.best_ask) / 2) / t.price * 10000
        END) AS markout_bps
FROM commodities_trades t
HORIZON JOIN commodities_market_data m ON (symbol)
    LIST (1s, 5s, 10s, 30s, 1m, 2m, 5m) AS h
WHERE t.counterparty IN ('HFT_ARB_01', 'BANK_TIER1_03', 'MINER_01')
  AND t.timestamp IN '$yesterday'
GROUP BY horizon_s, t.counterparty
ORDER BY t.counterparty, horizon_s;


-- Venue scorecard. Compare venues WITHIN a symbol: screen venues carry the
-- instruments their exchange lists, so a cross-instrument venue average
-- measures the product mix rather than the venue.
SELECT
    t.venue,
    count() AS fills,
    avg(abs(t.price - (m.best_bid + m.best_ask) / 2)
        / ((m.best_bid + m.best_ask) / 2) * 10000) AS eff_spread_bps,
    avg(CASE WHEN t.passive THEN 1.0 ELSE 0.0 END) AS passive_rate
FROM commodities_trades t
ASOF JOIN commodities_market_data m ON (symbol)
WHERE t.symbol = 'GC' AND t.timestamp IN '$yesterday'
GROUP BY t.venue
ORDER BY eff_spread_bps;


-- What immediacy costs: passive fills rest and get taken, aggressive fills pay.
SELECT
    t.passive,
    count() AS fills,
    avg(abs(t.price - (m.best_bid + m.best_ask) / 2)
        / ((m.best_bid + m.best_ask) / 2) * 10000) AS eff_spread_bps
FROM commodities_trades t
ASOF JOIN commodities_market_data m ON (symbol)
WHERE t.symbol = 'GC' AND t.timestamp IN '$yesterday'
GROUP BY t.passive;


-- Native execution benchmarks. TWAP weights every interval equally via
-- step-function integration; VWAP weights by size. They diverge exactly when
-- volume clusters, which is when the benchmark choice changes the verdict.
SELECT timestamp, symbol,
    vwap(price, size)      AS vwap,
    twap(price, timestamp) AS twap,
    last(price)            AS last_price
FROM commodities_trades
WHERE symbol = 'GC' AND timestamp IN '$today'
SAMPLE BY 15m;


--------------------------------------------------
-- Precious metals term structure (real contract months)
--------------------------------------------------

-- Gold trades in contango, crude in backwardation. Both come from live prices:
-- each deferred leg is anchored to a real listed contract month.
SELECT symbol,
    (last(best_bid) + last(best_ask)) / 2 AS mid,
    last(best_ask) - last(best_bid)       AS spread
FROM commodities_market_data
WHERE symbol IN ('GC', 'GC6', 'GC12', 'SI', 'SI12', 'CL', 'CL12')
  AND timestamp IN '$today'
GROUP BY symbol;


-- The gold carry in dollars per ounce, over time.
WITH front AS (
    SELECT timestamp, (best_bid + best_ask) / 2 AS mid
    FROM commodities_bbo_1s WHERE symbol = 'GC' AND timestamp IN '$today'
), deferred AS (
    SELECT timestamp, (best_bid + best_ask) / 2 AS mid
    FROM commodities_bbo_1s WHERE symbol = 'GC12' AND timestamp IN '$today'
)
SELECT front.timestamp, deferred.mid - front.mid AS gold_12m_basis
FROM front ASOF JOIN deferred;


--------------------------------------------
-- demo end --
--------------------------------------------------

-- Commodities-specific queries below


-- Cross-class analytics: compare volatility and spread across sectors
select timestamp, commodity_class,
       count() as events,
       avg(best_ask - best_bid) as avg_spread,
       stddev(best_bid) as price_stddev
from commodities_market_data
where timestamp in '$today'
sample by 1h;


-- Deferred vs front month: CL vs CL12, NG vs NG12
-- Positive basis = backwardation (front premium), negative = contango
select timestamp, symbol,
       last(best_bid) as bid,
       last(best_ask) as ask,
       last((best_bid + best_ask) / 2) as mid
from commodities_market_data
where symbol in ('CL', 'CL12', 'NG', 'NG12')
and timestamp in '$today'
sample by 1m;


-- Power spike detection across all power hubs (PJM, ERCT, CISO, NBPL)
-- Power symbols have a spike/revert state machine: 2% chance per second
-- to enter a spike (5-30s duration), 10x drift, 50% shock probability
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


-- Settlement vs current price: how far has each symbol moved from last settlement?
select t.*, s.settlement_price, s.open_interest,
       t.price - s.settlement_price as change_from_settle,
       (t.price - s.settlement_price) / s.settlement_price * 100 as change_pct
from commodities_trades t
asof join commodities_settlements s on (symbol)
where t.symbol = 'CL' and t.timestamp in '$today';

-- Settlements overview
select * from commodities_settlements
order by timestamp desc;


--- dedup



CREATE TABLE 'commodities_trades_test' (
	timestamp TIMESTAMP_NS,
	symbol SYMBOL,
	price DOUBLE
) timestamp(timestamp) PARTITION BY HOUR
DEDUP UPSERT KEYS(timestamp,symbol);

insert into commodities_trades_test values ('2026-02-27T00:00:00', 'CL', 91);
select * from commodities_trades_test;
insert into commodities_trades_test values ('2026-02-27T01:00:00', 'CL', 92);
select * from commodities_trades_test;
insert into commodities_trades_test values ('2026-02-27T01:00:00', 'CL', 93);
select * from commodities_trades_test;
insert into commodities_trades_test values ('2026-02-27T01:00:00', 'GC', 2400);
select * from commodities_trades_test;
insert into commodities_trades_test values ('2026-02-27T01:00:00', 'GC', 2500);

UPDATE commodities_trades_test set price = 95 where symbol = 'CL';

-- settlement joins
select t.*, s.settlement_price, s.open_interest
from commodities_trades t
asof join commodities_settlements s on (symbol)
where t.symbol = 'CL' and t.timestamp in '$today';


select * from _query_trace where principal <> 'admin';



select ts, principal, count(), sum(execution_micros) from _query_trace
sample by 1h;

--------------------------------------------
-- demo end --
--------------------------------------------------

--wal_tables();
--materialized_views();

--table_partitions('commodities_market_data') where name like '2025-07-09%';
--select view_name, base_table_name, view_status, last_refresh_start_timestamp,last_refresh_finish_timestamp,refresh_base_table_txn, base_table_txn from materialized_views() order by view_status;

--select * from (table_storage()) order by tableName;

--(show parameters) where value_source <> 'default';

--show partitions from commodities_market_data;
--table_partitions('commodities_market_data');

--wal_tables() where name ilike '%commodities%' or name ilike '%bbo%';
