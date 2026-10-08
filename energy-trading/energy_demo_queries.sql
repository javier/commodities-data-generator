-- =====================================================================================
-- Energy trading desk: real-time analytics on QuestDB
-- Seven acts, one cell per analytic, each cell self-contained. Run everything with
-- check.py, or paste cells into the Web Console.
--
-- Variables are DECLAREd at the top of each cell. Every value written as
-- @name := '...' (demo day, planted-event timestamps, front-month symbols) is
-- rewritten by energy_trading_data_generator.py after each backfill, so the pack
-- always points at the data that was just loaded. In a Web Console notebook you
-- can define them once as notebook variables and delete the DECLARE lines.
--
-- Table names carry the generator's default --prefix (energy_). A different prefix
-- means a search and replace on "energy_".
--
-- Presenter notes sit above each cell: what it shows, what to say, which planted
-- event it reveals. Cell 0a lists the planted events; keep it open.
-- =====================================================================================


-- @@ 0a_planted_events
-- The presenter's cheat sheet: what the generator planted and when.
SELECT ts, act, what FROM energy_demo_events ORDER BY ts;

-- @@ 0b_volumes_today
-- How much data the demo day carries per table.
DECLARE @demo_day := '2026-10-07'
SELECT 'quotes' AS table_name, count() AS rows_today FROM energy_quotes WHERE ts IN @demo_day
UNION ALL SELECT 'curve_marks', count() FROM energy_curve_marks WHERE ts IN @demo_day
UNION ALL SELECT 'fills', count() FROM energy_fills WHERE ts IN @demo_day
UNION ALL SELECT 'trade_events', count() FROM energy_trade_events WHERE booked_ts IN @demo_day
UNION ALL SELECT 'model_prices', count() FROM energy_model_prices WHERE ts IN @demo_day
UNION ALL SELECT 'iv_marks', count() FROM energy_iv_marks WHERE ts IN @demo_day
UNION ALL SELECT 'da_prices', count() FROM energy_da_prices WHERE ts IN @demo_day
UNION ALL SELECT 'position_snapshots', count() FROM energy_position_snapshots WHERE ts IN @demo_day;


-- =====================================================================================
-- ACT 1 · INTRADAY PnL
-- PnL since the close = the opening book revalued from yesterday's settlement, plus
-- today's fills marked from their execution price. The ledger view rewrites the EOD
-- snapshot as a pseudo-fill at settlement, so every row contributes qty * (mark - px)
-- and any breakdown is a GROUP BY. Converted to USD at the latest FX quote.
-- =====================================================================================

-- @@ 1a_pnl_by_book
-- Say: one query, one convention, every book. Marks are the curve builder's latest
-- price (liquid or interpolated), FX is the latest tick, px_factor turns pence into pounds.
DECLARE @demo_day := '2026-10-07'
WITH marks AS (
  SELECT symbol, price AS mark FROM energy_curve_marks
  WHERE ts IN @demo_day
  LATEST ON ts PARTITION BY symbol
),
fx AS (
  SELECT symbol, mid(bid, ask) AS usd FROM energy_quotes
  WHERE curve = 'FX' AND ts IN @demo_day
  LATEST ON ts PARTITION BY symbol
),
pnl AS (
  SELECT l.book,
         sum(CASE WHEN l.src = 'sod'   THEN l.qty * (m.mark - l.px) * c.px_factor * coalesce(x.usd, 1.0) ELSE 0 END) AS sod_revalued,
         sum(CASE WHEN l.src = 'trade' THEN l.qty * (m.mark - l.px) * c.px_factor * coalesce(x.usd, 1.0) ELSE 0 END) AS todays_trading,
         sum(CASE WHEN l.src = 'trade' THEN 1 ELSE 0 END) AS fills
  FROM energy_ledger l
  JOIN marks m ON (symbol)
  JOIN energy_instruments c ON (symbol)
  LEFT JOIN fx x ON x.symbol = c.fx_symbol
  WHERE l.ts IN @demo_day
  GROUP BY l.book
)
SELECT book,
       round(sod_revalued)::decimal(14,0)                   AS sod_book_revalued_usd,
       round(todays_trading)::decimal(14,0)                 AS todays_trading_usd,
       round(sod_revalued + todays_trading)::decimal(14,0)  AS total_pnl_usd,
       fills
FROM pnl
ORDER BY book;

-- @@ 1b_pnl_curve_and_drawdown
-- Chart: x = ts, y = pnl_usd, one series per book. Position and cash come from the
-- live view (running sums maintained per fill, reset at 00:00), marks from the
-- 5-minute bars. Today's trading only; the opening book is in 1a.
-- Live views are beta in QuestDB 10: 1b_twin_window_function is the same cell over fills.
DECLARE @demo_day := '2026-10-07'
WITH pos AS (
  SELECT ts, book, symbol, last(pos) AS pos, last(cost) AS cost
  FROM energy_positions_live
  WHERE ts IN @demo_day
  SAMPLE BY 5m FILL(PREV)
),
mk AS (
  SELECT ts, symbol, last(close) AS mark
  FROM energy_quotes_5m
  WHERE ts IN @demo_day
  SAMPLE BY 5m FILL(PREV)
),
fx AS (
  SELECT symbol, mid(bid, ask) AS usd FROM energy_quotes
  WHERE curve = 'FX' AND ts IN @demo_day
  LATEST ON ts PARTITION BY symbol
),
pnl AS (
  SELECT p.ts, p.book, sum((p.pos * m.mark - p.cost) * c.px_factor * coalesce(x.usd, 1.0)) AS pnl_usd
  FROM pos p
  JOIN mk m ON (ts, symbol)
  JOIN energy_instruments c ON (symbol)
  LEFT JOIN fx x ON x.symbol = c.fx_symbol
  GROUP BY p.ts, p.book
),
dd AS (
  SELECT ts, book, pnl_usd, max(pnl_usd) OVER (PARTITION BY book ORDER BY ts) - pnl_usd AS drawdown
  FROM pnl
)
SELECT ts, book, round(pnl_usd)::decimal(14,0) AS pnl_usd, round(drawdown)::decimal(14,0) AS drawdown_usd
FROM dd
ORDER BY ts, book;

-- @@ 1b_twin_window_function
-- The same curve without the live view: running position and cash as window functions
-- over fills. Identical output, computed at query time instead of at ingestion time.
DECLARE @demo_day := '2026-10-07'
WITH run AS (
  SELECT ts, book, symbol,
         sum(qty)      OVER (PARTITION BY book, symbol ORDER BY ts) AS pos,
         sum(qty * px) OVER (PARTITION BY book, symbol ORDER BY ts) AS cost
  FROM energy_fills
  WHERE ts IN @demo_day
  ORDER BY ts
),
pos AS (
  SELECT ts, book, symbol, last(pos) AS pos, last(cost) AS cost
  FROM (run TIMESTAMP(ts))
  SAMPLE BY 5m FILL(PREV)
),
mk AS (
  SELECT ts, symbol, last(close) AS mark
  FROM energy_quotes_5m
  WHERE ts IN @demo_day
  SAMPLE BY 5m FILL(PREV)
),
fx AS (
  SELECT symbol, mid(bid, ask) AS usd FROM energy_quotes
  WHERE curve = 'FX' AND ts IN @demo_day
  LATEST ON ts PARTITION BY symbol
),
pnl AS (
  SELECT p.ts, p.book, sum((p.pos * m.mark - p.cost) * c.px_factor * coalesce(x.usd, 1.0)) AS pnl_usd
  FROM pos p
  JOIN mk m ON (ts, symbol)
  JOIN energy_instruments c ON (symbol)
  LEFT JOIN fx x ON x.symbol = c.fx_symbol
  GROUP BY p.ts, p.book
),
dd AS (
  SELECT ts, book, pnl_usd, max(pnl_usd) OVER (PARTITION BY book ORDER BY ts) - pnl_usd AS drawdown
  FROM pnl
)
SELECT ts, book, round(pnl_usd)::decimal(14,0) AS pnl_usd, round(drawdown)::decimal(14,0) AS drawdown_usd
FROM dd
ORDER BY ts, book;

-- @@ 1c_markouts_by_desk
-- Execution quality: how the mid moved after each fill, in bps, at fixed horizons.
-- HORIZON JOIN pairs every fill with the prevailing quote at t+0, t+1s, ... t+15m.
-- CRUDE's markout rises with the horizon (it trades with a view); PRODUCTS always
-- crosses the spread and never earns it back, so it sits flat and negative.
-- The day filter sits on the fills side, inside the parentheses: a WHERE after the
-- join is applied after it and makes the join read every quote in the table.
DECLARE @demo_day := '2026-10-07'
SELECT h.offset / 1000000 AS horizon_s,
       t.book,
       count() AS fills,
       round(avg(10000 * CASE WHEN t.qty > 0 THEN 1 ELSE -1 END * (mid(q.bid, q.ask) - t.px) / t.px), 2)::decimal(8,2) AS markout_bps
FROM (energy_fills WHERE ts IN @demo_day) AS t
HORIZON JOIN energy_quotes AS q ON (symbol)
LIST (0, 1s, 10s, 1m, 5m, 15m) AS h
ORDER BY t.book, horizon_s;

-- @@ 1d_markouts_by_counterparty
-- Who adversely selects us: the same markout on broker and bilateral deals, by
-- counterparty type, from the deal time (trade_ts), over the last three days. Banks and
-- trading houses tend to be on the right side; utilities and industrials are not.
-- Voice deals are timed to the minute, so the horizons run from one minute to four
-- hours against the 1-minute bars (last quote of the bar), which keeps the join cheap.
DECLARE @demo_day := '2026-10-07'
WITH t AS (
  (SELECT trade_ts AS ts, symbol, qty, px, counterparty
   FROM energy_trade_events
   WHERE version = 1 AND channel != 'EXCH' AND trade_ts >= dateadd('d', -2, @demo_day::timestamp))
  ORDER BY ts
)
SELECT h.offset / 60000000 AS horizon_min,
       regexp_replace(t.counterparty, '_.*$', '') AS counterparty_type,
       count() AS deals,
       round(avg(10000 * CASE WHEN t.qty > 0 THEN 1 ELSE -1 END * (mid(q.last_bid, q.last_ask) - t.px) / t.px), 2)::decimal(8,2) AS markout_bps
FROM (t TIMESTAMP(ts)) AS t
HORIZON JOIN energy_quotes_1m AS q ON (symbol)
LIST (1m, 5m, 15m, 1h, 4h) AS h
ORDER BY counterparty_type, horizon_min;


-- =====================================================================================
-- ACT 2 · INTRADAY EXPOSURE
-- Net position per book and curve against limits, now versus the intraday peak (a
-- desk can be flat at the close and still have breached at 08:00), the path of the
-- breach, and exposure by delivery month with strips spread over their months.
-- =====================================================================================

-- @@ 2a_limits_now_vs_peak
-- Reveals 8.1: CRUDE / BRENT peaked above 100% of its limit this morning and is well
-- inside it now. The running sum over the ledger (opening position + fills) is the
-- position at every instant; max(abs()) is the peak.
-- The opening book (the 00:00 snapshot) enters as one number per book and curve,
-- then every fill moves it; the peak is the largest absolute value on that path.
DECLARE @demo_day := '2026-10-07'
WITH sod AS (
  SELECT book, curve, sum(qty) AS qty FROM energy_position_snapshots
  WHERE ts = @demo_day::timestamp
  GROUP BY book, curve
),
traded AS (
  SELECT ts, book, curve, sum(qty) OVER (PARTITION BY book, curve ORDER BY ts) AS traded
  FROM energy_fills
  WHERE ts IN @demo_day
),
running AS (
  SELECT @demo_day::timestamp AS ts, book, curve, qty AS net_qty FROM sod
  UNION ALL
  SELECT t.ts, t.book, t.curve, coalesce(s.qty, 0) + t.traded AS net_qty
  FROM traded t LEFT JOIN sod s ON (book, curve)
),
ordered AS (
  SELECT * FROM running ORDER BY ts
),
lim AS (
  SELECT book, curve, max_abs_qty, unit FROM energy_limits
  LATEST ON ts PARTITION BY book, curve
)
SELECT r.book, r.curve, l.unit,
       round(last(r.net_qty))::decimal(16,0)                                   AS net_now,
       round(max(abs(r.net_qty)))::decimal(16,0)                               AS intraday_peak,
       round(100 * abs(last(r.net_qty)) / l.max_abs_qty, 1)::decimal(6,1)      AS pct_of_limit_now,
       round(100 * max(abs(r.net_qty)) / l.max_abs_qty, 1)::decimal(6,1)       AS pct_of_limit_peak
FROM ordered r
JOIN lim l ON (book, curve)
GROUP BY r.book, r.curve, l.unit, l.max_abs_qty
ORDER BY pct_of_limit_peak DESC;

-- @@ 2b_breach_chart
-- Chart: CRUDE's net Brent position through the day against its limit (8.1).
DECLARE @demo_day := '2026-10-07'
WITH sod AS (
  SELECT sum(qty) AS qty FROM energy_position_snapshots
  WHERE book = 'CRUDE' AND curve = 'BRENT' AND ts = @demo_day::timestamp
),
r AS (
  SELECT ts, sum(qty) OVER (ORDER BY ts) AS traded
  FROM energy_fills
  WHERE book = 'CRUDE' AND curve = 'BRENT' AND ts IN @demo_day
  ORDER BY ts
),
lim AS (
  SELECT max_abs_qty FROM energy_limits
  WHERE book = 'CRUDE' AND curve = 'BRENT'
  LATEST ON ts PARTITION BY book, curve
)
SELECT r.ts, last(s.qty + r.traded) AS net_bbl, last(l.max_abs_qty) AS limit_bbl, -last(l.max_abs_qty) AS limit_short_bbl
FROM (r TIMESTAMP(ts)) r
CROSS JOIN sod s
CROSS JOIN lim l
SAMPLE BY 5m FILL(PREV);

-- @@ 2c_delivery_month_ladder
-- Gas, LNG and power exposure by delivery month, in MWh. Quarters, seasons and cals
-- are spread over their months by delivery days (gas) or hours (power, DST-aware),
-- then PIVOTed into one column per curve.
DECLARE @demo_day := '2026-10-07'
WITH pos AS (
  SELECT symbol, sum(qty) AS pos FROM energy_ledger
  WHERE ts IN @demo_day
  GROUP BY symbol
),
legs AS (
  SELECT x.curve, m.delivery_start AS delivery_month,
         p.pos * x.to_mwh * (CASE WHEN x.complex = 'GAS' THEN m.days * 1.0 / x.days ELSE m.hours * 1.0 / x.hours END) AS mwh
  FROM pos p
  JOIN energy_instruments x ON (symbol)
  JOIN energy_instruments m ON m.curve = x.curve AND m.granularity = 'M'
                           AND m.delivery_start >= x.delivery_start AND m.delivery_end <= x.delivery_end
  WHERE x.complex IN ('GAS', 'LNG', 'POWER')
)
SELECT * FROM legs
PIVOT (
  round(sum(mwh)) FOR curve IN ('TTF', 'NBP', 'JKM', 'UKPWR')
  GROUP BY delivery_month
) ORDER BY delivery_month;


-- =====================================================================================
-- ACT 3 · VOLATILITY
-- Realised vol from 5-minute bars (close-to-close, Parkinson range, RiskMetrics EWMA),
-- implied vol from the surface (term structure, skew, smile), and the gap between
-- them. Commodity specifics: front months move far more than back months, and power
-- prices go negative, which breaks log returns.
-- =====================================================================================

-- @@ 3a_realized_vol_by_tenor
-- The Samuelson effect: vol per contract along the curve, last two days of bars.
-- Front months carry about twice the vol of the 12th month on every curve.
DECLARE @demo_day := '2026-10-07'
WITH r AS (
  SELECT ts, symbol, curve,
         ln(close / lag(close) OVER (PARTITION BY symbol ORDER BY ts)) AS ret,
         datediff('s', lag(ts) OVER (PARTITION BY symbol ORDER BY ts), ts) AS gap_s
  FROM energy_quotes_5m
  WHERE curve IN ('BRENT', 'TTF', 'UKPWR') AND ts >= dateadd('d', -1, @demo_day::timestamp)
)
SELECT r.curve, r.symbol,
       round(datediff('d', @demo_day::timestamp, c.delivery_start) / 30.4, 1)::decimal(5,1) AS months_to_delivery,
       count()                                                                             AS returns,
       round(100 * sqrt(avg(ret * ret) * 365 * 288), 1)::decimal(6,1)                      AS realized_vol_pct
FROM r
JOIN energy_instruments c ON (symbol)
WHERE gap_s = 300 AND c.granularity = 'M' AND c.delivery_start < dateadd('M', 13, @demo_day::timestamp)
GROUP BY r.curve, r.symbol, c.delivery_start
ORDER BY r.curve, c.delivery_start;

-- @@ 3b_rolling_vol_front
-- Chart: three realised-vol estimators on the TTF front month through the day, each
-- annualised on calendar time (the market diffuses around the clock: 365 x 288 bars).
-- The EWMA is RiskMetrics' lambda 0.94 via avg(x, 'alpha', 0.06) OVER.
DECLARE @demo_day := '2026-10-07', @ttf := 'TTF_Nov-26'
WITH r AS (
  SELECT ts, high, low, ln(close / lag(close) OVER (ORDER BY ts)) AS ret
  FROM energy_quotes_5m
  WHERE symbol = @ttf AND ts IN @demo_day
)
SELECT ts,
       round(100 * sqrt(avg(ret * ret) OVER w * 365 * 288), 1)::decimal(6,1)                                     AS realized_1h,
       round(100 * sqrt(avg(ln(high / low) * ln(high / low)) OVER w / (4 * ln(2)) * 365 * 288), 1)::decimal(6,1) AS parkinson_1h,
       round(100 * sqrt(avg(ret * ret, 'alpha', 0.06) OVER (ORDER BY ts) * 365 * 288), 1)::decimal(6,1)         AS ewma_lambda_094
FROM r
WINDOW w AS (ORDER BY ts ROWS 11 PRECEDING)
ORDER BY ts;

-- @@ 3c_implied_term_structure_and_skew
-- The surface as it stands: ATM term structure, 25-delta risk reversal and butterfly.
-- Crude has put skew (risk reversal < 0); gas and power have call skew (the market
-- pays for upside spikes). LATEST ON per bucket picks the newest mark.
DECLARE @demo_day := '2026-10-07'
WITH smile AS (
  SELECT curve, symbol, delta_bucket, tau, iv FROM energy_iv_marks
  WHERE ts IN @demo_day
  LATEST ON ts PARTITION BY symbol, delta_bucket
),
p AS (
  SELECT * FROM smile
  PIVOT (
    avg(iv) FOR delta_bucket IN ('10P' AS p10, '25P' AS p25, 'ATM' AS atm, '25C' AS c25, '10C' AS c10)
    GROUP BY curve, symbol, tau
  )
)
SELECT curve, symbol,
       round(tau * 12, 1)::decimal(5,1)                       AS months_to_expiry,
       round(100 * atm, 1)::decimal(6,1)                      AS atm_vol,
       round(100 * (c25 - p25), 1)::decimal(6,1)              AS risk_reversal_25d,
       round(100 * ((c25 + p25) / 2 - atm), 1)::decimal(6,1)  AS butterfly_25d
FROM p
ORDER BY curve, tau;

-- @@ 3d_implied_vs_realized
-- Implied ATM against the last 24 hours of realised vol, per contract.
DECLARE @demo_day := '2026-10-07'
WITH atm AS (
  SELECT symbol, curve, iv FROM energy_iv_marks
  WHERE delta_bucket = 'ATM' AND ts IN @demo_day
  LATEST ON ts PARTITION BY symbol
),
rv AS (
  SELECT symbol, sqrt(avg(ret * ret) * 365 * 288) AS realized
  FROM (
    SELECT ts, symbol, ln(close / lag(close) OVER (PARTITION BY symbol ORDER BY ts)) AS ret,
           datediff('s', lag(ts) OVER (PARTITION BY symbol ORDER BY ts), ts) AS gap_s
    FROM energy_quotes_5m
    WHERE curve IN ('BRENT', 'TTF', 'UKPWR') AND ts >= dateadd('d', -1, @demo_day::timestamp)
  )
  WHERE gap_s = 300
  GROUP BY symbol
)
SELECT a.curve, a.symbol,
       round(100 * a.iv, 1)::decimal(6,1)                AS implied_atm,
       round(100 * r.realized, 1)::decimal(6,1)          AS realized_24h,
       round(100 * (a.iv - r.realized), 1)::decimal(6,1) AS implied_minus_realized
FROM atm a
JOIN rv r ON (symbol)
JOIN energy_instruments c ON (symbol)
ORDER BY a.curve, c.delivery_start;

-- @@ 3e_negative_power_breaks_log_returns
-- UK day-ahead hourly prices. On the windy night (8.8) hours 01:00 to 05:00 clear
-- negative, ln(p_t / p_t-1) is undefined across zero, and a vol model built on log
-- returns silently drops them. Energy desks measure absolute changes (normal vol).
WITH h AS (
  SELECT ts, price,
         price - lag(price) OVER (ORDER BY ts)     AS abs_change,
         ln(price / lag(price) OVER (ORDER BY ts)) AS log_return
  FROM energy_da_prices
  WHERE market = 'UK_DA'
)
SELECT timestamp_floor('d', ts)                          AS day,
       sum(CASE WHEN price < 0 THEN 1 ELSE 0 END)        AS negative_hours,
       round(min(price), 2)::decimal(8,2)                AS min_price_gbp,
       round(stddev(abs_change), 2)::decimal(8,2)        AS normal_vol_gbp_per_hour,
       count(abs_change) - count(log_return)             AS log_returns_lost
FROM h
GROUP BY day
ORDER BY day;


-- =====================================================================================
-- ACT 4 · FORWARD CURVES
-- The strip at mixed granularity (months, quarters, seasons, cals), how it moved
-- against the last settlement, its shape, consistency across granularities (now and
-- through history), and the curve as it stood at any past instant.
-- =====================================================================================

-- @@ 4a_live_vs_settlement
-- TTF months: the curve builder's latest mark against the last official settlement.
DECLARE @demo_day := '2026-10-07'
WITH live AS (
  SELECT symbol, price, source FROM energy_curve_marks
  WHERE curve = 'TTF' AND ts IN @demo_day
  LATEST ON ts PARTITION BY symbol
),
settle AS (
  SELECT symbol, price, ts FROM energy_settlements
  WHERE curve = 'TTF' AND ts < @demo_day::timestamp
  LATEST ON ts PARTITION BY symbol
)
SELECT c.symbol,
       round(s.price, 3)::decimal(10,3)             AS last_settlement,
       round(l.price, 3)::decimal(10,3)             AS live_mark,
       round(l.price - s.price, 3)::decimal(10,3)   AS change,
       l.source
FROM live l
JOIN energy_instruments c ON (symbol)
LEFT JOIN settle s ON (symbol)
WHERE c.granularity = 'M' AND c.delivery_start < dateadd('M', 13, @demo_day::timestamp)
ORDER BY c.delivery_start;

-- @@ 4b_curve_shape
-- Prompt spread (M1 - M2 > 0 is backwardation), M1 - M12, and winter minus summer.
-- Brent is backwardated, gas and power are seasonal, carbon is in contango.
DECLARE @demo_day := '2026-10-07'
WITH k AS (
  SELECT k.symbol, k.price, c.curve, c.granularity, c.delivery_start
  FROM (SELECT symbol, price FROM energy_curve_marks
        WHERE curve IN ('BRENT', 'WTI', 'TTF', 'NBP', 'UKPWR', 'EUA') AND ts IN @demo_day
        LATEST ON ts PARTITION BY symbol) k
  JOIN energy_instruments c ON (symbol)
),
n AS (
  SELECT curve, granularity, price, month(delivery_start) AS start_month,
         row_number() OVER (PARTITION BY curve, granularity ORDER BY delivery_start) AS k
  FROM k
)
SELECT curve,
       round(max(CASE WHEN granularity IN ('M', 'Z') AND k = 1 THEN price END)
             - max(CASE WHEN granularity IN ('M', 'Z') AND k = 2 THEN price END), 3)::decimal(8,3)  AS prompt_spread,
       round(max(CASE WHEN granularity = 'M' AND k = 1 THEN price END)
             - max(CASE WHEN granularity = 'M' AND k = 12 THEN price END), 3)::decimal(8,3)         AS m1_minus_m12,
       round(max(CASE WHEN granularity = 'S' AND start_month = 10 AND k <= 2 THEN price END)
             - max(CASE WHEN granularity = 'S' AND start_month = 4 AND k <= 2 THEN price END), 3)::decimal(8,3) AS winter_minus_summer
FROM n
GROUP BY curve
ORDER BY curve;

-- @@ 4c_consistency_now
-- A quarter, season or year must equal the weighted average of its months: days for
-- gas, hours for power (DST-aware). Gaps are tick noise unless something is wrong.
DECLARE @demo_day := '2026-10-07'
WITH k AS (
  SELECT k.symbol, k.price, k.source, k.version, c.curve, c.complex, c.granularity, c.delivery_start, c.delivery_end, c.hours, c.days
  FROM (SELECT symbol, price, source, version FROM energy_curve_marks
        WHERE curve IN ('TTF', 'NBP', 'UKPWR') AND ts IN @demo_day
        LATEST ON ts PARTITION BY symbol) k
  JOIN energy_instruments c ON (symbol)
),
g AS (
  SELECT s.symbol, s.price AS strip_mark, s.source, s.version,
         sum(m.price * (CASE WHEN s.complex = 'GAS' THEN m.days ELSE m.hours END))
           / sum(CASE WHEN s.complex = 'GAS' THEN m.days ELSE m.hours END) AS from_months,
         count() AS months
  FROM k s
  JOIN k m ON m.curve = s.curve AND m.granularity = 'M'
          AND m.delivery_start >= s.delivery_start AND m.delivery_end <= s.delivery_end
  WHERE s.granularity IN ('Q', 'S', 'Y')
  GROUP BY s.symbol, s.price, s.source, s.version
)
SELECT symbol,
       round(strip_mark, 3)::decimal(10,3)               AS strip_mark,
       round(from_months, 3)::decimal(10,3)              AS from_months,
       round(strip_mark - from_months, 3)::decimal(10,3) AS gap,
       months, source, version
FROM g
ORDER BY abs(strip_mark - from_months) DESC
LIMIT 15;

-- @@ 4d_consistency_history
-- The same check over every one-minute snapshot of the day, as the marks were
-- published (version 1). Reveals 8.2: a manual mark on a TTF quarter sat 2.50 EUR
-- above its months for half an hour before the curve service corrected it. The
-- correction is version 2 at the same ts; nothing was overwritten.
DECLARE @demo_day := '2026-10-07'
WITH k AS (
  SELECT k.ts, k.symbol, k.price, k.source, k.marked_by, c.curve, c.complex, c.granularity, c.delivery_start, c.delivery_end, c.hours, c.days
  FROM energy_curve_marks k
  JOIN energy_instruments c ON (symbol)
  WHERE k.curve IN ('TTF', 'NBP', 'UKPWR') AND k.ts IN @demo_day AND k.version = 1
),
gaps AS (
  SELECT s.ts, s.symbol, s.source, s.marked_by,
         s.price - sum(m.price * (CASE WHEN s.complex = 'GAS' THEN m.days ELSE m.hours END))
                   / sum(CASE WHEN s.complex = 'GAS' THEN m.days ELSE m.hours END) AS gap
  FROM k s
  JOIN k m ON m.ts = s.ts AND m.curve = s.curve AND m.granularity = 'M'
          AND m.delivery_start >= s.delivery_start AND m.delivery_end <= s.delivery_end
  WHERE s.granularity IN ('Q', 'S', 'Y')
  GROUP BY s.ts, s.symbol, s.source, s.marked_by, s.price
)
SELECT symbol, source, marked_by,
       min(ts) AS first_seen, max(ts) AS last_seen, count() AS minutes,
       round(max(abs(gap)), 3)::decimal(10,3) AS worst_gap
FROM gaps
WHERE abs(gap) > 0.25
GROUP BY symbol, source, marked_by
ORDER BY first_seen;

-- @@ 4e_curve_time_travel
-- The TTF curve as it stood at @asof against now: LATEST ON with ts <= @asof is the
-- whole reconstruction. Change @asof to any instant in history.
DECLARE @demo_day := '2026-10-07', @asof := '2026-10-07T12:00:00.000000Z'
WITH then_marks AS (
  SELECT symbol, price FROM energy_curve_marks
  WHERE curve = 'TTF' AND ts <= @asof AND ts > dateadd('m', -5, @asof)
  LATEST ON ts PARTITION BY symbol
),
now_marks AS (
  SELECT symbol, price FROM energy_curve_marks
  WHERE curve = 'TTF' AND ts IN @demo_day
  LATEST ON ts PARTITION BY symbol
)
SELECT c.symbol, c.granularity,
       round(t.price, 3)::decimal(10,3)           AS at_asof,
       round(n.price, 3)::decimal(10,3)           AS now,
       round(n.price - t.price, 3)::decimal(10,3) AS since_asof
FROM then_marks t
JOIN now_marks n ON (symbol)
JOIN energy_instruments c ON (symbol)
WHERE c.granularity IN ('M', 'Q', 'S') AND c.delivery_start < dateadd('M', 19, @demo_day::timestamp)
ORDER BY c.granularity, c.delivery_start;


-- =====================================================================================
-- ACT 5 · SPREADS
-- Legs tick at different times, so an exact join on timestamp returns nothing. ASOF
-- JOIN pairs each tick of one leg with the latest price of the others; TOLERANCE
-- refuses a match when the other leg is too old to trust.
-- =====================================================================================

-- @@ 5a_lng_arb_jkm_ttf
-- Chart: Asia versus Europe for the prompt month in $/MMBtu, TTF converted at EURUSD,
-- one point a minute for the whole day from the 1-minute bars (each leg's last quote
-- in the minute, ASOF-aligned with a 5-minute tolerance for minutes a leg did not
-- tick). A cargo decision nets freight off this; freight is not in the dataset.
DECLARE @demo_day := '2026-10-07', @jkm := 'JKM_Nov-26', @ttf := 'TTF_Nov-26'
SELECT j.ts,
       round(mid(j.last_bid, j.last_ask), 3)::decimal(10,3)                                                  AS jkm_usd_mmbtu,
       round(mid(t.last_bid, t.last_ask) * mid(e.last_bid, e.last_ask) / 3.412, 3)::decimal(10,3)            AS ttf_usd_mmbtu,
       round(mid(j.last_bid, j.last_ask) - mid(t.last_bid, t.last_ask) * mid(e.last_bid, e.last_ask) / 3.412, 3)::decimal(10,3) AS jkm_minus_ttf
FROM (energy_quotes_1m WHERE symbol = @jkm AND ts IN @demo_day) j
ASOF JOIN (energy_quotes_1m WHERE symbol = @ttf AND ts IN @demo_day) t TOLERANCE 5m
ASOF JOIN (energy_quotes_1m WHERE symbol = 'EURUSD' AND ts IN @demo_day) e TOLERANCE 5m
ORDER BY j.ts;

-- @@ 5a_ticks_half_hour
-- The same spread tick by tick over 30 minutes from @asof: every JKM tick paired with
-- the latest TTF and EURUSD ticks, and TOLERANCE 30s refuses a leg older than that.
-- This is the "legs tick at different times" point; the day-long series above is the
-- chart. Each leg is the quotes table with a filter, not a CTE: that is the form the
-- ASOF JOIN optimiser recognises; the same join over CTEs takes a slow path.
DECLARE @asof := '2026-10-07T12:00:00.000000Z', @jkm := 'JKM_Nov-26', @ttf := 'TTF_Nov-26'
SELECT jkm.ts,
       round(mid(jkm.bid, jkm.ask), 3)::decimal(10,3)                                                 AS jkm_usd_mmbtu,
       round(mid(ttf.bid, ttf.ask) * mid(eur.bid, eur.ask) / 3.412, 3)::decimal(10,3)                 AS ttf_usd_mmbtu,
       round(mid(jkm.bid, jkm.ask) - mid(ttf.bid, ttf.ask) * mid(eur.bid, eur.ask) / 3.412, 3)::decimal(10,3) AS jkm_minus_ttf,
       ttf.ts AS ttf_tick_ts,
       eur.ts AS eur_tick_ts
FROM (energy_quotes WHERE symbol = @jkm AND ts >= @asof AND ts < dateadd('m', 30, @asof)) jkm
ASOF JOIN (energy_quotes WHERE symbol = @ttf AND ts >= dateadd('m', -5, @asof) AND ts < dateadd('m', 30, @asof)) ttf TOLERANCE 30s
ASOF JOIN (energy_quotes WHERE symbol = 'EURUSD' AND ts >= dateadd('m', -5, @asof) AND ts < dateadd('m', 30, @asof)) eur TOLERANCE 30s
ORDER BY jkm.ts;

-- @@ 5b_gasoil_crack_and_brent_wti
-- Gasoil crack in $/bbl (7.45 bbl per tonne) and the Brent-WTI spread, hourly open and
-- last, from the 1-minute bars (each leg's last quote in the minute: the ASOF
-- alignment with a one-minute tolerance at a fraction of the cost on the most liquid
-- contracts of the day).
DECLARE @demo_day := '2026-10-07', @brent := 'BRENT_Dec-26', @gasoil := 'GASOIL_Nov-26', @wti := 'WTI_Nov-26'
WITH s AS (
  SELECT b.ts, mid(g.last_bid, g.last_ask) / 7.45 - mid(b.last_bid, b.last_ask) AS gasoil_crack,
         mid(b.last_bid, b.last_ask) - mid(w.last_bid, w.last_ask) AS brent_wti
  FROM (energy_quotes_1m WHERE symbol = @brent AND ts IN @demo_day) b
  JOIN (energy_quotes_1m WHERE symbol = @gasoil AND ts IN @demo_day) g ON (ts)
  JOIN (energy_quotes_1m WHERE symbol = @wti AND ts IN @demo_day) w ON (ts)
)
SELECT ts,
       round(first(gasoil_crack), 2)::decimal(8,2) AS crack_open,
       round(last(gasoil_crack), 2)::decimal(8,2)  AS crack_last,
       round(first(brent_wti), 2)::decimal(8,2)    AS brent_wti_open,
       round(last(brent_wti), 2)::decimal(8,2)     AS brent_wti_last
FROM s
SAMPLE BY 1h;

-- @@ 5c_clean_spark_zscore
-- Chart: UK clean spark spread for the prompt month, one point a minute from the
-- 1-minute bars, with a z-score over the trailing two hours. Power - gas / efficiency
-- - carbon x emission factor / efficiency; NBP p/therm x 0.341214 = GBP/MWh. 50%
-- efficiency and 0.2 tCO2/MWh of gas are assumptions, stated on the slide.
DECLARE @demo_day := '2026-10-07', @pwr := 'UKPWR_Nov-26', @nbp := 'NBP_Nov-26', @uka := 'UKA_Dec-26'
WITH s AS (
  SELECT p.ts, mid(p.last_bid, p.last_ask) - mid(n.last_bid, n.last_ask) * 0.341214 / 0.5 - mid(u.last_bid, u.last_ask) * 0.2 / 0.5 AS spread
  FROM (energy_quotes_1m WHERE symbol = @pwr AND ts IN @demo_day) p
  ASOF JOIN (energy_quotes_1m WHERE symbol = @nbp AND ts IN @demo_day) n TOLERANCE 5m
  ASOF JOIN (energy_quotes_1m WHERE symbol = @uka AND ts IN @demo_day) u TOLERANCE 5m
)
SELECT ts,
       round(spread, 2)::decimal(8,2)                                                    AS clean_spark_gbp_mwh,
       round((spread - avg(spread) OVER w) / stddev(spread) OVER w, 2)::decimal(6,2)     AS zscore_2h
FROM s
WINDOW w AS (ORDER BY ts ROWS 119 PRECEDING)
ORDER BY ts;

-- @@ 5c_ticks_half_hour
-- The same spread on every power tick over 30 minutes from @asof, each paired with the
-- latest gas and carbon ticks within a one-minute tolerance.
DECLARE @asof := '2026-10-07T12:00:00.000000Z', @pwr := 'UKPWR_Nov-26', @nbp := 'NBP_Nov-26', @uka := 'UKA_Dec-26'
SELECT pwr.ts,
       round(mid(pwr.bid, pwr.ask) - mid(nbp.bid, nbp.ask) * 0.341214 / 0.5 - mid(uka.bid, uka.ask) * 0.2 / 0.5, 2)::decimal(8,2) AS clean_spark_gbp_mwh,
       nbp.ts AS gas_tick_ts,
       uka.ts AS carbon_tick_ts
FROM (energy_quotes WHERE symbol = @pwr AND ts >= @asof AND ts < dateadd('m', 30, @asof)) pwr
ASOF JOIN (energy_quotes WHERE symbol = @nbp AND ts >= dateadd('m', -5, @asof) AND ts < dateadd('m', 30, @asof)) nbp TOLERANCE 1m
ASOF JOIN (energy_quotes WHERE symbol = @uka AND ts >= dateadd('m', -5, @asof) AND ts < dateadd('m', 30, @asof)) uka TOLERANCE 1m
ORDER BY pwr.ts;

-- @@ 5d_forward_clean_spark_curve
-- The same spread along the strip: what a gas plant's hedging desk actually manages.
-- Power and gas months are joined on delivery month, carbon on delivery year.
DECLARE @demo_day := '2026-10-07'
WITH k AS (
  SELECT symbol, price FROM energy_curve_marks
  WHERE curve IN ('UKPWR', 'NBP', 'UKA') AND ts IN @demo_day
  LATEST ON ts PARTITION BY symbol
),
legs AS (
  SELECT c.curve, c.delivery_start, year(c.delivery_start) AS yr, month(c.delivery_start) AS mo, k.price
  FROM k JOIN energy_instruments c ON (symbol)
  WHERE c.granularity IN ('M', 'Z')
)
SELECT p.delivery_start                                                                   AS delivery_month,
       round(p.price, 2)::decimal(8,2)                                                    AS power_gbp_mwh,
       round(g.price * 0.341214, 2)::decimal(8,2)                                         AS gas_gbp_mwh,
       round(u.price, 2)::decimal(8,2)                                                    AS uka_gbp_t,
       round(p.price - g.price * 0.341214 / 0.5 - u.price * 0.2 / 0.5, 2)::decimal(8,2)   AS clean_spark_gbp_mwh
FROM legs p
JOIN legs g ON g.curve = 'NBP' AND g.yr = p.yr AND g.mo = p.mo
JOIN legs u ON u.curve = 'UKA' AND u.yr = p.yr
WHERE p.curve = 'UKPWR' AND p.delivery_start < dateadd('M', 25, @demo_day::timestamp)
ORDER BY delivery_month;

-- @@ 5e_leg_correlation_and_hedge_ratio
-- How much gas hedges a MWh of power: correlation and regression slope of 5-minute
-- changes in the prompt power month against the prompt gas month in GBP/MWh, over
-- the last two days. The slope is the minimum-variance hedge ratio.
DECLARE @demo_day := '2026-10-07', @pwr := 'UKPWR_Nov-26', @nbp := 'NBP_Nov-26'
WITH p AS (SELECT ts, close FROM energy_quotes_5m WHERE symbol = @pwr AND ts >= dateadd('d', -1, @demo_day::timestamp)),
     g AS (SELECT ts, close FROM energy_quotes_5m WHERE symbol = @nbp AND ts >= dateadd('d', -1, @demo_day::timestamp)),
     j AS (
       SELECT p.ts, p.close AS pwr, g.close * 0.341214 AS gas
       FROM p ASOF JOIN g TOLERANCE 5m
     ),
     r AS (
       SELECT ts, pwr - lag(pwr) OVER (ORDER BY ts) AS d_pwr, gas - lag(gas) OVER (ORDER BY ts) AS d_gas
       FROM j
     )
SELECT count(d_pwr)                                       AS bars,
       round(corr(d_pwr, d_gas), 3)::decimal(6,3)         AS correlation,
       round(regr_slope(d_pwr, d_gas), 3)::decimal(6,3)   AS hedge_ratio_mwh_gas_per_mwh_power,
       round(stddev(d_pwr), 3)::decimal(8,3)              AS power_5m_stdev_gbp,
       round(stddev(d_gas), 3)::decimal(8,3)              AS gas_5m_stdev_gbp
FROM r;


-- =====================================================================================
-- ACT 6 · HISTORICAL VALUATION RECONSTRUCTION
-- trade_events is append-only with booking time as the designated timestamp: every
-- booking, amendment and cancel is a new row. "As known at T" filters on booking time
-- before picking each trade's latest version; "as restated" picks the latest version
-- known now. The two cells differ by ONE line: WHERE booked_ts <= @asof. That is the
-- punchline: reconstruction is not a separate system.
-- Status is filtered after LATEST ON (a WHERE at the same level runs before it, and
-- a cancelled trade's earlier NEW row would come back).
-- =====================================================================================

-- @@ 6a_book_as_known_vs_as_restated
-- Mark-to-market per book at @asof, as the desk saw it at @asof versus as the booking
-- log now says it was. Same marks for both, so the difference is purely the bookings.
DECLARE @asof := '2026-10-07T12:00:00.000000Z'
WITH known AS (
  (SELECT * FROM energy_trade_events WHERE booked_ts <= @asof LATEST ON booked_ts PARTITION BY trade_id)   -- as known at @asof
  WHERE status != 'CANCELLED' AND trade_ts <= @asof
),
restated AS (
  (SELECT * FROM energy_trade_events LATEST ON booked_ts PARTITION BY trade_id)                            -- as restated now
  WHERE status != 'CANCELLED' AND trade_ts <= @asof
),
book AS (
  SELECT 'known' AS view, book, symbol, qty, px FROM known
  UNION ALL
  SELECT 'restated' AS view, book, symbol, qty, px FROM restated
),
marks AS (
  SELECT symbol, price FROM energy_curve_marks
  WHERE ts <= @asof AND ts > dateadd('m', -5, @asof)
  LATEST ON ts PARTITION BY symbol
),
fx AS (
  SELECT symbol, mid(bid, ask) AS usd FROM energy_quotes
  WHERE curve = 'FX' AND ts <= @asof AND ts > dateadd('h', -1, @asof)
  LATEST ON ts PARTITION BY symbol
),
v AS (
  SELECT b.book,
         sum(CASE WHEN b.view = 'known'    THEN b.qty * (m.price - b.px) * c.px_factor * coalesce(x.usd, 1.0) ELSE 0 END) AS known_usd,
         sum(CASE WHEN b.view = 'restated' THEN b.qty * (m.price - b.px) * c.px_factor * coalesce(x.usd, 1.0) ELSE 0 END) AS restated_usd,
         sum(CASE WHEN b.view = 'known'    THEN 1 ELSE 0 END) AS trades_known,
         sum(CASE WHEN b.view = 'restated' THEN 1 ELSE 0 END) AS trades_restated
  FROM book b
  JOIN marks m ON (symbol)
  JOIN energy_instruments c ON (symbol)
  LEFT JOIN fx x ON x.symbol = c.fx_symbol
  GROUP BY b.book
)
SELECT book,
       round(known_usd)::decimal(14,0)                  AS mtm_as_known,
       round(restated_usd)::decimal(14,0)               AS mtm_as_restated,
       round(restated_usd - known_usd)::decimal(14,0)   AS restatement,
       trades_known, trades_restated
FROM v
ORDER BY abs(restated_usd - known_usd) DESC;

-- @@ 6b_what_changed_since
-- Trade by trade: what the restatement is made of. Reveals 8.3 (a fill booked with
-- 10x the quantity, amended), 8.4 (a broker deal booked twice, duplicate cancelled)
-- and 8.5 (a voice deal booked five hours late), plus a handful of background
-- amendments. FULL JOIN on trade_id between the two views.
DECLARE @asof := '2026-10-07T12:00:00.000000Z'
WITH known AS (
  (SELECT * FROM energy_trade_events WHERE booked_ts <= @asof LATEST ON booked_ts PARTITION BY trade_id)
  WHERE status != 'CANCELLED' AND trade_ts <= @asof
),
restated AS (
  (SELECT * FROM energy_trade_events LATEST ON booked_ts PARTITION BY trade_id)
  WHERE status != 'CANCELLED' AND trade_ts <= @asof
)
SELECT coalesce(k.trade_id, r.trade_id)   AS trade_id,
       coalesce(k.book, r.book)           AS book,
       coalesce(k.symbol, r.symbol)       AS symbol,
       coalesce(r.channel, k.channel)     AS channel,
       k.qty                              AS qty_as_known,
       r.qty                              AS qty_as_restated,
       k.px                               AS px_as_known,
       r.px                               AS px_as_restated,
       CASE WHEN k.trade_id IS NULL THEN 'booked late'
            WHEN r.trade_id IS NULL THEN 'cancelled since'
            ELSE 'amended since' END      AS what_changed,
       r.reason,
       coalesce(r.booked_ts, k.booked_ts) AS last_booking
FROM known k
FULL JOIN restated r ON k.trade_id = r.trade_id
WHERE k.trade_id IS NULL OR r.trade_id IS NULL OR k.qty != r.qty OR k.px != r.px OR k.book != r.book
ORDER BY abs(coalesce(r.qty, 0) - coalesce(k.qty, 0)) DESC;

-- @@ 6c_pnl_forensics_t1_to_t2
-- Why did the book's value change between @t1 and @t2? Market moves on the @t1 book,
-- new trades, late bookings, amendments and cancellations, in USD. The five columns
-- add up to total_change for every book, so nothing is unexplained.
DECLARE @t1 := '2026-10-07T12:00:00.000000Z', @t2 := '2026-10-07T15:00:00.000000Z'
WITH b1 AS (
  (SELECT * FROM energy_trade_events WHERE booked_ts <= @t1 LATEST ON booked_ts PARTITION BY trade_id)
  WHERE status != 'CANCELLED' AND trade_ts <= @t1
),
b2 AS (
  (SELECT * FROM energy_trade_events WHERE booked_ts <= @t2 LATEST ON booked_ts PARTITION BY trade_id)
  WHERE status != 'CANCELLED' AND trade_ts <= @t2
),
both AS (
  SELECT coalesce(b1.book, b2.book) AS book, coalesce(b1.symbol, b2.symbol) AS symbol,
         b1.qty AS q1, b1.px AS p1, b2.qty AS q2, b2.px AS p2, b2.trade_ts AS ts2
  FROM b1 FULL JOIN b2 ON b1.trade_id = b2.trade_id
),
m1 AS (SELECT symbol, price FROM energy_curve_marks WHERE ts <= @t1 AND ts > dateadd('m', -5, @t1) LATEST ON ts PARTITION BY symbol),
m2 AS (SELECT symbol, price FROM energy_curve_marks WHERE ts <= @t2 AND ts > dateadd('m', -5, @t2) LATEST ON ts PARTITION BY symbol),
f1 AS (SELECT symbol, mid(bid, ask) AS usd FROM energy_quotes
       WHERE curve = 'FX' AND ts <= @t1 AND ts > dateadd('h', -1, @t1) LATEST ON ts PARTITION BY symbol),
f2 AS (SELECT symbol, mid(bid, ask) AS usd FROM energy_quotes
       WHERE curve = 'FX' AND ts <= @t2 AND ts > dateadd('h', -1, @t2) LATEST ON ts PARTITION BY symbol),
x AS (
  SELECT t.book, t.q1, t.p1, t.q2, t.p2, t.ts2, a.price AS m1, b.price AS m2,
         c.px_factor * coalesce(u1.usd, 1.0) AS k1, c.px_factor * coalesce(u2.usd, 1.0) AS k2
  FROM both t
  JOIN m1 a ON a.symbol = t.symbol
  JOIN m2 b ON b.symbol = t.symbol
  JOIN energy_instruments c ON c.symbol = t.symbol
  LEFT JOIN f1 u1 ON u1.symbol = c.fx_symbol
  LEFT JOIN f2 u2 ON u2.symbol = c.fx_symbol
)
SELECT book,
       round(sum(CASE WHEN q1 IS NOT NULL THEN q1 * ((m2 - p1) * k2 - (m1 - p1) * k1) ELSE 0 END))::decimal(14,0)                       AS market_move,
       round(sum(CASE WHEN q1 IS NULL AND ts2 > @t1 THEN q2 * (m2 - p2) * k2 ELSE 0 END))::decimal(14,0)                              AS new_trades,
       round(sum(CASE WHEN q1 IS NULL AND ts2 <= @t1 THEN q2 * (m2 - p2) * k2 ELSE 0 END))::decimal(14,0)                             AS late_bookings,
       round(sum(CASE WHEN q1 IS NOT NULL AND q2 IS NOT NULL THEN (q2 * (m2 - p2) - q1 * (m2 - p1)) * k2 ELSE 0 END))::decimal(14,0)  AS amendments,
       round(sum(CASE WHEN q2 IS NULL THEN -q1 * (m2 - p1) * k2 ELSE 0 END))::decimal(14,0)                                           AS cancellations,
       round(sum(CASE WHEN q2 IS NOT NULL THEN q2 * (m2 - p2) * k2 ELSE 0 END)
             - sum(CASE WHEN q1 IS NOT NULL THEN q1 * (m1 - p1) * k1 ELSE 0 END))::decimal(14,0)                                      AS total_change
FROM x
GROUP BY book
ORDER BY book;

-- @@ 6d_booking_latency_by_channel
-- How long a deal takes to reach the book, by channel: STP mirrors exchange fills in
-- milliseconds, broker deals take minutes, bilateral deals hours. The 5h20m outlier
-- is 8.5. Governance angle: this is the data-quality monitor for trade capture.
DECLARE @demo_day := '2026-10-07'
WITH b AS (
  SELECT channel, (booked_ts - trade_ts) / 60000000.0 AS latency_min
  FROM energy_trade_events
  WHERE version = 1 AND booked_ts IN @demo_day
)
SELECT channel,
       count()                                                     AS bookings,
       round(avg(latency_min), 2)::decimal(10,2)                   AS avg_min,
       round(approx_percentile(latency_min, 0.5, 2), 2)::decimal(10,2) AS p50_min,
       round(approx_percentile(latency_min, 0.95, 2), 2)::decimal(10,2) AS p95_min,
       round(max(latency_min), 2)::decimal(10,2)                   AS max_min
FROM b
GROUP BY channel
ORDER BY avg_min;


-- =====================================================================================
-- ACT 7 · PRICING MODEL VALIDATION
-- Continuous evidence that the fair-value model agrees with the market: bias, RMSE
-- and the share of model prices inside the live bid-ask, champion versus challenger,
-- drift over time, no-arbitrage checks on the vol surface, and whether the grading
-- itself trusted stale quotes. Errors in bps so curves are comparable.
-- =====================================================================================

-- @@ 7a_champion_vs_challenger
-- Every model price is graded ASOF against the quote at that instant, with a
-- tolerance: never grade a model against a stale quote. Reveals 8.6: after the
-- cold-snap repricing the champion is biased on winter months; the challenger refit
-- within the hour, so its RMSE carries that one hour and its bias does not.
-- Graded on the first 12 delivery months, where the quotes are liquid, split into
-- before and after the repricing so the headline is not diluted by the morning.
-- Each model price is graded against the last quote of the preceding 1-minute bar
-- (quotes_1m), the ASOF match with a one-minute tolerance at a fraction of the cost
-- over a day of tick data; a minute with no quote is counted, not graded.
-- 7d does the tight-tolerance version on raw ticks around the outage.
DECLARE @demo_day := '2026-10-07', @repricing := '2026-10-07T11:00:00.000000Z'
WITH m AS (
  SELECT ts, dateadd('m', -1, ts) AS bar_ts, model_version, symbol, model_px
  FROM energy_model_prices
  WHERE ts IN @demo_day
),
v AS (
  SELECT m.model_version,
         CASE WHEN m.ts < @repricing THEN '1_before' ELSE '2_after' END AS period,
         CASE WHEN month(c.delivery_start) IN (11, 12, 1, 2) THEN 'winter' ELSE 'summer' END AS season,
         m.model_px, b.last_bid AS bid, b.last_ask AS ask,
         10000 * (m.model_px - mid(b.last_bid, b.last_ask)) / mid(b.last_bid, b.last_ask) AS err_bps
  FROM m
  JOIN energy_instruments c ON c.symbol = m.symbol
  LEFT JOIN energy_quotes_1m b ON b.symbol = m.symbol AND b.ts = m.bar_ts
  WHERE c.delivery_start < dateadd('M', 13, @demo_day::timestamp)
)
SELECT model_version, period, season,
       count(bid)                                          AS graded,
       count() - count(bid)                                AS not_graded_no_quote,
       round(avg(err_bps), 1)::decimal(8,1)                AS bias_bps,
       round(sqrt(avg(err_bps * err_bps)), 1)::decimal(8,1) AS rmse_bps,
       round(100.0 * sum(CASE WHEN model_px >= bid AND model_px <= ask THEN 1 ELSE 0 END) / count(bid), 1)::decimal(5,1) AS pct_inside_quote
FROM v
GROUP BY model_version, period, season
ORDER BY model_version, period, season;

-- @@ 7b_model_drift
-- Chart: hourly bias on winter gas and power contracts, champion against challenger.
-- The champion breaks at 11:00 on the demo day and never recovers; the challenger is
-- back within a few bps after its next hourly refit.
-- Graded against the last quote of the previous 1-minute bar (the quotes_1m
-- materialized view), which is the ASOF match with a one-minute tolerance at a
-- fraction of the cost over two days of tick data.
DECLARE @demo_day := '2026-10-07'
WITH m AS (
  SELECT ts, dateadd('m', -1, ts) AS bar_ts, model_version, symbol, model_px
  FROM energy_model_prices
  WHERE ts >= dateadd('d', -1, @demo_day::timestamp)
),
v AS (
  SELECT m.ts, m.model_version, 10000 * (m.model_px - mid(b.last_bid, b.last_ask)) / mid(b.last_bid, b.last_ask) AS err_bps
  FROM m
  JOIN energy_instruments c ON c.symbol = m.symbol
  JOIN energy_quotes_1m b ON b.symbol = m.symbol AND b.ts = m.bar_ts
  WHERE month(c.delivery_start) IN (11, 12, 1, 2) AND c.delivery_start < dateadd('M', 13, @demo_day::timestamp)
)
SELECT ts,
       round(avg(CASE WHEN model_version = 'champion_v1' THEN err_bps END), 1)::decimal(8,1)   AS champion_bias_bps,
       round(avg(CASE WHEN model_version = 'challenger_v2' THEN err_bps END), 1)::decimal(8,1) AS challenger_bias_bps
FROM v
SAMPLE BY 1h;

-- @@ 7c_vol_surface_calendar_arbitrage
-- Total implied variance iv^2 * tau must not fall with expiry at a fixed delta. Checked
-- on every 5-minute surface of the day as published (version 1). Reveals 8.7: one
-- bad ATM mark on a TTF month for 20 minutes. 7c_as_corrected shows the fix.
DECLARE @demo_day := '2026-10-07'
WITH w AS (
  SELECT ts, curve, symbol, delta_bucket, iv * iv * tau AS total_var,
         lag(iv * iv * tau) OVER (PARTITION BY ts, curve, delta_bucket ORDER BY tau) AS prev_total_var
  FROM energy_iv_marks
  WHERE ts IN @demo_day AND version = 1
)
SELECT curve, symbol, delta_bucket, min(ts) AS first_seen, max(ts) AS last_seen, count() AS violations
FROM w
WHERE total_var < prev_total_var
GROUP BY curve, symbol, delta_bucket
ORDER BY first_seen;

-- @@ 7c_as_corrected
-- The same check on the latest version of every mark: the version 2 corrections at
-- the same ts replace the bad marks and the surface is arbitrage-free again.
DECLARE @demo_day := '2026-10-07'
WITH latest AS (
  SELECT ts, curve, symbol, delta_bucket, tau, iv FROM energy_iv_marks
  WHERE ts IN @demo_day
  LATEST ON ts PARTITION BY ts, symbol, delta_bucket
),
w AS (
  SELECT ts, curve, symbol, delta_bucket, iv * iv * tau AS total_var,
         lag(iv * iv * tau) OVER (PARTITION BY ts, curve, delta_bucket ORDER BY tau) AS prev_total_var
  FROM latest
)
SELECT curve, symbol, delta_bucket, min(ts) AS first_seen, max(ts) AS last_seen, count() AS violations
FROM w
WHERE total_var < prev_total_var
GROUP BY curve, symbol, delta_bucket
ORDER BY first_seen;

-- @@ 7d_stale_quote_coverage
-- Chart: the share of model prices that could be graded against a fresh quote, minute
-- by minute around the planted feed outage (8.9). ASOF JOIN with TOLERANCE returns
-- null instead of a stale quote, so the coverage drops to zero for three minutes on
-- the EEX-routed power contracts and the validation never grades against stale data.
-- Front six power months (the liquid ones): outside the outage every minute grades.
DECLARE @demo_day := '2026-10-07', @outage_start := '2026-10-07T10:00:00.000000Z', @outage_end := '2026-10-07T10:03:00.000000Z'
SELECT m.ts,
       count()                                                   AS model_rows,
       count(q.bid)                                              AS graded,
       round(100.0 * count(q.bid) / count(), 1)::decimal(5,1)    AS graded_pct
FROM energy_model_prices m
ASOF JOIN energy_quotes q ON (symbol) TOLERANCE 10s
JOIN energy_instruments c ON (symbol)
WHERE m.curve = 'UKPWR' AND m.model_version = 'champion_v1'
  AND c.delivery_start < dateadd('M', 7, @demo_day::timestamp)
  AND m.ts >= dateadd('m', -5, @outage_start) AND m.ts < dateadd('m', 6, @outage_end)
SAMPLE BY 1m;
