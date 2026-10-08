# Energy Trading Desk dashboard

One Grafana dashboard for the live session, built on the plain views the generator
creates (see "Views" in `../README.md`). Every panel is as of the right edge of the time
range: leave the range at "Last 6 hours" with auto-refresh for the live desk, or drag its
end into the past and the whole dashboard shows the desk as it stood then.

![Energy Trading Desk dashboard](screenshot.png)

## Import

Requirements: Grafana 12.3 or later with the QuestDB datasource plugin, and a QuestDB
datasource pointing at the instance the generator writes to. Checked on Grafana 13.2.2
with plugin `questdb-questdb-datasource` 0.1.8, against QuestDB 10.0.2.

1. Dashboards > New > Import, upload `energy_desk_dashboard.json`.
2. Pick the QuestDB datasource in the `QuestDB` selector at the top left (the
   `DS_QUESTDB` variable; every panel follows it, so the same dashboard serves a local
   instance and a cluster).
3. Set the dashboard time range to include data. Times are UTC throughout: the data, the
   planted storyline and the dashboard.

The hidden `prefix` variable (`energy_`) is put in front of every table and view name;
change it if the generator ran with a different `--prefix`.

The refresh picker offers 250 ms, 500 ms and 750 ms as well as the usual intervals. Grafana
only honours them when the server's `min_refresh_interval` is 250 ms or lower; its
default is 5 s (`[dashboards] min_refresh_interval` in `grafana.ini`, or
`GF_DASHBOARDS_MIN_REFRESH_INTERVAL=250ms` for a container).

## How the as-of works

Every panel query starts by binding the views' overridable variables to the end of the
range:

```sql
DECLARE @asof := '${__to:date:iso}',
        @day  := timestamp_floor('d', '${__to:date:iso}'::timestamp)
SELECT ...
```

`${__to:date:iso}` is Grafana's range end as an ISO timestamp, which the views take as
`@asof`; `@day` is the start of that day. The plugin's macros bound the series windows:

| Macro | Expands to |
|---|---|
| `$__timeFilter(ts)` | `ts >= cast(<from> as timestamp) AND ts <= cast(<to> as timestamp)` |
| `$__fromTime`, `$__toTime` | `cast(<epoch micros> as timestamp)` |
| `$__sampleByInterval` | a `SAMPLE BY` unit for the panel's width, for example `60s` |

There are no per-panel time overrides: the picker is the only control.

## Panels

| Panel | Type | Reads | Matches |
|---|---|---|---|
| Intraday trading PnL by book, with drawdown | time series | `positions_running_day`, `quotes_5m`, `usd_factor_asof` | `1b_twin_window_function` |
| PnL and position blotter | table | `ledger`, `marks_asof`, `usd_factor_asof`, `listings` | `1a_pnl_by_book` (book totals) |
| Limit utilisation, now and intraday peak | table, gauge cells | `position_snapshots`, `fills`, `limits` | `2a_limits_now_vs_peak` |
| Curve evolution | status history | `curve_marks`, `settlements`, `tenors_asof` | |
| Live curve versus last settlement | trend | `marks_asof`, `tenors_asof`, `settlements` | `4a_live_vs_settlement` |
| Book as known versus as restated | time series | `trade_events`, `marks_asof`, `usd_factor_asof` | `book_asof` at each minute |
| PnL explain across the time range | bar chart | `trade_events`, `curve_marks`, `quotes`, `instruments` | `6c_pnl_forensics_t1_to_t2` |

Notes on three of them:

- The curve evolution shows each tenor's change against the last settlement in percent,
  so one colour scale serves every curve (gas moves several percent a day, crude under
  one). Rows are tenors front to back, months, then quarters, seasons and cals.
- The book-as-known line is the value of the day's deals as the booking log showed it at
  each minute; the as-restated line puts every deal at its final version, as known at the
  range end, at its trade time. Both use the marks of the range end, so the gap is purely
  bookings. The lines meet at the right edge.
- PnL explain splits the change in value of the day's deals between the range start and
  end into market move, new trades, late bookings, amendments and cancellations, which
  add up to the total. Example, 12:00 to 15:00 on the demo day, all books (USD):
  market move -2,681,727, new trades 356,094, late bookings 157,946, amendments -730,544,
  cancellations -236,450; sum -3,134,681, the total.

The planted events from `demo_events` appear as annotations on the time-series panels,
one colour per act; the toggles at the top hide them by act.

## The two demo moves

1. **Live.** Range "Last 6 hours", refresh 5 s, with `energy_realtime.sh` running. The
   blotter and the limits move with every fill, the PnL lines advance, the curve
   evolution adds a column every five minutes.
2. **Reconstruction.** Pause the refresh and set the range to 12:00 to 14:00 UTC on the
   demo day. The blotter and the limits show the desk as of 14:00; the as-known line
   steps at 13:20, when the LNG fill booked with ten times its quantity at 10:05 is
   corrected, and PnL explain carries that correction under amendments. Then move the range end to 14:35:
   the duplicate TTF quarter cancelled at 14:05 lands under cancellations, and the UK
   power season dealt at 09:10 and booked at 14:30 lands under late bookings and in the
   restated line.

A cancellation or amendment shows in PnL explain only when the trade existed at the range
start: a deal booked and cancelled inside the range was never in the opening book.

## Checks

On the scale 1 dataset, against the query pack at the same instants: blotter book totals
equal `1a_pnl_by_book` to the dollar; limits equal `2a_limits_now_vs_peak` (within the
cell's one-decimal rounding); the book-as-known line equals `book_asof` valued at the
range-end marks at every minute checked, and its right edge equals `6a`'s
`mtm_as_known`; PnL explain equals `6c` component by component. Every panel renders with
`book` set to All and to two books.

Load: every panel query sent through Grafana every 5 seconds for 10 minutes against the
live generator, range "Last 6 hours", all books (120 rounds, no errors). Median per panel
18 to 92 ms, the slowest single query 582 ms (the hero). At 250 ms refresh the heavier
panels can skip a beat.
