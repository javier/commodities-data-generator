#!/usr/bin/env bash
#
# Backfill the energy trading demo into the Enterprise cluster over QWP.
# RUN THIS FROM INSIDE THE VPC. It will not work from outside: the public
# endpoint presents a certificate that rustls rejects (UnsupportedCertVersion),
# and released questdb wheels refuse tls_verify=unsafe_off.
#
# Tables created (nothing else is touched), all prefixed energy_ so they cannot
# collide with the FX or commodities datasets:
#   energy_instruments, energy_listings, energy_limits, energy_quotes, energy_curve_marks,
#   energy_settlements, energy_iv_marks, energy_model_prices, energy_da_prices,
#   energy_fills, energy_trade_events, energy_position_snapshots, energy_demo_events
#   energy_quotes_10s / _1m / _5m / _1d, energy_curve_marks_1h  (materialized views)
#   energy_positions_live, energy_positions_live_by_venue      (live views, beta)
#   energy_ledger, energy_curve_marks_latest, energy_trade_events_latest,
#   energy_tenors, energy_instrument_master, energy_marks_asof, energy_marks_live, energy_fx_asof,
#   energy_usd_factor_asof, energy_tenors_asof, energy_book_asof,
#   energy_book_restated, energy_mid_1m_day, energy_positions_running_day,
#   energy_model_graded_day, energy_strip_gaps_day              (views)
#
# If the cluster already holds energy_ objects from before the listings change
# (instruments, limits, curve_marks, position_snapshots and quotes_1m all changed
# shape), drop every energy_ view, live view, materialized view and table once,
# by name, then run this: the generator refuses to write into a table whose
# schema differs from its own.
#
# Every table is WAL with per-column Parquet encodings and partitioned by hour.
# With --enterprise every table goes to object storage: the time-series tables take
# TO REMOTE 1 hour, TO PARQUET 7 days, DROP LOCAL 3 months, and so do the lookup
# tables (instruments, listings, limits, one row per key stamped when first
# written), --short_ttl or not. Backfilled partitions are already hours or days old, so they go to object
# storage within minutes (and those more than a week old to Parquet) and stay
# queryable. Materialized views keep a TTL (3 months, the 10-second bars 1 month).
# Add --short_ttl true to keep the time-series data for 7 days only, remote copies
# included (the lookup tables stay); then keep the backfill window under 7 days.
#
# This is a CORRECTNESS run, not a throughput run. The scale factor is kept
# modest on purpose for the gp3 volume: 0.5 is roughly 35M quotes a weekday, which is
# enough for every cell in the query pack and for the markouts to converge.
set -euo pipefail

cd "$(dirname "$0")"
PY="${PY:-python}"

# Cluster endpoints, primary FIRST: DDL and metadata go to the writable node,
# and the client rotates across the rest for failover. These are the VPC-internal
# addresses; override for a different cluster, e.g.
#   HOST=primary.internal:9000 ./energy_enterprise_backfill.sh
HOST="${HOST:-172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000}"

# Five days ending now by default, with the storyline on the last day. Set
# START_TS / END_TS (UTC ISO) or DEMO_DAY (YYYY-MM-DD) to pin the window, for
# instance to backfill the days before the session and plant the story on the
# session day itself.
SCALE="${SCALE:-0.5}"
PROCESSES="${PROCESSES:-3}"
SEED="${SEED:-7}"

EXTRA=()
[[ -n "${START_TS:-}" ]] && EXTRA+=(--start_ts "$START_TS")
[[ -n "${END_TS:-}" ]] && EXTRA+=(--end_ts "$END_TS")
[[ -n "${DEMO_DAY:-}" ]] && EXTRA+=(--demo_day "$DEMO_DAY")

exec "$PY" -u energy_trading_data_generator.py \
  --host "$HOST" \
  --qwp_tls true \
  --tls_verify unsafe_off \
  --token_file "$HOME/qwp_token.txt" \
  --durable_ack false \
  --enterprise true \
  --mode faster-than-life \
  --processes "$PROCESSES" \
  --scale_factor "$SCALE" \
  --seed "$SEED" \
  --create_views true \
  --create_live_view true \
  --create_plain_views true \
  --incremental false \
  --state_file .energy_state.pkl \
  --demo_sql energy_demo_queries.sql \
  "${EXTRA[@]}"
