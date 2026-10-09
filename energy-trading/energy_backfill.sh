#!/usr/bin/env bash
#
# Local backfill of the energy trading demo into a QuestDB OSS instance on
# 127.0.0.1:9000: five days of history ending now, the storyline planted on the
# last day, then the query pack's @variables rewritten to match.
#
# Scale: at --scale_factor 1 the quotes table grows by roughly 50M rows a
# weekday (low thousands of ticks a second in European hours), which is what a
# consolidated top-of-book feed for this many contracts looks like; four days
# load in about 90 seconds on a laptop. Use 0.2 for a quick functional check.
# Fill rates scale with the same factor.
#
# No --short_ttl: the data is in the past and any retention threshold would
# fire against it immediately.
set -euo pipefail

cd "$(dirname "$0")"
PY="${PY:-python}"

HOST="${HOST:-127.0.0.1:9000}"
SCALE="${SCALE:-1}"
PROCESSES="${PROCESSES:-3}"
SEED="${SEED:-7}"

# Window: default is demo day (today, UTC) minus 4 days at 00:00 up to now.
# Override with START_TS / END_TS (UTC ISO) or DEMO_DAY (YYYY-MM-DD).
EXTRA=()
[[ -n "${START_TS:-}" ]] && EXTRA+=(--start_ts "$START_TS")
[[ -n "${END_TS:-}" ]] && EXTRA+=(--end_ts "$END_TS")
[[ -n "${DEMO_DAY:-}" ]] && EXTRA+=(--demo_day "$DEMO_DAY")

exec "$PY" -u energy_trading_data_generator.py \
  --host "$HOST" \
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
