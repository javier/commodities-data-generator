#!/usr/bin/env bash
#
# Local real-time ingestion of the energy trading demo into QuestDB OSS on
# 127.0.0.1:9000. Run it after energy_backfill.sh: --incremental resumes from
# the state file the backfill wrote (factor path, desk positions, planted
# events not yet due), catches up the gap between the last backfilled
# timestamp and now, then paces itself in 250 ms slices two seconds ahead of
# the wall clock. Planted events scheduled later in the day fire at their
# wall-clock time.
#
# Without a state file it rebuilds its state from the database (last mids,
# last snapshot plus today's fills and bookings) and carries on from there.
#
# Ctrl-C stops it cleanly and saves the state file again.
set -euo pipefail

cd "$(dirname "$0")"
PY="${PY:-python}"

HOST="${HOST:-127.0.0.1:9000}"
SCALE="${SCALE:-1}"
SEED="${SEED:-7}"

exec "$PY" -u energy_trading_data_generator.py \
  --host "$HOST" \
  --mode real-time \
  --scale_factor "$SCALE" \
  --seed "$SEED" \
  --create_views true \
  --create_live_view true \
  --create_plain_views true \
  --incremental true \
  --state_file .energy_state.pkl \
  --realtime_slice_ms 250 \
  --yahoo_refresh_secs 300
