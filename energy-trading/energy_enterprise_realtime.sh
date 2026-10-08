#!/usr/bin/env bash
#
# Real-time energy trading ingestion into the Enterprise cluster over QWP.
#
# Run this after energy_enterprise_backfill.sh, not instead of it. The two serve
# different halves of the session:
#   backfill  -> history, which the reconstruction, vol and markout cells need
#                in order to be meaningful rather than noise, and which gives
#                the cold storage tiers something to tier
#   real-time -> "now", which every trailing-window panel needs, and which lets
#                the planted events scheduled after the session start (13:20,
#                14:05, 14:10, 14:30 UTC) fire at their wall-clock time
#
# --incremental resumes from the state file the backfill wrote in this folder
# (factor path, desk positions, planted rows not yet due), catches up the gap
# since the last backfilled timestamp faster than life, then paces itself in
# 250 ms slices two seconds ahead of the wall clock. If the state file is not
# here (backfill run from another box), it rebuilds its state from the database.
#
# Tables must already exist (run the backfill first); the DDL is idempotent and
# re-running it is harmless. Keep it under a process supervisor (systemd,
# screen, tmux) if it needs to outlive your shell. Ctrl-C saves the state file.
set -euo pipefail

cd "$(dirname "$0")"
PY="${PY:-python}"

# Cluster endpoints, primary FIRST. These are the VPC-internal addresses;
# override for a different cluster, e.g.
#   HOST=primary.internal:9000 ./energy_enterprise_realtime.sh
HOST="${HOST:-172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000}"

# Same scale as the backfill so there is no visible change in tick rate at the
# boundary. 0.5 is a few hundred quotes a second in European hours, gentle on
# the gp3 volume and plenty for live panels.
SCALE="${SCALE:-0.5}"
SEED="${SEED:-7}"

exec "$PY" -u energy_trading_data_generator.py \
  --host "$HOST" \
  --qwp_tls true \
  --tls_verify unsafe_off \
  --token_file "$HOME/qwp_token.txt" \
  --durable_ack false \
  --enterprise true \
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
