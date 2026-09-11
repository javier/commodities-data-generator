#!/usr/bin/env bash
#
# Real-time commodities ingestion into the Enterprise cluster over QWP.
#
# Run this alongside the Sept 9-10 backfill, not instead of it. The two serve
# different halves of the demo:
#   backfill  -> history, which the counterparty/venue markout panels need in
#                order to be statistically meaningful rather than noise
#   real-time -> "now", which every trailing-window panel needs: the whole crude
#                dashboard is pinned to 1m/10m/15m/30m windows, and Market Depth
#                on the metals dashboard is pinned to 10m
#
# Tables must already exist (run commodities_enterprise_backfill.sh first), so
# --create_views is false here. The continuous materialized views keep refreshing
# from base-table writes regardless.
#
# From inside the VPC use the private IPs. Keep it under a process supervisor
# (systemd, screen, tmux) if it needs to outlive your shell.
set -euo pipefail

PY="${PY:-python}"

# Cluster endpoints, primary FIRST. These are the VPC-internal addresses;
# override for a different cluster, e.g.
#   HOST=primary.internal:9000 ./commodities_enterprise_realtime.sh
HOST="${HOST:-172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000}"

# Deliberately modest for the gp3 volume: scale_factor 2 over the 15-40 eps
# default is ~30-80 order book events/sec, which is plenty to make the depth
# chart and 1s candles look alive without stressing the disk.
SCALE="${SCALE:-2}"
PROCESSES="${PROCESSES:-1}"

# offsession_trades=full keeps data flowing outside CME Globex hours, so the
# dashboards never go flat mid-demo just because of the time of day.
"$PY" -u commodities_data_generator.py \
  --host "$HOST" \
  --qwp_tls true \
  --tls_verify unsafe_off \
  --token_file "$HOME/qwp_token.txt" \
  --durable_ack false \
  --enterprise true \
  --mode real-time \
  --processes "$PROCESSES" \
  --scale_factor "$SCALE" \
  --min_levels 20 \
  --max_levels 20 \
  --create_views false \
  --incremental false \
  --session_pacing true \
  --offsession_trades full \
  --toxicity_horizon_s 60 \
  --yahoo_refresh_secs 300
