#!/usr/bin/env bash
#
# Faster-than-life commodities backfill over QWP/WebSocket.
#
# Same transport notes as commodities_real_time_ingest.sh: one endpoint on port
# 9000, bearer token via --token_file, primary listed first in --host.
set -euo pipefail

python commodities_data_generator.py \
  --host REPLACE_ME_host:9000 \
  --token_file "$HOME/qwp_token.txt" \
  --qwp_tls true \
  --durable_ack true \
  --enterprise true \
  --mode faster-than-life \
  --processes 6 \
  --scale_factor 50 \
  --total_market_data_events 100_000_000 \
  --start_ts "2025-11-11T00:00:00.000000Z" \
  --end_ts "2025-11-11T14:00:00.000000Z" \
  --create_views false \
  --incremental false
