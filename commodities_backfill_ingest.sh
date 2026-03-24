#!/usr/bin/env bash
python commodities_data_generator.py \
  --host REPLACE_ME_host \
  --token "REPLACE_ME_token" \
  --token_x "REPLACE_ME_token_x" \
  --token_y "REPLACE_ME_token_y" \
  --ilp_user ilp_ingest \
  --protocol tcp \
  --mode faster-than-life \
  --processes 6 \
  --scale_factor 50 \
  --total_market_data_events 100_000_000 \
  --start_ts "2025-11-11T00:00:00.000000Z" \
  --end_ts "2025-11-11T14:00:00.000000Z" \
  --create_views false \
  --incremental false
