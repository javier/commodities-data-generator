#!/usr/bin/env bash
python commodities_data_generator.py \
  --host REPLACE_ME_host \
  --token "REPLACE_ME_token" \
  --token_x "REPLACE_ME_token_x" \
  --token_y "REPLACE_ME_token_y" \
  --ilp_user ilp_ingest \
  --protocol tcp \
  --mode real-time \
  --processes 1 \
  --scale_factor 1 \
  --create_views false \
  --incremental false
