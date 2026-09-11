#!/usr/bin/env bash
#
# Real-time commodities ingestion over QWP/WebSocket.
#
# QWP carries both SQL and writes, so there is one endpoint (port 9000) and one
# credential: a bearer token via --token_file, NOT the old ILP JWK x/y coords.
# The --host list gives automatic failover; the client rotates to the writable
# primary and replays each worker's store-and-forward spool on reconnect, so
# list the primary FIRST.
#
# Add --tls_verify unsafe_off if the server uses a self-signed certificate.
set -euo pipefail

python commodities_data_generator.py \
  --host REPLACE_ME_host:9000 \
  --token_file "$HOME/qwp_token.txt" \
  --qwp_tls true \
  --durable_ack true \
  --enterprise true \
  --mode real-time \
  --processes 1 \
  --scale_factor 1 \
  --create_views false \
  --incremental false
