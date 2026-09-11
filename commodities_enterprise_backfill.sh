#!/usr/bin/env bash
#
# Backfill the commodities dataset into the Enterprise cluster over QWP.
# RUN THIS FROM INSIDE THE VPC. It will not work from outside: the public
# endpoint presents a certificate that rustls rejects (UnsupportedCertVersion),
# and released questdb wheels refuse tls_verify=unsafe_off.
#
# Tables created (nothing else is touched):
#   commodities_market_data, commodities_trades, commodities_settlements
#   commodities_bbo_1s / _1m / _1h
#   commodities_trades_ohlcv_1s / _1m / _15m
# All are prefixed commodities_, so they cannot collide with the FX dataset
# (market_data, core_price, fx_trades, bbo_*, and their _demo suffixed twins).
#
# NOTE ON RETENTION: --short_ttl is deliberately NOT set. This is a demo dataset
# for dates already in the past, so any retention threshold would fire against it
# immediately. Without the flag no retention clause is emitted and the data
# simply persists.
#
# This is a CORRECTNESS run, not a throughput run. Rates are kept low on purpose
# for the gp3 volume.
set -euo pipefail

PY="${PY:-python}"

# Cluster endpoints, primary FIRST: DDL and metadata go to the writable node,
# and the client rotates across the rest for failover. These are the VPC-internal
# addresses; override for a different cluster, e.g.
#   HOST=primary.internal:9000 ./commodities_enterprise_backfill.sh
HOST="${HOST:-172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000}"

# Sept 9-10 UTC, matching the FX data already on the cluster so the two datasets
# overlap for cross-asset ASOF JOIN work later.
START_TS="${START_TS:-2026-09-09T00:00:00.000000Z}"
END_TS="${END_TS:-2026-09-11T00:00:00.000000Z}"

# IMPORTANT: --total_market_data_events is a hard cap that STOPS the run, and it
# binds before --end_ts. At the default 15-40 eps a 48h window needs ~4.7M events;
# anything less and the backfill simply stops partway through Sept 9 and never
# reaches Sept 10.
#
# So the event RATE is what to tune, and the cap is set generously above what the
# window can consume. 6-14 eps averages ~10/s, which is ~1.7M order book rows
# across the full 48h: enough for the counterparty markout to converge, gentle
# enough for the gp3 volume.
MD_MIN_EPS="${MD_MIN_EPS:-6}"
MD_MAX_EPS="${MD_MAX_EPS:-14}"
TR_MIN_EPS="${TR_MIN_EPS:-2}"
TR_MAX_EPS="${TR_MAX_EPS:-6}"
TOTAL_MD="${TOTAL_MD:-4000000}"
PROCESSES="${PROCESSES:-3}"

"$PY" -u commodities_data_generator.py \
  --host "$HOST" \
  --qwp_tls true \
  --tls_verify unsafe_off \
  --token_file "$HOME/qwp_token.txt" \
  --durable_ack false \
  --enterprise true \
  --mode faster-than-life \
  --processes "$PROCESSES" \
  --scale_factor 1 \
  --market_data_min_eps "$MD_MIN_EPS" \
  --market_data_max_eps "$MD_MAX_EPS" \
  --trades_min_eps "$TR_MIN_EPS" \
  --trades_max_eps "$TR_MAX_EPS" \
  --total_market_data_events "$TOTAL_MD" \
  --start_ts "$START_TS" \
  --end_ts "$END_TS" \
  --min_levels 20 \
  --max_levels 20 \
  --create_views true \
  --incremental false \
  --session_pacing true \
  --offsession_trades trickle \
  --toxicity_horizon_s 60
