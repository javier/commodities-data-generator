#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# commodities_data_generator.py
#
# Commodities synthetic data generator for QuestDB using TIMESTAMP_NS everywhere.
# Mirrors the equities/FX architecture:
# - Arrays only in commodities_market_data (DOUBLE[][] for bids/asks)
# - commodities_trades with price/size/side per trade
# - commodities_settlements with daily settlement + open interest
# - Continuous mat views at 1s, timed rollups at 1m/15m/1h
# - Per-class volatility (energy, power, metals, ags)
# - Power spike state machine for PJM/ERCT/CISO/NBPL
# - Deferred contract linkage (CL12 -> CL, NG12 -> NG)
# - Session pacing (NYMEX/COMEX/CBOT/ICE hours)
# - WAL lag monitor pauses and resumes ingestion
# - Yahoo seeding for 19 symbols, EIA API for 4 power hubs
#
# Transport: QWP (QuestDB Wire Protocol) over WebSocket, for both SQL and writes.
# A single questdb.QuestDB handle replaces the previous psycopg (PG wire, 8812)
# plus questdb.ingress.Sender (ILP, 9000/9009) pairing, so DDL, metadata probes
# and row ingestion all share one client library, one port and one credential.
# Requires questdb>=5.0.0 (Python 3.10+) and a QWP-capable QuestDB server.


import argparse
import datetime
import math
import os
import random
import tempfile
import time
import sys
import multiprocessing as mp
from multiprocessing import Event
from zoneinfo import ZoneInfo
from typing import Optional

import numpy as np
import requests
import yfinance as yf
import questdb
from questdb import QuestDBError, TimestampNanos


# ----------------------------
# Instrument set (23 symbols)
# ----------------------------

# (symbol, name, yahoo_ticker, low, high, precision, tick, lot_size, unit, exchange, commodity_class, rank)
COMMODITIES = [
    # Energy (7) - Yahoo Finance
    # Static low/high are wide fallbacks for when Yahoo is unreachable.
    # In normal operation Yahoo returns live prices and brackets are +/-1% around them.
    ("CL",   "WTI Crude Oil",         "CL=F",  30.0, 150.0, 2, 0.01,   1000, "bbl",   "NYMEX", "energy",  1),
    ("BZ",   "Brent Crude Oil",       "BZ=F",  33.0, 155.0, 2, 0.01,   1000, "bbl",   "ICE",   "energy",  2),
    ("NG",   "Natural Gas Henry Hub", "NG=F",   1.0,  10.0, 3, 0.001, 10000, "MMBtu", "NYMEX", "energy",  1),
    ("TTF",  "Dutch TTF Gas",         "TTF=F", 10.0, 100.0, 2, 0.01,   1000, "MWh",   "ICE",   "energy",  3),
    ("RB",   "RBOB Gasoline",         "RB=F",   1.0,   5.0, 4, 0.0001, 42000, "gal",  "NYMEX", "energy",  4),
    ("HO",   "Heating Oil",           "HO=F",   1.0,   5.0, 4, 0.0001, 42000, "gal",  "NYMEX", "energy",  5),
    ("JKM",  "LNG Japan-Korea",       "JKM=F",  5.0,  40.0, 2, 0.01,  10000, "MMBtu", "ICE",   "energy",  6),

    # Deferred months (2) - derived from front month
    ("CL12", "WTI Crude 12-Month",    None,    28.0, 145.0, 2, 0.01,   1000, "bbl",   "NYMEX", "energy", 10),
    ("NG12", "Nat Gas 12-Month",      None,     0.8,   9.0, 3, 0.001, 10000, "MMBtu", "NYMEX", "energy", 10),

    # Power (4) - EIA API (demand-based bracket scaling)
    ("PJM",  "PJM Western Hub",       None,    15.0, 200.0, 2, 0.05,    40, "MWh",   "ICE",   "power",   3),
    ("ERCT", "ERCOT North Texas",     None,    10.0, 300.0, 2, 0.05,    50, "MWh",   "ICE",   "power",   4),
    ("CISO", "CAISO South (SP15)",    None,    12.0, 200.0, 2, 0.05,    50, "MWh",   "ICE",   "power",   5),
    ("NBPL", "New England (NEPOOL)",  None,    15.0, 150.0, 2, 0.05,    50, "MWh",   "ICE",   "power",   7),

    # Metals (5) - Yahoo Finance
    ("GC",   "Gold",                  "GC=F", 1200.0, 3500.0, 2, 0.10, 100,  "oz",   "COMEX", "metals",  1),
    ("SI",   "Silver",                "SI=F",   15.0,   50.0, 3, 0.005, 5000, "oz",   "COMEX", "metals",  2),
    ("HG",   "Copper",                "HG=F",    2.0,    7.0, 4, 0.0005, 25000, "lb", "COMEX", "metals",  3),
    ("PL",   "Platinum",              "PL=F",  600.0, 1500.0, 2, 0.10,   50,  "oz",   "NYMEX", "metals",  5),
    ("PA",   "Palladium",             "PA=F",  500.0, 2500.0, 2, 0.10,  100,  "oz",   "NYMEX", "metals",  5),

    # Agriculture (7) - Yahoo Finance
    ("ZC",   "Corn",                  "ZC=F",  300.0, 800.0,  2, 0.25,  5000, "bu",   "CBOT",  "ags",     2),
    ("ZS",   "Soybeans",              "ZS=F",  800.0, 1800.0, 2, 0.25,  5000, "bu",   "CBOT",  "ags",     2),
    ("ZW",   "Wheat",                 "ZW=F",  350.0, 1000.0, 2, 0.25,  5000, "bu",   "CBOT",  "ags",     3),
    ("KC",   "Coffee",                "KC=F",  100.0, 500.0,  2, 0.05, 37500, "lb",   "ICE",   "ags",     4),
    ("CC",   "Cocoa",                 "CC=F", 1500.0, 8000.0, 0, 1.00,    10, "MT",   "ICE",   "ags",     4),
    ("SB",   "Sugar",                 "SB=F",    8.0,  35.0,  2, 0.01,112000, "lb",   "ICE",   "ags",     5),
    ("CT",   "Cotton",                "CT=F",   50.0, 150.0,  2, 0.01, 50000, "lb",   "ICE",   "ags",     5),
]

DEFERRED_LINKS = {"CL12": "CL", "NG12": "NG"}
POWER_SYMBOLS = {"PJM", "ERCT", "CISO", "NBPL"}

# Build lookup dicts from the tuple list
_SYM_INFO = {}
for _row in COMMODITIES:
    _sym = _row[0]
    _SYM_INFO[_sym] = {
        "name": _row[1], "yahoo": _row[2], "low": _row[3], "high": _row[4],
        "precision": _row[5], "tick": _row[6], "lot_size": _row[7],
        "unit": _row[8], "exchange": _row[9], "commodity_class": _row[10],
        "rank": _row[11],
    }

SYMBOLS = [row[0] for row in COMMODITIES]

# Per-class volatility profiles
VOLATILITY = {
    "energy": {"drift_ticks": 3.0, "shock_prob": 0.005, "shock_mult": 20},
    "power":  {"drift_ticks": 3.0, "shock_prob": 0.015, "shock_mult": 40},
    "metals": {"drift_ticks": 2.0, "shock_prob": 0.003, "shock_mult": 15},
    "ags":    {"drift_ticks": 2.5, "shock_prob": 0.004, "shock_mult": 18},
}

# Per-class volume ladder bounds (lots)
LADDER_PROFILES = {
    "energy": (1, 500),
    "power":  (1, 200),
    "metals": (1, 300),
    "ags":    (1, 400),
}

# EIA hub mapping: our symbol -> state code (for retail-sales industrial prices)
EIA_HUB_STATE = {
    "PJM":  "PA",    # PJM spans many states, PA is the largest
    "ERCT": "TX",
    "CISO": "CA",
    "NBPL": "MA",
}


# ----------------------------
# Helpers
# ----------------------------

def now_ns() -> int:
    return time.time_ns()

def parse_ts_arg(ts: str) -> int:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)
    return int(dt.timestamp() * 1e9)

def ns_to_iso(ns: int) -> str:
    dt = datetime.datetime.utcfromtimestamp(ns / 1e9)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def quantize_price(price: float, tick: float, precision: int) -> float:
    return round(round(price / tick) * tick, precision)

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def make_ladder(levels: int, v_min: int = 1, v_max: int = 500) -> list:
    if levels <= 1:
        return [v_min]
    step = (math.log10(max(v_max, 2)) - math.log10(max(v_min, 1))) / max(1, levels - 1)
    return [int(round(10 ** (math.log10(max(v_min, 1)) + i * step))) for i in range(levels)]

def split_event_counts(total: int, num_workers: int) -> list:
    base = total // num_workers
    remainder = total % num_workers
    return [base + (1 if i < remainder else 0) for i in range(num_workers)]

def table_name(name: str, prefix: str) -> str:
    return f"{prefix}{name}" if prefix else name


# ----------------------------
# QWP connection
# ----------------------------

# Retention differs by edition and by object kind, verified against QuestDB
# Enterprise: ALTER TABLE ... SET TTL is rejected on Enterprise ("use a storage
# policy instead"), and STORAGE POLICY is rejected on materialized views
# ("storage policy is not supported for materialized views"). So tables take a
# storage policy on Enterprise and a TTL on OSS, while views always take a TTL.
#
# DROP LOCAL must never run without a remote tier ahead of it, or it is simply
# deletion. These mirror the policy the FX generator already runs in production.
MD_ENTERPRISE_POLICY = "TO REMOTE 1 hour, TO PARQUET 2 days, DROP LOCAL 3 months"
TR_ENTERPRISE_POLICY = "TO REMOTE 1 hour, TO PARQUET 2 days, DROP LOCAL 3 months"


def table_retention(short_ttl: bool, enterprise: bool, oss_ttl: str, policy: str) -> str:
    if not short_ttl:
        return ""
    if enterprise:
        return f" STORAGE POLICY({policy})"
    return f" TTL {oss_ttl}"


def view_retention(short_ttl: bool, oss_ttl: str) -> str:
    # Materialized views take a TTL on both editions; storage policies are rejected.
    return f" TTL {oss_ttl}" if short_ttl else ""


def qwp_addr_list(host: str, default_port: int = 9000) -> str:
    """Build a QWP addr= value from --host, supporting multi-host HA failover.

    Accepts "h1", "h1:9000" or a comma-separated list of either. Any entry
    without an explicit port gets default_port. The client rotates across the
    listed nodes and replays unacknowledged frames on reconnect, so list the
    writable primary first.
    """
    parts = []
    for raw in str(host).split(","):
        node = raw.strip()
        if not node:
            continue
        parts.append(node if ":" in node else f"{node}:{default_port}")
    return ",".join(parts) if parts else f"127.0.0.1:{default_port}"


def qwp_conf(args, sender_id: Optional[str] = None,
             auto_flush_interval: Optional[int] = None) -> str:
    """Build the QWP configuration string used for both SQL and ingestion.

    sender_id is set only for ingestion workers, where each worker needs its own
    store-and-forward slot; SQL-only handles leave it unset.
    """
    scheme = "wss" if args.qwp_tls else "ws"
    parts = [f"{scheme}::addr={qwp_addr_list(args.host)};"]
    if args.token:
        parts.append(f"token={args.token};")
    else:
        parts.append(f"username={args.user};password={args.password};")
    if args.qwp_tls and args.tls_ca:
        parts.append(f"tls_ca={args.tls_ca};")
    # Self-signed cluster certificates chain to nothing, so no choice of trust
    # root helps; verification has to be turned off explicitly.
    if args.qwp_tls and args.tls_verify != "on":
        parts.append(f"tls_verify={args.tls_verify};")
    if sender_id:
        # The client opens sf_dir but does not create it, so make it here.
        sf_dir = os.path.join(args.store_forward_dir, sender_id)
        os.makedirs(sf_dir, exist_ok=True)
        parts.append(f"sender_id={sender_id};")
        parts.append(f"sf_dir={sf_dir};")
        if args.durable_ack:
            parts.append("request_durable_ack=on;")
    if auto_flush_interval is not None:
        parts.append(f"auto_flush_interval={auto_flush_interval};")
    return "".join(parts)


def connect_qwp(args, sender_id: Optional[str] = None,
                auto_flush_interval: Optional[int] = None):
    """Open a QWP handle. connect() does no network I/O; errors surface on use."""
    return questdb.connect(qwp_conf(args, sender_id, auto_flush_interval))


# ----------------------------
# DB setup
# ----------------------------

def ensure_tables_and_views(args, prefix: str):
    # Tables: storage policy on Enterprise, TTL on OSS. Views: TTL on both.
    ret_md = table_retention(args.short_ttl, args.enterprise, "3 DAYS", MD_ENTERPRISE_POLICY)
    ret_tr = table_retention(args.short_ttl, args.enterprise, "1 MONTH", TR_ENTERPRISE_POLICY)
    ttl_md = view_retention(args.short_ttl, "3 DAYS")
    ttl_tr = view_retention(args.short_ttl, "1 MONTH")
    with connect_qwp(args) as conn:
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name("commodities_market_data", prefix)} (
          timestamp TIMESTAMP_NS,
          symbol SYMBOL CAPACITY 64,
          exchange SYMBOL CAPACITY 16,
          commodity_class SYMBOL CAPACITY 16,
          bids   DOUBLE[][],
          asks   DOUBLE[][],
          best_bid DOUBLE,
          best_ask DOUBLE
        ) timestamp(timestamp) PARTITION BY HOUR{ret_md};
        """)
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name("commodities_trades", prefix)} (
          timestamp TIMESTAMP_NS,
          symbol SYMBOL CAPACITY 64,
          exchange SYMBOL CAPACITY 16,
          commodity_class SYMBOL CAPACITY 16,
          price  DOUBLE,
          size   LONG,
          side   SYMBOL
        ) timestamp(timestamp) PARTITION BY HOUR{ret_tr};
        """)
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name("commodities_settlements", prefix)} (
          timestamp TIMESTAMP_NS,
          symbol SYMBOL CAPACITY 64,
          settlement_price DOUBLE,
          open_interest LONG,
          volume LONG
        ) timestamp(timestamp) PARTITION BY DAY;
        """)
        if not args.create_views:
            return
        # Continuous views
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("commodities_bbo_1s", prefix)} AS (
          SELECT
            timestamp,
            symbol,
            last(best_bid) AS best_bid,
            last(best_ask) AS best_ask,
            last(best_ask) - last(best_bid) AS spread
          FROM {table_name("commodities_market_data", prefix)}
          SAMPLE BY 1s
        ) PARTITION BY HOUR{ttl_md};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("commodities_trades_ohlcv_1s", prefix)} AS (
          SELECT
            timestamp,
            symbol,
            first(price) AS open,
            max(price)   AS high,
            min(price)   AS low,
            last(price)  AS close,
            sum(size)    AS volume
          FROM {table_name("commodities_trades", prefix)}
          SAMPLE BY 1s
        ) PARTITION BY HOUR{ttl_md};
        """)
        # Timed rollups
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("commodities_bbo_1m", prefix)}
        REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
          SELECT
            timestamp,
            symbol,
            max(best_bid) AS max_bid,
            min(best_ask) AS min_ask,
            min(best_ask) - max(best_bid) AS min_spread
          FROM {table_name("commodities_bbo_1s", prefix)}
          SAMPLE BY 1m
        ) PARTITION BY HOUR{ttl_tr};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("commodities_bbo_1h", prefix)}
        REFRESH EVERY 10m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
          SELECT
            timestamp,
            symbol,
            max(max_bid) AS max_bid,
            min(min_ask) AS min_ask,
            min(min_ask) - max(max_bid) AS min_spread
          FROM {table_name("commodities_bbo_1m", prefix)}
          SAMPLE BY 1h
        ) PARTITION BY DAY{ttl_tr};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("commodities_trades_ohlcv_1m", prefix)}
        REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
          SELECT
            timestamp,
            symbol,
            first(open)  AS open,
            max(high)    AS high,
            min(low)     AS low,
            last(close)  AS close,
            sum(volume)  AS volume
          FROM {table_name("commodities_trades_ohlcv_1s", prefix)}
          SAMPLE BY 1m
        ) PARTITION BY HOUR{ttl_tr};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("commodities_trades_ohlcv_15m", prefix)}
        REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
          SELECT
            timestamp,
            symbol,
            first(open)  AS open,
            max(high)    AS high,
            min(low)     AS low,
            last(close)  AS close,
            sum(volume)  AS volume
          FROM {table_name("commodities_trades_ohlcv_1m", prefix)}
          SAMPLE BY 15m
        ) PARTITION BY DAY{ttl_tr};
        """)


def get_latest_timestamp_ns(conn, table: str):
    """Latest designated timestamp in `table`, as epoch nanoseconds, or None.

    A missing table is not an error here: on a fresh database the generator is
    about to create it, and there is nothing to advance past.
    """
    try:
        with conn.query(
            f"SELECT timestamp FROM {table} ORDER BY timestamp DESC LIMIT 1"
        ) as result:
            df = result.to_pandas()
    except QuestDBError as e:
        print(f"[INFO] Could not read latest timestamp from {table}: {e}", flush=True)
        return None
    if df.empty:
        return None
    ts = df["timestamp"].iloc[0]
    if pd_is_null(ts):
        return None
    # QWP returns TIMESTAMP_NS as tz-naive datetime64[ns] holding a UTC instant.
    return int(ts.value) if hasattr(ts, "value") else int(ts)


def pd_is_null(value) -> bool:
    import pandas as pd
    return bool(pd.isna(value))


# ----------------------------
# Data source: Yahoo Finance
# ----------------------------

def fetch_yahoo_brackets(pct: float = 1.0):
    out = {}
    frac = pct / 100.0
    print("[INFO] Refreshing commodity brackets from Yahoo Finance.", flush=True)
    for row in COMMODITIES:
        sym, name, yahoo_ticker = row[0], row[1], row[2]
        default_low, default_high = row[3], row[4]
        if yahoo_ticker is None:
            continue
        try:
            bars = yf.Ticker(yahoo_ticker).history(period="5d", interval="1d")
            if not bars.empty:
                closes = bars["Close"].dropna()
                if closes.empty:
                    raise ValueError("No non-null closes")
                mid = float(closes.iloc[-1])
                if math.isnan(mid) or mid == 0.0:
                    raise ValueError("Price NaN or zero")
                low = mid * (1 - frac)
                high = mid * (1 + frac)
            else:
                raise ValueError("Empty dataframe")
        except Exception:
            low = default_low
            high = default_high
            print(
                f"[YF] {sym}: Yahoo returned no usable data, fallback "
                f"bracket [{low:.4f}, {high:.4f}]",
                flush=True,
            )
        out[sym] = (low, high)
    return out


# ----------------------------
# Data source: EIA API
# ----------------------------

def fetch_eia_brackets(api_key: Optional[str] = None, pct: float = 1.0):
    """Fetch industrial electricity retail prices from EIA as power hub anchors.

    Uses the retail-sales endpoint (monthly, industrial sector) which returns
    actual $/kWh prices per state. We convert to $/MWh and build brackets
    around the latest available month. This tracks real price movements
    including crisis-driven spikes (with ~1 month lag).
    """
    out = {}
    frac = pct / 100.0
    print("[INFO] Refreshing power brackets from EIA retail-sales API.", flush=True)
    base_url = "https://api.eia.gov/v2/electricity/retail-sales/data/"
    for sym, state in EIA_HUB_STATE.items():
        info = _SYM_INFO[sym]
        try:
            if not api_key:
                raise ValueError("No EIA API key provided")
            params = {
                "api_key": api_key,
                "frequency": "monthly",
                "data[0]": "price",
                "facets[stateid][]": state,
                "facets[sectorid][]": "IND",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 1,
            }
            resp = requests.get(base_url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            records = data.get("response", {}).get("data", [])
            if not records:
                raise ValueError("No data returned")
            price_ckwh = records[0].get("price")
            if price_ckwh is None:
                raise ValueError("Price is null")
            price_mwh = float(price_ckwh) * 10  # cents/kWh -> $/MWh
            if price_mwh <= 0:
                raise ValueError("Non-positive price")
            low = price_mwh * (1 - frac)
            high = price_mwh * (1 + frac)
            print(
                f"[EIA] {sym} ({state}): {records[0]['period']} industrial "
                f"= {float(price_ckwh):.2f} c/kWh = ${price_mwh:.1f}/MWh, "
                f"bracket [{low:.2f}, {high:.2f}]",
                flush=True,
            )
        except Exception as e:
            low = info["low"]
            high = info["high"]
            print(
                f"[EIA] {sym}: EIA returned no usable data ({e}), fallback "
                f"bracket [{low:.2f}, {high:.2f}]",
                flush=True,
            )
        out[sym] = (low, high)
    return out


def fetch_all_brackets(eia_api_key: Optional[str] = None, pct: float = 1.0):
    brackets = {}
    # Yahoo-sourced symbols
    yahoo = fetch_yahoo_brackets(pct)
    brackets.update(yahoo)
    # EIA-sourced power symbols
    eia = fetch_eia_brackets(eia_api_key, pct)
    brackets.update(eia)
    # Deferred months: derive from front month if available, else use static
    for deferred_sym, front_sym in DEFERRED_LINKS.items():
        info = _SYM_INFO[deferred_sym]
        if front_sym in brackets:
            front_low, front_high = brackets[front_sym]
            # Deferred trades at slight discount (contango/backwardation range)
            brackets[deferred_sym] = (front_low * 0.97, front_high * 1.03)
        else:
            brackets[deferred_sym] = (info["low"], info["high"])
    return brackets


# ----------------------------
# Session pacing
# ----------------------------

def commodity_session_phase(ts_ns: int, tz: str = "America/Chicago") -> str:
    tzinfo = ZoneInfo(tz)
    dt_local = datetime.datetime.fromtimestamp(ts_ns / 1e9, tzinfo)
    weekday = dt_local.weekday()
    t = dt_local.time()

    # Weekend: Saturday all day, Sunday before 5pm CT
    if weekday == 5:
        return "off"
    if weekday == 6 and t < datetime.time(17, 0):
        return "off"

    # Daily maintenance halt: 4:00 PM - 5:00 PM CT
    if datetime.time(16, 0) <= t < datetime.time(17, 0):
        return "maintenance"

    # Friday: close at 4:00 PM CT
    if weekday == 4 and t >= datetime.time(16, 0):
        return "off"

    # Settlement window: 2:25 PM - 2:30 PM CT (NYMEX/COMEX)
    if datetime.time(14, 25) <= t <= datetime.time(14, 30):
        return "settlement"

    # Active session: everything else (Globex nearly 23h/day)
    return "active"


# ----------------------------
# SortedEmitter
# ----------------------------

class SortedEmitter:
    def __init__(self, sender, buffer_limit, prefix):
        self.sender = sender
        self.buffer_limit = buffer_limit
        self.prefix = prefix
        self._md_buffer = []
        self._tr_buffer = []
        self._st_buffer = []

    def _send_buffer(self, buf, table):
        if not buf:
            return
        buf.sort(key=lambda r: r["ts"])
        for row in buf:
            self.sender.row(
                table_name(table, self.prefix),
                symbols=row["symbols"],
                columns=row["columns"],
                at=TimestampNanos(row["ts"]),
            )
        self.sender.flush()
        buf.clear()

    def emit_market(self, ts_ns, sym, exchange, commodity_class, bids, asks):
        self._md_buffer.append({
            "ts": ts_ns,
            "symbols": {"symbol": sym, "exchange": exchange, "commodity_class": commodity_class},
            "columns": {
                "bids": bids.copy(),
                "asks": asks.copy(),
                "best_bid": float(bids[0][0]),
                "best_ask": float(asks[0][0]),
            },
        })
        if len(self._md_buffer) >= self.buffer_limit:
            self._send_buffer(self._md_buffer, "commodities_market_data")

    def emit_trade(self, ts_ns, sym, exchange, commodity_class, side, price, size):
        self._tr_buffer.append({
            "ts": ts_ns,
            "symbols": {"symbol": sym, "exchange": exchange, "commodity_class": commodity_class, "side": side},
            "columns": {
                "price": float(price),
                "size": int(size),
            },
        })
        if len(self._tr_buffer) >= self.buffer_limit:
            self._send_buffer(self._tr_buffer, "commodities_trades")

    def emit_settlement(self, ts_ns, sym, settlement_price, open_interest, volume):
        self._st_buffer.append({
            "ts": ts_ns,
            "symbols": {"symbol": sym},
            "columns": {
                "settlement_price": float(settlement_price),
                "open_interest": int(open_interest),
                "volume": int(volume),
            },
        })
        if len(self._st_buffer) >= self.buffer_limit:
            self._send_buffer(self._st_buffer, "commodities_settlements")

    def flush_all(self):
        self._send_buffer(self._md_buffer, "commodities_market_data")
        self._send_buffer(self._tr_buffer, "commodities_trades")
        self._send_buffer(self._st_buffer, "commodities_settlements")


# ----------------------------
# State evolution
# ----------------------------

def evolve_mid(prev_mid: float, low: float, high: float, tick: float,
               precision: int, drift_ticks: float = 2.0,
               shock_prob: float = 0.002, shock_mult: float = 10) -> float:
    change = random.uniform(-drift_ticks * tick, drift_ticks * tick)
    if random.random() < shock_prob:
        change += random.uniform(-shock_mult * tick, shock_mult * tick)
    mid = clamp(prev_mid + change, low, high)
    return quantize_price(mid, tick, precision)


def generate_l2_for_symbol(
    best_bid: float, best_ask: float,
    levels: int, ladder: list, tick: float, precision: int,
    bids_buf: np.ndarray, asks_buf: np.ndarray,
):
    for i in range(levels):
        bids_buf[0][i] = quantize_price(best_bid - i * tick, tick, precision)
        asks_buf[0][i] = quantize_price(best_ask + i * tick, tick, precision)
        base = ladder[min(i, len(ladder) - 1)]
        bids_buf[1][i] = int(random.randint(max(1, base // 2), max(1, base)))
        asks_buf[1][i] = int(random.randint(max(1, base // 2), max(1, base)))
    return bids_buf, asks_buf


# ----------------------------
# Power spike state machine
# ----------------------------

def init_power_state():
    state = {}
    for sym in POWER_SYMBOLS:
        state[sym] = {
            "spike_active": False,
            "spike_remaining": 0,
            "revert_remaining": 0,
            "pre_spike_mid": None,
        }
    return state


def evolve_power_spike(sym, power_state, prev_mid, low, high, tick, precision):
    ps = power_state[sym]

    if ps["spike_active"]:
        ps["spike_remaining"] -= 1
        if ps["spike_remaining"] <= 0:
            # End spike, start revert
            ps["spike_active"] = False
            ps["revert_remaining"] = 5
        else:
            # During spike: extreme drift
            vol = VOLATILITY["power"]
            return evolve_mid(prev_mid, low, high, tick, precision,
                              drift_ticks=vol["drift_ticks"] * 10,
                              shock_prob=0.5, shock_mult=vol["shock_mult"] * 2)

    if ps["revert_remaining"] > 0:
        # Revert toward pre-spike midpoint
        ps["revert_remaining"] -= 1
        target = ps["pre_spike_mid"] if ps["pre_spike_mid"] else (low + high) / 2.0
        revert_step = (target - prev_mid) * 0.3
        mid = clamp(prev_mid + revert_step, low, high)
        return quantize_price(mid, tick, precision)

    # Not spiking: small chance to enter spike
    if random.random() < 0.02:
        ps["spike_active"] = True
        ps["spike_remaining"] = random.randint(5, 30)
        ps["pre_spike_mid"] = prev_mid
        vol = VOLATILITY["power"]
        return evolve_mid(prev_mid, low, high, tick, precision,
                          drift_ticks=vol["drift_ticks"] * 10,
                          shock_prob=0.5, shock_mult=vol["shock_mult"] * 2)

    return None  # No spike behavior, use normal evolution


# ----------------------------
# Open/close state management
# ----------------------------

def build_initial_state(symbols, brackets):
    state = {}
    for sym in symbols:
        info = _SYM_INFO[sym]
        low, high = brackets[sym]
        mid = (low + high) / 2.0
        tick = info["tick"]
        precision = info["precision"]
        spread = tick * 2
        state[sym] = {
            "bid": quantize_price(mid - spread / 2.0, tick, precision),
            "ask": quantize_price(mid + spread / 2.0, tick, precision),
            "spread": spread,
        }
    return state


# Deferred contract basis offsets (persistent across seconds)
_basis_offsets = {}


def evolve_open_close_for_second(symbols, brackets, prev_state, power_state=None):
    global _basis_offsets
    open_state = {}
    close_state = {}

    # Process non-deferred symbols first
    for sym in symbols:
        if sym in DEFERRED_LINKS:
            continue

        info = _SYM_INFO[sym]
        low, high = brackets[sym]
        tick = info["tick"]
        precision = info["precision"]
        commodity_class = info["commodity_class"]
        vol = VOLATILITY[commodity_class]

        prev_mid = (prev_state[sym]["bid"] + prev_state[sym]["ask"]) / 2.0

        # Check power spike
        new_mid = None
        if sym in POWER_SYMBOLS and power_state is not None:
            new_mid = evolve_power_spike(sym, power_state, prev_mid, low, high, tick, precision)

        if new_mid is None:
            new_mid = evolve_mid(
                prev_mid, low, high, tick, precision,
                drift_ticks=vol["drift_ticks"],
                shock_prob=vol["shock_prob"],
                shock_mult=vol["shock_mult"],
            )

        spread = clamp(
            prev_state[sym]["spread"] + random.uniform(-0.3 * tick, 0.3 * tick),
            tick,
            5 * tick,
        )

        bid = quantize_price(new_mid - spread / 2.0, tick, precision)
        ask = quantize_price(new_mid + spread / 2.0, tick, precision)

        open_state[sym] = {
            "bid": prev_state[sym]["bid"],
            "ask": prev_state[sym]["ask"],
            "spread": prev_state[sym]["spread"],
        }
        close_state[sym] = {"bid": bid, "ask": ask, "spread": spread}
        prev_state[sym] = {"bid": bid, "ask": ask, "spread": spread}

    # Process deferred symbols after their front months
    for deferred_sym, front_sym in DEFERRED_LINKS.items():
        if deferred_sym not in symbols:
            continue

        info = _SYM_INFO[deferred_sym]
        low, high = brackets[deferred_sym]
        tick = info["tick"]
        precision = info["precision"]

        # Slow contango/backwardation drift
        if deferred_sym not in _basis_offsets:
            _basis_offsets[deferred_sym] = 0.0
        _basis_offsets[deferred_sym] += random.uniform(-0.002, 0.002)
        _basis_offsets[deferred_sym] = clamp(_basis_offsets[deferred_sym], -2.0, 2.0)

        front_close_mid = (close_state[front_sym]["bid"] + close_state[front_sym]["ask"]) / 2.0
        deferred_mid = clamp(front_close_mid + _basis_offsets[deferred_sym], low, high)

        spread = clamp(
            prev_state[deferred_sym]["spread"] + random.uniform(-0.3 * tick, 0.3 * tick),
            tick,
            5 * tick,
        )

        bid = quantize_price(deferred_mid - spread / 2.0, tick, precision)
        ask = quantize_price(deferred_mid + spread / 2.0, tick, precision)

        open_state[deferred_sym] = {
            "bid": prev_state[deferred_sym]["bid"],
            "ask": prev_state[deferred_sym]["ask"],
            "spread": prev_state[deferred_sym]["spread"],
        }
        close_state[deferred_sym] = {"bid": bid, "ask": ask, "spread": spread}
        prev_state[deferred_sym] = {"bid": bid, "ask": ask, "spread": spread}

    return open_state, close_state, prev_state


def precompute_open_close_state(total_seconds, symbols, brackets, initial_state):
    open_per_second = []
    close_per_second = []
    state = {s: initial_state[s].copy() for s in symbols}
    power_state = init_power_state()

    for _ in range(total_seconds):
        open_state, close_state, state = evolve_open_close_for_second(
            symbols, brackets, state, power_state
        )
        open_per_second.append(open_state)
        close_per_second.append(close_state)

    return open_per_second, close_per_second


# ----------------------------
# Per-second generation
# ----------------------------

def generate_second(
    ts_ns_base: int,
    symbols: list,
    open_state: dict,
    close_state: dict,
    emitter: SortedEmitter,
    ladders: dict,
    min_levels: int,
    max_levels: int,
    md_events: int,
    tr_events: int,
    allow_trades: bool,
    is_settlement: bool = False,
    scale_factor: int = 1,
):
    # Prebuilt arrays for each depth
    prebuilt_bids = [
        np.zeros((2, lvl), dtype=np.float64) for lvl in range(1, max_levels + 1)
    ]
    prebuilt_asks = [
        np.zeros((2, lvl), dtype=np.float64) for lvl in range(1, max_levels + 1)
    ]

    # Build rank-weighted symbol list for event distribution
    rank_weights = []
    for sym in symbols:
        info = _SYM_INFO[sym]
        # Lower rank number = more events. Weight = 1/rank
        w = 1.0 / max(1, info["rank"])
        rank_weights.append(w)
    total_weight = sum(rank_weights)
    norm_weights = [w / total_weight for w in rank_weights]

    # Distribute md_events across symbols proportionally
    md_per_sym = {}
    remaining = md_events
    for i, sym in enumerate(symbols):
        count = int(round(md_events * norm_weights[i]))
        count = min(count, remaining)
        md_per_sym[sym] = max(1, count) if remaining > 0 else 0
        remaining -= md_per_sym[sym]
    # Distribute any leftover
    while remaining > 0:
        sym = random.choice(symbols)
        md_per_sym[sym] += 1
        remaining -= 1

    total_md = sum(md_per_sym.values())

    # Build event list: (global_idx, sym)
    md_pairs = []
    for sym in symbols:
        for _ in range(md_per_sym.get(sym, 0)):
            md_pairs.append(sym)
    random.shuffle(md_pairs)

    # Random offsets inside the second
    md_offsets = sorted(random.randint(0, 999_999_999) for _ in range(len(md_pairs)))

    # Group global indices by symbol for interpolation
    per_symbol_indices = {sym: [] for sym in symbols}
    for idx, sym in enumerate(md_pairs):
        per_symbol_indices[sym].append(idx)

    # L2 market data, smooth per symbol
    for sym, idx_list in per_symbol_indices.items():
        if not idx_list:
            continue

        info = _SYM_INFO[sym]
        tick = info["tick"]
        precision = info["precision"]
        exchange = info["exchange"]
        commodity_class = info["commodity_class"]
        ladder = ladders.get(commodity_class, ladders.get("energy"))

        ob = open_state[sym]
        cb = close_state[sym]
        n = len(idx_list)

        for j, global_idx in enumerate(idx_list):
            ofs = md_offsets[global_idx]
            frac = 0.0 if n == 1 else j / (n - 1)

            mid_bid = ob["bid"] + frac * (cb["bid"] - ob["bid"])
            mid_ask = ob["ask"] + frac * (cb["ask"] - ob["ask"])

            best_bid = quantize_price(mid_bid, tick, precision)
            best_ask = quantize_price(mid_ask, tick, precision)

            levels = random.randint(min_levels, max_levels)
            bids = prebuilt_bids[levels - 1]
            asks = prebuilt_asks[levels - 1]

            generate_l2_for_symbol(
                best_bid, best_ask, levels, ladder, tick, precision, bids, asks
            )
            emitter.emit_market(
                ts_ns_base + ofs, sym, exchange, commodity_class, bids, asks
            )

    # Trades
    if allow_trades and tr_events > 0:
        tr_offsets = sorted(random.randint(0, 999_999_999) for _ in range(tr_events))
        # Weighted symbol selection for trades
        tr_symbols = random.choices(symbols, weights=norm_weights, k=tr_events)

        for ofs, sym in zip(tr_offsets, tr_symbols):
            info = _SYM_INFO[sym]
            tick = info["tick"]
            precision = info["precision"]
            exchange = info["exchange"]
            commodity_class = info["commodity_class"]

            ob = open_state[sym]
            cb = close_state[sym]
            frac = random.random()
            best_bid = ob["bid"] + frac * (cb["bid"] - ob["bid"])
            best_ask = ob["ask"] + frac * (cb["ask"] - ob["ask"])

            mid = (best_bid + best_ask) / 2.0
            slip = random.uniform(-0.15 * tick, 0.15 * tick)
            price = quantize_price(clamp(mid + slip, best_bid, best_ask), tick, precision)

            side = "B" if random.random() < 0.5 else "S"

            # Trade size distribution (lots)
            r = random.random()
            if r < 0.60:
                size = random.randint(1, 5)
            elif r < 0.92:
                size = random.choice([10, 20, 25, 50, 100])
            else:
                size = random.randint(100, 500)

            emitter.emit_trade(
                ts_ns_base + ofs, sym, exchange, commodity_class, side, price, size
            )

    # Settlements (emitted at settlement time, ~21 non-deferred symbols)
    if is_settlement:
        settlement_ns = ts_ns_base + 500_000_000  # mid-second
        for sym in symbols:
            if sym in DEFERRED_LINKS:
                continue
            info = _SYM_INFO[sym]
            cb = close_state[sym]
            settlement_price = (cb["bid"] + cb["ask"]) / 2.0
            # Synthetic open interest and volume
            base_oi = info["lot_size"] * random.randint(50, 500)
            daily_vol = random.randint(10000, 200000)
            emitter.emit_settlement(
                settlement_ns, sym, settlement_price, base_oi, daily_vol
            )
        # Flush settlements immediately - too few rows to hit buffer_limit
        emitter._send_buffer(emitter._st_buffer, "commodities_settlements")


# ----------------------------
# Backpressure (WAL)
# ----------------------------

def wal_monitor(args, pause_event, processes, interval=5, prefix=""):
    threshold = 3 * processes
    last_logged_paused = False
    tbl = table_name("commodities_market_data", prefix)
    with connect_qwp(args) as conn:
        while True:
            try:
                with conn.query(
                    "SELECT sequencerTxn, writerTxn FROM wal_tables() WHERE name = $1",
                    [tbl],
                ) as result:
                    df = result.to_pandas()
                if not df.empty:
                    seq = int(df["sequencerTxn"].iloc[0])
                    wrt = int(df["writerTxn"].iloc[0])
                    lag = seq - wrt
                    if lag > threshold:
                        pause_event.set()
                        if not last_logged_paused:
                            print(f"[WAL] Pause: sequencerTxn={seq}, writerTxn={wrt}, lag={lag}")
                            last_logged_paused = True
                    elif last_logged_paused:
                        if seq == wrt:
                            time.sleep(interval)
                            print(f"[WAL] Resume: sequencerTxn={seq}, writerTxn={wrt}, lag={lag}")
                            pause_event.clear()
                            last_logged_paused = False
            except Exception as e:
                print(f"[WAL] Monitor error: {e}", flush=True)
            time.sleep(interval)


def wait_if_paused(pause_event: Event, pid: int):
    while pause_event.is_set():
        print(f"[WORKER {pid}] Paused due to WAL lag...")
        time.sleep(5)


# ----------------------------
# Worker loop
# ----------------------------

def ingest_worker(
    args,
    per_second_plan,
    start_ns: int,
    end_ns: Optional[int],
    symbols: list,
    brackets: dict,
    global_states,
    process_idx: int,
    processes: int,
    pause_event: Event,
    global_sec_offset,
):
    # Build per-class ladders
    ladders = {}
    for cls, (v_min, v_max) in LADDER_PROFILES.items():
        ladders[cls] = make_ladder(args.max_levels, v_min, v_max)

    local_brackets = dict(brackets)

    # Initialize per-symbol state
    if global_states is None:
        init_state = build_initial_state(symbols, local_brackets)
        open_per_second = None
        close_per_second = None
    else:
        init_state = None
        open_per_second, close_per_second = global_states

    power_state = init_power_state()

    if args.mode == "real-time":
        base_flush = 200
    else:
        base_flush = 10000

    buffer_limit = base_flush
    auto_flush_interval = base_flush * 2

    # Each worker gets its own QWP handle, sender_id and store-and-forward slot,
    # so un-acked frames replay per worker rather than colliding on one spool.
    # auto_flush_interval keeps the previous ILP cadence: SortedEmitter flushes
    # when a buffer fills, and the interval bounds latency when it does not, so
    # a quiet real-time symbol still reaches the dashboard promptly.
    sender_id = f"commodities-{process_idx}"
    with connect_qwp(args, sender_id=sender_id,
                     auto_flush_interval=auto_flush_interval) as db, \
            db.sender() as sender:
        emitter = SortedEmitter(sender, buffer_limit, args.prefix)
        ts = start_ns
        sec_idx = 0
        wall_start = time.time() if args.mode == "real-time" else None
        last_yf_refresh = time.time()
        last_settlement_day = None

        while True:
            if end_ns is not None and ts >= end_ns:
                emitter.flush_all()
                break

            # Real-time: refresh brackets periodically
            if args.mode == "real-time" and args.yahoo_refresh_secs > 0:
                now = time.time()
                if now - last_yf_refresh >= args.yahoo_refresh_secs:
                    try:
                        local_brackets = fetch_all_brackets(args.eia_api_key, pct=1.0)
                        last_yf_refresh = now
                        print(
                            f"[INFO] Refreshed commodity brackets (worker {process_idx}).",
                            flush=True,
                        )
                    except Exception as e:
                        print(
                            f"[WARN] Failed to refresh brackets: {e}",
                            flush=True,
                        )

            if args.mode == "faster-than-life" and per_second_plan is not None:
                if sec_idx >= len(per_second_plan):
                    emitter.flush_all()
                    break
                md_events, tr_events = per_second_plan[sec_idx]
            else:
                md_events = random.randint(args.market_data_min_eps, args.market_data_max_eps)
                tr_events = random.randint(args.trades_min_eps, args.trades_max_eps)

            # Apply scale_factor
            md_events = int(md_events * args.scale_factor)
            tr_events = int(tr_events * args.scale_factor)

            # Settlement check (once per simulated day, regardless of pacing)
            tzinfo = ZoneInfo(args.session_tz)
            dt_local = datetime.datetime.fromtimestamp(ts / 1e9, tzinfo)
            today_str = dt_local.strftime("%Y-%m-%d")
            is_settlement = False
            if last_settlement_day != today_str:
                is_settlement = True
                last_settlement_day = today_str

            # Session pacing
            if not args.session_pacing:
                allow_trades = True
                md_scale = 1.0
                tr_scale = 1.0
            else:
                phase = commodity_session_phase(ts, args.session_tz)

                allow_trades = True
                md_scale = 1.0
                tr_scale = 1.0

                if phase == "active":
                    md_scale = 1.0
                    tr_scale = 1.0
                elif phase == "settlement":
                    md_scale = 1.3
                    tr_scale = 1.3
                elif phase == "maintenance":
                    allow_trades = False
                    md_scale = 0.0
                    tr_scale = 0.0
                elif phase == "off":
                    allow_trades = False
                    md_scale = 0.0
                    tr_scale = 0.0

                # Off-session trade override
                if not allow_trades:
                    if args.offsession_trades == "full":
                        allow_trades = True
                        md_scale = 1.0
                        tr_scale = 1.0
                    elif args.offsession_trades == "trickle":
                        allow_trades = True
                        md_scale = 0.1
                        tr_scale = 0.1

            md_events = int(max(0, md_events * md_scale))
            tr_events = int(max(0, tr_events * tr_scale))

            if md_events == 0 and tr_events == 0 and not is_settlement:
                ts += int(1e9)
                sec_idx += 1
                if args.mode == "real-time":
                    target = wall_start + sec_idx
                    sleep_for = target - time.time()
                    if sleep_for > 0:
                        time.sleep(sleep_for)
                continue

            wait_if_paused(pause_event, process_idx)

            # Choose open/close state
            if (
                args.mode == "faster-than-life"
                and per_second_plan is not None
                and open_per_second is not None
                and close_per_second is not None
            ):
                if sec_idx >= len(open_per_second):
                    emitter.flush_all()
                    break
                open_state = open_per_second[sec_idx]
                close_state = close_per_second[sec_idx]
            else:
                open_state, close_state, init_state = evolve_open_close_for_second(
                    symbols, local_brackets, init_state, power_state
                )

            generate_second(
                ts_ns_base=ts,
                symbols=symbols,
                open_state=open_state,
                close_state=close_state,
                emitter=emitter,
                ladders=ladders,
                min_levels=args.min_levels,
                max_levels=args.max_levels,
                md_events=md_events,
                tr_events=tr_events,
                allow_trades=allow_trades,
                is_settlement=is_settlement,
                scale_factor=args.scale_factor,
            )

            ts += int(1e9)
            sec_idx += 1
            if args.mode == "real-time":
                target = wall_start + sec_idx
                sleep_for = target - time.time()
                if sleep_for > 0:
                    time.sleep(sleep_for)

        # Drain before the lease closes. Leaving the `with` block only publishes
        # to the store-and-forward queue without waiting, so a clean exit would
        # otherwise drop whatever the background runner had not yet delivered.
        try:
            emitter.flush_all()
            sender.flush(wait=True)
        except QuestDBError as e:
            print(f"[WORKER {process_idx}] Drain failed: {e}", flush=True)


# ----------------------------
# Main
# ----------------------------

def main():
    p = argparse.ArgumentParser(
        description="Commodities synthetic data generator for QuestDB"
    )
    # QWP carries both SQL and ingestion, so there is one endpoint and one
    # credential. --host accepts a comma-separated list for HA failover; list
    # the writable primary first.
    p.add_argument("--host", default="127.0.0.1",
                   help="QWP host, or comma-separated list for failover. "
                        "Entries without a port default to 9000.")
    p.add_argument("--user", default="admin")
    p.add_argument("--password", default="quest")
    p.add_argument("--token", default=None,
                   help="QWP bearer token. Takes precedence over --user/--password.")
    p.add_argument("--token_file", default=None,
                   help="Read the bearer token from this file, keeping it off the "
                        "command line.")
    p.add_argument("--qwp_tls", type=lambda x: str(x).lower() == "true", default=False,
                   help="Use wss instead of ws.")
    p.add_argument("--tls_ca", default=None,
                   help="TLS root store: os_roots, webpki_roots, or a CA bundle path.")
    p.add_argument("--tls_verify", choices=["on", "unsafe_off"], default="on",
                   help="Set unsafe_off for a cluster with a self-signed certificate. "
                        "No tls_ca setting can validate one, since it chains to no "
                        "trusted root.")
    p.add_argument("--durable_ack", type=lambda x: str(x).lower() == "true", default=False,
                   help="request_durable_ack=on, so a failover cannot lose acked rows.")
    p.add_argument("--store_forward_dir",
                   default=os.path.join(tempfile.gettempdir(), "commodities_qwp_sf"),
                   help="Base dir for per-worker store-and-forward spill; each worker "
                        "gets a <dir>/commodities-<idx> subdir, created if absent.")
    p.add_argument("--enterprise", type=lambda x: str(x).lower() == "true", default=False,
                   help="Enterprise server: tables take a STORAGE POLICY rather than a "
                        "TTL. Materialized views take a TTL on both editions.")

    p.add_argument("--mode", choices=["real-time", "faster-than-life"], required=True)

    p.add_argument("--market_data_min_eps", type=int, default=15)
    p.add_argument("--market_data_max_eps", type=int, default=40)
    p.add_argument("--trades_min_eps", type=int, default=5)
    p.add_argument("--trades_max_eps", type=int, default=20)
    p.add_argument("--scale_factor", type=int, default=1,
                   help="Multiplier for all event rates (e.g. 100 for throughput demos)")

    p.add_argument("--total_market_data_events", type=int, default=1_000_000)
    p.add_argument("--start_ts", type=str)
    p.add_argument("--end_ts", type=str)
    p.add_argument("--processes", type=int, default=1)

    p.add_argument("--min_levels", type=int, default=20)
    p.add_argument("--max_levels", type=int, default=20)

    p.add_argument("--incremental", type=lambda x: str(x).lower() != "false", default=False)
    p.add_argument("--create_views", type=lambda x: str(x).lower() != "false", default=True)
    p.add_argument("--short_ttl", type=lambda x: str(x).lower() == "true", default=False)
    p.add_argument("--prefix", type=str, default="")
    p.add_argument("--yahoo_refresh_secs", type=int, default=300)

    p.add_argument("--session_pacing", type=lambda x: str(x).lower() != "false", default=True)
    p.add_argument("--offsession_trades", choices=["none", "trickle", "full"], default="none")
    p.add_argument("--session_tz", type=str, default="America/Chicago")
    p.add_argument("--chunk_seconds", type=int, default=900,
                   help="Max seconds to precompute at once in faster-than-life mode")
    p.add_argument("--eia_api_key", type=str, default=None,
                   help="Optional EIA API key for higher rate limits")

    args = p.parse_args()
    prefix = args.prefix

    if args.min_levels > args.max_levels:
        print("ERROR: min_levels cannot be greater than max_levels.")
        sys.exit(1)

    if args.token_file:
        try:
            with open(args.token_file, "r", encoding="utf-8") as fh:
                args.token = fh.read().strip()
        except OSError as e:
            print(f"ERROR: could not read --token_file {args.token_file}: {e}")
            sys.exit(1)
        if not args.token:
            print(f"ERROR: --token_file {args.token_file} is empty.")
            sys.exit(1)

    # Ensure base tables and views
    ensure_tables_and_views(args, prefix)

    # Determine window
    if args.mode == "real-time":
        if args.start_ts:
            print("ERROR: --start_ts is not allowed in real-time mode.")
            sys.exit(1)
        start_ns = now_ns()
        end_ns = parse_ts_arg(args.end_ts) if args.end_ts else None
    else:
        start_ns = parse_ts_arg(args.start_ts) if args.start_ts else now_ns()
        end_ns = parse_ts_arg(args.end_ts) if args.end_ts else None

    # Advance start_ns past latest in tables to avoid overlap
    with connect_qwp(args) as conn:
        latest_md = get_latest_timestamp_ns(conn, table_name("commodities_market_data", prefix))
        latest_tr = get_latest_timestamp_ns(conn, table_name("commodities_trades", prefix))
        latest_st = get_latest_timestamp_ns(conn, table_name("commodities_settlements", prefix))
    max_latest = max([x for x in [latest_md, latest_tr, latest_st] if x is not None], default=None)
    if max_latest is not None:
        next_ns = max_latest + 1_000
        if next_ns > start_ns:
            print(f"[INFO] Advancing start_ns from {ns_to_iso(start_ns)} to {ns_to_iso(next_ns)} to avoid overlap.")
            start_ns = next_ns

    if end_ns is not None and start_ns >= end_ns:
        print("[INFO] No work to do. start_ts >= end_ts.")
        sys.exit(0)

    # Seed brackets
    symbols = SYMBOLS
    brackets = fetch_all_brackets(args.eia_api_key, pct=1.0)

    # WAL monitor
    pause_event = Event()
    wal_proc = mp.Process(
        target=wal_monitor, args=(args, pause_event, args.processes),
        kwargs={"prefix": prefix}
    )
    wal_proc.start()

    if args.mode == "faster-than-life":
        max_seconds_from_window = None
        if end_ns is not None:
            max_seconds_from_window = (end_ns - start_ns) // 1_000_000_000
            if max_seconds_from_window <= 0:
                print("[INFO] No work to do: time window is zero or negative.")
                sys.exit(0)

        # Build full per_second_plan
        full_per_second_plan = []
        md_total = 0

        while md_total < args.total_market_data_events:
            if max_seconds_from_window is not None and len(full_per_second_plan) >= max_seconds_from_window:
                print(f"[INFO] Reached end_ts limit at {len(full_per_second_plan)} seconds ({md_total} events). "
                      f"Requested {args.total_market_data_events} events but time window only allows {md_total}.")
                break
            md_this = random.randint(args.market_data_min_eps, args.market_data_max_eps)
            tr_this = random.randint(args.trades_min_eps, args.trades_max_eps)
            # Apply scale_factor to the plan
            md_this_scaled = md_this * args.scale_factor
            tr_this_scaled = tr_this * args.scale_factor
            full_per_second_plan.append((md_this, tr_this))
            md_total += md_this_scaled

        # Trim excess from final second
        over = md_total - args.total_market_data_events
        if over > 0 and (max_seconds_from_window is None or len(full_per_second_plan) < max_seconds_from_window):
            last_md, last_tr = full_per_second_plan[-1]
            trimmed = max(0, last_md - (over // max(1, args.scale_factor)))
            full_per_second_plan[-1] = (trimmed, last_tr)

        total_seconds = len(full_per_second_plan)
        if total_seconds == 0:
            print("[INFO] No work to do: zero seconds in plan.")
            sys.exit(0)

        chunk_size = args.chunk_seconds
        num_chunks = (total_seconds + chunk_size - 1) // chunk_size
        print(f"[INFO] Processing {total_seconds} seconds in {num_chunks} chunk(s) of up to {chunk_size} seconds each.")

        carry_forward_state = build_initial_state(symbols, brackets)
        chunk_start_ns = start_ns

        for chunk_idx in range(num_chunks):
            chunk_start_sec = chunk_idx * chunk_size
            chunk_end_sec = min((chunk_idx + 1) * chunk_size, total_seconds)
            chunk_seconds = chunk_end_sec - chunk_start_sec

            chunk_plan = full_per_second_plan[chunk_start_sec:chunk_end_sec]
            chunk_events = sum(md * args.scale_factor for md, _ in chunk_plan)

            print(f"[CHUNK {chunk_idx + 1}/{num_chunks}] Seconds {chunk_start_sec}-{chunk_end_sec - 1}, "
                  f"{chunk_events} market_data events, starting at {ns_to_iso(chunk_start_ns)}")

            open_per_second, close_per_second = precompute_open_close_state(
                chunk_seconds, symbols, brackets, carry_forward_state
            )
            global_states = (open_per_second, close_per_second)

            worker_plans = [[] for _ in range(args.processes)]
            for md_sec, tr_sec in chunk_plan:
                md_splits = split_event_counts(md_sec, args.processes)
                tr_splits = split_event_counts(tr_sec, args.processes)
                for i in range(args.processes):
                    worker_plans[i].append((md_splits[i], tr_splits[i]))

            global_sec_offsets = []
            acc = 0
            for plan in worker_plans:
                global_sec_offsets.append(acc)
                acc += len(plan)

            chunk_end_ns = chunk_start_ns + chunk_seconds * 1_000_000_000
            effective_end_ns = min(chunk_end_ns, end_ns) if end_ns else chunk_end_ns

            procs = []
            for i in range(args.processes):
                w = mp.Process(
                    target=ingest_worker,
                    args=(
                        args,
                        worker_plans[i],
                        chunk_start_ns,
                        effective_end_ns,
                        symbols,
                        brackets,
                        global_states,
                        i,
                        args.processes,
                        pause_event,
                        global_sec_offsets[i],
                    ),
                )
                w.start()
                procs.append(w)

            for w in procs:
                w.join()

            carry_forward_state = close_per_second[-1]
            chunk_start_ns = chunk_end_ns
            print(f"[CHUNK {chunk_idx + 1}/{num_chunks}] Completed.")

    else:
        # Real-time single process
        w = mp.Process(
            target=ingest_worker,
            args=(
                args,
                None,
                start_ns,
                end_ns,
                symbols,
                brackets,
                None,
                0,
                1,
                pause_event,
                0,
            ),
        )
        w.start()
        w.join()

    wal_proc.terminate()
    wal_proc.join()
    print("[INFO] Completed.")


if __name__ == "__main__":
    main()
