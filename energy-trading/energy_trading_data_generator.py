#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# energy_trading_data_generator.py
#
# Synthetic energy trading desk data for QuestDB: oil, gas, LNG, power and carbon
# forward curves anchored to live front-month prices, the desk's own fills and
# booking log on top, and a planted storyline for the seven-act demo in
# energy_demo_queries.sql.
#
# Transport: QWP (QuestDB Wire Protocol) over WebSocket for both SQL and writes,
# through a single questdb.connect() handle. Requires questdb>=5.0.0 and a
# QWP-capable server (QuestDB 10.x). No PGWire, no ILP/HTTP.
#
# The schema (ensure_tables_and_views) is the single source of truth: every
# table is WAL with a designated timestamp, deduplicated on its natural key,
# and carries per-column Parquet encodings so cold partitions stay compact.
# Nothing is ever updated in place: corrections are new rows with version + 1.
#
# Modes
#   faster-than-life  backfill a window with several workers, each owning a
#                     contiguous run of hour-aligned slices
#   real-time         single process, 250 ms slices, 2 s ahead of wall clock;
#                     started with --incremental after a backfill it first
#                     catches up the gap, then paces itself

import argparse
import datetime
import json
import math
import os
import pickle
import re
import sys
import tempfile
import time
import uuid
import multiprocessing as mp
from multiprocessing import Event
from typing import Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yfinance as yf
import questdb
from questdb import QuestDBError

UTC = datetime.timezone.utc
NS = 1_000_000_000
MINUTE_NS = 60 * NS
YEAR_S = 365.0 * 86400.0
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
EPOCH = pd.Timestamp("1970-01-01T00:00:00")

# Fallback anchors, checked by hand on 2026-10-07 (Yahoo Finance daily closes of
# that day; EUA and UKA from market reports because no reliable Yahoo ticker
# exists for either). Used only when the live fetch fails; the generator logs
# every fallback it takes. Prices are quoted units: $/bbl, EUR/MWh, $/MMBtu,
# $/gal, EUR/t, GBP/t.
FALLBACK_BRACKETS = {
    "as_of": "2026-10-07",
    "BRENT": 101.94,
    "WTI": 89.95,
    "HO": 4.7348,        # NY Harbor ULSD, $/gal; x42 = $/bbl, x7.45 = $/t
    "TTF": 77.11,
    "JKM": 25.88,
    "EUA": 84.20,
    "UKA": 61.30,
    "EURUSD": 1.1189,
    "GBPUSD": 1.3211,
}

YAHOO_TICKERS = {
    "BRENT": "BZ=F", "WTI": "CL=F", "HO": "HO=F", "TTF": "TTF=F", "NG": "NG=F",
    "JKM": "JKM=F", "EURUSD": "EURUSD=X", "GBPUSD": "GBPUSD=X",
}

# Static centres for the spread processes, used when a live pair of anchors is
# not available to derive them (section 6.2 of the brief).
STATIC_SPREADS = {
    "wti_spread": 3.60,    # Brent - WTI, $/bbl
    "crack": 22.0,         # gasoil crack over Brent, $/bbl
    "nbp_basis": 0.5,      # p/therm over TTF converted
    "jkm_premium": 2.0,    # $/MMBtu over TTF converted
    "uka_discount": 0.15,  # UKA below EUA x EURGBP
    "css": 4.0,            # clean spark margin, GBP/MWh
}

THERM_MWH = 0.0293071       # 1 therm in MWh
MMBTU_PER_MWH = 3.412
THERM_TO_GBP_MWH = 0.341214 # p/therm -> GBP/MWh
BBL_PER_T = 7.45
GAL_PER_BBL = 42.0
CCGT_EFF = 0.50
GAS_EF = 0.2                # tCO2 per MWh of gas burned

# ----------------------------
# Universe
# ----------------------------

# curve: complex, unit, ccy, px_factor, fx_symbol, to_mwh, primary exchange, tz,
#        n_months, strips, tick, front_tick_rate (quotes/s at full activity, rank
#        0), base_spread_ticks (front), max_spread_ticks, display precision.
#
# An instrument (one row here per contract) is the risk object: one fair value,
# one position for risk and PnL. Where it trades is a listing (LISTINGS below).
CURVES = {
    "BRENT":  dict(complex="OIL",    unit="bbl",   ccy="USD", px_factor=1.0,  fx=None,     to_mwh=None,
                   exchange="ICE", tz="UTC", n_months=24, strips=False, tick=0.01,
                   front_rate=180.0, spread_ticks=1.0, max_spread_ticks=12.0, precision=2),
    "WTI":    dict(complex="OIL",    unit="bbl",   ccy="USD", px_factor=1.0,  fx=None,     to_mwh=None,
                   exchange="CME", tz="UTC", n_months=24, strips=False, tick=0.01,
                   front_rate=150.0, spread_ticks=1.0, max_spread_ticks=12.0, precision=2),
    "GASOIL": dict(complex="OIL",    unit="t",     ccy="USD", px_factor=1.0,  fx=None,     to_mwh=None,
                   exchange="ICE", tz="UTC", n_months=18, strips=False, tick=0.25,
                   front_rate=40.0, spread_ticks=1.0, max_spread_ticks=12.0, precision=2),
    "TTF":    dict(complex="GAS",    unit="MWh",   ccy="EUR", px_factor=1.0,  fx="EURUSD", to_mwh=1.0,
                   exchange="ICE_ENDEX", tz="Europe/Amsterdam", n_months=38, strips=True, tick=0.005,
                   front_rate=120.0, spread_ticks=3.0, max_spread_ticks=12.0, precision=3),
    "NBP":    dict(complex="GAS",    unit="therm", ccy="GBP", px_factor=0.01, fx="GBPUSD", to_mwh=THERM_MWH,
                   exchange="ICE", tz="Europe/London", n_months=38, strips=True, tick=0.005,
                   front_rate=50.0, spread_ticks=3.0, max_spread_ticks=12.0, precision=3),
    # JKM ticks in tenths of a cent; the screen spread is a few cents wide.
    "JKM":    dict(complex="LNG",    unit="MMBtu", ccy="USD", px_factor=1.0,  fx=None,     to_mwh=1.0 / MMBTU_PER_MWH,
                   exchange="ICE", tz="UTC", n_months=12, strips=False, tick=0.001,
                   front_rate=10.0, spread_ticks=20.0, max_spread_ticks=60.0, precision=3),
    "UKPWR":  dict(complex="POWER",  unit="MWh",   ccy="GBP", px_factor=1.0,  fx="GBPUSD", to_mwh=1.0,
                   exchange="ICE", tz="Europe/London", n_months=38, strips=True, tick=0.01,
                   front_rate=30.0, spread_ticks=3.0, max_spread_ticks=12.0, precision=2),
    "EUA":    dict(complex="CARBON", unit="tCO2",  ccy="EUR", px_factor=1.0,  fx="EURUSD", to_mwh=None,
                   exchange="ICE_ENDEX", tz="UTC", n_months=0, strips=False, tick=0.01,
                   front_rate=60.0, spread_ticks=1.0, max_spread_ticks=12.0, precision=2),
    "UKA":    dict(complex="CARBON", unit="tCO2",  ccy="GBP", px_factor=1.0,  fx="GBPUSD", to_mwh=None,
                   exchange="ICE", tz="UTC", n_months=0, strips=False, tick=0.01,
                   front_rate=10.0, spread_ticks=2.0, max_spread_ticks=12.0, precision=2),
}
CARBON_YEARS = (2026, 2027, 2028)
FX_PAIRS = ["EURUSD", "GBPUSD", "EURGBP"]
FX_TICK_RATE = 8.0

# Where each curve trades, primary venue first. Codes are the exchanges' own,
# checked on 2026-10-08:
# - ICE, from ICE's product code list (ice.com/api/productguide/info/codes/all/csv):
#   the contract symbol is built from the LOGICAL code; the one-letter PHYSICAL
#   code is kept separately. Brent BRN / B, Low Sulphur Gasoil ULS / G, UK NBP
#   gas GWM / M, JKM LNG (Platts) JKM, UK Base Electricity (Gregorian) UBL, UKA
#   futures UKA (ICE Futures Europe, MIC IFEU); Dutch TTF gas TFM and EUA
#   futures ECF / C (ICE Endex, MIC NDEX).
# - EEX, from EEX's product short-code list: TTF Natural Gas Month / Quarter /
#   Season / Year Futures G3BM / G3BQ / G3BS / G3BY, EUA Future FEUA, GB Power
#   Base Month / Quarter / Season / Year Futures FUBM / FUBQ / FUBS / FUBY
#   (MIC XEEE, cleared at ECC). Lot and tick match the ICE contracts: 1 MW x
#   hours and EUR 0.005 for TTF, 1 MW x hours and GBP 0.01 for GB power,
#   1,000 allowances and EUR 0.01 for EUA.
# - CME NYMEX, WTI CL (MIC XNYM).
# EEX product codes are per granularity, hence the dict.
VENUES = {
    "ICE":       dict(mic="IFEU", ccp="ICE_CLEAR_EU"),
    "ICE_ENDEX": dict(mic="NDEX", ccp="ICE_CLEAR_EU"),
    "EEX":       dict(mic="XEEE", ccp="ECC"),
    "CME":       dict(mic="XNYM", ccp="CME_CLEARING"),
}
LISTINGS = {
    "BRENT":  [("ICE", "BRN", "B")],
    "WTI":    [("CME", "CL", "CL")],
    "GASOIL": [("ICE", "ULS", "G")],
    "TTF":    [("ICE_ENDEX", "TFM", "TFM"),
               ("EEX", dict(M="G3BM", Q="G3BQ", S="G3BS", Y="G3BY"), None)],
    "NBP":    [("ICE", "GWM", "M")],
    "JKM":    [("ICE", "JKM", "JKM")],
    "UKPWR":  [("ICE", "UBL", "UBL"),
               ("EEX", dict(M="FUBM", Q="FUBQ", S="FUBS", Y="FUBY"), None)],
    "EUA":    [("ICE_ENDEX", "ECF", "C"),
               ("EEX", dict(Z="FEUA"), None)],
    "UKA":    [("ICE", "UKA", "UKA")],
}
# Expected share of the desk's screen fills on the secondary venue: higher on
# strips, which is where a second venue picks up business.
SECONDARY_SHARE = {"M": 0.2, "Z": 0.2, "Q": 0.3, "S": 0.3, "Y": 0.3}
# The secondary venue's book relative to the primary's.
SECONDARY_RATE = 0.4          # tick rate
SECONDARY_SIZE = 0.5          # displayed size
SECONDARY_SPREAD = (1.5, 2.0) # spread multiplier range, drawn per listing
SECONDARY_LAG_S = (0.05, 0.3) # quote lag behind fair value, drawn per tick
SECONDARY_BASIS_TICKS = 0.3   # amplitude of the slow cross-venue basis
# Planted cross-venue divergences on dual-listed front months, European hours.
DIVERGENCE_PER_HOUR = 1.0 / 3.0
DIVERGENCE_TICKS = (2, 4)
DIVERGENCE_SECS = (5, 30)
# Fill routing: weight multipliers on a venue where the book holds the opposite
# side (closing there saves margin) and on the venue with the better touch.
CLOSING_BONUS = 2.0
TOUCH_BONUS = 1.5
CCP = {v: d["ccp"] for v, d in VENUES.items()}
OTC_VENUE = "OTC"             # venue of positions that come from broker and bilateral deals
# The planted feed outage (8.9) silences the EEX quote feed; ICE keeps ticking.
OUTAGE_EXCHANGE = "EEX"

# Futures month codes, industry-wide.
MONTH_CODE = {1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
              7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"}
TERM_CODE = {"M": "M", "Z": "M", "Q": "Q", "S": "S", "Y": "Y"}


def exchange_symbol(exchange: str, code: str, gran: str, ys: int, ms: int, ye: int, me: int) -> str:
    """A listing's contract name in its exchange's own format.

    ICE (Futures Europe and Endex): logical product code left-justified to four
    characters, F for futures, the term letter, the month code, 00 for the whole
    period, the two-digit year, and for seasons a '.' switch followed by the
    last delivery month (ICE's own example: GWM FSV0007.H0008 is the winter
    2007 NBP strip). CME: root, month code, two-digit year (CLF27). EEX
    identifies a contract by product code plus expiry year and month (separate
    fields in its contract details file); they are joined here for display,
    with the first delivery month (G3BM 2027-01).
    """
    mc, yy = MONTH_CODE[ms], ys % 100
    if exchange == "CME":
        return f"{code}{mc}{yy:02d}"
    if exchange == "EEX":
        return f"{code} {ys:04d}-{ms:02d}"
    sym = f"{code:<4}F{TERM_CODE[gran]}{mc}00{yy:02d}"
    if gran == "S":
        ly, lm = madd(ye, me, -1)
        sym += f".{MONTH_CODE[lm]}00{ly % 100:02d}"
    return sym


def build_listings(inst: pd.DataFrame, seed: int) -> pd.DataFrame:
    """One row per (instrument, venue). Primary venue first; the secondary
    venue's spread multiplier and basis phases are drawn once per listing."""
    r = np.random.default_rng([seed, 17])
    rows = []
    for i, x in enumerate(inst.itertuples()):
        venues = LISTINGS[x.curve]
        dual = len(venues) > 1
        ys, ms = x.delivery_start.year, x.delivery_start.month
        ye, me = x.delivery_end.year, x.delivery_end.month
        for k, (exch, code, phys) in enumerate(venues):
            c = code[x.granularity] if isinstance(code, dict) else code
            primary = k == 0
            share = 1.0 if not dual else (1.0 - SECONDARY_SHARE[x.granularity] if primary
                                          else SECONDARY_SHARE[x.granularity])
            rows.append(dict(
                ci=i, symbol=x.symbol, curve=x.curve, granularity=x.granularity, rank=x.rank,
                exchange=exch, mic=VENUES[exch]["mic"], exchange_code=c, exchange_physical_code=phys,
                exchange_symbol=exchange_symbol(exch, c, x.granularity, ys, ms, ye, me),
                ccp=VENUES[exch]["ccp"], lot_size=x.lot_size, tick_size=x.tick_size,
                is_primary=primary, liquidity_share=share,
                rate_mult=1.0 if primary else SECONDARY_RATE,
                size_mult=1.0 if primary else SECONDARY_SIZE,
                spread_mult=1.0 if primary else float(r.uniform(*SECONDARY_SPREAD)),
                basis_amp=0.0 if primary else SECONDARY_BASIS_TICKS * x.tick_size,
                basis_ph1=float(r.uniform(0, 2 * math.pi)), basis_ph2=float(r.uniform(0, 2 * math.pi)),
                basis_p1=float(r.uniform(2, 4)) * 3600.0, basis_p2=float(r.uniform(20, 40)) * 60.0,
                divergent=(not primary) and x.granularity in ("M", "Z") and int(x.rank) == 0,
            ))
    df = pd.DataFrame(rows)
    # Bars (quotes_1m and up) are built from primary-venue quotes by exchange, so
    # no exchange may be primary for one curve and secondary for another.
    prim = set(df[df.is_primary].exchange)
    sec = set(df[~df.is_primary].exchange)
    assert not (prim & sec), f"exchanges both primary and secondary: {prim & sec}"
    return df


# Seasonal shape by delivery month (index 0 = Jan). Oil and carbon are flat.
GAS_SEASONAL = np.array([1.18, 1.14, 1.05, 0.96, 0.92, 0.90, 0.90, 0.92, 0.96, 1.04, 1.10, 1.16])
POWER_SEASONAL = np.array([1.25, 1.18, 1.06, 0.95, 0.90, 0.88, 0.88, 0.90, 0.95, 1.05, 1.14, 1.22])
ASIA_SEASONAL = np.array([0.6, 0.5, 0.2, 0.0, 0.0, 0.0, 0.1, 0.1, 0.0, 0.1, 0.3, 0.6])   # $/MMBtu
DIESEL_WINTER = np.array([2.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.0, 2.0])  # $/bbl
WINTER_MONTHS = {11, 12, 1, 2}

# Two-factor model: L daily vol, S diffusion vol (annualised) and the Samuelson
# damping kappa (12-month contract carries about half the front month's vol).
# Targets: front realised vol roughly 32% Brent, 60% TTF, 65% UK power, 35% carbon.
FACTOR_PARAMS = {
    "BRENT": dict(l_daily_vol=0.008, s_vol=0.34, kappa=1.8, shape=-0.02),
    "TTF":   dict(l_daily_vol=0.015, s_vol=0.63, kappa=1.8, shape=0.0),
    "EUA":   dict(l_daily_vol=0.012, s_vol=0.32, kappa=1.8, shape=0.04),
    "PWR":   dict(l_daily_vol=0.0,   s_vol=0.40, kappa=1.8, shape=0.0),   # extra power factor
}
S_HALF_LIFE_H = 2.0
ANCHOR_PULL_HALF_LIFE_H = 2.0

# Mean-reverting spread processes: (static centre key, stationary stdev, half-life in days)
SPREAD_PARAMS = {
    "wti_spread":  ("wti_spread", 0.5, 1.0),
    "crack":       ("crack", 3.0, 2.0),
    "nbp_basis":   ("nbp_basis", 0.3, 1.0),
    "jkm_premium": ("jkm_premium", 0.3, 2.0),
    "uka_discount": ("uka_discount", 0.02, 3.0),
    "css":         ("css", 2.0, 1.0),
}
FX_VOL = {"EURUSD": 0.07, "GBPUSD": 0.08}

# Vol surface parameters per curve: front ATM premium over realised, long-run ATM,
# risk reversal (25C - 25P) and butterfly, all in vol units.
IV_PARAMS = {
    "BRENT": dict(front=0.35, long=0.24, k=3.0, rr=-0.02, bf=0.010),
    "TTF":   dict(front=0.63, long=0.38, k=3.0, rr=0.025, bf=0.010),
    "UKPWR": dict(front=0.68, long=0.42, k=3.0, rr=0.030, bf=0.012),
}
DELTA_BUCKETS = ["10P", "25P", "ATM", "25C", "10C"]

# ----------------------------
# The desk
# ----------------------------

# book: traders, fills per hour at full activity, universe [(curve, granularity, max rank)],
#       foresight probability, probability of crossing the spread
BOOKS = {
    "CRUDE":    dict(traders=["trader_01", "trader_02", "trader_03", "trader_04"], rate=120,
                     universe=[("BRENT", "M", 8), ("WTI", "M", 6)], foresight=0.6, aggressive=0.6),
    "PRODUCTS": dict(traders=["trader_05", "trader_06", "trader_07", "trader_08"], rate=60,
                     universe=[("GASOIL", "M", 6)], foresight=0.0, aggressive=1.0),
    "EU_GAS":   dict(traders=["trader_09", "trader_10", "trader_11", "trader_12", "trader_13"], rate=90,
                     universe=[("TTF", "M", 8), ("TTF", "Q", 4), ("TTF", "S", 2), ("NBP", "M", 6), ("NBP", "Q", 2)],
                     foresight=0.0, aggressive=0.6),
    "LNG":      dict(traders=["trader_14", "trader_15", "trader_16"], rate=20,
                     universe=[("JKM", "M", 6), ("TTF", "M", 6)], foresight=0.0, aggressive=0.6),
    "UK_POWER": dict(traders=["trader_17", "trader_18", "trader_19", "trader_20"], rate=60,
                     universe=[("UKPWR", "M", 6), ("UKPWR", "Q", 3), ("UKPWR", "S", 2), ("NBP", "M", 4), ("UKA", "Z", 2)],
                     foresight=0.0, aggressive=0.6),
    "CARBON":   dict(traders=["trader_21", "trader_22", "trader_23", "trader_24"], rate=30,
                     universe=[("EUA", "Z", 3), ("UKA", "Z", 2)], foresight=0.0, aggressive=0.6),
}
OPS_USERS = [f"ops_{i:02d}" for i in range(1, 7)]

LIMITS = {
    ("CRUDE", "BRENT"): 2_000_000, ("CRUDE", "WTI"): 1_000_000,
    ("PRODUCTS", "GASOIL"): 150_000,
    ("LNG", "JKM"): 3_000_000, ("LNG", "TTF"): 1_500_000,
    ("EU_GAS", "TTF"): 6_000_000, ("EU_GAS", "NBP"): 60_000_000,
    ("UK_POWER", "UKPWR"): 2_000_000, ("UK_POWER", "NBP"): 20_000_000, ("UK_POWER", "UKA"): 500_000,
    ("CARBON", "EUA"): 2_000_000, ("CARBON", "UKA"): 1_000_000,
}

# Counterparty pool for broker and bilateral deals, with an informedness score
# that drives the markout-by-counterparty story: banks and trading houses are
# mildly informed, utilities and industrials uninformed, producers neutral.
COUNTERPARTIES = (
    [(f"BANK_{i:02d}", 0.3) for i in range(1, 9)]
    + [(f"UTILITY_{i:02d}", -0.3) for i in range(1, 11)]
    + [(f"TRADER_{i:02d}", 0.3) for i in range(1, 9)]
    + [(f"PRODUCER_{i:02d}", 0.0) for i in range(1, 7)]
    + [(f"INDUSTRIAL_{i:02d}", -0.3) for i in range(1, 9)]
)

ALL_TABLES = ["instruments", "listings", "limits", "quotes", "curve_marks", "settlements", "iv_marks",
              "model_prices", "da_prices", "fills", "trade_events", "position_snapshots",
              "demo_events"]


# ----------------------------
# Calendar helpers
# ----------------------------

def madd(y: int, m: int, k: int):
    z = m - 1 + k
    return y + z // 12, z % 12 + 1


def prev_bday(d: datetime.date) -> datetime.date:
    while d.weekday() >= 5:
        d -= datetime.timedelta(days=1)
    return d


def bdays_before(d: datetime.date, n: int) -> datetime.date:
    while n > 0:
        d -= datetime.timedelta(days=1)
        if d.weekday() < 5:
            n -= 1
    return d


def last_bday_of_month(y: int, m: int) -> datetime.date:
    ny, nm = madd(y, m, 1)
    return prev_bday(datetime.date(ny, nm, 1) - datetime.timedelta(days=1))


def last_monday_of_month(y: int, m: int) -> datetime.date:
    ny, nm = madd(y, m, 1)
    d = datetime.date(ny, nm, 1) - datetime.timedelta(days=1)
    while d.weekday() != 0:
        d -= datetime.timedelta(days=1)
    return d


def local_midnight_utc(y: int, m: int, tz: str) -> datetime.datetime:
    """00:00 local on the first of the month, as a UTC instant."""
    return datetime.datetime(y, m, 1, tzinfo=ZoneInfo(tz)).astimezone(UTC)


def delivery_hours(y1, m1, y2, m2, tz) -> int:
    a = local_midnight_utc(y1, m1, tz)
    b = local_midnight_utc(y2, m2, tz)
    return int(round((b - a).total_seconds() / 3600))


def expiry_for(curve: str, y: int, m: int) -> datetime.datetime:
    """Last trading day per the exchange's rule (weekends only, no holiday
    calendar), stamped 16:30 UTC.

    Brent: last business day of the second month before delivery.
    WTI: 3 business days before the 25th of the month before delivery (4 if
    the 25th is not a business day). Gasoil: 2 business days before the 14th
    of the delivery month, so the front month trades into its own month. JKM:
    the 15th of the month before delivery, or the business day before it.
    Gas and power months and strips: 2 business days before delivery.
    Carbon December contracts: the last Monday of December, or the penultimate
    one if that Monday is a UK bank holiday or one falls in the 4 days after
    it; for December that is always the case (Christmas, Boxing Day or New
    Year's Day is always within reach), so it is always the penultimate Monday.
    """
    cx = CURVES[curve]["complex"]
    if curve == "BRENT":
        d = last_bday_of_month(*madd(y, m, -2))
        if d.month == 12 and d.day >= 24:
            # The business day before Christmas or New Year's Day rolls back one.
            d = prev_bday(d - datetime.timedelta(days=1))
    elif curve == "WTI":
        py, pm = madd(y, m, -1)
        d25 = datetime.date(py, pm, 25)
        d = bdays_before(d25, 3 if d25.weekday() < 5 else 4)
    elif curve == "GASOIL":
        d = bdays_before(datetime.date(y, m, 14), 2)
    elif curve == "JKM":
        py, pm = madd(y, m, -1)
        d = prev_bday(datetime.date(py, pm, 15))
    elif cx == "CARBON":
        d = last_monday_of_month(y, 12) - datetime.timedelta(days=7)
    else:
        d = bdays_before(datetime.date(y, m, 1), 2)
    return datetime.datetime(d.year, d.month, d.day, 16, 30, tzinfo=UTC)


def build_instruments(day0: datetime.datetime) -> pd.DataFrame:
    """One row per tradable contract as of day0 (UTC).

    Months first, then strips over those months, so every quarter, season and
    calendar year is decomposable into listed months and the strip consistency
    check holds by construction. For gas and power the month list runs far
    enough to cover the last listed cal (3 cals, 8 quarters, 4 seasons).
    """
    rows = []

    def add(curve, gran, label, ys, ms, ye, me, expiry, months=None):
        c = CURVES[curve]
        cx = c["complex"]
        tz = c["tz"]
        # Delivery periods are stored as calendar-month boundaries at 00:00 UTC so
        # month() and year() on them are the delivery month; the hours are counted
        # on the local calendar (DST-aware) for gas and power.
        start = datetime.datetime(ys, ms, 1, tzinfo=UTC)
        end = datetime.datetime(ye, me, 1, tzinfo=UTC)
        hours = delivery_hours(ys, ms, ye, me, tz) if cx in ("GAS", "POWER") else 0
        days = (datetime.date(ye, me, 1) - datetime.date(ys, ms, 1)).days
        if curve in ("TTF", "UKPWR"):
            lot = float(hours)
        elif curve == "NBP":
            lot = 1000.0 * days
        elif curve in ("BRENT", "WTI"):
            lot = 1000.0
        elif curve == "GASOIL":
            lot = 100.0
        elif curve == "JKM":
            lot = 10_000.0
        else:
            lot = 1000.0
        rows.append(dict(
            symbol=f"{curve}_{label}", curve=curve, complex=cx, granularity=gran,
            delivery_start=start.replace(tzinfo=None), delivery_end=end.replace(tzinfo=None),
            hours=hours, days=days, expiry=expiry.replace(tzinfo=None), exchange=c["exchange"],
            term_code=TERM_CODE[gran], month_code=MONTH_CODE[ms],
            unit=c["unit"], ccy=c["ccy"], px_factor=c["px_factor"], fx_symbol=c["fx"],
            to_mwh=c["to_mwh"], lot_size=lot, tick_size=c["tick"], moy=ms, months=months or [],
        ))

    for curve, c in CURVES.items():
        if c["complex"] == "CARBON":
            for y in CARBON_YEARS:
                ex = expiry_for(curve, y, 12)
                if ex > day0:
                    add(curve, "Z", f"Dec-{y % 100:02d}", y, 12, y + 1, 1, ex)
            continue
        month_keys = []
        y, m = day0.year, day0.month
        while len(month_keys) < c["n_months"]:
            ex = expiry_for(curve, y, m)
            if ex > day0:
                ny, nm = madd(y, m, 1)
                add(curve, "M", f"{MONTHS[m - 1]}-{y % 100:02d}", y, m, ny, nm, ex)
                month_keys.append((y, m))
            y, m = madd(y, m, 1)
        if not c["strips"]:
            continue
        mset = set(month_keys)
        counts = {"Q": 0, "S": 0, "Y": 0}
        caps = {"Q": 8, "S": 4, "Y": 3}

        def strip(gran, label, ys, ms, n):
            keys = [madd(ys, ms, i) for i in range(n)]
            if all(k in mset for k in keys) and counts[gran] < caps[gran]:
                ye, me = madd(ys, ms, n)
                add(curve, gran, label, ys, ms, ye, me, expiry_for(curve, ys, ms),
                    months=[f"{curve}_{MONTHS[km - 1]}-{ky % 100:02d}" for ky, km in keys])
                counts[gran] += 1

        for yy in range(day0.year, day0.year + 5):
            for qn in range(4):
                strip("Q", f"Q{qn + 1}-{yy % 100:02d}", yy, 3 * qn + 1, 3)
            strip("S", f"Sum-{yy % 100:02d}", yy, 4, 6)
            strip("S", f"Win-{yy % 100:02d}", yy, 10, 6)
            strip("Y", f"Cal-{yy % 100:02d}", yy, 1, 12)

    df = pd.DataFrame(rows)
    df["rank"] = df.sort_values("delivery_start").groupby(["curve", "granularity"]).cumcount()
    # Liquidity: tick rate per second at full activity, by rank and granularity.
    rate = np.zeros(len(df))
    spread = np.zeros(len(df))
    for i, r in enumerate(df.itertuples()):
        c = CURVES[r.curve]
        k = int(r.rank)
        if r.granularity in ("M", "Z"):
            if k < 3:
                rate[i] = c["front_rate"] * (0.55 ** k)
            elif k < 12:
                rate[i] = 1.5 * (0.8 ** (k - 3))
            else:
                rate[i] = 0.05
            spread[i] = c["spread_ticks"] + (0.4 * k if k < 12 else 4.0 + 0.3 * (k - 12))
        else:
            rate[i] = {"Q": 0.15, "S": 0.10, "Y": 0.08}[r.granularity] * (0.8 ** k)
            spread[i] = c["spread_ticks"] + 3.0 + 0.8 * k
    df["tick_rate"] = rate
    df["spread_ticks"] = np.minimum(spread, [CURVES[c]["max_spread_ticks"] for c in df.curve])
    return df.reset_index(drop=True)


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
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return int(dt.timestamp() * 1e9)


def ns_to_iso(ns: int) -> str:
    dt = datetime.datetime.fromtimestamp(ns / 1e9, UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def ns_to_dt(ns: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ns / 1e9, UTC)


def ts_col_ns(ns_array) -> pd.Series:
    return pd.Series(pd.to_datetime(np.asarray(ns_array, dtype=np.int64), unit="ns"))


def quantize(x, tick: float, precision: int):
    return np.round(np.round(np.asarray(x, dtype=float) / tick) * tick, precision)


def quantize_bbo(mid, spread, tick, precision):
    """Bid and ask from mid and spread, always at least a tick apart."""
    bid = quantize(mid - spread / 2.0, tick, precision)
    ask = quantize(mid + spread / 2.0, tick, precision)
    ask = np.where(ask <= bid, np.round(bid + tick, precision), ask)
    return bid, ask


def table_name(name: str, prefix: str) -> str:
    return f"{prefix}{name}" if prefix else name


def tod_weight(sec_epoch, cx: str = "GAS"):
    """Activity by UTC hour and weekday. European hours full rate, the US oil
    session at 40% (70% for oil), nights and weekends at 10% but never zero."""
    t = np.asarray(sec_epoch, dtype=np.int64)
    hour = (t % 86400) // 3600
    dow = ((t // 86400) + 3) % 7
    us = 0.7 if cx == "OIL" else 0.4
    w = np.where((hour >= 7) & (hour < 17), 1.0, np.where((hour >= 17) & (hour < 22), us, 0.1))
    return np.where(dow >= 5, 0.1, w)


def bool_arg(x) -> bool:
    return str(x).lower() not in ("false", "0", "no", "off")


# ----------------------------
# QWP connection
# ----------------------------

# Retention differs by edition and by object kind: tables take a STORAGE POLICY
# on Enterprise and a TTL on OSS, materialized views take a TTL on both (storage
# policies are rejected on views). Nothing is emitted unless --short_ttl is set,
# because the demo data sits in the past and any threshold would fire at once.
# Reference tables (instruments, limits) carry the epoch as their timestamp and
# never take a retention clause for the same reason.
ENTERPRISE_POLICY = "TO REMOTE 1 hour, TO PARQUET 2 days, DROP LOCAL 3 months"


def table_retention(short_ttl: bool, enterprise: bool, oss_ttl: str, policy: str) -> str:
    if not short_ttl:
        return ""
    if enterprise:
        return f" STORAGE POLICY({policy})"
    return f" TTL {oss_ttl}"


def view_retention(short_ttl: bool, oss_ttl: str) -> str:
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


def sender_tag(args, role: str) -> str:
    """Sender id for a producer: carries the target host and the table prefix so
    two generators on one box (say a local test and a cluster load) never
    contend for the same store-and-forward slot."""
    host = re.sub(r"[^A-Za-z0-9]+", "_", qwp_addr_list(args.host).split(",")[0])
    return f"energy_{args.prefix.strip('_') or 'x'}_{host}_{role}"


def query_df(conn, sql: str, binds=None) -> pd.DataFrame:
    with conn.query(sql, binds) as result:
        return result.to_pandas()


# ----------------------------
# DB setup
# ----------------------------

def _parquet_clauses(enabled: bool):
    """Per-column Parquet encodings: delta_binary_packed for timestamps,
    rle_dictionary for symbols (bloom_filter on the high-cardinality ones),
    default for everything else, zstd(4) throughout."""
    if not enabled:
        return "", "", "", ""
    return (" PARQUET(delta_binary_packed, zstd(4))",
            " PARQUET(rle_dictionary, zstd(4), bloom_filter)",
            " PARQUET(rle_dictionary, zstd(4))",
            " PARQUET(default, zstd(4))")


def table_ddl(args, prefix: str) -> dict:
    """The schema. Names are bare here and table_name() applies --prefix.

    Retention is appended after PARTITION BY and before WAL (the only placement
    the parser accepts for TTL). Reference tables never take retention: their
    timestamp is the epoch and any threshold would drop them at once.
    """
    TS, SYM, SYML, DEF = _parquet_clauses(args.parquet_encodings)
    ret_h = table_retention(args.short_ttl, args.enterprise, "3 DAYS", ENTERPRISE_POLICY)
    ret_d = table_retention(args.short_ttl, args.enterprise, "1 MONTH", ENTERPRISE_POLICY)
    ret_m = table_retention(args.short_ttl, args.enterprise, "3 MONTHS", ENTERPRISE_POLICY)
    t = lambda n: table_name(n, prefix)
    ddl = {}

    # One row per instrument: the risk object (one fair value, one position
    # for risk and PnL), identified by the desk's readable symbol. Where it
    # trades lives in listings. Reference data: ts is the epoch and the last
    # column, so SELECT * does not lead with 1970; DEDUP turns a re-run into
    # an upsert. term_code and month_code describe the delivery period.
    ddl["instruments"] = f"""
    CREATE TABLE IF NOT EXISTS {t("instruments")} (
      symbol         SYMBOL CAPACITY 2048{SYM},
      curve          SYMBOL CAPACITY 32{SYML},
      complex        SYMBOL CAPACITY 8{SYML},
      granularity    SYMBOL CAPACITY 8{SYML},
      delivery_start TIMESTAMP{TS},
      delivery_end   TIMESTAMP{TS},
      hours          INT{DEF},
      days           INT{DEF},
      expiry         TIMESTAMP{TS},
      term_code      SYMBOL CAPACITY 8{SYML},
      month_code     SYMBOL CAPACITY 16{SYML},
      unit           SYMBOL CAPACITY 8{SYML},
      ccy            SYMBOL CAPACITY 4{SYML},
      px_factor      DOUBLE{DEF},
      fx_symbol      SYMBOL CAPACITY 4{SYML},
      to_mwh         DOUBLE{DEF},
      ts             TIMESTAMP{TS}
    ) TIMESTAMP(ts) PARTITION BY YEAR WAL
      DEDUP UPSERT KEYS(ts, symbol)"""

    # One row per listing: an instrument on a venue. exchange_symbol is the
    # contract's name on that exchange, exchange_code the (logical) product
    # code it is built from, exchange_physical_code ICE's one-letter clearing
    # code where it differs. Fills reference a listing through (symbol, venue),
    # quotes through (symbol, source). liquidity_share is the expected share
    # of the desk's screen fills per venue and sums to 1 per symbol.
    ddl["listings"] = f"""
    CREATE TABLE IF NOT EXISTS {t("listings")} (
      symbol                 SYMBOL CAPACITY 2048{SYM},
      exchange               SYMBOL CAPACITY 8{SYML},
      mic                    SYMBOL CAPACITY 8{SYML},
      exchange_code          SYMBOL CAPACITY 32{SYML},
      exchange_physical_code SYMBOL CAPACITY 32{SYML},
      exchange_symbol        VARCHAR{DEF},
      ccp                    SYMBOL CAPACITY 8{SYML},
      lot_size               DOUBLE{DEF},
      tick_size              DOUBLE{DEF},
      is_primary             BOOLEAN{DEF},
      liquidity_share        DOUBLE{DEF},
      ts                     TIMESTAMP{TS}
    ) TIMESTAMP(ts) PARTITION BY YEAR WAL
      DEDUP UPSERT KEYS(ts, symbol, exchange)"""

    # Position limits per book and curve, in delivery units. A limit change is
    # a new row with a later effective ts; the old one stays for audit.
    ddl["limits"] = f"""
    CREATE TABLE IF NOT EXISTS {t("limits")} (
      book        SYMBOL CAPACITY 16{SYML},
      curve       SYMBOL CAPACITY 32{SYML},
      max_abs_qty DOUBLE{DEF},
      unit        SYMBOL CAPACITY 8{SYML},
      approved_by SYMBOL CAPACITY 16{SYML},
      ts          TIMESTAMP{TS}
    ) TIMESTAMP(ts) PARTITION BY YEAR WAL
      DEDUP UPSERT KEYS(ts, book, curve)"""

    # Top of book for every quoted contract plus FX. The high-rate table.
    ddl["quotes"] = f"""
    CREATE TABLE IF NOT EXISTS {t("quotes")} (
      ts       TIMESTAMP_NS{TS},
      symbol   SYMBOL CAPACITY 2048{SYM},
      curve    SYMBOL CAPACITY 32{SYML},
      bid      DOUBLE{DEF},
      ask      DOUBLE{DEF},
      bid_size DOUBLE{DEF},
      ask_size DOUBLE{DEF},
      source   SYMBOL CAPACITY 16{SYML}
    ) TIMESTAMP(ts) PARTITION BY HOUR{ret_h} WAL
      DEDUP UPSERT KEYS(ts, symbol, source)"""

    # Curve builder output: every contract on every curve, once a minute.
    # Corrections are new rows with version + 1 and the same ts. venue is the
    # listing a MARKET mark came from (null for INTERP), for lineage.
    ddl["curve_marks"] = f"""
    CREATE TABLE IF NOT EXISTS {t("curve_marks")} (
      ts        TIMESTAMP{TS},
      curve     SYMBOL CAPACITY 32{SYML},
      symbol    SYMBOL CAPACITY 2048{SYM},
      price     DOUBLE{DEF},
      source    SYMBOL CAPACITY 16{SYML},
      venue     SYMBOL CAPACITY 8{SYML},
      version   INT{DEF},
      marked_by SYMBOL CAPACITY 16{SYML}
    ) TIMESTAMP(ts) PARTITION BY DAY{ret_d} WAL
      DEDUP UPSERT KEYS(ts, symbol, version)"""

    # Official daily settlements, published after the close.
    ddl["settlements"] = f"""
    CREATE TABLE IF NOT EXISTS {t("settlements")} (
      ts            TIMESTAMP{TS},
      curve         SYMBOL CAPACITY 32{SYML},
      symbol        SYMBOL CAPACITY 2048{SYM},
      price         DOUBLE{DEF},
      volume        DOUBLE{DEF},
      open_interest DOUBLE{DEF},
      source        SYMBOL CAPACITY 16{SYML}
    ) TIMESTAMP(ts) PARTITION BY MONTH{ret_m} WAL
      DEDUP UPSERT KEYS(ts, symbol)"""

    # Implied vol surface: one row per contract and delta bucket, every 5 minutes.
    ddl["iv_marks"] = f"""
    CREATE TABLE IF NOT EXISTS {t("iv_marks")} (
      ts           TIMESTAMP{TS},
      curve        SYMBOL CAPACITY 32{SYML},
      symbol       SYMBOL CAPACITY 2048{SYM},
      delta_bucket SYMBOL CAPACITY 8{SYML},
      tau          DOUBLE{DEF},
      iv           DOUBLE{DEF},
      source       SYMBOL CAPACITY 16{SYML},
      version      INT{DEF}
    ) TIMESTAMP(ts) PARTITION BY DAY{ret_d} WAL
      DEDUP UPSERT KEYS(ts, symbol, delta_bucket, version)"""

    # Fair-value model output, champion and challenger side by side, every minute.
    ddl["model_prices"] = f"""
    CREATE TABLE IF NOT EXISTS {t("model_prices")} (
      ts            TIMESTAMP{TS},
      model_version SYMBOL CAPACITY 16{SYML},
      curve         SYMBOL CAPACITY 32{SYML},
      symbol        SYMBOL CAPACITY 2048{SYM},
      model_px      DOUBLE{DEF},
      inputs_ts     TIMESTAMP{TS}
    ) TIMESTAMP(ts) PARTITION BY DAY{ret_d} WAL
      DEDUP UPSERT KEYS(ts, model_version, symbol)"""

    # Day-ahead hourly auction prices. These go negative.
    ddl["da_prices"] = f"""
    CREATE TABLE IF NOT EXISTS {t("da_prices")} (
      ts           TIMESTAMP{TS},
      market       SYMBOL CAPACITY 8{SYML},
      price        DOUBLE{DEF},
      volume       DOUBLE{DEF},
      source       SYMBOL CAPACITY 16{SYML},
      published_ts TIMESTAMP{TS}
    ) TIMESTAMP(ts) PARTITION BY MONTH{ret_m} WAL
      DEDUP UPSERT KEYS(ts, market)"""

    # Exchange executions as the gateway reports them. Voice and bilateral
    # deals never land here; they exist only in trade_events.
    ddl["fills"] = f"""
    CREATE TABLE IF NOT EXISTS {t("fills")} (
      ts       TIMESTAMP_NS{TS},
      book     SYMBOL CAPACITY 16{SYML},
      trader   SYMBOL CAPACITY 64{SYML},
      symbol   SYMBOL CAPACITY 2048{SYM},
      curve    SYMBOL CAPACITY 32{SYML},
      qty      DOUBLE{DEF},
      px       DOUBLE{DEF},
      venue    SYMBOL CAPACITY 16{SYML},
      order_id UUID{DEF},
      trade_id UUID{DEF},
      passive  BOOLEAN{DEF}
    ) TIMESTAMP(ts) PARTITION BY DAY{ret_d} WAL
      DEDUP UPSERT KEYS(ts, trade_id)"""

    # Trade capture log, bitemporal. Booking time is the designated timestamp;
    # every booking, amendment and cancel is a new row with version + 1.
    ddl["trade_events"] = f"""
    CREATE TABLE IF NOT EXISTS {t("trade_events")} (
      booked_ts    TIMESTAMP{TS},
      trade_id     UUID{DEF},
      version      INT{DEF},
      status       SYMBOL CAPACITY 4{SYML},
      trade_ts     TIMESTAMP{TS},
      book         SYMBOL CAPACITY 16{SYML},
      trader       SYMBOL CAPACITY 64{SYML},
      symbol       SYMBOL CAPACITY 2048{SYM},
      curve        SYMBOL CAPACITY 32{SYML},
      qty          DOUBLE{DEF},
      px           DOUBLE{DEF},
      channel      SYMBOL CAPACITY 4{SYML},
      counterparty SYMBOL CAPACITY 256{SYML},
      booked_by    SYMBOL CAPACITY 64{SYML},
      reason       SYMBOL CAPACITY 16{SYML}
    ) TIMESTAMP(booked_ts) PARTITION BY DAY{ret_d} WAL
      DEDUP UPSERT KEYS(booked_ts, trade_id, version)"""

    # End-of-day positions per book, contract and venue at 00:00 UTC, at
    # settlement. Risk sums over venue; margin groups by it. Positions from
    # broker and bilateral deals carry the venue OTC.
    ddl["position_snapshots"] = f"""
    CREATE TABLE IF NOT EXISTS {t("position_snapshots")} (
      ts        TIMESTAMP{TS},
      book      SYMBOL CAPACITY 16{SYML},
      symbol    SYMBOL CAPACITY 2048{SYM},
      curve     SYMBOL CAPACITY 32{SYML},
      venue     SYMBOL CAPACITY 8{SYML},
      qty       DOUBLE{DEF},
      settle_px DOUBLE{DEF},
      source    SYMBOL CAPACITY 16{SYML}
    ) TIMESTAMP(ts) PARTITION BY MONTH{ret_m} WAL
      DEDUP UPSERT KEYS(ts, book, symbol, venue)"""

    # What the generator planted and when: the presenter's cheat sheet.
    ddl["demo_events"] = f"""
    CREATE TABLE IF NOT EXISTS {t("demo_events")} (
      ts   TIMESTAMP{TS},
      act  SYMBOL CAPACITY 8{SYML},
      what VARCHAR{DEF}
    ) TIMESTAMP(ts) PARTITION BY YEAR WAL
      DEDUP UPSERT KEYS(ts, act)"""
    return ddl


def expected_columns(ddl: str):
    """(column, type) pairs parsed from a CREATE TABLE statement, so the live
    schema can be compared against the one this generator expects."""
    body = ddl.split("(", 1)[1]
    out = []
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        m = re.match(r"^(\w+)\s+(TIMESTAMP_NS|TIMESTAMP|SYMBOL|DOUBLE|INT|UUID|BOOLEAN|VARCHAR)\b", line)
        if m:
            out.append((m.group(1), m.group(2)))
        if line.startswith(")"):
            break
    return out


def check_table_schema(conn, full_name: str, ddl: str):
    live = query_df(conn, f"SELECT \"column\", \"type\" FROM table_columns('{full_name}')")
    got = [(str(r.column), str(r.type)) for r in live.itertuples()]
    want = expected_columns(ddl)
    if got != want:
        raise SystemExit(
            f"ERROR: table {full_name} exists with a different schema.\n"
            f"  expected: {want}\n  found:    {got}\n"
            f"Drop it or use a different --prefix.")


def ensure_tables_and_views(args, prefix: str):
    """Create every table, materialized view, live view and plain view that the
    run needs, idempotently. A table that already exists with a different
    schema is a hard error, never silently reused."""
    ddl = table_ddl(args, prefix)
    wanted = [n for n in ALL_TABLES if n in args.table_set]
    t = lambda n: table_name(n, prefix)
    ttl_h = view_retention(args.short_ttl, "3 DAYS")
    ttl_d = view_retention(args.short_ttl, "1 MONTH")
    ttl_m = view_retention(args.short_ttl, "3 MONTHS")
    with connect_qwp(args) as conn:
        for name in wanted:
            sql = ddl[name]
            try:
                conn.execute(sql)
            except QuestDBError as e:
                if args.short_ttl and args.enterprise and "STORAGE POLICY" in sql:
                    # Fallback for a parser that wants the policy as a separate
                    # statement: create bare, then ALTER.
                    print(f"[DDL] {t(name)}: inline storage policy rejected ({e}); "
                          f"creating without it and applying ALTER TABLE SET STORAGE POLICY", flush=True)
                    conn.execute(sql.replace(f" STORAGE POLICY({ENTERPRISE_POLICY})", ""))
                    conn.execute(f"ALTER TABLE {t(name)} SET STORAGE POLICY({ENTERPRISE_POLICY})")
                else:
                    raise
            check_table_schema(conn, t(name), sql)
        print(f"[DDL] Tables ready: {', '.join(t(n) for n in wanted)}", flush=True)

        has = lambda *names: all(n in args.table_set for n in names)

        if args.create_views and has("quotes"):
            # 1-minute bars on mid: realised vol, the PnL curve, the charts.
            # Built from the primary venue's quotes (and FX), so a bar is one
            # book, not a mix of two venues' spreads; the secondary venue is
            # read from ticks (5f, 7d). No exchange is primary for one curve and
            # secondary for another, which build_listings asserts.
            primaries = ", ".join(f"'{e}'" for e in sorted({v[0][0] for v in LISTINGS.values()} | {"FX_FEED"}))
            # 10-second bars, same venues: what a live dashboard samples on its own
            # interval (10 s and up), so a panel that ticks reads a few thousand
            # bars instead of millions of quotes.
            conn.execute(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {t("quotes_10s")} AS (
              SELECT ts, symbol, curve,
                     last(mid(bid, ask)) AS close,
                     last(bid)           AS last_bid,
                     last(ask)           AS last_ask,
                     count()             AS ticks
              FROM {t("quotes")}
              WHERE source IN ({primaries})
              SAMPLE BY 10s
            ) PARTITION BY DAY{ttl_h}""")
            conn.execute(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {t("quotes_1m")} AS (
              SELECT ts, symbol, curve,
                     first(mid(bid, ask)) AS open,
                     max(mid(bid, ask))   AS high,
                     min(mid(bid, ask))   AS low,
                     last(mid(bid, ask))  AS close,
                     last(bid)            AS last_bid,
                     last(ask)            AS last_ask,
                     count()              AS ticks
              FROM {t("quotes")}
              WHERE source IN ({primaries})
              SAMPLE BY 1m
            ) PARTITION BY DAY{ttl_d}""")
            # 5-minute bars, the standard grid for realised variance.
            conn.execute(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {t("quotes_5m")} AS (
              SELECT ts, symbol, curve,
                     first(open) AS open,
                     max(high)   AS high,
                     min(low)    AS low,
                     last(close) AS close,
                     sum(ticks)  AS ticks
              FROM {t("quotes_1m")}
              SAMPLE BY 5m
            ) PARTITION BY DAY{ttl_d}""")
            # Daily bars, timed refresh.
            conn.execute(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {t("quotes_1d")}
            REFRESH EVERY 1h DEFERRED START '2026-01-01T00:00:00.000000Z' AS (
              SELECT ts, symbol, curve,
                     first(open) AS open,
                     max(high)   AS high,
                     min(low)    AS low,
                     last(close) AS close,
                     sum(ticks)  AS ticks
              FROM {t("quotes_5m")}
              SAMPLE BY 1d
            ) PARTITION BY MONTH{ttl_m}""")
            print(f"[DDL] Materialized views ready: {t('quotes_10s')}, {t('quotes_1m')}, {t('quotes_5m')}, "
                  f"{t('quotes_1d')}", flush=True)
        if args.create_views and has("curve_marks"):
            # Hourly curve history for curve-evolution charts.
            conn.execute(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {t("curve_marks_1h")} AS (
              SELECT ts, curve, symbol, last(price) AS price
              FROM {t("curve_marks")}
              SAMPLE BY 1h
            ) PARTITION BY MONTH{ttl_m}""")
            print(f"[DDL] Materialized view ready: {t('curve_marks_1h')}", flush=True)

        if args.create_live_view and has("fills"):
            # Running position and cash per book and contract from exchange
            # fills, reset daily. Beta in 10.0: log and carry on if rejected;
            # every query that reads it has a window-function twin over fills.
            try:
                conn.execute(f"""
                CREATE LIVE VIEW IF NOT EXISTS {t("positions_live")} FLUSH EVERY 1s START FROM BEGINNING AS
                SELECT ts, book, symbol, curve,
                       sum(qty)      OVER w AS pos,
                       sum(qty * px) OVER w AS cost
                FROM {t("fills")}
                WINDOW w AS (PARTITION BY book, symbol ORDER BY ts ANCHOR DAILY '00:00')""")
                print(f"[DDL] Live view ready: {t('positions_live')}", flush=True)
            except QuestDBError as e:
                print(f"[DDL] Live view {t('positions_live')} not created, server said: {e}. "
                      f"Use the window-function twins in the query pack.", flush=True)
            # The same per venue: margin is per clearing house, risk is not.
            try:
                conn.execute(f"""
                CREATE LIVE VIEW IF NOT EXISTS {t("positions_live_by_venue")} FLUSH EVERY 1s START FROM BEGINNING AS
                SELECT ts, book, symbol, curve, venue,
                       sum(qty)      OVER w AS pos,
                       sum(qty * px) OVER w AS cost
                FROM {t("fills")}
                WINDOW w AS (PARTITION BY book, symbol, venue ORDER BY ts ANCHOR DAILY '00:00')""")
                print(f"[DDL] Live view ready: {t('positions_live_by_venue')}", flush=True)
            except QuestDBError as e:
                print(f"[DDL] Live view {t('positions_live_by_venue')} not created, server said: {e}. "
                      f"Use the window-function twins in the query pack.", flush=True)

        if args.create_plain_views:
            # Plain views store nothing: each runs as an inlined subquery of the
            # query that references it. The *_asof views take @asof, the *_day
            # views take @day (the day's start; a 'YYYY-MM-DD' string works);
            # both are OVERRIDABLE, so a cell sets them with its own leading
            # DECLARE and every view it touches, nested ones included, sees that
            # value. Every view is bounded in time: a lookback before @asof, or
            # the day. Parameters are times only: instrument filters stay in the
            # calling query and are pushed down. CREATE OR REPLACE so a re-run
            # updates a definition in place. Nesting is at most two levels.
            created = []

            def view(name, body):
                conn.execute(f"CREATE OR REPLACE VIEW {t(name)} AS (\n{body}\n)")
                created.append(t(name))

            fx_pairs = ", ".join(f"'{p}'" for p in FX_PAIRS)
            if has("position_snapshots", "fills"):
                # The ledger for intraday PnL: opening position as a pseudo-fill
                # at settlement, plus today's exchange fills.
                view("ledger", f"""
                SELECT ts, book, symbol, curve, qty, settle_px AS px, 'sod' AS src
                FROM {t("position_snapshots")}
                UNION ALL
                SELECT ts, book, symbol, curve, qty, px, 'trade' AS src
                FROM {t("fills")}""")
            if has("curve_marks"):
                # The curve as it stood at @asof: latest mark per contract in the
                # day up to @asof (marks are published every minute). A correction
                # at the same ts wins because it was written later.
                view("marks_asof", f"""
                DECLARE OVERRIDABLE @asof := now()
                SELECT symbol, curve, price, version, source, venue, ts
                FROM {t("curve_marks")}
                WHERE ts <= @asof AND ts > dateadd('d', -1, @asof::timestamp)
                LATEST ON ts PARTITION BY symbol""")
                view("curve_marks_latest", f"SELECT * FROM {t('marks_asof')}")
            if has("curve_marks", "quotes"):
                # What a trader's screen shows: the latest mid on each instrument's
                # primary listing in the five minutes up to @asof (source QUOTE,
                # venue the exchange), falling back to the marks_asof row where
                # there is no fresh quote (illiquid tenor, outage). Same columns as
                # marks_asof plus age_s, the seconds between the price and @asof.
                # The official valuation stays on marks_asof: a controller signs
                # off the curve builder's marks, not the last tick.
                primary_sources = ", ".join(f"'{e}'" for e in sorted({v[0][0] for v in LISTINGS.values()}))
                view("marks_live", f"""
                DECLARE OVERRIDABLE @asof := now()
                WITH q AS (
                  SELECT symbol, mid(bid, ask) AS price, source, ts FROM {t("quotes")}
                  WHERE source IN ({primary_sources}) AND ts <= @asof AND ts > dateadd('m', -5, @asof::timestamp)
                  LATEST ON ts PARTITION BY symbol
                )
                SELECT m.symbol, m.curve,
                       CASE WHEN q.symbol IS NULL THEN m.price ELSE q.price END AS price,
                       CASE WHEN q.symbol IS NULL THEN m.version ELSE NULL END AS version,
                       CASE WHEN q.symbol IS NULL THEN m.source ELSE 'QUOTE' END AS source,
                       CASE WHEN q.symbol IS NULL THEN m.venue ELSE q.source END AS venue,
                       CASE WHEN q.symbol IS NULL THEN m.ts ELSE q.ts::timestamp END AS ts,
                       datediff('s', CASE WHEN q.symbol IS NULL THEN m.ts ELSE q.ts::timestamp END,
                                @asof::timestamp) AS age_s
                FROM {t("marks_asof")} m
                LEFT JOIN q ON q.symbol = m.symbol""")
            if has("quotes"):
                # Latest FX mid per pair in the hour up to @asof (FX ticks several
                # times a second). Naming the pairs lets LATEST ON stop at each
                # pair's last tick.
                view("fx_asof", f"""
                DECLARE OVERRIDABLE @asof := now()
                SELECT symbol, mid(bid, ask) AS usd, ts
                FROM {t("quotes")}
                WHERE symbol IN ({fx_pairs}) AND ts <= @asof AND ts > dateadd('h', -1, @asof::timestamp)
                LATEST ON ts PARTITION BY symbol""")
            if has("quotes", "instruments"):
                # What one unit of price is worth in USD at @asof: px_factor
                # (pence to pounds) times the FX rate of the contract's currency.
                view("usd_factor_asof", f"""
                DECLARE OVERRIDABLE @asof := now()
                SELECT i.symbol, i.px_factor * coalesce(x.usd, 1.0) AS factor
                FROM {t("instruments")} i
                LEFT JOIN {t("fx_asof")} x ON x.symbol = i.fx_symbol""")
            if has("trade_events"):
                # Deals done on the day of @asof (trade_ts from that day's 00:00),
                # as known at @asof versus as restated now. The two definitions
                # differ by one line, AND booked_ts <= @asof: that is the whole
                # reconstruction. Scope is the deal date, so every amendment and
                # cancel of those deals is caught whenever it was booked, and a
                # correction to an earlier day's deal is out of scope by
                # definition. The booked_ts >= 00:00 bound changes no result (a
                # booking never precedes its deal); it starts the scan at the
                # day's partitions. Status is filtered after LATEST ON (a WHERE at
                # the same level would run first and bring back a cancelled
                # trade's earlier NEW row).
                view("book_asof", f"""
                DECLARE OVERRIDABLE @asof := now()
                (SELECT * FROM {t("trade_events")}
                 WHERE booked_ts >= timestamp_floor('d', @asof::timestamp)
                   AND booked_ts <= @asof
                 LATEST ON booked_ts PARTITION BY trade_id)
                WHERE status != 'CANCELLED' AND trade_ts >= timestamp_floor('d', @asof::timestamp) AND trade_ts <= @asof""")
                view("book_restated", f"""
                DECLARE OVERRIDABLE @asof := now()
                (SELECT * FROM {t("trade_events")}
                 WHERE booked_ts >= timestamp_floor('d', @asof::timestamp)
                 LATEST ON booked_ts PARTITION BY trade_id)
                WHERE status != 'CANCELLED' AND trade_ts >= timestamp_floor('d', @asof::timestamp) AND trade_ts <= @asof""")
                view("trade_events_latest", f"SELECT * FROM {t('book_restated')}")
            if has("instruments", "listings"):
                # Relative tenor at @asof: position on the curve, counted per curve
                # and granularity over contracts unexpired at @asof (M1 is the
                # front month, Q1 the first listed quarter, Z1 the first carbon
                # December). tenor_n is the same position as a number, for
                # bucketing and for sorting M2 before M10. exchange_symbol is the
                # primary listing's, the code an instrument-level row is known by.
                # tenors is the same at the default, now().
                view("tenors_asof", f"""
                DECLARE OVERRIDABLE @asof := now()
                SELECT i.symbol, l.exchange AS primary_exchange, l.exchange_symbol,
                       i.curve, i.complex, i.granularity,
                       i.delivery_start, i.delivery_end, i.hours, i.days, i.expiry,
                       i.granularity || row_number() OVER (PARTITION BY i.curve, i.granularity ORDER BY i.delivery_start) AS tenor,
                       row_number() OVER (PARTITION BY i.curve, i.granularity ORDER BY i.delivery_start) AS tenor_n,
                       datediff('M', @asof, i.delivery_start) AS months_to_delivery
                FROM {t("instruments")} i
                JOIN {t("listings")} l ON l.symbol = i.symbol AND l.is_primary
                WHERE i.expiry > @asof""")
                view("tenors", f"SELECT * FROM {t('tenors_asof')}")
                # One row per listing with the instrument's delivery and tenor:
                # what the instrument master cell shows.
                view("instrument_master", f"""
                SELECT t.curve, t.tenor, t.tenor_n, l.symbol, l.exchange, l.mic, l.exchange_code,
                       l.exchange_physical_code, l.exchange_symbol, l.ccp, l.is_primary, l.liquidity_share,
                       l.lot_size, l.tick_size, i.unit, i.ccy, t.granularity, t.delivery_start, t.delivery_end, t.expiry
                FROM {t("listings")} l
                JOIN {t("instruments")} i ON i.symbol = l.symbol
                JOIN {t("tenors_asof")} t ON t.symbol = l.symbol""")
            if has("quotes"):
                # One day of 1-minute bars: each leg of a spread is this view with
                # a WHERE symbol = ... filter, which is pushed down to the bars.
                view("mid_1m_day", f"""
                DECLARE OVERRIDABLE @day := timestamp_floor('d', now())
                SELECT ts, symbol, curve, close, last_bid, last_ask, ticks
                FROM {t("quotes_1m")}
                WHERE ts >= @day::timestamp AND ts < dateadd('d', 1, @day::timestamp)""")
            if has("fills"):
                # Running position and cash per book and instrument over the day's
                # fills (what the positions_live live view maintains at ingestion),
                # plus the running position per venue.
                view("positions_running_day", f"""
                DECLARE OVERRIDABLE @day := timestamp_floor('d', now())
                SELECT ts, book, symbol, curve, venue, qty, px,
                       sum(qty)      OVER (PARTITION BY book, symbol ORDER BY ts)        AS pos,
                       sum(qty * px) OVER (PARTITION BY book, symbol ORDER BY ts)        AS cost,
                       sum(qty)      OVER (PARTITION BY book, symbol, venue ORDER BY ts) AS venue_pos
                FROM {t("fills")}
                WHERE ts >= @day::timestamp AND ts < dateadd('d', 1, @day::timestamp)""")
            if has("model_prices", "quotes", "instruments", "listings"):
                # Every model price of the day graded against the last quote of
                # the preceding 1-minute bar (the ASOF match with a one-minute
                # tolerance, at a fraction of the cost over a day of ticks). A
                # minute with no quote keeps its row with a null bid: counted,
                # not graded.
                view("model_graded_day", f"""
                DECLARE OVERRIDABLE @day := timestamp_floor('d', now())
                SELECT m.ts, m.model_version, m.symbol, m.curve, m.model_px,
                       b.last_bid AS bid, b.last_ask AS ask,
                       10000 * (m.model_px - mid(b.last_bid, b.last_ask)) / mid(b.last_bid, b.last_ask) AS err_bps,
                       CASE WHEN month(i.delivery_start) IN (11, 12, 1, 2) THEN 'winter' ELSE 'summer' END AS season,
                       i.delivery_start, i.granularity, t.tenor, t.tenor_n, t.exchange_symbol
                FROM (SELECT ts, dateadd('m', -1, ts) AS bar_ts, model_version, symbol, curve, model_px
                      FROM {t("model_prices")} WHERE ts >= @day::timestamp AND ts < dateadd('d', 1, @day::timestamp)) m
                JOIN {t("instruments")} i ON i.symbol = m.symbol
                LEFT JOIN (SELECT ts, symbol, last_bid, last_ask FROM {t("quotes_1m")}
                           WHERE ts >= dateadd('m', -1, @day::timestamp) AND ts < dateadd('d', 1, @day::timestamp)) b
                       ON b.symbol = m.symbol AND b.ts = m.bar_ts
                LEFT JOIN {t("tenors_asof")} t ON t.symbol = m.symbol""")
            if has("curve_marks", "instruments"):
                # Strip consistency per minute as the marks were published
                # (version 1): each quarter, season and cal against the days- or
                # hours-weighted average of its months (DST-aware hours).
                view("strip_gaps_day", f"""
                DECLARE OVERRIDABLE @day := timestamp_floor('d', now())
                WITH k AS (
                  SELECT k.ts, k.symbol, k.price, k.source, k.marked_by, c.curve, c.complex, c.granularity,
                         c.delivery_start, c.delivery_end, c.hours, c.days
                  FROM {t("curve_marks")} k
                  JOIN {t("instruments")} c ON (symbol)
                  WHERE k.curve IN ('TTF', 'NBP', 'UKPWR') AND k.version = 1
                    AND k.ts >= @day::timestamp AND k.ts < dateadd('d', 1, @day::timestamp)
                )
                SELECT s.ts, s.symbol, s.source, s.marked_by, s.price AS strip_mark,
                       sum(m.price * (CASE WHEN s.complex = 'GAS' THEN m.days ELSE m.hours END))
                         / sum(CASE WHEN s.complex = 'GAS' THEN m.days ELSE m.hours END) AS from_months,
                       s.price - sum(m.price * (CASE WHEN s.complex = 'GAS' THEN m.days ELSE m.hours END))
                         / sum(CASE WHEN s.complex = 'GAS' THEN m.days ELSE m.hours END) AS gap,
                       count() AS months
                FROM k s
                JOIN k m ON m.ts = s.ts AND m.curve = s.curve AND m.granularity = 'M'
                        AND m.delivery_start >= s.delivery_start AND m.delivery_end <= s.delivery_end
                WHERE s.granularity IN ('Q', 'S', 'Y')
                GROUP BY s.ts, s.symbol, s.source, s.marked_by, s.price""")
            if created:
                print(f"[DDL] Views ready: {', '.join(created)}", flush=True)


def get_latest_timestamp_ns(conn, table: str, ts_col: str = "ts"):
    """Latest designated timestamp in `table`, as epoch nanoseconds, or None.
    A missing table is not an error: on a fresh database there is nothing to
    advance past."""
    try:
        df = query_df(conn, f"SELECT {ts_col} FROM {table} ORDER BY {ts_col} DESC LIMIT 1")
    except QuestDBError as e:
        print(f"[INFO] Could not read latest timestamp from {table}: {e}", flush=True)
        return None
    if df.empty:
        return None
    ts = df[ts_col].iloc[0]
    if pd.isna(ts):
        return None
    return int(pd.Timestamp(ts).value)


# ----------------------------
# Anchors: live front-month prices
# ----------------------------

def fetch_yahoo_close(ticker: str):
    bars = yf.Ticker(ticker).history(period="5d", interval="1d")
    if bars.empty:
        raise ValueError("empty")
    closes = bars["Close"].dropna()
    if closes.empty:
        raise ValueError("no closes")
    v = float(closes.iloc[-1])
    if math.isnan(v) or v <= 0:
        raise ValueError("bad close")
    return v


def fetch_anchors(static_only: bool = False) -> dict:
    """Front-month anchors in quoted units for every curve, FX, and the centres
    of the spread processes derived from them. Falls back to FALLBACK_BRACKETS
    per item and logs each fallback; never raises on a data source."""
    raw = {}
    live = set()
    for key, ticker in YAHOO_TICKERS.items():
        if static_only:
            continue
        try:
            raw[key] = fetch_yahoo_close(ticker)
            live.add(key)
            print(f"[YF] {key}: {ticker} close={raw[key]:.4f}", flush=True)
        except Exception as e:
            print(f"[YF] {key}: {ticker} unavailable ({str(e)[:60]})", flush=True)
    for key in ("BRENT", "WTI", "HO", "TTF", "JKM", "EURUSD", "GBPUSD"):
        if key not in raw:
            if key == "TTF" and "NG" in raw:
                # Henry Hub times a Europe premium, converted to EUR/MWh.
                eurusd = raw.get("EURUSD", FALLBACK_BRACKETS["EURUSD"])
                derived = raw["NG"] * 2.75 * MMBTU_PER_MWH / eurusd
                print(f"[YF] TTF derived from NG=F x 2.75: {derived:.2f} EUR/MWh "
                      f"(static {FALLBACK_BRACKETS['TTF']:.2f}); using the static bracket, "
                      f"dated {FALLBACK_BRACKETS['as_of']}", flush=True)
            raw[key] = FALLBACK_BRACKETS[key]
            print(f"[YF] {key}: using static fallback {raw[key]:.4f} (as of {FALLBACK_BRACKETS['as_of']})",
                  flush=True)
    raw["EUA"] = FALLBACK_BRACKETS["EUA"]
    raw["UKA"] = FALLBACK_BRACKETS["UKA"]
    eurgbp = raw["EURUSD"] / raw["GBPUSD"]

    anchors = dict(BRENT=raw["BRENT"], WTI=raw["WTI"], TTF=raw["TTF"], JKM=raw["JKM"],
                   EUA=raw["EUA"], UKA=raw["UKA"], EURUSD=raw["EURUSD"], GBPUSD=raw["GBPUSD"],
                   EURGBP=eurgbp)
    spreads = dict(STATIC_SPREADS)
    # Derive the spread centres from the anchors (live, or the dated static
    # ones), so the synthetic market sits at today's relationships rather than
    # at textbook defaults. The static values above only cover a leg that has
    # neither.
    # The front gas month is next month; its seasonal adders (winter diesel,
    # Asia LNG) are netted out of the spread centres so the front months land
    # on the anchors, not the anchors plus the adder.
    front_moy = datetime.datetime.now(UTC).month % 12   # index of next month, 0 = Jan
    spreads["wti_spread"] = raw["BRENT"] - raw["WTI"]
    ho_bbl = raw["HO"] * GAL_PER_BBL
    if 2.0 < ho_bbl - raw["BRENT"] < 150.0:
        spreads["crack"] = ho_bbl - raw["BRENT"] - DIESEL_WINTER[front_moy]
    else:
        print(f"[YF] HO-implied crack {ho_bbl - raw['BRENT']:.1f} $/bbl is implausible; using {STATIC_SPREADS['crack']}",
              flush=True)
    ttf_usd_mmbtu = raw["TTF"] * raw["EURUSD"] / MMBTU_PER_MWH
    spreads["jkm_premium"] = (raw["JKM"] - ttf_usd_mmbtu - ASIA_SEASONAL[front_moy]) / math.exp(-0.7 * 0.07)
    spreads["uka_discount"] = 1.0 - raw["UKA"] / (raw["EUA"] * eurgbp)
    anchors["GASOIL"] = (raw["BRENT"] + spreads["crack"]) * BBL_PER_T
    ttf_pth = raw["TTF"] * THERM_MWH * eurgbp * 100.0
    anchors["NBP"] = ttf_pth + spreads["nbp_basis"]
    anchors["UKPWR"] = (anchors["NBP"] * THERM_TO_GBP_MWH / CCGT_EFF
                        + raw["UKA"] * GAS_EF / CCGT_EFF + spreads["css"])
    print("[INFO] Anchors: " + ", ".join(f"{k}={v:.3f}" for k, v in anchors.items()), flush=True)
    print("[INFO] Spread centres: " + ", ".join(f"{k}={v:.3f}" for k, v in spreads.items()), flush=True)
    return dict(anchors=anchors, spreads=spreads, live=sorted(live), fetched_at=time.time())


# ----------------------------
# Market model
# ----------------------------

DT_MIN = 1.0 / (365.0 * 1440.0)      # one minute in years
DT_SEC = 1.0 / YEAR_S

BASE_CURVES = ["BRENT", "TTF", "EUA"]
LOG_FACTORS = ["L_BRENT", "S_BRENT", "L_TTF", "S_TTF", "L_EUA", "S_EUA", "S_PWR", "FX_EURUSD", "FX_GBPUSD"]
BRIDGED = {"S_BRENT": FACTOR_PARAMS["BRENT"]["s_vol"], "S_TTF": FACTOR_PARAMS["TTF"]["s_vol"],
           "S_EUA": FACTOR_PARAMS["EUA"]["s_vol"], "S_PWR": FACTOR_PARAMS["PWR"]["s_vol"],
           "FX_EURUSD": FX_VOL["EURUSD"], "FX_GBPUSD": FX_VOL["GBPUSD"]}
IV_LEVEL = {"IV_BRENT": 0.04, "IV_TTF": 0.05, "IV_UKPWR": 0.05}
IV_SKEW = {"SKEW_BRENT": 0.004, "SKEW_TTF": 0.004, "SKEW_UKPWR": 0.004}
FACTOR_NAMES = (LOG_FACTORS + list(SPREAD_PARAMS) + list(IV_LEVEL) + list(IV_SKEW))
FIDX = {f: i for i, f in enumerate(FACTOR_NAMES)}


class StoryClock:
    """Where the planted events sit on the clock (UTC, demo day)."""

    def __init__(self, demo_day: datetime.date):
        self.demo_day = demo_day
        d = datetime.datetime(demo_day.year, demo_day.month, demo_day.day, tzinfo=UTC)
        at = lambda h, m=0: int((d + datetime.timedelta(hours=h, minutes=m)).timestamp())
        self.breach_start, self.breach_peak, self.breach_end = at(7, 40), at(8, 10), at(11, 30)
        self.bad_mark_start, self.bad_mark_end = at(9, 15), at(9, 45)
        self.fat_finger_fill, self.fat_finger_fix = at(10, 5), at(13, 20)
        self.duplicate_deal, self.duplicate_fix = at(10, 30), at(14, 5)
        self.late_deal, self.late_booking = at(9, 10), at(14, 30)
        self.repricing = at(11, 0)
        self.bad_iv_start, self.bad_iv_end = at(14, 10), at(14, 30)
        self.outage_start, self.outage_end = at(10, 0), at(10, 3)
        self.windy_day = demo_day - datetime.timedelta(days=2)
        self.day_start = at(0)


class MarketModel:
    """Two-factor forward curves anchored to live front months, with every
    derived curve built from its parent plus a mean-reverting spread.

    The factor path is simulated per minute (parent process); within the
    minute, the short factors and FX get a Brownian bridge whose randomness is
    keyed on (seed, minute, factor), so the parent and every worker compute the
    same intra-minute path without sharing it.
    """

    def __init__(self, instruments: pd.DataFrame, anchors: dict, seed: int, story: StoryClock,
                 winter_repricing: float = 0.08):
        self.inst = instruments
        self.seed = seed
        self.story = story
        self.winter_repricing = winter_repricing
        self.set_anchors(anchors)
        n = len(instruments)
        self.n = n
        self.sym = instruments.symbol.to_numpy()
        self.sym_idx = {s: i for i, s in enumerate(self.sym)}
        self.curve = instruments.curve.to_numpy()
        self.cx = instruments["complex"].to_numpy()
        self.gran = instruments.granularity.to_numpy()
        self.dstart = np.array([pd.Timestamp(d).value / 1e9 for d in instruments.delivery_start])
        self.moy = instruments.moy.to_numpy() - 1
        self.hours = instruments.hours.to_numpy().astype(float)
        self.days = instruments.days.to_numpy().astype(float)
        self.tick = instruments.tick_size.to_numpy().astype(float)
        self.precision = np.array([CURVES[c]["precision"] for c in self.curve])
        self.spread_ticks = instruments.spread_ticks.to_numpy().astype(float)
        self.tick_rate = instruments.tick_rate.to_numpy().astype(float)
        self.exchange = instruments.exchange.to_numpy()
        self.rank = instruments["rank"].to_numpy()
        self.is_month = np.isin(self.gran, ["M", "Z"])
        # Listings: where each instrument trades. Arrays are indexed by listing.
        self.listings = build_listings(instruments, seed)
        L = self.listings
        self.nl = len(L)
        self.l_ci = L.ci.to_numpy()
        self.l_exchange = L.exchange.to_numpy()
        self.l_primary = L.is_primary.to_numpy().astype(bool)
        self.l_rate = L.rate_mult.to_numpy()
        self.l_size = L.size_mult.to_numpy()
        self.l_spread = L.spread_mult.to_numpy()
        self.l_basis_amp = L.basis_amp.to_numpy()
        self.l_ph1, self.l_ph2 = L.basis_ph1.to_numpy(), L.basis_ph2.to_numpy()
        self.l_p1, self.l_p2 = L.basis_p1.to_numpy(), L.basis_p2.to_numpy()
        self.l_divergent = L.divergent.to_numpy().astype(bool)
        self.l_share = L.liquidity_share.to_numpy()
        self.l_ccp = L.ccp.to_numpy()
        self.listings_of = {}
        for li, ci in enumerate(self.l_ci):
            self.listings_of.setdefault(int(ci), []).append(li)
        self.primary_listing = np.array([self.listings_of[i][0] for i in range(n)])
        self.secondary_listing = np.array([self.listings_of[i][1] if len(self.listings_of[i]) > 1 else -1
                                           for i in range(n)])
        self._div_cache = {}
        # Strip weights: identity for months, hours (power) or days (gas) for strips.
        W = np.eye(n)
        for i, r in instruments.iterrows():
            if r.months:
                parts = np.array([self.sym_idx[s] for s in r.months])
                w = self.days[parts] if r.complex == "GAS" else self.hours[parts]
                W[i, i] = 0.0
                W[i, parts] = w / w.sum()
        self.W = W.T  # months @ W -> all contracts
        self.front = {c: int(np.where((self.curve == c) & self.is_month)[0][
            np.argmin(self.dstart[(self.curve == c) & self.is_month])]) for c in CURVES}
        # Seasonal tables, jittered +/- 2% at startup so the shape is not textbook.
        jr = np.random.default_rng([seed, 3])
        self.gas_seas = GAS_SEASONAL * (1 + jr.uniform(-0.02, 0.02, 12))
        self.pwr_seas = POWER_SEASONAL * (1 + jr.uniform(-0.02, 0.02, 12))
        self.pwr_extra = (self.pwr_seas / self.gas_seas)
        self.pwr_extra = self.pwr_extra / self.pwr_extra.mean()
        # Factor path, per minute, indexed from m0 (epoch minutes).
        self.m0 = None
        self.path = None
        self.rng = np.random.default_rng([seed, 1])
        self.pull_to_anchor = False

    # ---- anchors
    def set_anchors(self, anchors: dict):
        self.anchors = anchors["anchors"]
        self.spreads = anchors["spreads"]

    # ---- factor path
    def init_path(self, m0: int, initial: Optional[np.ndarray] = None):
        self.m0 = m0
        x = np.zeros(len(FACTOR_NAMES)) if initial is None else initial.copy()
        if initial is None:
            x[FIDX["L_BRENT"]] = math.log(self.anchors["BRENT"])
            x[FIDX["L_TTF"]] = math.log(self.anchors["TTF"] / self.gas_seas[self.moy[self.front["TTF"]]])
            x[FIDX["L_EUA"]] = math.log(self.anchors["EUA"])
            x[FIDX["FX_EURUSD"]] = math.log(self.anchors["EURUSD"])
            x[FIDX["FX_GBPUSD"]] = math.log(self.anchors["GBPUSD"])
            for name, (key, sd, hl) in SPREAD_PARAMS.items():
                x[FIDX[name]] = self.spreads[key]
        self.path = x[None, :].copy()

    def _step(self, x: np.ndarray, minute: int) -> np.ndarray:
        """One minute of factor dynamics."""
        z = self.rng.standard_normal(len(FACTOR_NAMES))
        y = x.copy()
        theta_s = math.log(2) / (S_HALF_LIFE_H / 24.0 / 365.0)
        for c in BASE_CURVES:
            p = FACTOR_PARAMS[c]
            li, si = FIDX[f"L_{c}"], FIDX[f"S_{c}"]
            y[li] = x[li] + p["l_daily_vol"] / math.sqrt(1440.0) * z[li]
            y[si] = x[si] - theta_s * x[si] * DT_MIN + p["s_vol"] * math.sqrt(DT_MIN) * z[si]
        pi = FIDX["S_PWR"]
        y[pi] = x[pi] - theta_s * x[pi] * DT_MIN + FACTOR_PARAMS["PWR"]["s_vol"] * math.sqrt(DT_MIN) * z[pi]
        for pair, vol in FX_VOL.items():
            fi = FIDX[f"FX_{pair}"]
            y[fi] = x[fi] + vol * math.sqrt(DT_MIN) * z[fi]
        for name, (key, sd, hl) in SPREAD_PARAMS.items():
            i = FIDX[name]
            theta = math.log(2) / (hl / 365.0)
            sigma = sd * math.sqrt(2 * theta)
            y[i] = x[i] + theta * (self.spreads[key] - x[i]) * DT_MIN + sigma * math.sqrt(DT_MIN) * z[i]
        for table, hl_days in ((IV_LEVEL, 1.0), (IV_SKEW, 1.0)):
            for name, sd in table.items():
                i = FIDX[name]
                theta = math.log(2) / (hl_days / 365.0)
                sigma = sd * math.sqrt(2 * theta)
                y[i] = x[i] - theta * x[i] * DT_MIN + sigma * math.sqrt(DT_MIN) * z[i]
        if self.pull_to_anchor:
            # Real-time: a gentle pull of each base curve's front month and of FX
            # towards the live anchor, half-life ANCHOR_PULL_HALF_LIFE_H.
            k = math.log(2) / (ANCHOR_PULL_HALF_LIFE_H * 60.0)
            t = minute * 60.0
            for c in BASE_CURVES:
                li = FIDX[f"L_{c}"]
                target = self._log_l_for_anchor(c, y, t)
                y[li] += -k * (y[li] - target)
            for pair in FX_VOL:
                fi = FIDX[f"FX_{pair}"]
                y[fi] += -k * (y[fi] - math.log(self.anchors[pair]))
        return y

    def _log_l_for_anchor(self, c: str, x: np.ndarray, t: float) -> float:
        """log L such that the front month of base curve c prices at its anchor."""
        f = self.front[c]
        tau = max(self.dstart[f] - t, 0.0) / YEAR_S
        p = FACTOR_PARAMS[c]
        s = x[FIDX[f"S_{c}"]]
        seas = self.gas_seas[self.moy[f]] if c == "TTF" else 1.0
        # Only gas carries the winter repricing; oil and carbon front months in
        # December are not "winter" contracts.
        uplift = self._uplift(np.array([t]), np.array([self.moy[f]]), np.array([tau]), "market")[0] if c == "TTF" else 0.0
        return math.log(self.anchors[c] / seas) - (s * math.exp(-p["kappa"] * tau) + p["shape"] * min(tau, 3.0) + uplift)

    def extend_to(self, minute: int):
        """Make sure the path covers epoch minute `minute` (inclusive)."""
        need = minute - self.m0 + 1 - len(self.path)
        if need <= 0:
            return
        rows = [self.path[-1]]
        m = self.m0 + len(self.path) - 1
        for _ in range(need):
            m += 1
            rows.append(self._step(rows[-1], m))
        self.path = np.vstack([self.path, np.array(rows[1:])])

    def pin_end_to_anchors(self, end_minute: int):
        """Backfill: shift the level factors so the front months and FX hit the
        live anchors at the end of the window. A shift leaves the increments
        (and so the realised vol) untouched."""
        i = end_minute - self.m0
        x = self.path[i]
        t = end_minute * 60.0
        for c in BASE_CURVES:
            li = FIDX[f"L_{c}"]
            self.path[:, li] += self._log_l_for_anchor(c, x, t) - x[li]
        for pair in FX_VOL:
            fi = FIDX[f"FX_{pair}"]
            self.path[:, fi] += math.log(self.anchors[pair]) - x[fi]

    def trim_before(self, minute: int):
        """Drop path history older than `minute` (real-time housekeeping)."""
        i = minute - self.m0
        if i > 0:
            self.path = self.path[i:]
            self.m0 = minute

    # ---- factor values at seconds
    def _bridge(self, minute: int, name: str) -> np.ndarray:
        vol = BRIDGED[name]
        r = np.random.default_rng([self.seed, 7, int(minute), FIDX[name]])
        w = np.cumsum(r.standard_normal(60)) * vol * math.sqrt(DT_SEC)
        k = np.arange(1, 61)
        b = w - k / 60.0 * w[-1]
        return np.concatenate([[0.0], b[:-1]])

    def factors_at(self, secs: np.ndarray) -> np.ndarray:
        """Factor matrix (len(secs), n_factors) at epoch seconds, linear between
        minutes plus the deterministic intra-minute bridge."""
        secs = np.asarray(secs, dtype=np.int64)
        minutes = secs // 60
        self.extend_to(int(minutes.max()) + 1)
        # Instants before the path starts (the first day's day-ahead publication)
        # read the first minute.
        i = np.maximum(minutes - self.m0, 0)
        frac = (secs % 60) / 60.0
        a = self.path[i]
        b = self.path[i + 1]
        x = a + (b - a) * frac[:, None]
        for name in BRIDGED:
            col = FIDX[name]
            for m in np.unique(minutes):
                sel = minutes == m
                x[sel, col] += self._bridge(int(m), name)[secs[sel] % 60]
        return x

    # ---- pricing
    def _uplift(self, t, moy, tau, mode: str):
        """Winter repricing (8.6): Nov to Feb delivery months step up by
        --winter_repricing_pct of their PRICE over 20 minutes from the planted
        time, damped along the curve (exp(-tau): the front winter takes most of
        it, the next winter about a third). The champion model never sees it;
        the challenger refits on the hour."""
        t = np.asarray(t, dtype=float)
        if mode == "champion":
            return np.zeros_like(t)
        if mode == "challenger":
            t = np.floor(t / 3600.0) * 3600.0
        w = self.winter_repricing * np.clip((t - self.story.repricing) / 1200.0, 0.0, 1.0)
        winter = np.isin(np.asarray(moy) + 1, list(WINTER_MONTHS))
        return np.log1p(w * np.exp(-1.0 * np.asarray(tau)) * winter)

    def fair(self, secs, mode: str = "market") -> np.ndarray:
        """Fair mid for every contract at every second: (len(secs), n)."""
        secs = np.asarray(secs, dtype=np.int64)
        X = self.factors_at(secs)
        t = secs.astype(float)[:, None]
        tau = np.maximum(self.dstart[None, :] - t, 0.0) / YEAR_S
        moy = self.moy[None, :]
        f = lambda name: X[:, FIDX[name]][:, None]
        d = lambda c: np.exp(-FACTOR_PARAMS[c]["kappa"] * tau)
        eurusd = np.exp(f("FX_EURUSD"))
        gbpusd = np.exp(f("FX_GBPUSD"))
        eurgbp = eurusd / gbpusd
        uplift = self._uplift(np.repeat(t, self.n, axis=1), np.repeat(moy, len(secs), axis=0), tau, mode)
        brent = np.exp(f("L_BRENT") + f("S_BRENT") * d("BRENT") + FACTOR_PARAMS["BRENT"]["shape"] * np.minimum(tau, 3.0))
        ttf = np.exp(f("L_TTF") + f("S_TTF") * d("TTF") + uplift) * self.gas_seas[moy]
        eua = np.exp(f("L_EUA") + f("S_EUA") * d("EUA") + FACTOR_PARAMS["EUA"]["shape"] * tau)
        out = np.zeros((len(secs), self.n))
        c = self.curve
        out[:, c == "BRENT"] = brent[:, c == "BRENT"]
        out[:, c == "WTI"] = (brent - f("wti_spread"))[:, c == "WTI"]
        out[:, c == "GASOIL"] = ((brent + f("crack") + DIESEL_WINTER[moy]) * BBL_PER_T)[:, c == "GASOIL"]
        out[:, c == "TTF"] = ttf[:, c == "TTF"]
        nbp = ttf * THERM_MWH * eurgbp * 100.0 + f("nbp_basis")
        out[:, c == "NBP"] = nbp[:, c == "NBP"]
        out[:, c == "JKM"] = (ttf * eurusd / MMBTU_PER_MWH + f("jkm_premium") * np.exp(-0.7 * tau) + ASIA_SEASONAL[moy])[:, c == "JKM"]
        out[:, c == "EUA"] = eua[:, c == "EUA"]
        uka = eua * eurgbp * (1.0 - f("uka_discount"))
        out[:, c == "UKA"] = uka[:, c == "UKA"]
        uka_front = uka[:, self.front["UKA"]][:, None]
        pwr = (nbp * THERM_TO_GBP_MWH / CCGT_EFF + uka_front * GAS_EF / CCGT_EFF + f("css")) \
            * self.pwr_extra[moy] * np.exp(f("S_PWR") * d("TTF"))
        out[:, c == "UKPWR"] = pwr[:, c == "UKPWR"]
        # Strips from their months, weighted by hours (power) or days (gas).
        return out @ self.W

    def fx_at(self, secs) -> dict:
        X = self.factors_at(secs)
        eurusd = np.exp(X[:, FIDX["FX_EURUSD"]])
        gbpusd = np.exp(X[:, FIDX["FX_GBPUSD"]])
        return {"EURUSD": eurusd, "GBPUSD": gbpusd, "EURGBP": eurusd / gbpusd}

    def iv_surface(self, sec: int, curve: str):
        """ATM term structure and smile for the front 12 months of `curve` at
        one instant: returns (contract indices, tau, iv matrix (k, 5))."""
        p = IV_PARAMS[curve]
        # Options expire a month before delivery, so a month whose option has
        # already expired carries no vol mark.
        sel = np.where((self.curve == curve) & self.is_month & (self.dstart - 30 * 86400 > sec + 86400))[0]
        sel = sel[np.argsort(self.dstart[sel])][:12]
        X = self.factors_at(np.array([sec]))[0]
        tau = (self.dstart[sel] - 30 * 86400 - sec) / YEAR_S
        level = math.exp(X[FIDX[f"IV_{curve}"]])
        atm = (p["long"] + (p["front"] - p["long"]) * np.exp(-p["k"] * tau)) * level
        rr = p["rr"] + X[FIDX[f"SKEW_{curve}"]]
        bf = p["bf"]
        iv = np.stack([atm - 0.9 * rr + 2.8 * bf, atm - rr / 2 + bf, atm, atm + rr / 2 + bf, atm + 0.9 * rr + 2.8 * bf], axis=1)
        return sel, tau, iv

    def bbo(self, mid: np.ndarray, ci: np.ndarray, tod: np.ndarray, spread_mult=1.0):
        """Quantized bid and ask for contract indices ci at fair mids, spread
        widening off-hours (and on a secondary venue, by spread_mult)."""
        tick = self.tick[ci]
        mult = np.where(tod < 0.2, 2.0, np.where(tod < 0.6, 1.3, 1.0))
        spread = self.spread_ticks[ci] * tick * mult * spread_mult
        bid = np.round(np.round((mid - spread / 2.0) / tick) * tick, 6)
        ask = np.round(np.round((mid + spread / 2.0) / tick) * tick, 6)
        ask = np.where(ask <= bid, np.round(bid + tick, 6), ask)
        return bid, ask

    # ---- listings
    def _divergences(self, li: int, day: int):
        """Planted cross-venue divergences on one secondary listing for one UTC
        day: (start, end, offset) arrays. A Poisson number of events between
        07:00 and 17:00 on weekdays; each moves the secondary 2 to 4 ticks away
        for 5 to 30 seconds, then closes. Keyed on (seed, listing, day), so every
        process computes the same schedule."""
        key = (li, day)
        if key not in self._div_cache:
            if not self.l_divergent[li] or ((day + 3) % 7) >= 5:
                ev = (np.zeros(0), np.zeros(0), np.zeros(0))
            else:
                r = np.random.default_rng([self.seed, 19, li, day])
                k = r.poisson(DIVERGENCE_PER_HOUR * 10)
                start = day * 86400 + 7 * 3600 + np.sort(r.uniform(0, 10 * 3600, k))
                dur = r.uniform(*DIVERGENCE_SECS, k)
                off = r.integers(DIVERGENCE_TICKS[0], DIVERGENCE_TICKS[1] + 1, k) * r.choice([-1.0, 1.0], k)
                ev = (start, start + dur, off * self.tick[self.l_ci[li]])
            self._div_cache[key] = ev
        return self._div_cache[key]

    def listing_offset(self, li: np.ndarray, t: np.ndarray) -> np.ndarray:
        """A listing's mid relative to fair value at epoch seconds t: zero on the
        primary; on a secondary, a slow mean-reverting basis of a fraction of a
        tick (two sinusoids with listing-specific periods and phases) plus any
        planted divergence."""
        li = np.asarray(li)
        t = np.asarray(t, dtype=float)
        out = self.l_basis_amp[li] * (0.6 * np.sin(2 * np.pi * t / self.l_p1[li] + self.l_ph1[li])
                                      + 0.4 * np.sin(2 * np.pi * t / self.l_p2[li] + self.l_ph2[li]))
        for one in np.unique(li[self.l_divergent[li]]) if len(li) else []:
            m = li == one
            for day in np.unique((t[m] // 86400).astype(np.int64)):
                start, end, off = self._divergences(int(one), int(day))
                if not len(start):
                    continue
                tm = t[m]
                idx = np.searchsorted(start, tm, side="right") - 1
                ok = (idx >= 0) & (tm < end[np.maximum(idx, 0)])
                add = np.where(ok, off[np.maximum(idx, 0)], 0.0)
                out[np.where(m)[0]] += add
        return out

    def displayed_lots(self, li: np.ndarray, sec: np.ndarray, side: int) -> np.ndarray:
        """Displayed size in lots on a listing's bid (side -1) or ask (side 1)
        during an epoch second: log-normal around 10 lots on the first three
        months and 3 elsewhere, halved on a secondary venue. Keyed on (seed,
        listing, second, side), so quotes and the fill router see the same
        size."""
        li = np.asarray(li, dtype=np.int64)
        sec = np.asarray(sec, dtype=np.int64)
        med = np.where(self.rank[self.l_ci[li]] < 3, 10.0, 3.0) * self.l_size[li]
        z = np.zeros(len(li))
        minute = sec // 60
        col = 1 if side > 0 else 0
        for a, m in set(zip(li.tolist(), minute.tolist())):
            sel = (li == a) & (minute == m)
            draws = np.random.default_rng([self.seed, 23, a, m]).standard_normal((60, 2))
            z[sel] = draws[sec[sel] % 60, col]
        return np.maximum(1.0, np.round(med * np.exp(0.7 * z)))

    def tradable(self, t: float, margin_s: float = 3 * 86400) -> np.ndarray:
        expiry = np.array([pd.Timestamp(d).value / 1e9 for d in self.inst.expiry])
        return expiry > t + margin_s


# ----------------------------
# The desk: fills, bookings, positions, planted events
# ----------------------------

FILL_COLS = ["ts", "book", "trader", "symbol", "curve", "qty", "px", "venue", "order_id", "trade_id", "passive"]
EVENT_COLS = ["booked_ts", "trade_id", "version", "status", "trade_ts", "book", "trader", "symbol", "curve",
              "qty", "px", "channel", "counterparty", "booked_by", "reason"]
SNAP_COLS = ["ts", "book", "symbol", "curve", "venue", "qty", "settle_px", "source"]
QUOTE_COLS = ["ts", "symbol", "curve", "bid", "ask", "bid_size", "ask_size", "source"]
FORESIGHT_MIN = 15
BOOK_TRANSFER_PEERS = {"TTF": ["EU_GAS", "LNG"], "NBP": ["EU_GAS", "UK_POWER"], "UKA": ["CARBON", "UK_POWER"]}


class DeskPlanner:
    """Plans the desk's activity one minute at a time, as a pure function of
    the factor path (which it reads 15 minutes ahead, so informed flow can be
    informed) and its own state. Backfill runs it over the whole window up
    front; real-time runs it minute by minute. Rows it produces carry their
    final timestamps, and whichever process owns that instant emits them.
    """

    def __init__(self, mkt: MarketModel, story: StoryClock, seed: int, scale: float):
        self.mkt = mkt
        self.story = story
        self.scale = scale
        self.rng = np.random.default_rng([seed, 5])
        # Venue routing draws from its own stream, so the planned orders (time,
        # side, size) are the same whichever venues they end up on.
        self.route_rng = np.random.default_rng([seed, 29])
        self.pos = {}            # (book, symbol) -> units, fills and deals at trade time (the trader's view, risk)
        self.pos_fills = {}      # (book, symbol, venue) -> units from exchange fills only (margin)
        self.effects = []        # (booked_sec, book, symbol, venue, dqty): what the booking log adds on top of
                                 # the fills as of booking time (voice deals, amendments, cancels, planted errors)
        self.settle = {}         # symbol -> last settlement price
        self.targets = {}        # (book, curve) -> target net position
        self.fills, self.events, self.snapshots = [], [], []
        self.book_quotes = []    # the secondary venue's book just before each fill routed there
        self.scripted = {}       # minute -> [(sec, book, trader, ci, side, units, aggressive, tag)]
        self.planned_through = None
        self.demo_events = self._demo_events()
        for key, lim in LIMITS.items():
            self.targets[key] = float(self.rng.uniform(-0.2, 0.2) * lim)
        # Book universes resolved to contract indices, with liquidity weights.
        self.universe = {}
        for book, b in BOOKS.items():
            idx, w = [], []
            for curve, gran, maxr in b["universe"]:
                ok = np.where((mkt.curve == curve) & (mkt.gran == gran))[0]
                ok = ok[np.argsort(mkt.dstart[ok])][:maxr]
                idx += list(ok)
                w += list((0.6 ** np.arange(len(ok))) * (1.0 if gran in ("M", "Z") else 0.4))
            self.universe[book] = (np.array(idx), np.array(w) / sum(w))
        self.otc_universe = {}
        for book, b in BOOKS.items():
            idx = []
            for curve, gran, maxr in b["universe"]:
                ok = np.where((mkt.curve == curve) & (mkt.gran == gran))[0]
                ok = ok[np.argsort(mkt.dstart[ok])]
                if gran in ("Q", "S", "Y"):
                    idx += list(ok[:6])
                else:
                    idx += list(ok[3:8] if len(ok) > 3 else ok)
            self.otc_universe[book] = np.array(idx)

    # ---- helpers
    def _uuid(self) -> str:
        return str(uuid.UUID(bytes=self.rng.bytes(16), version=4))

    def _lots(self, ci: int, book: Optional[str] = None) -> int:
        if self.mkt.gran[ci] in ("Q", "S", "Y"):
            lots = int(self.rng.integers(1, 11))
        else:
            lots = int(np.clip(round(math.exp(math.log(5.0) + 0.9 * self.rng.standard_normal())), 1, 50))
        # A single screen fill never exceeds 2% of the book's limit on that curve
        # (a 50-lot clip on a small carbon limit would be a tenth of it).
        lim = LIMITS.get((book, self.mkt.curve[ci])) if book else None
        if lim:
            lots = max(1, min(lots, int(0.02 * lim / float(self.mkt.inst.lot_size.iat[ci]))))
        return lots

    def _units(self, ci: int, lots: int) -> float:
        return float(lots) * float(self.mkt.inst.lot_size.iat[ci])

    def _net(self, book: str, curve: str) -> float:
        return sum(q for (b, s), q in self.pos.items() if b == book and self.mkt.curve[self.mkt.sym_idx[s]] == curve)

    def _add_pos(self, book: str, sym: str, dq: float, venue: Optional[str] = None):
        """Instrument position (risk); with a venue, also the per-venue fill
        position (margin)."""
        self.pos[(book, sym)] = self.pos.get((book, sym), 0.0) + dq
        if venue is not None:
            key = (book, sym, venue)
            self.pos_fills[key] = self.pos_fills.get(key, 0.0) + dq

    def _side_for(self, book: str, ci: int, now_px: float, fut_px: float, foresight: float, t0: int) -> int:
        curve = self.mkt.curve[ci]
        if book == "CRUDE" and curve == "BRENT" and self.story.breach_start <= t0 < self.story.breach_end:
            # The scripted clips own the Brent position during the breach; the
            # background flow neither helps nor fights them.
            return 1 if self.rng.uniform() < 0.5 else -1
        if self.rng.uniform() < foresight and fut_px != now_px:
            return 1 if fut_px > now_px else -1
        lim = LIMITS.get((book, curve))
        if lim:
            net = self._net(book, curve)
            p_buy = 0.5 + 0.45 * float(np.clip((self.targets[(book, curve)] - net) / (0.15 * lim), -1, 1))
        else:
            p_buy = 0.5
        return 1 if self.rng.uniform() < p_buy else -1

    def _px(self, mid: float, ci: int, side: int, aggressive: bool, tod: float, spread_mult: float = 1.0) -> float:
        bid, ask = self.mkt.bbo(np.array([mid]), np.array([ci]), np.array([tod]), spread_mult)
        if aggressive:
            return float(ask[0] if side > 0 else bid[0])
        return float(bid[0] if side > 0 else ask[0])

    def _route(self, book: str, ci: int, side: int, sec: int, fair: float, tod: float) -> int:
        """Pick the listing for a screen order: start from the liquidity share,
        double the weight of a venue where the book holds the opposite side
        (closing where you are open saves margin), 1.5x the venue with the
        better touch for the order's side, normalise and draw. A venue whose
        feed is down is skipped: the desk cannot see its book."""
        mkt = self.mkt
        lis = mkt.listings_of[ci]
        if self.story.outage_start <= sec < self.story.outage_end:
            lis = [li for li in lis if mkt.l_exchange[li] != OUTAGE_EXCHANGE]
        if len(lis) == 1:
            return lis[0]
        # liquidity_share is the target share of fills. The primary nearly always
        # has the better touch (the secondary is wider), so its starting weight
        # is taken net of that bonus, or the bonus alone would skew the split.
        w = np.array([mkt.l_share[li] / (TOUCH_BONUS if mkt.l_primary[li] else 1.0) for li in lis], dtype=float)
        for k, li in enumerate(lis):
            if self.pos_fills.get((book, mkt.sym[ci], mkt.l_exchange[li]), 0.0) * side < 0:
                w[k] *= CLOSING_BONUS
        touch = []
        for li in lis:
            mid = fair + float(mkt.listing_offset(np.array([li]), np.array([float(sec)]))[0])
            bid, ask = mkt.bbo(np.array([mid]), np.array([ci]), np.array([tod]), mkt.l_spread[li])
            touch.append(-ask[0] if side > 0 else bid[0])
        best = int(np.argmax(touch))
        if touch.count(touch[best]) == 1:
            w[best] *= TOUCH_BONUS
        return lis[int(self.route_rng.choice(len(lis), p=w / w.sum()))]

    def _book_quote(self, ts_ns: int, ci: int, li: int, sec: int, mid: float, tod: float):
        mkt = self.mkt
        bid, ask = mkt.bbo(np.array([mid]), np.array([ci]), np.array([tod]), mkt.l_spread[li])
        lot = float(mkt.inst.lot_size.iat[ci])
        bsz = float(mkt.displayed_lots(np.array([li]), np.array([sec]), -1)[0]) * lot
        asz = float(mkt.displayed_lots(np.array([li]), np.array([sec]), 1)[0]) * lot
        self.book_quotes.append([ts_ns, mkt.sym[ci], mkt.curve[ci], float(bid[0]), float(ask[0]), bsz, asz,
                                 mkt.l_exchange[li]])

    def _route_uuid(self) -> str:
        return str(uuid.UUID(bytes=self.route_rng.bytes(16), version=4))

    def _book_row(self, booked_ns, trade_id, version, status, trade_ns, book, trader, ci, qty, px,
                  channel, counterparty, booked_by, reason):
        self.events.append([booked_ns, trade_id, version, status, trade_ns, book, trader, self.mkt.sym[ci],
                            self.mkt.curve[ci], float(qty), float(px), channel, counterparty, booked_by, reason])

    def _fill_row(self, ts_ns, book, trader, ci, qty, px, passive, trade_id, venue, order_id):
        self.fills.append([ts_ns, book, trader, self.mkt.sym[ci], self.mkt.curve[ci], float(qty), float(px),
                           venue, order_id, trade_id, bool(passive)])

    def _background_amendment(self, booked_ns, trade_id, trade_ns, book, trader, ci, qty, px, channel, cpty, by,
                              venue):
        """Background amendments 10 minutes to 3 hours after booking, version 2.
        STP-mirrored exchange fills are rarely touched (0.3% amended, 0.1%
        cancelled); hand-booked voice deals much more often (5% and 1%).
        Amended rows repeat every column with the corrected value."""
        u = self.rng.uniform()
        p_amend, p_cancel = (0.003, 0.001) if channel == "EXCH" else (0.05, 0.01)
        if u >= p_amend + p_cancel:
            return
        later = booked_ns + int(self.rng.uniform(600, 10800) * NS)
        if u < p_cancel:
            self._book_row(later, trade_id, 2, "CANCELLED", trade_ns, book, trader, ci, qty, px, channel, cpty, by,
                           "DUPLICATE" if self.rng.uniform() < 0.5 else "ERROR")
            self.effects.append((later / NS, book, self.mkt.sym[ci], venue, -qty))
            return
        reason = ["PX_CORRECTION", "QTY_CORRECTION", "BOOK_TRANSFER"][int(self.rng.integers(0, 3))]
        new_book, new_qty, new_px = book, qty, px
        if reason == "BOOK_TRANSFER":
            peers = [b for b in BOOK_TRANSFER_PEERS.get(self.mkt.curve[ci], []) if b != book]
            if peers:
                new_book = peers[0]
            else:
                reason = "PX_CORRECTION"
        if reason == "PX_CORRECTION":
            tick = float(self.mkt.tick[ci])
            new_px = round(px + tick * int(self.rng.integers(1, 3)) * (1 if qty > 0 else -1), 6)
        elif reason == "QTY_CORRECTION":
            lot = float(self.mkt.inst.lot_size.iat[ci])
            lots = max(1, int(round(abs(qty) / lot)) + int(self.rng.integers(-2, 3)))
            new_qty = math.copysign(lots * lot, qty)
            if new_qty == qty:
                new_qty = qty + math.copysign(lot, qty)
        self._book_row(later, trade_id, 2, "AMENDED", trade_ns, new_book, trader, ci, new_qty, new_px,
                       channel, cpty, by, reason)
        if new_book != book:
            self.effects.append((later / NS, book, self.mkt.sym[ci], venue, -qty))
            self.effects.append((later / NS, new_book, self.mkt.sym[ci], venue, qty))
        elif new_qty != qty:
            self.effects.append((later / NS, book, self.mkt.sym[ci], venue, new_qty - qty))

    # ---- the storyline
    def _demo_events(self):
        s, m = self.story, self.mkt
        sym = lambda c, g, k=0: m.sym[self._nth(c, g, k)]
        iso = lambda t: datetime.datetime.fromtimestamp(t, UTC).strftime("%H:%M UTC")
        return [
            (s.windy_day, "3_vol", f"Windy night: UK day-ahead hours 01:00 to 05:00 on {s.windy_day} clear negative"),
            (s.breach_start, "2_exposure", f"CRUDE buys {sym('BRENT', 'M')} to about 3.0M bbl against a 2.0M limit, "
                                           f"unwinds to 0.4M by {iso(s.breach_end)}"),
            (s.late_deal, "6_recon", f"UK_POWER sells 100 MW {sym('UKPWR', 'S', 1)} by voice (OTC), booked 5h20m late "
                                     f"at {iso(s.late_booking)}"),
            (s.bad_mark_start, "4_curves", f"Manual mark on {sym('TTF', 'Q')} +2.50 EUR above its months (trader_11), "
                                           f"corrected (version 2) at {iso(s.bad_mark_end)}"),
            (s.outage_start, "7_models", "3-minute outage of the EEX quote feed: EEX-listed TTF, EUA and UK power go "
                                         "dark, ICE keeps ticking and the curve marks stay MARKET"),
            (s.fat_finger_fill, "6_recon", f"LNG fill in {sym('JKM', 'M')} booked with 10x quantity by STP, amended "
                                           f"(QTY_CORRECTION) at {iso(s.fat_finger_fix)}"),
            (s.duplicate_deal, "6_recon", f"EU_GAS {sym('TTF', 'Q')} broker trade booked twice; duplicate cancelled "
                                          f"at {iso(s.duplicate_fix)}"),
            (s.repricing, "7_models", "Cold-snap forecast: Nov to Feb gas and power premium steps up 8% over 20 minutes. "
                                      "champion_v1 does not adapt, challenger_v2 refits on the hour"),
            (s.bad_iv_start, "7_models", f"Bad ATM vol mark on {sym('TTF', 'M', 3)} (-8 points) breaks calendar "
                                         f"no-arbitrage, corrected (version 2) at {iso(s.bad_iv_end)}"),
        ]

    def _nth(self, curve, gran, k=0) -> int:
        ok = np.where((self.mkt.curve == curve) & (self.mkt.gran == gran))[0]
        ok = ok[np.argsort(self.mkt.dstart[ok])]
        return int(ok[min(k, len(ok) - 1)])

    def _schedule_breach(self, minute: int, phase: str):
        """8.1: CRUDE pushes Brent through its limit, then unwinds."""
        ci = self._nth("BRENT", "M", 0)
        net = self._net("CRUDE", "BRENT")
        lot = float(self.mkt.inst.lot_size.iat[ci])
        if phase == "build":
            needed, clips, span = 3_050_000 - net, 60, 30
            side = 1
        else:
            needed, clips, span = net - 400_000, 100, 200
            side = -1
            self.targets[("CRUDE", "BRENT")] = 400_000.0
        if needed <= 0:
            return
        per = max(1, int(round(needed / clips / lot))) * lot
        for k in range(clips):
            mm = minute + int(k * span / clips)
            sec = mm * 60 + int(self.rng.integers(0, 60))
            self.scripted.setdefault(mm, []).append((sec, "CRUDE", "trader_01", ci, side, per, True, "breach"))

    # ---- settlements and snapshots
    def _settle_minute(self, minute: int):
        hhmm = minute % 1440
        weekend = ((minute // 1440) + 3) % 7 >= 5
        if not weekend and (hhmm == 16 * 60 + 30 or hhmm == 19 * 60 + 30):
            px = self.mkt.fair(np.array([minute * 60]))[0]
            oil = hhmm == 19 * 60 + 30
            for i in range(self.mkt.n):
                if (self.mkt.cx[i] == "OIL") == oil:
                    self.settle[self.mkt.sym[i]] = float(quantize(px[i], self.mkt.tick[i], int(self.mkt.precision[i])))

    def snapshot(self, sec: int):
        """EOD positions per book, contract and venue at `sec` (00:00 UTC):
        exchange fills to that point plus the booking log as known at that
        point (voice deals once booked, under venue OTC; amendments, cancels),
        at settlement."""
        qty = dict(self.pos_fills)
        for booked_sec, book, sym, venue, dq in self.effects:
            if booked_sec < sec:
                qty[(book, sym, venue)] = qty.get((book, sym, venue), 0.0) + dq
        fair = None
        for (book, sym, venue), q in sorted(qty.items()):
            if abs(q) < 1e-9:
                continue
            ci = self.mkt.sym_idx[sym]
            px = self.settle.get(sym)
            if px is None:
                if fair is None:
                    fair = self.mkt.fair(np.array([sec]))[0]
                px = float(quantize(fair[ci], self.mkt.tick[ci], int(self.mkt.precision[ci])))
            self.snapshots.append([sec * NS, book, sym, self.mkt.curve[ci], venue, float(q), px, "EOD_BATCH"])

    def initial_positions(self, sec: int):
        """Legacy book at the start of the window: a few front contracts per
        book and curve, written as the first snapshot."""
        for (book, curve), lim in LIMITS.items():
            gran = next(g for cv, g, r in BOOKS[book]["universe"] if cv == curve)
            net = self.rng.uniform(-0.3, 0.3) * lim
            for k in range(2):
                ci = self._nth(curve, gran, k)
                lot = float(self.mkt.inst.lot_size.iat[ci])
                q = round(net / 2 / lot) * lot
                if not q:
                    continue
                # Held across the contract's venues in proportion to their share.
                lis = self.mkt.listings_of[ci]
                left = q
                for li in lis[1:]:
                    part = round(q * self.mkt.l_share[li] / lot) * lot
                    if part:
                        self._add_pos(book, self.mkt.sym[ci], part, self.mkt.l_exchange[li])
                        left -= part
                self._add_pos(book, self.mkt.sym[ci], left, self.mkt.l_exchange[lis[0]])
        self.snapshot(sec)

    # ---- one minute
    def plan_minute(self, minute: int):
        mkt, s, rng = self.mkt, self.story, self.rng
        t0 = minute * 60
        self._settle_minute(minute)
        if minute % 1440 == 0 and self.planned_through is not None:
            self.snapshot(t0)
        # Slow mean-reverting targets per book and curve.
        theta = math.log(2) / 1440.0
        for key, lim in LIMITS.items():
            if key == ("CRUDE", "BRENT") and s.breach_peak <= t0 < s.day_start + 86400:
                continue
            x = self.targets[key]
            self.targets[key] = x - theta * x + 0.15 * lim * math.sqrt(2 * theta) * rng.standard_normal()
        if t0 == s.breach_start:
            self._schedule_breach(minute, "build")
        if t0 == s.breach_peak:
            self._schedule_breach(minute, "unwind")

        fair_now, fair_fut = mkt.fair(np.array([t0, t0 + FORESIGHT_MIN * 60]))
        arrivals = []   # (sec, book, trader, ci, side, units, aggressive, tag)
        for book, b in BOOKS.items():
            tod = float(tod_weight(np.array([t0]), "OIL" if book in ("CRUDE", "PRODUCTS") else "GAS")[0])
            n = rng.poisson(b["rate"] / 60.0 * tod * self.scale)
            idx, w = self.universe[book]
            for _ in range(n):
                ci = int(rng.choice(idx, p=w))
                trader = b["traders"][int(rng.integers(len(b["traders"])))]
                arrivals.append((t0 + int(rng.integers(0, 60)), book, trader, ci, 0, None,
                                 rng.uniform() < b["aggressive"], "random"))
        # Planted fill (8.3): a real LNG fill whose STP booking is 10x.
        if t0 == s.fat_finger_fill:
            ci = self._nth("JKM", "M", 0)
            arrivals.append((t0 + 12, "LNG", "trader_14", ci, 1, self._units(ci, 5), True, "fat_finger"))
        arrivals += self.scripted.pop(minute, [])
        arrivals.sort(key=lambda a: a[0])
        if arrivals:
            secs = np.array([a[0] for a in arrivals])
            cis = np.array([a[3] for a in arrivals])
            uniq, inv = np.unique(secs, return_inverse=True)
            mids = mkt.fair(uniq)[inv, cis]
            for k, (sec, book, trader, ci, side, units, aggressive, tag) in enumerate(arrivals):
                cx = mkt.cx[ci]
                tod = float(tod_weight(np.array([sec]), cx)[0])
                if side == 0:
                    side = self._side_for(book, ci, fair_now[ci], fair_fut[ci], BOOKS[book]["foresight"], t0)
                    units = self._units(ci, self._lots(ci, book))
                qty = side * units
                ts_ns = sec * NS + int(rng.integers(0, NS))
                tid = self._uuid()
                oid = self._uuid()
                # Route the order to a listing. A secondary-venue fill is capped at
                # that venue's displayed size; any remainder sweeps the primary as a
                # second fill of the same order, a microsecond later.
                li = self._route(book, ci, side, sec, float(mids[k]), tod)
                lot = float(mkt.inst.lot_size.iat[ci])
                fill_units = units
                rest = 0.0
                if not mkt.l_primary[li]:
                    shown = float(mkt.displayed_lots(np.array([li]), np.array([sec]), side)[0]) * lot
                    if units > shown:
                        fill_units, rest = shown, units - shown
                venue = mkt.l_exchange[li]
                ccp = mkt.l_ccp[li]
                mid_v = float(mids[k]) + float(mkt.listing_offset(np.array([li]), np.array([float(sec)]))[0])
                px = self._px(mid_v, ci, side, aggressive, tod, mkt.l_spread[li])
                if not mkt.l_primary[li]:
                    # The book the router saw, published a nanosecond before the
                    # fill: the quote the fill priced against and the sizes it was
                    # capped at, so the quotes table shows what the fill hit.
                    self._book_quote(max(sec * NS, ts_ns - 1), ci, li, sec, mid_v, tod)
                qty = side * fill_units
                self._fill_row(ts_ns, book, trader, ci, qty, px, not aggressive, tid, venue, oid)
                self._add_pos(book, mkt.sym[ci], qty, venue)
                booked_ns = ts_ns + int(rng.integers(50, 500)) * 1_000_000
                if rest > 0:
                    pli = mkt.primary_listing[ci]
                    px2 = self._px(float(mids[k]), ci, side, aggressive, tod)
                    tid2 = self._route_uuid()
                    ts2 = ts_ns + 1000
                    self._fill_row(ts2, book, trader, ci, side * rest, px2, not aggressive, tid2,
                                   mkt.l_exchange[pli], oid)
                    self._add_pos(book, mkt.sym[ci], side * rest, mkt.l_exchange[pli])
                    self._book_row(ts2 + int(self.route_rng.integers(50, 500)) * 1_000_000, tid2, 1, "NEW", ts2,
                                   book, trader, ci, side * rest, px2, "EXCH", mkt.l_ccp[pli], "STP", None)
                if tag == "fat_finger":
                    # The fill is right; the STP booking carries ten times the
                    # quantity until trade support corrects it.
                    self._book_row(booked_ns, tid, 1, "NEW", ts_ns, book, trader, ci, qty * 10, px, "EXCH", ccp, "STP", None)
                    self._book_row(s.fat_finger_fix * NS + 7 * NS, tid, 2, "AMENDED", ts_ns, book, trader, ci, qty, px,
                                   "EXCH", ccp, "STP", "QTY_CORRECTION")
                    self.effects.append((booked_ns / NS, book, mkt.sym[ci], venue, qty * 9))
                    self.effects.append((s.fat_finger_fix + 7, book, mkt.sym[ci], venue, -qty * 9))
                    continue
                self._book_row(booked_ns, tid, 1, "NEW", ts_ns, book, trader, ci, qty, px, "EXCH", ccp, "STP", None)
                if tag == "random":
                    self._background_amendment(booked_ns, tid, ts_ns, book, trader, ci, qty, px, "EXCH", ccp, "STP",
                                               venue)

        # Broker and bilateral deals: about 10% of the fill count, in strips and
        # back months, larger, booked late. Never in fills.
        for book, b in BOOKS.items():
            tod = float(tod_weight(np.array([t0]), "GAS")[0])
            n = rng.poisson(0.1 * b["rate"] / 60.0 * tod * self.scale)
            for _ in range(n):
                ci = int(rng.choice(self.otc_universe[book]))
                self._deal(t0 + int(rng.integers(0, 60)), book, ci, None, None, "BROKER" if rng.uniform() < 0.6 else "OTC",
                           None, None, fair_now[ci], fair_fut[ci])
        if t0 == s.duplicate_deal:
            ci = self._nth("TTF", "Q", 0)
            self._deal(t0 + 20, "EU_GAS", ci, 1, self._units(ci, 30), "BROKER", "BANK_02",
                       s.duplicate_deal + 18 * 60, fair_now[ci], fair_fut[ci], duplicate=True)
        if t0 == s.late_deal:
            ci = self._nth("UKPWR", "S", 1)
            self._deal(t0 + 40, "UK_POWER", ci, -1, self._units(ci, 100), "OTC", "UTILITY_03", s.late_booking,
                       fair_now[ci], fair_fut[ci], trader="trader_17")
        self.planned_through = minute

    def _deal(self, sec, book, ci, side, units, channel, cpty, booked_sec, now_px, fut_px, duplicate=False, trader=None):
        mkt, rng, s = self.mkt, self.rng, self.story
        if cpty is None:
            cpty, score = COUNTERPARTIES[int(rng.integers(len(COUNTERPARTIES)))]
        else:
            score = dict(COUNTERPARTIES)[cpty]
        if side is None:
            if rng.uniform() < abs(score) and fut_px != now_px:
                side = (1 if fut_px > now_px else -1) * (1 if score > 0 else -1)
            elif rng.uniform() < 0.5:
                # Half the uninformed voice flow is the desk working its own
                # book towards its target (offloading, hedging), so voice deals
                # do not walk the position away over days.
                side = self._side_for(book, ci, now_px, fut_px, 0.0, sec)
            else:
                side = 1 if rng.uniform() < 0.5 else -1
        if units is None:
            lot = float(mkt.inst.lot_size.iat[ci])
            lots = int(rng.integers(10, 101))
            # A single voice deal never exceeds 5% of the book's limit on that
            # curve, so the desk's exposure story is the planted one.
            lim = LIMITS.get((book, mkt.curve[ci]))
            if lim:
                lots = max(1, min(lots, int(0.05 * lim / lot)))
            units = self._units(ci, lots)
        if trader is None:
            trader = BOOKS[book]["traders"][int(rng.integers(len(BOOKS[book]["traders"])))]
        mid = float(mkt.fair(np.array([sec]))[0, ci])
        tick = float(mkt.tick[ci])
        # Voice deals print near mid, a tick or two against the desk.
        px = float(quantize(mid + side * tick * rng.uniform(0, 2), tick, int(mkt.precision[ci])))
        if booked_sec is None:
            if channel == "BROKER":
                delay = math.exp(math.log(20 * 60) + 0.6 * rng.standard_normal())
            else:
                delay = math.exp(math.log(120 * 60) + 0.9 * rng.standard_normal())
            booked_sec = sec + delay
            hhmm = booked_sec % 86400
            if hhmm > 21 * 3600 or hhmm < 6 * 3600:
                # Booked next morning, when trade support is back.
                day = booked_sec - hhmm + (86400 if hhmm > 21 * 3600 else 0)
                booked_sec = day + 7 * 3600 + 1800 + rng.uniform(0, 5400)
        trade_ns = sec * NS + int(rng.integers(0, NS))
        booked_ns = int(booked_sec * NS)
        by = OPS_USERS[int(rng.integers(len(OPS_USERS)))]
        qty = side * units
        tid = self._uuid()
        self._add_pos(book, mkt.sym[ci], qty)
        self._book_row(booked_ns, tid, 1, "NEW", trade_ns, book, trader, ci, qty, px, channel, cpty, by, None)
        self.effects.append((booked_ns / NS, book, mkt.sym[ci], OTC_VENUE, qty))
        if duplicate:
            dup = self._uuid()
            self._book_row(booked_ns + 15 * NS, dup, 1, "NEW", trade_ns, book, trader, ci, qty, px, channel, cpty, by, None)
            self._book_row(s.duplicate_fix * NS + 3 * NS, dup, 2, "CANCELLED", trade_ns, book, trader, ci, qty, px,
                           channel, cpty, by, "DUPLICATE")
            self.effects.append((booked_sec + 15, book, mkt.sym[ci], OTC_VENUE, qty))
            self.effects.append((s.duplicate_fix + 3, book, mkt.sym[ci], OTC_VENUE, -qty))
        else:
            self._background_amendment(booked_ns, tid, trade_ns, book, trader, ci, qty, px, channel, cpty, by,
                                       OTC_VENUE)

    # ---- output
    def take_rows(self) -> dict:
        """Hand out everything planned so far as DataFrames, keyed by table, and
        clear the buffers."""
        out = {"fills": pd.DataFrame(self.fills, columns=FILL_COLS),
               "trade_events": pd.DataFrame(self.events, columns=EVENT_COLS),
               "position_snapshots": pd.DataFrame(self.snapshots, columns=SNAP_COLS),
               "quotes": pd.DataFrame(self.book_quotes, columns=QUOTE_COLS)}
        self.fills, self.events, self.snapshots, self.book_quotes = [], [], [], []
        return out


def demo_events_df(rows) -> pd.DataFrame:
    out = []
    for t, act, what in rows:
        if isinstance(t, datetime.date):
            t = int(datetime.datetime(t.year, t.month, t.day, 1, tzinfo=UTC).timestamp())
        out.append([int(t) * NS, act, what])
    return pd.DataFrame(out, columns=["ts", "act", "what"])


# ----------------------------
# SortedEmitter: column-wise buffers, dataframe() sends
# ----------------------------

TABLE_SYMBOLS = {
    "instruments": ["symbol", "curve", "complex", "granularity", "term_code", "month_code", "unit", "ccy",
                    "fx_symbol"],
    "listings": ["symbol", "exchange", "mic", "exchange_code", "exchange_physical_code", "ccp"],
    "limits": ["book", "curve", "unit", "approved_by"],
    "quotes": ["symbol", "curve", "source"],
    "curve_marks": ["curve", "symbol", "source", "venue", "marked_by"],
    "settlements": ["curve", "symbol", "source"],
    "iv_marks": ["curve", "symbol", "delta_bucket", "source"],
    "model_prices": ["model_version", "curve", "symbol"],
    "da_prices": ["market", "source"],
    "fills": ["book", "trader", "symbol", "curve", "venue"],
    "trade_events": ["status", "book", "trader", "symbol", "curve", "channel", "counterparty", "booked_by", "reason"],
    "position_snapshots": ["book", "symbol", "curve", "venue", "source"],
    "demo_events": ["act"],
}
TABLE_TS = {name: ("booked_ts" if name == "trade_events" else "ts") for name in TABLE_SYMBOLS}
TABLE_TS_COLS = {
    "instruments": ["ts", "delivery_start", "delivery_end", "expiry"],
    "model_prices": ["ts", "inputs_ts"],
    "da_prices": ["ts", "published_ts"],
    "trade_events": ["booked_ts", "trade_ts"],
}


def prepare_frame(table: str, df: pd.DataFrame) -> (pd.DataFrame, list):
    """Timestamps from int64 nanoseconds to datetime64, symbols to categoricals,
    all-null symbol columns dropped (the client cannot type them)."""
    df = df.copy()
    for col in TABLE_TS_COLS.get(table, [TABLE_TS[table]]):
        if col in df and not np.issubdtype(df[col].dtype, np.datetime64):
            df[col] = pd.to_datetime(df[col].astype(np.int64), unit="ns")
    symbols = []
    for col in TABLE_SYMBOLS[table]:
        if col not in df:
            continue
        if df[col].isna().all():
            df = df.drop(columns=[col])
            continue
        df[col] = pd.Categorical(df[col].astype(object).where(df[col].notna(), None))
        symbols.append(col)
    return df, symbols


class SortedEmitter:
    """Per-table frame buffers. Each flush concatenates, sorts on the designated
    timestamp and hands one DataFrame to the sender, which frames it under the
    QWP message limit. Flushes when a table's buffer reaches buffer_rows."""

    def __init__(self, sender, prefix: str, table_set: set, buffer_rows: int):
        self.sender = sender
        self.prefix = prefix
        self.table_set = table_set
        self.buffer_rows = buffer_rows
        self.frames = {}
        self.counts = {}
        self.sent = {}

    def emit(self, table: str, df: pd.DataFrame):
        if table not in self.table_set or df is None or len(df) == 0:
            return
        self.frames.setdefault(table, []).append(df)
        self.counts[table] = self.counts.get(table, 0) + len(df)
        if self.counts[table] >= self.buffer_rows:
            self._send(table)

    def _send(self, table: str):
        frames = self.frames.pop(table, [])
        self.counts[table] = 0
        if not frames:
            return
        df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        at = TABLE_TS[table]
        df = df.sort_values(at, kind="stable")
        df, symbols = prepare_frame(table, df)
        self.sender.dataframe(df, table_name=table_name(table, self.prefix), symbols=symbols, at=at)
        self.sender.flush()
        self.sent[table] = self.sent.get(table, 0) + len(df)

    def flush_all(self):
        for table in list(self.frames):
            self._send(table)


# ----------------------------
# Market data for a span of time
# ----------------------------

class SpanGenerator:
    """Everything the market side writes for [t0, t1): quotes at the per-contract
    tick rates, and the rising-edge tables (marks, settlements, vols, model
    prices, day-ahead) whenever a boundary falls inside the span. Pure function
    of the timestamp and the shared model, so any process can own any span."""

    def __init__(self, mkt: MarketModel, story: StoryClock, seed: int, scale: float):
        self.mkt = mkt
        self.story = story
        self.seed = seed
        self.scale = scale
        self.l_outage = mkt.l_exchange == OUTAGE_EXCHANGE
        self.model_sel = np.where(np.isin(mkt.curve, ["TTF", "NBP", "UKPWR"]) & mkt.is_month & (mkt.rank < 24))[0]
        self.bad_quarter = int(np.where((mkt.curve == "TTF") & (mkt.gran == "Q"))[0][
            np.argmin(mkt.dstart[(mkt.curve == "TTF") & (mkt.gran == "Q")])])
        ttf_m = np.where((mkt.curve == "TTF") & mkt.is_month)[0]
        self.bad_iv_contract = int(ttf_m[np.argsort(mkt.dstart[ttf_m])][3])

    def _rng(self, *key):
        return np.random.default_rng([self.seed, 9, *[int(k) for k in key]])

    # ---- quotes
    def quotes(self, t0_ns: int, t1_ns: int) -> pd.DataFrame:
        mkt = self.mkt
        s0, s1 = t0_ns // NS, (t1_ns - 1) // NS + 1
        secs = np.arange(s0, s1)
        rng = self._rng(t0_ns // 1_000_000, 1)
        frac = np.ones(len(secs))
        lo = np.full(len(secs), 0, dtype=np.int64)
        hi = np.full(len(secs), NS, dtype=np.int64)
        lo[0] = t0_ns - s0 * NS
        hi[-1] = t1_ns - (s1 - 1) * NS
        frac = (hi - lo) / NS
        tod_oil = tod_weight(secs, "OIL")
        tod_gas = tod_weight(secs, "GAS")
        tod = np.where(mkt.cx[None, :] == "OIL", tod_oil[:, None], tod_gas[:, None])
        # One independent stream per listing: the secondary venue ticks at a
        # fraction of the primary's rate, and nothing during its feed outage.
        lci = mkt.l_ci
        lam = (mkt.tick_rate[lci][None, :] * tod[:, lci] * mkt.l_rate[None, :]) * self.scale * frac[:, None]
        outage = (secs >= self.story.outage_start) & (secs < self.story.outage_end)
        lam[np.ix_(outage, self.l_outage)] = 0.0
        counts = rng.poisson(lam)
        si = np.repeat(np.arange(len(secs)), counts.sum(axis=1))
        li = np.concatenate([np.repeat(np.arange(mkt.nl), counts[i]) for i in range(len(secs))]) if si.size else np.zeros(0, int)
        frames = []
        if si.size:
            ci = lci[li]
            prim = mkt.l_primary[li]
            u = rng.random(len(si))
            ts = secs[si] * NS + lo[si] + (u * (hi[si] - lo[si])).astype(np.int64)
            # Fair value at the tick time, interpolated within the second; a
            # secondary venue quotes the fair value of 50 to 300 ms earlier.
            lag = np.where(prim, 0.0, rng.uniform(*SECONDARY_LAG_S, len(si)))
            t_eff = (ts - s0 * NS) / NS - lag
            grid = mkt.fair(np.arange(s0 - 1, s1 + 1))
            pos = np.clip(t_eff + 1.0, 0.0, len(grid) - 1.000001)
            k = np.floor(pos).astype(np.int64)
            w = pos - k
            fair = grid[k, ci] * (1 - w) + grid[np.minimum(k + 1, len(grid) - 1), ci] * w
            mid = fair + mkt.listing_offset(li, s0 + t_eff) + rng.uniform(-0.3, 0.3, len(si)) * mkt.tick[ci]
            bid, ask = mkt.bbo(mid, ci, tod[si, ci], mkt.l_spread[li])
            lot_size = mkt.inst.lot_size.to_numpy()[ci]
            lots = np.exp(np.log(np.where(mkt.rank[ci] < 3, 10.0, 3.0)) + 0.7 * rng.standard_normal(len(si)))
            bsz = np.maximum(1, np.round(lots))
            asz = np.maximum(1, np.round(lots * np.exp(0.5 * rng.standard_normal(len(si)))))
            sec_idx = np.where(~prim)[0]
            if len(sec_idx):
                tsec = secs[si[sec_idx]]
                bsz[sec_idx] = mkt.displayed_lots(li[sec_idx], tsec, -1)
                asz[sec_idx] = mkt.displayed_lots(li[sec_idx], tsec, 1)
            frames.append(pd.DataFrame({
                "ts": ts, "symbol": mkt.sym[ci], "curve": mkt.curve[ci], "bid": bid, "ask": ask,
                "bid_size": bsz * lot_size, "ask_size": asz * lot_size, "source": mkt.l_exchange[li]}))
        # FX draws from its own stream, so adding or removing a quote stream
        # leaves the FX ticks, and every USD conversion, exactly as they were.
        fx = mkt.fx_at(secs)
        rng = self._rng(t0_ns // 1_000_000, 6)
        for pair in FX_PAIRS:
            cnt = rng.poisson(FX_TICK_RATE * tod_gas * self.scale * frac)
            fi = np.repeat(np.arange(len(secs)), cnt)
            if not fi.size:
                continue
            m = fx[pair][fi] * (1 + rng.normal(0, 0.00002, len(fi)))
            half = 0.00005
            ts = secs[fi] * NS + lo[fi] + (rng.random(len(fi)) * (hi[fi] - lo[fi])).astype(np.int64)
            frames.append(pd.DataFrame({
                "ts": ts, "symbol": pair, "curve": "FX", "bid": np.round(m - half, 5), "ask": np.round(m + half, 5),
                "bid_size": np.round(rng.uniform(1, 5, len(fi))) * 1_000_000.0,
                "ask_size": np.round(rng.uniform(1, 5, len(fi))) * 1_000_000.0, "source": "FX_FEED"}))
        if not frames:
            return pd.DataFrame(columns=["ts", "symbol", "curve", "bid", "ask", "bid_size", "ask_size", "source"])
        return pd.concat(frames, ignore_index=True)

    # ---- rising-edge tables
    def minute_tables(self, minute_secs: np.ndarray, emitter: SortedEmitter):
        mkt, s = self.mkt, self.story
        if not len(minute_secs):
            return
        fair = mkt.fair(minute_secs)
        n = len(minute_secs)
        tod = tod_weight(minute_secs, "GAS")
        # curve_marks: every contract every minute. MARKET from the best
        # available venue (expected at least one quote in 5 minutes, feed up):
        # the primary if it qualifies, else the secondary; INTERP from the
        # model if neither does. venue records which listing the mark came from.
        px = np.vstack([quantize(fair[i], mkt.tick, 6) for i in range(n)])
        px = np.round(px, 6)
        in_outage = ((minute_secs >= s.outage_start) & (minute_secs < s.outage_end))[:, None]
        expected = mkt.tick_rate[None, :] * 300.0 * tod[:, None] * self.scale
        pl = mkt.primary_listing
        p_ok = (expected * mkt.l_rate[pl][None, :] >= 1.0) & ~(in_outage & self.l_outage[pl][None, :])
        sl = mkt.secondary_listing
        has2 = sl >= 0
        s_ok = has2[None, :] & (expected * np.where(has2, mkt.l_rate[np.maximum(sl, 0)], 0.0)[None, :] >= 1.0) \
            & ~(in_outage & np.where(has2, self.l_outage[np.maximum(sl, 0)], False)[None, :])
        market = p_ok | s_ok
        src = np.where(market, "MARKET", "INTERP")
        venue = np.where(p_ok, mkt.l_exchange[pl][None, :],
                         np.where(s_ok, np.where(has2, mkt.l_exchange[np.maximum(sl, 0)], None)[None, :], None)).astype(object)
        by = np.full((n, mkt.n), "CURVE_SVC", dtype=object)
        bad = ((minute_secs >= s.bad_mark_start) & (minute_secs < s.bad_mark_end))[:, None] \
            & (np.arange(mkt.n) == self.bad_quarter)[None, :]
        px = np.where(bad, np.round(px + 2.5, 6), px)
        src = np.where(bad, "MANUAL", src)
        by = np.where(bad, "trader_11", by)
        venue = np.where(bad, None, venue)
        ts = np.repeat(minute_secs, mkt.n) * NS
        emitter.emit("curve_marks", pd.DataFrame({
            "ts": ts, "curve": np.tile(mkt.curve, n), "symbol": np.tile(mkt.sym, n), "price": px.ravel(),
            "source": src.ravel(), "venue": venue.ravel(), "version": np.full(n * mkt.n, 1, dtype=np.int32),
            "marked_by": by.ravel()}))
        if np.any(minute_secs == s.bad_mark_end):
            # 8.2 correction: version 2 for every bad minute, same ts, by the curve service.
            bad_secs = np.arange(s.bad_mark_start, s.bad_mark_end, 60)
            good = mkt.fair(bad_secs)[:, self.bad_quarter]
            emitter.emit("curve_marks", pd.DataFrame({
                "ts": bad_secs * NS, "curve": "TTF", "symbol": mkt.sym[self.bad_quarter],
                "price": quantize(good, mkt.tick[self.bad_quarter], 6), "source": "MARKET",
                "venue": mkt.l_exchange[mkt.primary_listing[self.bad_quarter]],
                "version": np.full(len(bad_secs), 2, dtype=np.int32), "marked_by": "CURVE_SVC"}))
        # model_prices: champion (never refits) and challenger (hourly refit).
        sel = self.model_sel
        champ = mkt.fair(minute_secs, "champion")[:, sel]
        chal = mkt.fair(minute_secs, "challenger")[:, sel]
        rng = self._rng(minute_secs[0], 2)
        champ = champ * (1 + rng.normal(0, 0.0010, champ.shape))
        chal = chal * (1 + rng.normal(0, 0.0010, chal.shape))
        k = len(sel)
        tsm = np.repeat(minute_secs, k) * NS
        inputs = tsm - 500_000_000
        # Models price off the primary venue; its inputs go stale only if that
        # venue's feed is the one down.
        stale = ((minute_secs >= s.outage_start) & (minute_secs < s.outage_end))[:, None] \
            & self.l_outage[mkt.primary_listing[sel]][None, :]
        inputs = np.where(stale.ravel(), s.outage_start * NS - 500_000_000, inputs)
        frames = []
        for name, m in (("champion_v1", champ), ("challenger_v2", chal)):
            frames.append(pd.DataFrame({
                "ts": tsm, "model_version": name, "curve": np.tile(mkt.curve[sel], n),
                "symbol": np.tile(mkt.sym[sel], n), "model_px": np.round(m.ravel(), 6), "inputs_ts": inputs}))
        emitter.emit("model_prices", pd.concat(frames, ignore_index=True))
        # settlements: 16:30 UTC gas, power and carbon; 19:30 UTC oil.
        for i, sec in enumerate(minute_secs):
            hhmm = sec % 86400
            if hhmm not in (16 * 3600 + 1800, 19 * 3600 + 1800):
                continue
            if ((sec // 86400) + 3) % 7 >= 5:
                continue   # exchanges do not settle on weekends
            oil = hhmm == 19 * 3600 + 1800
            sel_s = np.where((mkt.cx == "OIL") == oil)[0]
            r = self._rng(sec, 3)
            vol = np.round(np.exp(np.log(np.maximum(mkt.tick_rate[sel_s], 0.05) * 400) + 0.3 * r.standard_normal(len(sel_s)))) \
                * mkt.inst.lot_size.to_numpy()[sel_s]
            oi = np.round(np.exp(np.log(np.maximum(mkt.tick_rate[sel_s], 0.05) * 20000) + 0.2 * r.standard_normal(len(sel_s)))) \
                * mkt.inst.lot_size.to_numpy()[sel_s]
            emitter.emit("settlements", pd.DataFrame({
                "ts": np.full(len(sel_s), sec * NS), "curve": mkt.curve[sel_s], "symbol": mkt.sym[sel_s],
                "price": quantize(fair[i, sel_s], mkt.tick[sel_s], 6), "volume": vol, "open_interest": oi,
                "source": "EXCHANGE"}))
        # iv_marks every 5 minutes; 8.7 bad ATM mark and its correction.
        fives = minute_secs[minute_secs % 300 == 0]
        for sec in fives:
            frames = []
            r = self._rng(sec, 4)
            for curve in IV_PARAMS:
                idx, tau, iv = mkt.iv_surface(int(sec), curve)
                iv = iv + r.normal(0, 0.002, iv.shape)
                # A vol service publishes an arbitrage-free surface: total
                # variance iv^2 * tau must not fall with expiry at any delta, so
                # the noise is cleaned up before publication. Only the planted
                # bad mark below breaks it.
                var = np.maximum.accumulate(iv ** 2 * tau[:, None], axis=0)
                iv = np.sqrt(var / tau[:, None])
                if curve == "TTF" and s.bad_iv_start <= sec < s.bad_iv_end:
                    row = np.where(idx == self.bad_iv_contract)[0]
                    if row.size:
                        iv[row[0], 2] -= 0.08
                frames.append(pd.DataFrame({
                    "ts": np.full(len(idx) * 5, sec * NS), "curve": curve, "symbol": np.repeat(mkt.sym[idx], 5),
                    "delta_bucket": np.tile(DELTA_BUCKETS, len(idx)), "tau": np.repeat(np.round(tau, 6), 5),
                    "iv": np.round(iv.ravel(), 5), "source": "VOL_SVC", "version": np.full(len(idx) * 5, 1, dtype=np.int32)}))
            emitter.emit("iv_marks", pd.concat(frames, ignore_index=True))
            if sec == s.bad_iv_end:
                rows = []
                for bsec in range(s.bad_iv_start, s.bad_iv_end, 300):
                    rr = self._rng(bsec, 4)
                    idx, tau, iv = mkt.iv_surface(int(bsec), "TTF")
                    # Same draw order as the original stamp, so the corrected row is the
                    # mark as it should have been published.
                    for curve in IV_PARAMS:
                        noise = rr.normal(0, 0.002, iv.shape)
                        if curve == "TTF":
                            iv_fixed = iv + noise
                            var = np.maximum.accumulate(iv_fixed ** 2 * tau[:, None], axis=0)
                            iv_fixed = np.sqrt(var / tau[:, None])
                    row = np.where(idx == self.bad_iv_contract)[0][0]
                    rows.append([bsec * NS, "TTF", mkt.sym[self.bad_iv_contract], "ATM", round(float(tau[row]), 6),
                                 round(float(iv_fixed[row, 2]), 5), "VOL_SVC", 2])
                emitter.emit("iv_marks", pd.DataFrame(rows, columns=["ts", "curve", "symbol", "delta_bucket", "tau", "iv", "source", "version"]))
        # da_prices: at 12:45 the next day's 24 hours are published.
        for sec in minute_secs[minute_secs % 86400 == 12 * 3600 + 2700]:
            emitter.emit("da_prices", self.day_ahead(int(sec) - 12 * 3600 - 2700 + 86400, int(sec)))

    def day_ahead(self, day_sec: int, published_sec: int) -> pd.DataFrame:
        """UK day-ahead hourly prices for the day starting at day_sec, shaped by
        a load profile around the power front month. The windy night clears negative."""
        mkt, s = self.mkt, self.story
        r = self._rng(day_sec, 5)
        level = float(mkt.fair(np.array([published_sec]))[0, mkt.front["UKPWR"]])
        hours = np.arange(24)
        profile = 0.65 + 0.45 * np.exp(-((hours - 8) / 2.5) ** 2) + 0.70 * np.exp(-((hours - 18) / 2.0) ** 2)
        price = level * profile * (1 + r.normal(0, 0.05, 24)) - 12.0 * np.maximum(0, np.sin(math.pi * (hours - 7) / 11))
        day = datetime.datetime.fromtimestamp(day_sec, UTC).date()
        if day == s.windy_day:
            price[1:6] = -r.uniform(5, 40, 5)
        return pd.DataFrame({
            "ts": (day_sec + hours * 3600) * NS, "market": "UK_DA", "price": np.round(price, 2),
            "volume": np.round(r.uniform(18000, 32000, 24) * profile), "source": "N2EX",
            "published_ts": np.full(24, published_sec * NS)})

    # ---- a whole span
    def generate(self, t0_ns: int, t1_ns: int, emitter: SortedEmitter, desk: dict):
        emitter.emit("quotes", self.quotes(t0_ns, t1_ns))
        s0 = -(-t0_ns // MINUTE_NS) * 60
        s1 = -(-t1_ns // MINUTE_NS) * 60
        if s1 > s0:
            self.minute_tables(np.arange(s0, s1, 60), emitter)
        for table, col in (("fills", "ts"), ("trade_events", "booked_ts"), ("position_snapshots", "ts"),
                           ("quotes", "ts")):
            df = desk.get(table)
            if df is not None and len(df):
                sel = df[(df[col] >= t0_ns) & (df[col] < t1_ns)]
                if len(sel):
                    emitter.emit(table, sel)


# ----------------------------
# Static rows
# ----------------------------

def instruments_df(inst: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "ts": np.zeros(len(inst), dtype=np.int64), "symbol": inst.symbol,
        "curve": inst.curve, "complex": inst["complex"], "granularity": inst.granularity,
        "delivery_start": pd.to_datetime(inst.delivery_start).astype("datetime64[ns]").astype("int64"),
        "delivery_end": pd.to_datetime(inst.delivery_end).astype("datetime64[ns]").astype("int64"),
        "hours": inst.hours.astype(np.int32), "days": inst.days.astype(np.int32),
        "expiry": pd.to_datetime(inst.expiry).astype("datetime64[ns]").astype("int64"),
        "term_code": inst.term_code, "month_code": inst.month_code, "unit": inst.unit,
        "ccy": inst.ccy, "px_factor": inst.px_factor.astype(float), "fx_symbol": inst.fx_symbol,
        "to_mwh": inst.to_mwh.astype(float)})


def listings_df(lst: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "ts": np.zeros(len(lst), dtype=np.int64), "symbol": lst.symbol, "exchange": lst.exchange,
        "mic": lst.mic, "exchange_code": lst.exchange_code,
        "exchange_physical_code": lst.exchange_physical_code.astype(object).where(lst.exchange_physical_code.notna(), None),
        "exchange_symbol": lst.exchange_symbol.astype(object), "ccp": lst.ccp,
        "lot_size": lst.lot_size.astype(float), "tick_size": lst.tick_size.astype(float),
        "is_primary": lst.is_primary.astype(bool), "liquidity_share": lst.liquidity_share.astype(float)})


def limits_df() -> pd.DataFrame:
    rows = [[0, b, c, float(v), CURVES[c]["unit"], "risk_control"] for (b, c), v in LIMITS.items()]
    return pd.DataFrame(rows, columns=["ts", "book", "curve", "max_abs_qty", "unit", "approved_by"])


# ----------------------------
# Backpressure (WAL)
# ----------------------------

def wal_monitor(args, pause_event, processes, interval=5, prefix=""):
    threshold = 3 * processes
    last_logged_paused = False
    tbl = table_name("quotes", prefix)
    with connect_qwp(args) as conn:
        while True:
            try:
                df = query_df(conn, "SELECT sequencerTxn, writerTxn FROM wal_tables() WHERE name = $1", [tbl])
                if not df.empty:
                    seq = int(df["sequencerTxn"].iloc[0])
                    wrt = int(df["writerTxn"].iloc[0])
                    lag = seq - wrt
                    if lag > threshold:
                        pause_event.set()
                        if not last_logged_paused:
                            print(f"[WAL] Pause: sequencerTxn={seq}, writerTxn={wrt}, lag={lag}", flush=True)
                            last_logged_paused = True
                    elif last_logged_paused:
                        if seq == wrt:
                            time.sleep(interval)
                            print(f"[WAL] Resume: sequencerTxn={seq}, writerTxn={wrt}, lag={lag}", flush=True)
                            pause_event.clear()
                            last_logged_paused = False
            except Exception as e:
                print(f"[WAL] Monitor error: {e}", flush=True)
            time.sleep(interval)


def wait_if_paused(pause_event: Event, pid: int):
    while pause_event.is_set():
        print(f"[WORKER {pid}] Paused due to WAL lag...", flush=True)
        time.sleep(5)


# ----------------------------
# Backfill worker
# ----------------------------

def ingest_worker(args, mkt, story, desk, slices, process_idx, pause_event):
    """Generate every table for a contiguous run of hour slices, in sub-chunks
    of --chunk_seconds, and send each sub-chunk as it completes."""
    gen = SpanGenerator(mkt, story, args.seed, args.scale_factor)
    sender_id = sender_tag(args, f"w{process_idx}")
    buffer_rows = 200_000
    with connect_qwp(args, sender_id=sender_id, auto_flush_interval=2000) as db, db.sender() as sender:
        emitter = SortedEmitter(sender, args.prefix, args.table_set, buffer_rows)
        t_start = time.time()
        total = sum(e - b for b, e in slices)
        done = 0
        for (b_ns, e_ns) in slices:
            t = b_ns
            while t < e_ns:
                wait_if_paused(pause_event, process_idx)
                t_next = min(t + args.chunk_seconds * NS, e_ns)
                gen.generate(t, t_next, emitter, desk)
                emitter.flush_all()
                done += t_next - t
                t = t_next
                el = time.time() - t_start
                print(f"[WORKER {process_idx}] {ns_to_iso(t)} {100 * done / total:5.1f}% "
                      f"quotes={emitter.sent.get('quotes', 0):,} {el:,.0f}s", flush=True)
        try:
            emitter.flush_all()
            sender.flush(wait=True)
        except QuestDBError as e:
            print(f"[WORKER {process_idx}] Drain failed: {e}", flush=True)
    print(f"[WORKER {process_idx}] Done: " + ", ".join(f"{k}={v:,}" for k, v in sorted(emitter.sent.items())), flush=True)


# ----------------------------
# Query pack variables, state file
# ----------------------------

def update_demo_sql(path, values):
    """Point the @name := '...' variables in demo.sql at the data just generated."""
    if not os.path.exists(path):
        print(f"({path} not found, skipping variable update)")
        return
    text = open(path).read()
    for name, value in values.items():
        text = re.sub(rf"(@{name}\s*:=\s*)'[^']*'", lambda m: f"{m.group(1)}'{value}'", text)
    open(path, "w").write(text)
    print(f"Updated {path}: " + ", ".join(f"@{k}={v}" for k, v in values.items()))


def demo_variables(mkt: MarketModel, story: StoryClock, end_ns: int) -> dict:
    iso = lambda sec: datetime.datetime.fromtimestamp(sec, UTC).strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    nth = lambda c, g, k=0: mkt.sym[sorted(np.where((mkt.curve == c) & (mkt.gran == g))[0], key=lambda i: mkt.dstart[i])[k]]
    asof = story.day_start + 12 * 3600
    t2 = min(end_ns // NS // 60 * 60, story.day_start + 15 * 3600)
    if t2 <= asof:
        t2 = asof + 1800
    return {
        "demo_day": story.demo_day.isoformat(), "asof": iso(asof), "t1": iso(asof), "t2": iso(t2),
        "bad_mark_start": iso(story.bad_mark_start), "bad_mark_end": iso(story.bad_mark_end),
        "outage_start": iso(story.outage_start), "outage_end": iso(story.outage_end),
        "iv_start": iso(story.bad_iv_start), "iv_end": iso(story.bad_iv_end), "repricing": iso(story.repricing),
        "windy_day": story.windy_day.isoformat(),
        "brent": nth("BRENT", "M"), "wti": nth("WTI", "M"), "gasoil": nth("GASOIL", "M"),
        "ttf": nth("TTF", "M"), "nbp": nth("NBP", "M"), "jkm": nth("JKM", "M"), "pwr": nth("UKPWR", "M"),
        "uka": nth("UKA", "Z"), "eua": nth("EUA", "Z"), "ttf_q1": nth("TTF", "Q"), "pwr_win": nth("UKPWR", "S", 1),
        "ttf_m4": nth("TTF", "M", 3),
    }


def save_state(path: str, mkt: MarketModel, planner: DeskPlanner, desk: dict, t_last_ns: int, anchors: dict):
    """Only what cannot be recovered from the database: RNG states, the factor
    path tail, the desk's running state and rows planned but not yet due."""
    keep = FORESIGHT_MIN + 40
    tail = mkt.path[-keep:]
    state = {
        "version": 1, "seed": mkt.seed, "demo_day": mkt.story.demo_day.isoformat(), "t_last_ns": int(t_last_ns),
        "anchors": anchors, "m0": mkt.m0 + len(mkt.path) - len(tail), "path": tail,
        "mkt_rng": mkt.rng.bit_generator.state,
        "planner": {
            "rng": planner.rng.bit_generator.state, "route_rng": planner.route_rng.bit_generator.state,
            "pos": planner.pos, "pos_fills": planner.pos_fills,
            "effects": [e for e in planner.effects if e[0] > t_last_ns / NS - 86400], "settle": planner.settle,
            "targets": planner.targets, "scripted": planner.scripted, "planned_through": planner.planned_through,
            "pending": {k: v for k, v in desk.items()},
        },
    }
    tmp = path + ".tmp"
    with open(tmp, "wb") as fh:
        pickle.dump(state, fh)
    os.replace(tmp, path)


def load_state(path: str):
    with open(path, "rb") as fh:
        return pickle.load(fh)


def restore_from_state(state: dict, mkt: MarketModel, planner: DeskPlanner):
    mkt.m0 = state["m0"]
    mkt.path = np.array(state["path"])
    mkt.rng.bit_generator.state = state["mkt_rng"]
    p = state["planner"]
    planner.rng.bit_generator.state = p["rng"]
    planner.pos = p["pos"]
    if "route_rng" in p:
        planner.route_rng.bit_generator.state = p["route_rng"]
    planner.pos_fills = {}
    for key, q in p.get("pos_fills", p["pos"]).items():
        # State files from before listings keyed fills by (book, symbol).
        if len(key) == 2:
            key = (key[0], key[1], mkt.l_exchange[mkt.primary_listing[mkt.sym_idx[key[1]]]])
        planner.pos_fills[key] = q
    planner.effects = [e if len(e) == 5 else (e[0], e[1], e[2], OTC_VENUE, e[3]) for e in p["effects"]]
    planner.settle = p["settle"]
    planner.targets = p["targets"]
    planner.scripted = p["scripted"]
    planner.planned_through = p["planned_through"]
    return p["pending"]


def restore_from_db(args, prefix: str, mkt: MarketModel, planner: DeskPlanner, start_sec: int):
    """No state file: rebuild the state from what the database holds. Last mid
    per contract gives the level factors (S restarts at zero and the pull to
    the live anchor does the rest); positions come from the last snapshot plus
    today's fills and non-cancelled bookings, all per venue."""
    t = lambda n: table_name(n, prefix)
    with connect_qwp(args) as conn:
        primaries = ", ".join(f"'{e}'" for e in sorted({v[0][0] for v in LISTINGS.values()} | {"FX_FEED"}))
        # Every read is bounded in time: the last day of quotes, snapshots of the
        # last three days, today's fills and bookings, the last week of settlements.
        day_start = start_sec - start_sec % 86400
        since = lambda days: ns_to_iso((start_sec - days * 86400) * NS)
        mids = query_df(conn, f"SELECT symbol, mid(bid, ask) AS mid FROM {t('quotes')} "
                              f"WHERE source IN ({primaries}) AND ts >= '{since(1)}' LATEST ON ts PARTITION BY symbol")
        mid = dict(zip(mids.symbol.astype(str), mids["mid"].astype(float)))
        snaps = query_df(conn, f"SELECT book, symbol, venue, qty FROM {t('position_snapshots')} "
                               f"WHERE ts = (SELECT max(ts) FROM {t('position_snapshots')} WHERE ts >= '{since(3)}')")
        fills = query_df(conn, f"SELECT book, symbol, venue, sum(qty) AS qty FROM {t('fills')} "
                               f"WHERE ts >= '{ns_to_iso(day_start * NS)}' GROUP BY book, symbol, venue")
        # Latest version of each trade among today's bookings: a trade whose
        # latest booking is today, which is what the filter after LATEST ON asked.
        deals = query_df(conn, f"SELECT book, symbol, sum(qty) AS qty FROM ("
                               f"(SELECT * FROM {t('trade_events')} WHERE booked_ts >= '{ns_to_iso(day_start * NS)}' "
                               f"LATEST ON booked_ts PARTITION BY trade_id) "
                               f"WHERE status != 'CANCELLED' AND channel != 'EXCH'"
                               f") GROUP BY book, symbol")
        settle = query_df(conn, f"SELECT symbol, price FROM {t('settlements')} WHERE ts >= '{since(7)}' "
                                f"LATEST ON ts PARTITION BY symbol")
    live = dict(mkt.anchors)
    for c in BASE_CURVES + ["EURUSD", "GBPUSD"]:
        key = mkt.sym[mkt.front[c]] if c in BASE_CURVES else c
        if key in mid:
            live[c] = mid[key]
    saved = mkt.anchors
    mkt.anchors = live
    mkt.init_path(start_sec // 60)
    mkt.anchors = saved
    planner.pos = {}
    planner.pos_fills = {}
    # The snapshot is the base position per venue (OTC included); today's fills
    # and today's voice bookings move it.
    for df in (snaps, fills):
        for r in df.itertuples():
            planner._add_pos(str(r.book), str(r.symbol), float(r.qty), str(r.venue))
    for r in deals.itertuples():
        planner._add_pos(str(r.book), str(r.symbol), float(r.qty), OTC_VENUE)
    planner.effects = []
    planner.settle = {str(r.symbol): float(r.price) for r in settle.itertuples()}
    planner.planned_through = start_sec // 60 - 1
    print(f"[INFO] State rebuilt from the database: {len(mid)} quoted contracts, "
          f"{len(planner.pos)} open positions.", flush=True)


# ----------------------------
# Modes
# ----------------------------

def write_static_rows(args, mkt: MarketModel, planner: DeskPlanner, gen: SpanGenerator, start_ns: int):
    """Reference data and the cheat sheet, plus the first day's day-ahead
    prices (published before the window opened, so no slice owns them)."""
    with connect_qwp(args, sender_id=sender_tag(args, "static"), auto_flush_interval=1000) as db, db.sender() as sender:
        emitter = SortedEmitter(sender, args.prefix, args.table_set, 1_000_000)
        emitter.emit("instruments", instruments_df(mkt.inst))
        emitter.emit("listings", listings_df(mkt.listings))
        emitter.emit("limits", limits_df())
        emitter.emit("demo_events", demo_events_df(planner.demo_events))
        day_sec = start_ns // NS - (start_ns // NS) % 86400
        emitter.emit("da_prices", gen.day_ahead(day_sec, day_sec - 86400 + 12 * 3600 + 2700))
        emitter.flush_all()
        sender.flush(wait=True)
    print("[INFO] Static rows written: instruments, limits, demo_events, day-ahead for the first day.", flush=True)


def hour_slices(start_ns: int, end_ns: int):
    out = []
    t = start_ns
    while t < end_ns:
        nxt = min((t // (3600 * NS) + 1) * 3600 * NS, end_ns)
        out.append((t, nxt))
        t = nxt
    return out


def run_backfill(args, start_ns: int, end_ns: int, anchors: dict, story: StoryClock, resume_from_ns: Optional[int]):
    inst = build_instruments(ns_to_dt(start_ns))
    mkt = MarketModel(inst, anchors, args.seed, story, args.winter_repricing_pct / 100.0)
    m0 = start_ns // MINUTE_NS
    m_end = -(-end_ns // MINUTE_NS)
    print(f"[INFO] Simulating {m_end - m0} minutes of factor path for {len(inst)} contracts.", flush=True)
    mkt.init_path(m0)
    mkt.extend_to(m_end + FORESIGHT_MIN + 2)
    mkt.pin_end_to_anchors(m_end)
    planner = DeskPlanner(mkt, story, args.seed, args.scale_factor)
    planner.initial_positions(m0 * 60)
    t0 = time.time()
    for m in range(m0, m_end):
        planner.plan_minute(m)
        if (m - m0) % 720 == 0:
            print(f"[PLAN] {ns_to_iso(m * MINUTE_NS)} fills={len(planner.fills):,} bookings={len(planner.events):,}", flush=True)
    desk = planner.take_rows()
    print(f"[PLAN] Desk activity planned in {time.time() - t0:.0f}s: {len(desk['fills']):,} fills, "
          f"{len(desk['trade_events']):,} booking rows, {len(desk['position_snapshots']):,} snapshot rows, "
          f"{len(desk['quotes']):,} secondary-venue book quotes.", flush=True)
    for t, act, what in planner.demo_events:
        when = t.isoformat() if isinstance(t, datetime.date) else ns_to_iso(t * NS)
        print(f"[STORY] {when}  {act:12s} {what}", flush=True)

    gen = SpanGenerator(mkt, story, args.seed, args.scale_factor)
    write_static_rows(args, mkt, planner, gen, start_ns)

    gen_start = start_ns if resume_from_ns is None else max(start_ns, resume_from_ns)
    slices = hour_slices(gen_start, end_ns)
    per = [[] for _ in range(args.processes)]
    for i, sl in enumerate(slices):
        per[min(i * args.processes // len(slices), args.processes - 1)].append(sl)
    pause_event = Event()
    wal_proc = mp.Process(target=wal_monitor, args=(args, pause_event, args.processes), kwargs={"prefix": args.prefix})
    wal_proc.start()
    procs = []
    for i in range(args.processes):
        if not per[i]:
            continue
        print(f"[INFO] Worker {i}: {ns_to_iso(per[i][0][0])} to {ns_to_iso(per[i][-1][1])} ({len(per[i])} slices)", flush=True)
        w = mp.Process(target=ingest_worker, args=(args, mkt, story, desk, per[i], i, pause_event))
        w.start()
        procs.append(w)
    for w in procs:
        w.join()
    wal_proc.terminate()
    wal_proc.join()
    failed = [w.exitcode for w in procs if w.exitcode != 0]
    if failed:
        print(f"ERROR: {len(failed)} worker(s) exited with {failed}", flush=True)
        sys.exit(1)
    # Rows planned past end_ts (late bookings, amendments) go to the state file
    # so a following real-time run emits them at their time.
    pending = {k: v[v[TABLE_TS[k]] >= end_ns] for k, v in desk.items()}
    save_state(args.state_file, mkt, planner, pending, end_ns, anchors)
    print(f"[INFO] State saved to {args.state_file} ({sum(len(v) for v in pending.values())} pending desk rows).", flush=True)
    if args.demo_sql:
        update_demo_sql(args.demo_sql, demo_variables(mkt, story, end_ns))
    print("[INFO] Backfill completed.", flush=True)


def run_realtime(args, start_ns: int, end_ns: Optional[int], anchors: dict, story: StoryClock,
                 state: Optional[dict], prefix: str):
    inst = build_instruments(ns_to_dt(start_ns))
    mkt = MarketModel(inst, anchors, args.seed, story, args.winter_repricing_pct / 100.0)
    planner = DeskPlanner(mkt, story, args.seed, args.scale_factor)
    if state is not None:
        desk = restore_from_state(state, mkt, planner)
        t = max(start_ns, state["t_last_ns"])
        print(f"[INFO] Resumed from {args.state_file}: path through {ns_to_iso((mkt.m0 + len(mkt.path) - 1) * MINUTE_NS)}, "
              f"{sum(len(v) for v in desk.values())} pending desk rows.", flush=True)
    else:
        t = start_ns
        desk = planner.take_rows()
        if args.incremental:
            restore_from_db(args, prefix, mkt, planner, t // NS)
        else:
            mkt.init_path(t // NS // 60)
            planner.initial_positions(t // NS)
            desk = planner.take_rows()
    mkt.pull_to_anchor = True
    gen = SpanGenerator(mkt, story, args.seed, args.scale_factor)
    if state is None and not args.incremental:
        write_static_rows(args, mkt, planner, gen, t)
    else:
        # Idempotent thanks to DEDUP; keeps the cheat sheet and instruments current.
        write_static_rows(args, mkt, planner, gen, t)

    def plan_to(t1_ns):
        nonlocal desk
        last = -(-t1_ns // MINUTE_NS) - 1
        if planner.planned_through is None:
            planner.planned_through = t // NS // 60 - 1
        changed = False
        while planner.planned_through < last:
            planner.plan_minute(planner.planned_through + 1)
            changed = True
        if changed:
            new = planner.take_rows()
            desk = {k: pd.concat([desk[k], v], ignore_index=True) if k in desk else v for k, v in new.items()}

    slice_ns = args.realtime_slice_ms * 1_000_000
    ahead_ns = 2 * NS
    last_refresh = time.time()
    last_save = time.time()
    pause_event = Event()
    with connect_qwp(args, sender_id=sender_tag(args, "rt"), auto_flush_interval=200) as db, db.sender() as sender:
        emitter = SortedEmitter(sender, args.prefix, args.table_set, 50_000)
        try:
            while True:
                if end_ns is not None and t >= end_ns:
                    print(f"[INFO] Reached --end_ts {ns_to_iso(end_ns)}.", flush=True)
                    break
                wall = time.time_ns() + ahead_ns
                if end_ns is not None:
                    wall = min(wall, end_ns)
                if wall - t > 5 * NS:
                    t1 = min(t + args.chunk_seconds * NS, wall)
                    print(f"[CATCH-UP] {ns_to_iso(t)} -> {ns_to_iso(t1)}", flush=True)
                else:
                    t1 = t + slice_ns
                plan_to(t1)
                gen.generate(t, t1, emitter, desk)
                emitter.flush_all()
                for k in desk:
                    desk[k] = desk[k][desk[k][TABLE_TS[k]] >= t1]
                t = t1
                now = time.time()
                if args.yahoo_refresh_secs > 0 and now - last_refresh >= args.yahoo_refresh_secs:
                    try:
                        anchors = fetch_anchors(args.static_anchors)
                        mkt.set_anchors(anchors)
                    except Exception as e:
                        print(f"[WARN] Anchor refresh failed: {e}", flush=True)
                    last_refresh = now
                if now - last_save >= 60:
                    save_state(args.state_file, mkt, planner, desk, t, anchors)
                    mkt.trim_before(t // NS // 60 - 5)
                    planner.effects = [e for e in planner.effects if e[0] > t / NS - 2 * 86400]
                    last_save = now
                    print(f"[RT] {ns_to_iso(t)} " + ", ".join(f"{k}={v:,}" for k, v in sorted(emitter.sent.items())), flush=True)
                sleep_for = (t - ahead_ns - time.time_ns()) / 1e9
                if sleep_for > 0:
                    time.sleep(sleep_for)
        except KeyboardInterrupt:
            print("[INFO] Stopping.", flush=True)
        finally:
            emitter.flush_all()
            try:
                sender.flush(wait=True)
            except QuestDBError as e:
                print(f"[RT] Drain failed: {e}", flush=True)
            save_state(args.state_file, mkt, planner, desk, t, anchors)
            print(f"[INFO] State saved to {args.state_file} at {ns_to_iso(t)}.", flush=True)


# ----------------------------
# Main
# ----------------------------

def main():
    p = argparse.ArgumentParser(description="Energy trading desk synthetic data generator for QuestDB")
    # QWP carries both SQL and ingestion, so there is one endpoint and one
    # credential. --host accepts a comma-separated list for HA failover; list
    # the writable primary first.
    p.add_argument("--host", default="127.0.0.1",
                   help="QWP host, or comma-separated list for failover. Entries without a port default to 9000.")
    p.add_argument("--user", default="admin")
    p.add_argument("--password", default="quest")
    p.add_argument("--token", default=None, help="QWP bearer token. Takes precedence over --user/--password.")
    p.add_argument("--token_file", default=None, help="Read the bearer token from this file, keeping it off the command line.")
    p.add_argument("--qwp_tls", type=bool_arg, default=False, help="Use wss instead of ws.")
    p.add_argument("--tls_ca", default=None, help="TLS root store: os_roots, webpki_roots, or a CA bundle path.")
    p.add_argument("--tls_verify", choices=["on", "unsafe_off"], default="on",
                   help="Set unsafe_off for a cluster with a self-signed certificate.")
    p.add_argument("--durable_ack", type=bool_arg, default=False,
                   help="request_durable_ack=on, so a failover cannot lose acked rows.")
    p.add_argument("--store_forward_dir", default=os.path.join(tempfile.gettempdir(), "energy_qwp_sf"),
                   help="Base dir for per-worker store-and-forward spill.")
    p.add_argument("--enterprise", type=bool_arg, default=False,
                   help="Enterprise server: with --short_ttl, tables take a STORAGE POLICY rather than a TTL. "
                        "Parquet encodings and everything else are the same on both editions.")

    p.add_argument("--mode", choices=["real-time", "faster-than-life"], required=True)
    p.add_argument("--processes", type=int, default=3,
                   help="Backfill workers, each owning a contiguous run of hour slices. More mostly buys O3 merges.")
    p.add_argument("--start_ts", type=str, default=None, help="Backfill start (UTC ISO). Default: demo day minus 4 days, 00:00.")
    p.add_argument("--end_ts", type=str, default=None, help="Backfill end (UTC ISO). Default: now.")
    p.add_argument("--demo_day", type=str, default=None, help="YYYY-MM-DD the storyline is planted on. Default: the end day.")
    p.add_argument("--chunk_seconds", type=int, default=900, help="Seconds generated per send in backfill; bounds memory.")
    p.add_argument("--scale_factor", type=float, default=1.0, help="Multiplies every tick and fill rate (floats allowed).")
    p.add_argument("--realtime_slice_ms", type=int, default=250)
    p.add_argument("--yahoo_refresh_secs", type=int, default=300)
    p.add_argument("--static_anchors", type=bool_arg, default=False, help="Skip Yahoo and use FALLBACK_BRACKETS.")
    p.add_argument("--winter_repricing_pct", type=float, default=8.0,
                   help="Size of the planted cold-snap repricing (8.6): Nov to Feb gas and power prices step up "
                        "by this percentage at the front, damped along the curve. The champion model never adapts.")

    p.add_argument("--incremental", type=bool_arg, default=False,
                   help="Backfill: skip what the database already holds. Real-time: resume from the state file "
                        "or, failing that, from the database.")
    p.add_argument("--create_views", type=bool_arg, default=True, help="Materialized views.")
    p.add_argument("--create_live_view", type=bool_arg, default=True, help="The positions live view (beta).")
    p.add_argument("--create_plain_views", type=bool_arg, default=True, help="Plain views: ledger, tenors, instrument_master and the parameterised *_asof and *_day views.")
    p.add_argument("--tables", type=str, default="all", help="Comma-separated base tables to create and populate.")
    p.add_argument("--parquet_encodings", type=bool_arg, default=True, help="Per-column PARQUET(...) encodings in the DDL.")
    p.add_argument("--short_ttl", type=bool_arg, default=False)
    p.add_argument("--prefix", type=str, default="energy_")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--state_file", type=str, default=".energy_state.pkl")
    p.add_argument("--demo_sql", type=str, default="energy_demo_queries.sql",
                   help="Query pack whose @name := '...' lines get rewritten after a backfill; '' to skip.")
    args = p.parse_args()
    prefix = args.prefix

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
    if args.tables.strip().lower() == "all":
        args.table_set = set(ALL_TABLES)
    else:
        args.table_set = {t.strip() for t in args.tables.split(",") if t.strip()}
        unknown = args.table_set - set(ALL_TABLES)
        if unknown:
            print(f"ERROR: unknown tables {sorted(unknown)}; known: {ALL_TABLES}")
            sys.exit(1)
    if args.scale_factor <= 0:
        print("ERROR: --scale_factor must be positive.")
        sys.exit(1)

    ensure_tables_and_views(args, prefix)

    now = now_ns() // NS * NS
    if args.mode == "faster-than-life":
        end_ns = parse_ts_arg(args.end_ts) if args.end_ts else now
        demo_day = datetime.date.fromisoformat(args.demo_day) if args.demo_day else ns_to_dt(end_ns).date()
        if args.start_ts:
            start_ns = parse_ts_arg(args.start_ts)
        else:
            d = datetime.datetime(demo_day.year, demo_day.month, demo_day.day, tzinfo=UTC) - datetime.timedelta(days=4)
            start_ns = int(d.timestamp()) * NS
        start_ns = start_ns // MINUTE_NS * MINUTE_NS
        if start_ns >= end_ns:
            print("ERROR: start_ts must be before end_ts.")
            sys.exit(1)
        resume_from = None
        if args.incremental:
            with connect_qwp(args) as conn:
                latest = [get_latest_timestamp_ns(conn, table_name(t, prefix), TABLE_TS[t])
                          for t in ("quotes", "curve_marks", "fills") if t in args.table_set]
            latest = [x for x in latest if x is not None]
            if latest:
                resume_from = max(latest) + 1
                print(f"[INFO] Incremental: resuming generation from {ns_to_iso(resume_from)}.", flush=True)
                if resume_from >= end_ns:
                    print("[INFO] Nothing to do: the database already covers the window.")
                    sys.exit(0)
        story = StoryClock(demo_day)
        anchors = fetch_anchors(args.static_anchors)
        print(f"[INFO] Backfill {ns_to_iso(start_ns)} -> {ns_to_iso(end_ns)}, demo day {demo_day}, "
              f"scale {args.scale_factor}, {args.processes} workers.", flush=True)
        run_backfill(args, start_ns, end_ns, anchors, story, resume_from)
        return

    if args.start_ts:
        print("ERROR: --start_ts is not allowed in real-time mode.")
        sys.exit(1)
    state = None
    if args.incremental and os.path.exists(args.state_file):
        state = load_state(args.state_file)
        if state.get("seed") != args.seed:
            print(f"[WARN] State file seed {state.get('seed')} differs from --seed {args.seed}; using the file's path anyway.")
    start_ns = now + 2 * NS
    if args.incremental:
        with connect_qwp(args) as conn:
            latest = [get_latest_timestamp_ns(conn, table_name(t, prefix), TABLE_TS[t])
                      for t in ("quotes", "curve_marks") if t in args.table_set]
        latest = [x for x in latest if x is not None]
        if latest:
            start_ns = max(latest) + 1_000
            if state is not None:
                start_ns = max(start_ns, state["t_last_ns"])
            print(f"[INFO] Incremental: continuing from {ns_to_iso(start_ns)}.", flush=True)
        elif state is not None:
            start_ns = state["t_last_ns"]
    if args.demo_day:
        demo_day = datetime.date.fromisoformat(args.demo_day)
    elif state is not None:
        demo_day = datetime.date.fromisoformat(state["demo_day"])
    else:
        demo_day = ns_to_dt(start_ns).date()
    story = StoryClock(demo_day)
    anchors = fetch_anchors(args.static_anchors)
    print(f"[INFO] Real-time from {ns_to_iso(start_ns)}, demo day {demo_day}, scale {args.scale_factor}.", flush=True)
    end_ns = parse_ts_arg(args.end_ts) if args.end_ts else None
    run_realtime(args, start_ns, end_ns, anchors, story, state, prefix)


if __name__ == "__main__":
    main()
