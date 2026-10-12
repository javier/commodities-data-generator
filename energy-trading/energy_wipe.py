#!/usr/bin/env python3
"""Drop the whole energy trading dataset: every table, materialized view, live view
and plain view the generator creates for one prefix, and the real-time state file.

    python energy_wipe.py                                   # local, prefix energy_
    python energy_wipe.py --prefix energy_test_
    python energy_wipe.py --host 172.31.42.41:9000,172.31.41.35:9000 --qwp_tls true \\
        --tls_verify unsafe_off --token_file ~/qwp_token.txt

It drops objects by their exact names, the ones the generator creates, never by
pattern; anything else that happens to start with the prefix is listed and left
alone. It shows what exists and asks for confirmation: anything other than YES,
in capitals, aborts with nothing dropped. Connection flags are the generator's.
"""
import argparse
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import energy_trading_data_generator as gen  # noqa: E402

# Drop order: dependents before what they read.
LIVE_VIEWS = ["positions_live", "positions_live_by_venue"]
PLAIN_VIEWS = ["instrument_master", "tenors", "curve_marks_latest", "trade_events_latest", "model_graded_day",
               "marks_live", "usd_factor_asof", "strip_gaps_day", "mid_1m_day", "positions_running_day",
               "tenors_asof", "marks_asof", "fx_asof", "book_asof", "book_restated", "ledger"]
MATERIALIZED_VIEWS = ["quotes_1d", "quotes_5m", "quotes_1m", "quotes_10s", "curve_marks_1h"]
TABLES = gen.ALL_TABLES
KINDS = [("live view", "L", LIVE_VIEWS, "DROP LIVE VIEW IF EXISTS"),
         ("view", "V", PLAIN_VIEWS, "DROP VIEW IF EXISTS"),
         ("materialized view", "M", MATERIALIZED_VIEWS, "DROP MATERIALIZED VIEW IF EXISTS"),
         ("table", "T", TABLES, "DROP TABLE IF EXISTS")]
CONNECT_TIMEOUT_S = 20


def existing_objects(conn) -> dict:
    df = gen.query_df(conn, "SELECT table_name, table_type FROM tables()")
    return dict(zip(df.table_name.astype(str), df.table_type.astype(str)))


def existing_with_timeout(conn, args) -> dict:
    """The first round trip, bounded: a TLS or network problem can leave the client
    retrying inside native code, where Ctrl-C does not reach it."""
    box = {}

    def work():
        try:
            box["objects"] = existing_objects(conn)
        except Exception as e:  # surfaced below
            box["error"] = e

    t = threading.Thread(target=work, daemon=True)
    t.start()
    t.join(CONNECT_TIMEOUT_S)
    if t.is_alive():
        hint = (" If the cluster uses a self-signed certificate, add --tls_verify unsafe_off."
                if args.qwp_tls and args.tls_verify == "on" else "")
        print(f"ERROR: no answer from {args.host} in {CONNECT_TIMEOUT_S} s.{hint}", flush=True)
        os._exit(2)
    if "error" in box:
        print(f"ERROR: {box['error']}", flush=True)
        sys.exit(2)
    return box["objects"]


def main():
    p = argparse.ArgumentParser(description="Drop the energy trading dataset for one prefix, after confirmation.")
    p.add_argument("--host", default="127.0.0.1",
                   help="QWP host, or comma-separated list (primary first). Entries without a port default to 9000.")
    p.add_argument("--user", default="admin")
    p.add_argument("--password", default="quest")
    p.add_argument("--token", default=None)
    p.add_argument("--token_file", default=None)
    p.add_argument("--qwp_tls", type=gen.bool_arg, default=False)
    p.add_argument("--tls_ca", default=None)
    p.add_argument("--tls_verify", choices=["on", "unsafe_off"], default="on")
    p.add_argument("--prefix", default="energy_")
    p.add_argument("--state_file", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".energy_state.pkl"),
                   help="Real-time state file to delete with the data, so a later real-time run cannot resume "
                        "from a dataset that no longer exists.")
    p.add_argument("--keep_state_file", type=gen.bool_arg, default=False)
    args = p.parse_args()
    if args.token_file:
        with open(os.path.expanduser(args.token_file), "r", encoding="utf-8") as fh:
            args.token = fh.read().strip()
    if not args.prefix:
        print("ERROR: --prefix must not be empty.")
        sys.exit(2)

    try:
        conn = gen.connect_qwp(args)
    except Exception as e:
        hint = (" If the cluster uses a self-signed certificate, add --tls_verify unsafe_off."
                if args.qwp_tls and args.tls_verify == "on" else "")
        print(f"ERROR: cannot connect to {args.host}: {str(e).rstrip('.')}.{hint}")
        sys.exit(2)
    with conn:
        objects = existing_with_timeout(conn, args)
        plan = []
        for label, code, names, stmt in KINDS:
            for n in names:
                full = f"{args.prefix}{n}"
                if full in objects:
                    plan.append((label, full, f"{stmt} {full}"))
        ours = {full for _, full, _ in plan}
        others = sorted(n for n in objects if n.startswith(args.prefix) and n not in ours)
        state = args.state_file if (not args.keep_state_file and os.path.exists(args.state_file)) else None

        print(f"QuestDB: {args.host}    prefix: {args.prefix}")
        if not plan and not state:
            print("Nothing to drop: no energy dataset objects with this prefix.")
            if others:
                print("Left alone (not created by the generator): " + ", ".join(others))
            return
        print(f"\nAbout to drop {len(plan)} objects:")
        for label, full, _ in plan:
            print(f"  {label:18s} {full}")
        if state:
            print(f"  {'state file':18s} {state}")
        if others:
            print("\nLeft alone (start with the prefix, not created by the generator): " + ", ".join(others))
        print("\nThis cannot be undone.")
        try:
            answer = input("Type YES to drop them: ")
        except EOFError:
            answer = ""
        if answer != "YES":
            print("Aborted. Nothing was dropped.")
            sys.exit(1)

        failures = 0
        for label, full, sql in plan:
            try:
                conn.execute(sql)
                print(f"dropped {label} {full}")
            except Exception as e:
                failures += 1
                print(f"FAILED  {label} {full}: {e}")
        if state:
            os.remove(state)
            print(f"deleted state file {state}")

        remaining = sorted(n for n in existing_objects(conn) if n in ours)
        if remaining:
            print("Still present: " + ", ".join(remaining))
        else:
            print(f"Done: no {args.prefix} dataset objects remain.")
    sys.exit(1 if failures or remaining else 0)


if __name__ == "__main__":
    main()
