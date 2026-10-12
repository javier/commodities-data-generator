#!/usr/bin/env python3
"""Wipe the whole energy trading dataset for one prefix: drop every materialized view,
live view and plain view the generator creates, empty or drop its tables, and delete the
real-time state file. The next generator run recreates the views.

    python energy_wipe.py                                   # local, prefix energy_
    python energy_wipe.py --prefix energy_test_
    python energy_wipe.py --host 172.31.42.41:9000,172.31.41.35:9000 --qwp_tls true \\
        --tls_verify unsafe_off --token_file ~/qwp_token.txt

It asks two questions. First the table wipe mode: TRUNCATE (the default, anything
other than DROP) empties each table and keeps it, so its internal table id and with
it its object storage location stay the same; DROP removes the tables too. Then it
shows the plan and asks for confirmation: anything other than YES, in capitals,
aborts with nothing touched. Views of every kind are always dropped.

It acts on objects by their exact names, the ones the generator creates, never by
pattern; anything else that happens to start with the prefix is listed and left
alone. Connection flags are the generator's.
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
VIEW_KINDS = [("live view", LIVE_VIEWS, "DROP LIVE VIEW IF EXISTS"),
              ("view", PLAIN_VIEWS, "DROP VIEW IF EXISTS"),
              ("materialized view", MATERIALIZED_VIEWS, "DROP MATERIALIZED VIEW IF EXISTS")]
TABLE_MODES = {"TRUNCATE": ("truncate", "truncated", "TRUNCATE TABLE IF EXISTS"),
               "DROP": ("drop", "dropped", "DROP TABLE IF EXISTS")}
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
    p = argparse.ArgumentParser(description="Wipe the energy trading dataset for one prefix, after confirmation: "
                                            "views dropped, tables truncated (default) or dropped.")
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
        views = [(label, f"{args.prefix}{n}", f"{stmt} {args.prefix}{n}")
                 for label, names, stmt in VIEW_KINDS for n in names if f"{args.prefix}{n}" in objects]
        tables = [f"{args.prefix}{n}" for n in TABLES if f"{args.prefix}{n}" in objects]
        ours = {full for _, full, _ in views} | set(tables)
        others = sorted(n for n in objects if n.startswith(args.prefix) and n not in ours)
        state = args.state_file if (not args.keep_state_file and os.path.exists(args.state_file)) else None

        print(f"QuestDB: {args.host}    prefix: {args.prefix}")
        if not ours and not state:
            print("Nothing to wipe: no energy dataset objects with this prefix.")
            if others:
                print("Left alone (not created by the generator): " + ", ".join(others))
            return

        mode = "TRUNCATE"
        if tables:
            print("\nTRUNCATE empties each table and keeps it (same table id, same object storage location);")
            print("DROP removes the tables. Views of every kind are dropped either way.")
            try:
                answer = input("Table wipe mode, TRUNCATE or DROP [TRUNCATE]: ")
            except EOFError:
                answer = ""
            mode = "DROP" if answer.strip() == "DROP" else "TRUNCATE"
        verb, done, table_stmt = TABLE_MODES[mode]
        plan = views + [("table", full, f"{table_stmt} {full}") for full in tables]

        print(f"\nAbout to drop {len(views)} views and {verb} {len(tables)} tables:")
        for label, full, _ in views:
            print(f"  {'drop':9s}{label:18s} {full}")
        for full in tables:
            print(f"  {verb:9s}{'table':18s} {full}")
        if state:
            print(f"  {'delete':9s}{'state file':18s} {state}")
        if others:
            print("\nLeft alone (start with the prefix, not created by the generator): " + ", ".join(others))
        print("\nThis cannot be undone.")
        try:
            answer = input("Type YES to go ahead: ")
        except EOFError:
            answer = ""
        if answer != "YES":
            print("Aborted. Nothing was dropped or truncated.")
            sys.exit(1)

        # Views first: they read the tables.
        failures = 0
        for label, full, sql in plan:
            try:
                conn.execute(sql)
                print(f"{done if label == 'table' else 'dropped'} {label} {full}")
            except Exception as e:
                failures += 1
                print(f"FAILED  {label} {full}: {e}")
        if state:
            os.remove(state)
            print(f"deleted state file {state}")

        after = existing_objects(conn)
        remaining = sorted(full for _, full, _ in views if full in after)
        if mode == "DROP":
            remaining += sorted(full for full in tables if full in after)
        missing = sorted(full for full in tables if full not in after) if mode == "TRUNCATE" else []
        if remaining:
            print("Still present: " + ", ".join(remaining))
        if missing:
            print("Missing after TRUNCATE: " + ", ".join(missing))
        if not remaining and not missing:
            if mode == "TRUNCATE" and tables:
                print(f"Done: no {args.prefix} views remain, {len(tables)} tables kept empty. "
                      f"The next generator run recreates the views.")
            else:
                print(f"Done: no {args.prefix} dataset objects remain.")
        remaining += missing
    sys.exit(1 if failures or remaining else 0)


if __name__ == "__main__":
    main()
