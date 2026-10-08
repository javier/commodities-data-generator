#!/usr/bin/env python3
"""Smoke test for the query pack: run every '-- @@' cell in energy_demo_queries.sql
against QuestDB over HTTP and print ok with row count and timing, or FAIL with the
server's error. Exits non-zero on any failure.

    python check.py                 # all cells
    python check.py 6a 7            # cells whose name starts with these prefixes
    HOST=http://10.0.0.8:9000 python check.py

    python check.py --dump before.json      # also save each cell's columns, row count,
    python check.py --compare before.json   # first ten rows and timing; compare them

--compare fails a cell whose columns, row count or first ten rows differ from the
saved run, and flags one more than 20% slower (timings are informative: warm both runs).
"""
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HOST = os.environ.get("HOST", "http://localhost:9000")
SQL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "energy_demo_queries.sql")


def run(sql, limit=None):
    params = {"query": sql, "count": "true"}
    if limit:
        params["limit"] = str(limit)
    url = HOST + "/exec?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=600) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read() or b"{}")


def cells(text):
    for cell in re.split(r"^-- @@ ", text, flags=re.M)[1:]:
        name, body = cell.split("\n", 1)
        body = re.split(r"^-- =+", body, flags=re.M)[0]          # stop at the next act banner
        sql = "\n".join(line for line in body.splitlines() if not line.strip().startswith("--"))
        yield name.strip(), sql.strip().rstrip(";")


def main():
    text = open(SQL_FILE).read()
    args = sys.argv[1:]
    dump = compare = None
    if "--dump" in args:
        i = args.index("--dump"); dump = args[i + 1]; del args[i:i + 2]
    if "--compare" in args:
        i = args.index("--compare"); compare = args[i + 1]; del args[i:i + 2]
    saved = json.load(open(compare)) if compare else {}
    out = {}
    failures = 0
    for name, sql in cells(text):
        if args and not any(name.startswith(p) for p in args):
            continue
        t = time.time()
        d = run(sql, 10 if (dump or compare) else None)
        ms = (time.time() - t) * 1000
        if "error" in d:
            failures += 1
            print(f"FAIL {name:40s} {d['error']}")
            continue
        snap = {"columns": [c["name"] for c in d["columns"]], "count": d.get("count", 0),
                "rows": d["dataset"][:10], "ms": round(ms)}
        out[name] = snap
        note = ""
        if compare:
            was = saved.get(name)
            if was is None:
                note = "new cell"
            else:
                diffs = [k for k in ("columns", "count", "rows") if was[k] != snap[k]]
                if diffs:
                    failures += 1
                    print(f"DIFF {name:40s} {', '.join(diffs)} differ")
                    continue
                note = f"same as saved, {was['ms']} ms before"
                if ms > 1.2 * was["ms"] and ms - was["ms"] > 20:
                    note += "  SLOWER"
        print(f"ok   {name:40s} {snap['count']:>8} rows {ms:8.0f} ms  {note}".rstrip())
    if compare:
        for name in saved:
            if name not in out and not args:
                print(f"GONE {name}")
    if dump:
        json.dump(out, open(dump, "w"), indent=1, default=str)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
