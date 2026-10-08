#!/usr/bin/env python3
"""Smoke test for the query pack: run every '-- @@' cell in energy_demo_queries.sql
against QuestDB over HTTP and print ok with row count and timing, or FAIL with the
server's error. Exits non-zero on any failure.

    python check.py                 # all cells
    python check.py 6a 7            # cells whose name starts with these prefixes
    HOST=http://10.0.0.8:9000 python check.py
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


def run(sql):
    url = HOST + "/exec?" + urllib.parse.urlencode({"query": sql, "count": "true"})
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
    prefixes = sys.argv[1:]
    failures = 0
    for name, sql in cells(text):
        if prefixes and not any(name.startswith(p) for p in prefixes):
            continue
        t = time.time()
        d = run(sql)
        ms = (time.time() - t) * 1000
        if "error" in d:
            failures += 1
            print(f"FAIL {name:40s} {d['error']}")
        else:
            print(f"ok   {name:40s} {d.get('count', 0):>8} rows {ms:8.0f} ms")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
