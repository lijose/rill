#!/usr/bin/env python3
"""
Quack Worker for Rill Dashboard.
Connects to a remote Rill DuckDB Quack server using DuckDB v1.5.4,
queries the live `rill_metrics` table every second, and outputs JSON for the Node server.
"""

import sys
import time
import json
import argparse
import duckdb


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", required=True, help="Quack address (e.g. quack:127.0.0.1:9494)")
    parser.add_argument("--token", default="", help="Optional authentication token")
    args = parser.parse_args()

    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL quack; LOAD quack;")
    except Exception as e:
        print(json.dumps({"error": f"Failed to load quack extension: {e}"}), flush=True)
        sys.exit(1)

    attach_cmd = f"ATTACH '{args.address}' AS remote_db"
    if args.token:
        attach_cmd += f" (TOKEN '{args.token}')"

    try:
        con.execute(attach_cmd)
    except Exception as e:
        print(json.dumps({"error": f"Connection failed to '{args.address}': {e}"}), flush=True)
        sys.exit(1)

    print(json.dumps({"status": "connected", "address": args.address}), flush=True)

    while True:
        try:
            # Query the shared single-row metrics table
            metrics_row = con.execute("SELECT * FROM remote_db.rill_metrics").fetchone()
            if metrics_row:
                cols = [desc[0] for desc in con.description]
                data = dict(zip(cols, metrics_row))
                print(json.dumps({"type": "metrics", "data": data}), flush=True)
            else:
                print(json.dumps({"error": "No metrics row found in remote_db.rill_metrics"}), flush=True)
        except Exception as e:
            print(json.dumps({"error": f"Query error: {e}"}), flush=True)
        time.sleep(1.0)


if __name__ == "__main__":
    main()
