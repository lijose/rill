#!/usr/bin/env python3
"""
Quack Worker for Rill Dashboard.
Connects to a remote Rill DuckDB Quack server using DuckDB v1.5.4.
Provides a metrics polling thread, engine alerts polling, and a stdin-reader thread for custom queries.
"""

import sys
import time
import json
import argparse
import duckdb
import threading
import re

db_lock = threading.RLock()
scheduled_queries = {}  # queryId -> { 'sql': str, 'interval': float, 'last_run': float }

ALLOWLIST_REGEX = re.compile(r'^\s*(SELECT|WITH|EXPLAIN|PRAGMA\s+show_tables|DESCRIBE)\b', re.IGNORECASE)

# Track the last alert timestamp we've seen to only send new alerts
_last_alert_ts = 0.0

def is_allowed_sql(sql):
    # Strip comments and trim
    stripped = "\n".join([line for line in sql.split('\n') if not line.strip().startswith('--')])
    return bool(ALLOWLIST_REGEX.match(stripped))

def execute_query(con, sql):
    if not is_allowed_sql(sql):
        raise ValueError("Statement not allowed. Only SELECT, WITH, EXPLAIN, PRAGMA show_tables, DESCRIBE are permitted.")
    t0 = time.perf_counter()
    res = con.execute(sql).fetchall()
    cols = [desc[0] for desc in con.description] if con.description else []
    t1 = time.perf_counter()
    return cols, res, (t1 - t0) * 1000.0

def metrics_loop(con):
    global _last_alert_ts

    while True:
        try:
            with db_lock:
                # Get the latest metric values for each metric_name
                sql = """
                    SELECT ts, metric_name, value 
                    FROM remote_db.rill_metrics 
                    WHERE ts = (SELECT MAX(ts) FROM remote_db.rill_metrics)
                """
                res = con.execute(sql).fetchall()
                if res:
                    data = {}
                    for row in res:
                        data[row[1]] = row[2]
                    # Also include the timestamp as a top-level or in data
                    data["timestamp"] = res[0][0]
                    print(json.dumps({"type": "metrics", "data": data}), flush=True)
                else:
                    print(json.dumps({"error": "No metrics found in remote_db.rill_metrics"}), flush=True)
        except Exception as e:
            print(json.dumps({"error": f"Metrics query error: {e}"}), flush=True)

        # Poll for new engine/user alerts
        try:
            with db_lock:
                alerts_sql = f"""
                    SELECT ts, severity, source, message, detail
                    FROM remote_db.rill_alerts
                    WHERE ts > {_last_alert_ts}
                    ORDER BY ts ASC
                    LIMIT 50
                """
                alert_rows = con.execute(alerts_sql).fetchall()
                if alert_rows:
                    alerts_data = []
                    for row in alert_rows:
                        alerts_data.append({
                            "ts": row[0],
                            "severity": row[1],
                            "source": row[2],
                            "message": row[3],
                            "detail": row[4] or ""
                        })
                        if row[0] > _last_alert_ts:
                            _last_alert_ts = row[0]
                    print(json.dumps({"type": "engine_alerts", "data": alerts_data}), flush=True)
        except Exception as e:
            # rill_alerts table may not exist yet, that's ok
            if "rill_alerts" not in str(e).lower() or "not found" not in str(e).lower():
                pass  # Silently ignore if table doesn't exist yet

        # Handle scheduled queries
        now = time.time()
        with db_lock:
            for query_id, qinfo in list(scheduled_queries.items()):
                if now - qinfo['last_run'] >= qinfo['interval']:
                    try:
                        cols, rows, elapsed = execute_query(con, qinfo['sql'])
                        print(json.dumps({
                            "type": "scheduled_result",
                            "queryId": query_id,
                            "columns": cols,
                            "rows": [dict(zip(cols, r)) for r in rows],
                            "elapsed_ms": elapsed,
                            "ranAt": now * 1000
                        }), flush=True)
                    except Exception as e:
                        print(json.dumps({
                            "type": "scheduled_result",
                            "queryId": query_id,
                            "error": str(e),
                            "ranAt": now * 1000
                        }), flush=True)
                    qinfo['last_run'] = now

        time.sleep(1.0)

def stdin_loop(con):
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            action = req.get("action")
            
            if action == "preview_query":
                query_id = req.get("queryId")
                sql = req.get("sql")
                try:
                    with db_lock:
                        cols, rows, elapsed = execute_query(con, sql)
                    print(json.dumps({
                        "type": "query_result",
                        "queryId": query_id,
                        "columns": cols,
                        "rows": [dict(zip(cols, r)) for r in rows],
                        "elapsed_ms": elapsed
                    }), flush=True)
                except Exception as e:
                    print(json.dumps({
                        "type": "query_result",
                        "queryId": query_id,
                        "error": str(e)
                    }), flush=True)
                    
            elif action == "schedule_query":
                query_id = req.get("queryId")
                sql = req.get("sql")
                interval = req.get("intervalSeconds", 0)
                if interval > 0:
                    with db_lock:
                        scheduled_queries[query_id] = {
                            "sql": sql,
                            "interval": interval,
                            "last_run": 0
                        }
            
            elif action == "unschedule_query":
                query_id = req.get("queryId")
                with db_lock:
                    if query_id in scheduled_queries:
                        del scheduled_queries[query_id]
        except Exception as e:
            print(json.dumps({"error": f"Stdin loop error: {e}"}), flush=True)

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

    metrics_thread = threading.Thread(target=metrics_loop, args=(con,), daemon=True)
    metrics_thread.start()
    
    stdin_loop(con)

if __name__ == "__main__":
    main()
