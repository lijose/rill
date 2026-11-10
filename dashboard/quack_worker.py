#!/usr/bin/env python3
"""
Quack Worker for Rill Dashboard.
Connects to a remote Rill DuckDB Quack server using the installed compatible DuckDB version.
Provides a metrics polling thread, engine alerts polling, and a stdin-reader thread for custom queries.
"""

import sys
import time
import json
import argparse
import os
import duckdb
import threading
import re
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from rills.sql_validation import validate_read_only_sql

# Encode DuckDB values without changing the global JSON encoder.
_orig_dumps = json.dumps
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        import decimal, datetime
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time, datetime.timedelta)):
            return str(obj)
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)

dumps = lambda obj, **kwargs: _orig_dumps(obj, cls=CustomJSONEncoder, **kwargs)

db_lock = threading.RLock()
# Track scheduled dashboard queries
scheduled_queries = {}
deleted_scheduled_queries = set()

# Track the last alert timestamp we've seen to only send new alerts
_last_alert_ts = ""


def is_allowed_sql(sql):
    try:
        with duckdb.connect() as validator:
            validate_read_only_sql(validator, sql)
        return True
    except (ValueError, duckdb.Error):
        return False


def execute_query(con, sql):
    validate_read_only_sql(con, sql)

    # Intercept queries for information_schema.tables to workaround Quack catalog limitations
    if "information_schema.tables" in sql.lower():
        sql = "SELECT 'remote_db' AS table_catalog, 'main' AS table_schema, table_name FROM remote_db.rill_tables;"

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
                max_ts_res = con.execute("SELECT MAX(ts) FROM remote_db.rill_metrics").fetchone()
                if max_ts_res and max_ts_res[0] is not None:
                    max_ts = max_ts_res[0]
                    sql = f"""
                        SELECT ts, metric_name, value 
                        FROM remote_db.rill_metrics 
                        WHERE ts = '{max_ts}'
                    """
                    res = con.execute(sql).fetchall()
                    if res:
                        data = {}
                        for row in res:
                            data[row[1]] = row[2]
                        # Also include the timestamp as a top-level or in data
                        data["timestamp"] = res[0][0]
                        print(dumps({"type": "metrics", "data": data}), flush=True)
                    else:
                        print(dumps({"error": "No metrics found in remote_db.rill_metrics"}), flush=True)
                else:
                    print(dumps({"error": "No metrics found in remote_db.rill_metrics"}), flush=True)
        except Exception as e:
            print(dumps({"error": f"Metrics query error: {e}"}), flush=True)

        # Poll for new engine/user alerts
        try:
            with db_lock:
                alerts_sql = f"""
                    SELECT ts, severity, source, message, detail
                    FROM remote_db.rill_alerts
                    WHERE ts > '{_last_alert_ts}'
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
                    print(dumps({"type": "engine_alerts", "data": alerts_data}), flush=True)
        except Exception as e:
            # rill_alerts table may not exist yet, that's ok
            if "rill_alerts" not in str(e).lower() and "not found" not in str(e).lower():
                pass  # Silently ignore if table doesn't exist yet

        # Poll for new dashboard queries from the backend
        try:
            with db_lock:
                queries_sql = "SELECT query_id, name, sql, interval, chart_type, x_col, y_col, color_col FROM remote_db.rill_dashboard_queries"
                query_rows = con.execute(queries_sql).fetchall()
                if query_rows:
                    for row in query_rows:
                        q_id, q_name, q_sql, q_interval, q_chart_type, q_x_col, q_y_col, q_color_col = row
                        if q_id not in scheduled_queries and q_id not in deleted_scheduled_queries:
                            scheduled_queries[q_id] = {
                                "sql": q_sql,
                                "interval": q_interval,
                                "last_run": 0
                            }
                            # Broadcast to the UI
                            print(dumps({
                                "type": "scheduled_query_sync",
                                "queryId": q_id,
                                "config": {
                                    "queryId": q_id,
                                    "name": q_name,
                                    "sql": q_sql,
                                    "intervalSeconds": q_interval,
                                    "type": q_chart_type,
                                    "xCol": q_x_col,
                                    "yCol": q_y_col,
                                    "colorCol": q_color_col
                                }
                            }), flush=True)
        except Exception as e:
            if "rill_dashboard_queries" not in str(e).lower() and "not found" not in str(e).lower():
                pass

        # Handle scheduled queries
        now = time.time()
        with db_lock:
            for query_id, qinfo in list(scheduled_queries.items()):
                if now - qinfo['last_run'] >= qinfo['interval']:
                    try:
                        cols, rows, elapsed = execute_query(con, qinfo['sql'])
                        print(dumps({
                            "type": "scheduled_result",
                            "queryId": query_id,
                            "columns": cols,
                            "rows": [dict(zip(cols, r)) for r in rows],
                            "elapsed_ms": elapsed,
                            "ranAt": now * 1000
                        }), flush=True)
                    except Exception as e:
                        print(dumps({
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
                    print(dumps({
                        "type": "query_result",
                        "queryId": query_id,
                        "columns": cols,
                        "rows": [dict(zip(cols, r)) for r in rows],
                        "elapsed_ms": elapsed
                    }), flush=True)
                except Exception as e:
                    print(dumps({
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
                        validate_read_only_sql(con, sql)
                        deleted_scheduled_queries.discard(query_id)
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
                    deleted_scheduled_queries.add(query_id)
        except Exception as e:
            print(dumps({"error": f"Stdin loop error: {e}"}), flush=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", required=True, help="Quack address (e.g. quack:127.0.0.1:9494)")
    parser.add_argument("--token", default="", help="Optional authentication token")
    args = parser.parse_args()

    config = {}
    if os.environ.get("RILL_EXTENSION_DIRECTORY"):
        config["extension_directory"] = os.environ["RILL_EXTENSION_DIRECTORY"]
    con = duckdb.connect(":memory:", config=config)
    try:
        con.execute("INSTALL quack; LOAD quack;")
    except Exception as e:
        print(dumps({"error": f"Failed to load quack extension: {e}"}), flush=True)
        sys.exit(1)

    attach_cmd = "ATTACH '" + args.address.replace("'", "''") + "' AS remote_db"
    if args.token:
        attach_cmd += " (TOKEN '" + args.token.replace("'", "''") + "')"

    try:
        con.execute(attach_cmd)
    except Exception as e:
        print(dumps({"error": f"Connection failed to '{args.address}': {e}"}), flush=True)
        sys.exit(1)

    print(dumps({"status": "connected", "address": args.address}), flush=True)

    metrics_thread = threading.Thread(target=metrics_loop, args=(con,), daemon=True)
    metrics_thread.start()
    
    stdin_loop(con)

if __name__ == "__main__":
    main()
