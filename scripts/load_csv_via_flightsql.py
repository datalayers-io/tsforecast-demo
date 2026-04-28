#!/usr/bin/env python3
import argparse
import csv
import re
import sys
from pathlib import Path
from flightsql import FlightSQLClient

_NUMERIC_RE = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch import CSV via FlightSQL."
    )
    parser.add_argument("--csv", required=True, help="Path to input CSV file")
    parser.add_argument("--table", required=True, help="Target table")
    parser.add_argument("--host", default="localhost", help="FlightSQL host")
    parser.add_argument("--port", type=int, default=8360, help="FlightSQL port")
    parser.add_argument("--user", default="admin", help="API username")
    parser.add_argument("--password", default="public", help="API password")
    parser.add_argument("--db", help="Metadata field: database")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Rows per insert statement (default: 100)",
    )
    parser.add_argument(
        "--skip-header-check",
        action="store_true",
        help="Allow importing even if header has empty column names",
    )
    return parser.parse_args()


def sql_literal(value: str) -> str:
    value = value.strip()
    if value == "":
        return "NULL"
    lower = value.lower()
    if lower in {"null", "none", "nan"}:
        return "NULL"
    if _NUMERIC_RE.match(value):
        return value
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def build_insert_sql(table: str, columns: list[str], rows: list[list[str]]) -> str:
    cols = ", ".join(f"`{c}`" for c in columns)
    values_sql = []
    for row in rows:
        literals = [sql_literal(v) for v in row]
        values_sql.append("(" + ", ".join(literals) + ")")
    return f"insert into {table} ({cols}) values " + ", ".join(values_sql)


def send_sql(client: FlightSQLClient, sql: str) -> tuple[int, str, str]:
    try:
        info = client.execute(sql)
        endpoint_count = 0
        for ep in getattr(info, "endpoints", []):
            endpoint_count += 1
            _ = client.do_get(ep.ticket).read_all()
        return 0, f"ok (endpoints={endpoint_count})", ""
    except Exception as e:
        return 1, "", str(e)


def query_count(client: FlightSQLClient, table: str) -> int:
    sql = f"select count(*) as cnt from {table}"
    info = client.execute(sql)
    for ep in getattr(info, "endpoints", []):
        table_data = client.do_get(ep.ticket).read_all()
        if "cnt" in table_data.column_names:
            vals = table_data["cnt"].to_pylist()
            if vals:
                return int(vals[0])
    raise RuntimeError("failed to read count(*) result")


def main() -> int:
    args = parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
        return 1
    if args.batch_size <= 0:
        print("ERROR: --batch-size must be > 0", file=sys.stderr)
        return 1
    if args.port <= 0:
        print("ERROR: --port must be > 0", file=sys.stderr)
        return 1

    client = FlightSQLClient(
        host=args.host,
        port=args.port,
        insecure=True,
        user=args.user,
        password=args.password,
        metadata={"database": args.db},
    )

    try:
        count_before = query_count(client, args.table)
        print(f"Pre-check count({args.table}) = {count_before}")
    except Exception as e:
        print(f"WARNING: count pre-check failed: {e}", file=sys.stderr)
        count_before = None

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            print("ERROR: CSV is empty", file=sys.stderr)
            return 1

        columns = [c.strip() for c in header]
        if not args.skip_header_check and any(not c for c in columns):
            print("ERROR: CSV header has empty column name(s)", file=sys.stderr)
            return 1

        total_rows = 0
        batch = []
        batch_no = 0

        for row_idx, row in enumerate(reader, start=2):
            if len(row) != len(columns):
                print(
                    f"ERROR: Row {row_idx} has {len(row)} columns, expected {len(columns)}",
                    file=sys.stderr,
                )
                return 1

            batch.append(row)
            if len(batch) >= args.batch_size:
                batch_no += 1
                sql = build_insert_sql(args.table, columns, batch)
                code, out, err = send_sql(client, sql)
                if code != 0:
                    print(
                        f"ERROR: FlightSQL execute failed on batch {batch_no}: {err}",
                        file=sys.stderr,
                    )
                    return code
                print(f"[batch {batch_no}] imported {len(batch)} rows | response: {out}")
                total_rows += len(batch)
                batch = []

        if batch:
            batch_no += 1
            sql = build_insert_sql(args.table, columns, batch)
            code, out, err = send_sql(client, sql)
            if code != 0:
                print(
                    f"ERROR: FlightSQL execute failed on batch {batch_no}: {err}",
                    file=sys.stderr,
                )
                return code
            print(f"[batch {batch_no}] imported {len(batch)} rows | response: {out}")
            total_rows += len(batch)

    print(f"Done. Total imported rows: {total_rows}")
    try:
        count_after = query_count(client, args.table)
        if count_before is not None:
            print(f"Post-check count({args.table}) = {count_after} (delta={count_after - count_before})")
        else:
            print(f"Post-check count({args.table}) = {count_after}")
    except Exception as e:
        print(f"WARNING: count post-check failed: {e}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
