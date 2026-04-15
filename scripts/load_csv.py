#!/usr/bin/env python3
import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path

_NUMERIC_RE = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch import CSV via SQL HTTP API using curl."
    )
    parser.add_argument("--csv", required=True, help="Path to input CSV file")
    parser.add_argument("--table", default="test.electricity", help="Target table")
    parser.add_argument("--url", default="http://localhost:8361/api/v1/sql", help="SQL API URL")
    parser.add_argument("--user", default="admin", help="API username")
    parser.add_argument("--password", default="public", help="API password")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
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
    cols = ", ".join(columns)
    values_sql = []
    for row in rows:
        literals = [sql_literal(v) for v in row]
        values_sql.append("(" + ", ".join(literals) + ")")
    return f"insert into {table} ({cols}) values " + ", ".join(values_sql)


def send_sql(url: str, user: str, password: str, sql: str) -> tuple[int, str, str]:
    cmd = [
        "curl",
        "-sS",
        "-u",
        f"{user}:{password}",
        "-X",
        "POST",
        url,
        "-H",
        "Content-Type: application/binary",
        "--data-binary",
        sql,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def main() -> int:
    args = parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
        return 1
    if args.batch_size <= 0:
        print("ERROR: --batch-size must be > 0", file=sys.stderr)
        return 1

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
                code, out, err = send_sql(args.url, args.user, args.password, sql)
                if code != 0:
                    print(f"ERROR: curl failed on batch {batch_no}: {err}", file=sys.stderr)
                    return code
                print(f"[batch {batch_no}] imported {len(batch)} rows | response: {out}")
                total_rows += len(batch)
                batch = []

        if batch:
            batch_no += 1
            sql = build_insert_sql(args.table, columns, batch)
            code, out, err = send_sql(args.url, args.user, args.password, sql)
            if code != 0:
                print(f"ERROR: curl failed on batch {batch_no}: {err}", file=sys.stderr)
                return code
            print(f"[batch {batch_no}] imported {len(batch)} rows | response: {out}")
            total_rows += len(batch)

    print(f"Done. Total imported rows: {total_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
