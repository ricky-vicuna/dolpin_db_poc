#!/usr/bin/env python3
"""Export a small, bounded slice of the POC to CSV or Parquet on the DB container.

Example: python3 scripts/export.py --table latency --symbol BTCUSDT --minutes 2 --format parquet
"""
import argparse
from datetime import datetime, timedelta, timezone
import re

from ddb import DB


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", choices=("books", "factors", "latency"), default="latency")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--minutes", type=int, default=1, help="recent complete minutes, 1–5")
    parser.add_argument("--format", choices=("csv", "parquet"), default="parquet")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Z0-9]{1,32}", args.symbol):
        parser.error("symbol must be 1–32 uppercase letters/digits")
    if not 1 <= args.minutes <= 5:
        parser.error("export 1–5 minutes at a time to bound database memory use")

    # Complete minutes, independent of DolphinDB's local clock. If the range crosses
    # UTC midnight, eventDate IN includes both partitions without scanning all history.
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = end - timedelta(minutes=args.minutes)
    start_ms, end_ms = (int(t.timestamp() * 1000) for t in (start, end))
    name = f"{args.table}-{args.symbol}-{start:%Y%m%dT%H%MZ}-{end:%H%MZ}.{args.format}"
    path = f"/data/poc/{name}"  # named volume inside the database container
    select = (f'select * from loadTable("dfs://perps", "{args.table}") '
              f'where eventDate in [date(timestamp({start_ms})),date(timestamp({end_ms}))],'
              f'symbol="{args.symbol}",eventTs>=timestamp({start_ms}),eventTs<timestamp({end_ms})')
    db = DB()
    if args.format == "parquet":
        # The official DolphinDB v3.00.5 image already bundles this plugin. It is
        # server-global, and a second loadPlugin call reports "already in use".
        try:
            db.execute('loadPlugin("parquet")')
        except RuntimeError as exc:
            if "already in use" not in str(exc):
                raise
        db.execute(f't = {select}; parquet::saveParquet(t, "{path}", "zstd")')
    else:
        # The metacode form streams SQL results through saveText rather than returning
        # all matching rows to this Python process or the browser.
        db.execute(f'saveText(<{select}>, "{path}")')
    print(f"Created {path} on the DolphinDB container (UTC [{start.isoformat()}, {end.isoformat()})).")
    print(f"Copy to this machine: podman cp dolphindb-perps_dolphindb_1:{path} .")


if __name__ == "__main__":
    main()
