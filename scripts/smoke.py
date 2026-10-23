#!/usr/bin/env python3
"""Integration test against a running POC. Adds two uniquely named TEST symbols (60 rows)."""
import json
import math
from pathlib import Path
import time
from ddb import DB


def rows(db, script):
    result = db.execute(script)
    assert len(result) == 1 and isinstance(result[0], list), result
    return result[0]


def main():
    db = DB()
    # Check the server clock against an independent UTC epoch, not another DDB conversion.
    before_ms = time.time_ns() // 1_000_000
    server_ms = db.execute('long(now())')[0]
    after_ms = time.time_ns() // 1_000_000
    assert before_ms - 5000 <= server_ms <= after_ms + 5000, 'DolphinDB must run with TZ=UTC and a synchronized clock'
    fixture_ms = after_ms - 1000
    suffix = str(time.time_ns())[-12:]
    symbols = ["TESTA" + suffix, "TESTB" + suffix]
    literal = json.dumps(symbols)
    schema = "schema(rawStream).colDefs"
    inserts = []
    # Interleave two symbols; their very different imbalance values detect state contamination.
    for i in range(30):
        for side, symbol in enumerate(symbols):
            bq = 1.0 + i / 10 if side == 0 else 0.5
            aq = 1.0 if side == 0 else 3.0 + i / 10
            levels = ",".join(f"{100.0-level},{bq},{102.0+level},{aq}" for level in range(5))
            inserts.append(f'tableInsert(fixture,"{symbol}",date(ts),ts+{i},ts+{i}-2,'
                           f'nanotimestamp(ts+{i})+8000000,now(true),long({i}),long({i}),long({i}-1),{levels})')
    db.execute(f'fixture=table(60:0,{schema}.name,{schema}.typeString); ts=timestamp({fixture_ms});'
               + ";".join(inserts)
               + '; tableInsert(rawStream,fixture[0:24]); tableInsert(rawStream,fixture[24:60]);')
    where = f'eventDate=date(timestamp({fixture_ms})),symbol in {literal}'
    deadline = time.monotonic() + 20
    while True:
        counts = [rows(db, f'select count(*) as n from loadTable("dfs://perps","{table}") where {where}')[0]['n']
                  for table in ['books', 'factors', 'latency']]
        if counts == [60, 60, 60]:
            break
        assert time.monotonic() < deadline, f"archive did not catch up: {counts}; inspect getStreamingStat()"
        time.sleep(0.2)
    factors = rows(db, f'select *,ema(imbalance5,20) as batchEma from loadTable("dfs://perps","factors") where {where} context by symbol csort updateId')
    for row in factors:
        i = row['updateId']
        bq, aq = (1 + i / 10, 1) if row['symbol'] == symbols[0] else (0.5, 3 + i / 10)
        obi = (bq - aq) / (bq + aq)
        expected = {'mid': 101, 'spread': 2, 'spreadBps': 20000 / 101,
                    'imbalance5': obi, 'micropriceEdgeBps': 10000 * obi / 101}
        for field, value in expected.items():
            assert math.isclose(row[field], value, rel_tol=1e-10, abs_tol=1e-10), (field, row, value)
        if row['batchEma'] is None:
            assert row['imbalanceEma20'] is None
        else:
            assert math.isclose(row['imbalanceEma20'], row['batchEma'], abs_tol=1e-12), row
    latency = rows(db, f'select * from loadTable("dfs://perps","latency") where {where}')
    for row in latency:
        assert row['exchangeToReceiveMs'] == 8, row
        assert 0 <= row['receiveToIngestMs'] < 10000, row  # Also detects accidental local-time vs UTC conversion.
        assert 0 <= row['ingestToComputeMs'] < 10000, row
    # Exercise every actual dashboard query against the database, with concrete time substitutions.
    now = int(time.time() * 1000)
    dashboard = json.loads(Path('grafana/dashboards/perps.json').read_text())
    for panel in dashboard['panels']:
        for target in panel['targets']:
            sql = target['queryText'].replace('${__from}', str(now - 300000)).replace('${__to}', str(now)).replace('$symbol', symbols[0])
            rows(db, sql)
    print(f"PASS: 60 raw/factor/latency rows; spread/OBI/microprice; two-symbol EMA batch/stream parity; UTC timing; 10 dashboard queries. Symbols: {symbols}")


if __name__ == '__main__':
    main()
