# DolphinDB perpetual-market POC

**Rust collects 50 Binance USDⓈ-M USDT perpetual depth-5 streams at the requested 100 ms cadence. DolphinDB computes factors on arrival, persists raw books/factors/timing, and serves Grafana queries concurrently.**

The interesting part is that the factor implementation, per-symbol state, streaming subscriptions, historical storage, and research queries live in **one database process**. Rust performs transport, validation, timestamps, and batching. Grafana queries DolphinDB directly through its official backend plugin.

## Start

Requirements: **rootless Podman with a Compose provider**, or Docker Engine + Compose v2 with BuildKit; Internet access; approximately 12 GB available RAM and several GB of disk for a short run. Images support Linux amd64/arm64; validation here used amd64. DolphinDB needs a valid license; the official image includes its supplied license. The configuration limits database memory to 8 GB and workers to 2; check `license()` for the actual entitlement/expiry of your image. The original 2 GB limit exhausted memory during overnight ingestion, queries and TSDB compaction; see [the incident record](docs/INCIDENT-2026-10-23.md).

### Podman

Install the Compose provider once (or use your distribution's `podman-compose` package):

```bash
uv tool install 'podman-compose==1.5.0'
```

The launcher below selects the provider from `~/.local/bin` automatically, so it works even if that directory is absent from your shell's `PATH`:

```bash
cp .env.example .env
./scripts/compose.sh up --build -d
./scripts/compose.sh logs -f collector
```

`scripts/compose.sh` runs **`podman compose`** as your current user. Image references are fully qualified, bind mounts use SELinux `Z` labels, and the Grafana build supports Podman's architecture detection. Named volumes retain the database and Grafana state.

For every `docker compose ...` command below, use `./scripts/compose.sh ...` with Podman. Use `podman stats` for resource usage. To stop without deleting history, run `./scripts/compose.sh down`.

After changing an image, explicitly recreate the affected service with this provider:

```bash
./scripts/compose.sh build grafana
./scripts/compose.sh up -d --force-recreate --no-deps grafana
```

The Grafana entrypoint renders credentials as literal JSON strings before provisioning, preserving numeric passwords such as DolphinDB's default `123456`. First-start Grafana database migrations can take longer than collector startup; wait for the Grafana login page before running its verification script.

### Docker

```bash
cp .env.example .env
docker compose up --build -d
docker compose logs -f collector
```

Open **http://localhost:3000/d/perps-poc**; Grafana login **admin / admin** (or `GRAFANA_PASSWORD` from `.env`). Select a perpetual from the dropdown. Dashboard provisioning is automatic. Reload the dashboard if it was opened before the first events arrived.

Binance public market data needs **no API key**. The collector discovers active USDT perpetuals, ranks them by 24h quote volume, and fixes the top 50 for that process lifetime. The selected list is logged. Set `SYMBOLS=BTCUSDT,ETHUSDT` to use an explicit list instead.

If Binance is unavailable from your network, change `FEED_MODE=synthetic` in `.env` and run:

```bash
docker compose up -d collector
```

Synthetic mode generates 50 `SIM000USDT` … `SIM049USDT` books every 100 ms: **500 events/s**, with changing spread and opposing bid/ask pressure. It uses the same ingestion, computation, storage, and visualization path. Synthetic symbols are visibly distinct; synthetic transport delay is fabricated and is not a network measurement.

### Useful commands

```bash
# Query through the same Rust REST client; scripts are packaged in the collector image.
docker compose exec collector perps-poc dolphindb/inspect.dos
docker compose exec collector perps-poc dolphindb/research.dos
docker compose stats

# Graceful stop; named volumes survive.
docker compose down
# Start again and query historical data using Grafana's time picker.
docker compose up -d

# Reset the POC completely, INCLUDING all database and Grafana data:
docker compose down -v
```

### CSV and Parquet export

The pinned DolphinDB 3.00.5 image already **bundles the official Parquet plugin**. `scripts/export.py` loads it on demand (idempotently) and writes a bounded UTC slice into the DolphinDB named volume. Nothing is fetched from a different plugin release or committed as a vendor binary.

```bash
python3 scripts/export.py --table latency --symbol BTCUSDT --minutes 2 --format parquet
python3 scripts/export.py --table factors --symbol BTCUSDT --minutes 1 --format csv
```

The script prints the resulting `/data/poc/...` filename. Copy it to the Podman host:

```bash
podman cp dolphindb-perps_dolphindb_1:/data/poc/<printed-filename>.parquet .
```

Exports end at the last **complete** UTC minute; the `[start,end)` interval spans 1–5 minutes to avoid materializing a whole trading day in database memory. Re-run the command for another window or adapt the script for paged historical exports. `parquet::saveParquet` materializes this small filtered table; CSV uses DolphinDB's `saveText` SQL metacode path. Both files are written on the database container first, not downloaded through the browser. For Parquet details see the [DolphinDB Parquet plugin docs](https://docs.dolphindb.com/en/Plugins/parquet.html).

The DolphinDB web console is at **http://localhost:8848** (database login `admin / 123456`). Bindings are localhost-only. `DDB_USER`/`DDB_PASSWORD` configure clients; setting them does not change the database's account. The upstream Grafana plugin takes its password in `jsonData`; this demo uses a local disposable account.

## Architecture

```text
Binance combined WebSocket: 50 × <symbol>@depth5@100ms
                  │
                  ▼
Rust / Tokio
  receive UTC timestamp → validate 5 levels → update-ID filter
  bounded queue → batch (20 ms or 250 rows) → authenticated REST
                  │
                  ▼
DolphinDB rawStream  ───────────────► TSDB books
  synchronous persisted stream log      full depth-5 + source timestamps/IDs
                  │
                  ▼
Reactive state engine, keyed by symbol
  spread + depth imbalance + EMA(20) + microprice edge
                  │
                  ▼
factorStream ───────────────────────► TSDB factors + TSDB latency
  bounded live cache                       │
                  │                        │
                  └────────────┬───────────┘
                               ▼
                       Grafana backend plugin
                     latest-state + historical SQL
```

**Three containers:** DolphinDB 3.00.5, the Rust collector, and Grafana 13.2.2 with DolphinDB Datasource Next 3.0.7. No additional message broker or visualization database is required for this POC.

Ingestion uses DolphinDB's documented REST script API, supported from 3.00.2. It is a simple Rust integration, not a native binary-protocol performance benchmark. HTTP requests send validated, typed numeric vectors rather than one request per book. `Cargo.lock` and image/plugin versions are pinned.

### Data semantics

This uses **partial-book snapshots**, not trades or diff-depth events. Each accepted event replaces the current top five levels for that symbol. No REST snapshot/bootstrap or incremental local order-book reconstruction is needed for this stream. The code must not be pointed at `@depth@100ms` and treated as a complete book.

100 ms is Binance's **requested update speed**, not a guarantee of 10 messages/s per symbol, exchange-to-database latency, or Grafana redraw rate. Quiet symbols can publish less often. The expected live ceiling is roughly 500 events/s across 50 symbols. Grafana uses **1-second historical buckets and a 2-second refresh**; raw events and computed factors retain event-level resolution.

## Factors computed inside DolphinDB

For best bid/ask prices `b`, `a`, best-level sizes `B1`, `A1`, and sums of five-level sizes `B5`, `A5`:

| Factor | Formula | What to look for |
|---|---|---|
| Mid | `(a+b)/2` | Reference price |
| Spread | `a-b` | Absolute cost of crossing the quote |
| Spread bps | `10000 × (a-b)/mid` | Compare across differently priced instruments |
| Depth-5 imbalance | `(B5-A5)/(B5+A5)` | Range −1 to +1; positive = more displayed bid quantity |
| Imbalance EMA(20) | DolphinDB `ema(imbalance,20)`, independently per symbol | Stateful smoothing maintained by the streaming engine |
| Microprice edge bps | `10000 × ((a×B1+b×A1)/(B1+A1)-mid)/mid` | Best-level quantity-weighted pressure relative to mid |

EMA is a **20-observation span**, not a fixed two-second window. Its initial warm-up values are null. It resets when the database/engine is restarted. The engine processes arrival order; it does not reorder by exchange timestamp.

**Suggested experiment:** does positive depth imbalance predict the next observed one-second mid-price return? `dolphindb/research.dos` creates one-second observations, excludes gaps, and calculates raw/smoothed information coefficients against forward returns. The label is calculated only in historical research. Evaluate longer out-of-sample periods, spread/fees and selection bias before drawing trading conclusions. Top-five depth measures displayed liquidity, not executable volume beyond those levels.

Prices and quantities use `DOUBLE` for an exploratory analytics demo. Use tick-scaled integers or decimal types if exact accounting arithmetic is required.

## Storage and clocks

TSDB database: **`dfs://perps`**, partitioned by UTC `eventDate` × 8 symbol-hash buckets, sorted by `symbol,eventTs,updateId`. The initial date domain is 2024–2035.

| Table | Contents |
|---|---|
| `books` | All 20 depth fields (price/size × bid/ask × 5), exchange `E`/`T`, update IDs `U`/`u`/`pu`, receiver and ingestion times |
| `factors` | Every computed event, factor values, identifiers and clocks |
| `latency` | Event identifiers and timestamps plus the three latency components |

`eventTs` = Binance `E`; `transactionTs` = Binance `T`. Both are millisecond timestamps. Rust records `receiveTs` immediately after receiving the WebSocket frame, before parsing/enqueueing, using UTC Unix nanoseconds. This is a user-space receive time, not a NIC hardware timestamp.

DolphinDB stamps `ingestTs` immediately before appending the incoming batch, and `computeTs` while evaluating engine metrics. The database process runs with **`TZ=UTC`**, and these stamps use `now(true)` directly. The collector checks the database clock against its independent UTC epoch at startup. Do not wrap `now()` in `gmtime()` in this container: the tested image applied an extra eight-hour shift. Native deployments must also launch DolphinDB with `TZ=UTC`. Timestamps are nanosecond-valued, which does not imply nanosecond measurement accuracy. Database-generated timing fields written before the 2026-10-23 correction may contain that offset; existing historical records were preserved.

- **Exchange → receive:** `receiveTs − eventTs`. Includes exchange publication/Internet delay and clock offset. Synchronize the host clock; negative values are kept rather than hidden.
- **Receive → ingest:** `ingestTs − receiveTs`. Includes parsing, queueing, batching and REST transport/server handling.
- **Ingest → compute:** `computeTs − ingestTs`. Includes stream persistence, publication and subscriber scheduling. It is **not isolated factor CPU time**, nor an end-to-end durable-archive acknowledgement.

For isolated engine CPU measurement, replay a known batch directly into a fresh engine and time that operation separately. For archival latency, additionally instrument the archive handlers.

## What this proves—and how to judge it

1. **Streaming computation:** change factors in `dolphindb/init.dos`; the Rust collector does not contain market-factor formulas.
2. **State management:** EMA state is maintained per instrument in DolphinDB; `scripts/smoke.py` compares streaming output with batch EMA for two interleaved symbols.
3. **Concurrent queries and writes:** keep the dashboard open while ingestion runs; use the console to aggregate `books` and `factors` over the same period.
4. **Storage:** stop the collector, let subscriptions drain, restart the stack, and select the previous period in Grafana. Historical charts read TSDB, not the live cache.
5. **Replay/research:** the original depth fields are retained, so you can recompute alternative factors without recollecting market data.

**500 events/s is a functional integration test, not a convincing maximum-throughput benchmark.** Use synthetic load to find this stack's limits on your hardware:

| Setting | Offered rate with 50 symbols |
|---|---:|
| `SYNTHETIC_INTERVAL_MS=100` | 500/s |
| `SYNTHETIC_INTERVAL_MS=10` | 5,000/s |
| `SYNTHETIC_INTERVAL_MS=1` | 50,000/s |

Actual delivered rate may be lower; missed timer ticks are skipped instead of creating catch-up bursts. Increase `BATCH_SIZE` and tune `BATCH_MS` deliberately. Record offered/accepted/archived rates, collector `dropped`, subscriber `failedMsgCount`/`queueDepth`, p50/p99 latency, Grafana query latency, CPU, memory and disk growth. Test with dashboards enabled and disabled. Keep durability, data model, query workload and hardware the same when comparing another system.

### Why consider DolphinDB rather than alternatives?

| Option | Strong fit | What this particular POC would need |
|---|---|---|
| **DolphinDB** | Stateful quantitative streaming plus time-series history/research in one engine | Its scripting language, licensing, streaming operations and recovery design |
| **ClickHouse** | Large historical scans, compression, broad analytical SQL workloads | Stateful per-symbol event processing usually in a separate service/stream processor; evaluate its native incremental features for the exact factors |
| **TimescaleDB / PostgreSQL** | Relational joins, transactions, PostgreSQL ecosystem | Event-by-event EMA state typically in an application or custom database logic; benchmark ingestion/query tuning |
| **QuestDB** | Time-series ingestion, SQL and ASOF-oriented analytics | Evaluate streaming stateful-factor implementation separately |
| **Kafka + Flink + analytical store** | Durable distributed event processing, replay and sophisticated failure semantics | More components, deployment and operational work, but stronger separation and independent scaling |

DolphinDB's compelling argument here is **less integration work between online factors and historical analytics**. This project does not establish that it is universally faster, cheaper or more reliable.

## Delivery and recovery contract

This is a single-node, best-effort live POC:

- Rust uses a bounded queue (10,000 events by default). Overflow drops **new** snapshots and increments `dropped`; it does not block socket reads and disguise stale data as low-latency data.
- Duplicate/out-of-order final update IDs are skipped per symbol within one collector process. The filter survives WebSocket reconnects but not process restarts. `pu` is stored for analysis; diff-depth continuity rules are not imposed on partial snapshots.
- WebSocket ping/pong, 60s idle timeout, and reconnect backoff (1–30s) are implemented. Missed snapshots during disconnection are not backfilled. Binance's periodic connection rotation is handled as a reconnect.
- A failed/timeout HTTP write has an ambiguous commit outcome. The collector exits and logs the failure rather than blindly retrying and biasing EMA with duplicates. Compose restarts it; events in flight or queued can be lost.
- `rawStream` uses **synchronous persistence + disk flush** before its append acknowledgement. TSDB archival is a separate asynchronous subscription. An HTTP acknowledgement does not mean all three archival tables are complete.
- Archive handlers do not form one atomic transaction across tables. Inspect `getStreamingStat().subWorkers`; failures are not automatically repaired by this project.
- Database restart recreates subscriptions at **new events only** (`offset=-1`), with cold EMA state. Persisted raw stream data is retained, but this POC does not automatically replay unarchived records or restore state/offset checkpoints. Exactly-once delivery, crash recovery and HA require additional design and testing.
- Live stream caches have a 100k-row purge target (the database may temporarily retain more); they are not full history. The raw stream log requests 60-minute retention, subject to DolphinDB log-file rotation rules. TSDB history has **no automatic expiry** here.

At a continuous 500/s, expect 43.2 million snapshots/day and three archival rows per snapshot. The numerical columns alone are on the order of tens of GB/day before compression, indexes and the stream log. Measure actual disk usage and define historical retention before leaving a run unattended.

## Verification and development

```bash
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings

# With DolphinDB + collector running (Python standard library only):
python3 scripts/smoke.py
python3 scripts/check_grafana.py
python3 scripts/ddb.py dolphindb/inspect.dos

# Local Rust collector against localhost:8848:
FEED_MODE=synthetic cargo run --locked
```

Run **one collector per database**. The initializer is repeatable after a successful startup and does not recreate live subscriptions on a collector restart. If initialization fails halfway through, fix the error and restart DolphinDB before retrying. After changing schemas, reset the disposable volume or perform a deliberate migration. After changing engine expressions, restart the database and collector to rebuild the engine.

The smoke test adds 60 rows under two uniquely named `TEST...` symbols. It checks raw/factor/latency persistence, independently calculated spread/imbalance/microprice, EMA batch/stream equality across interleaved symbols and batches, UTC timing, and all ten dashboard query texts.

REST execute returns only the final script result. To inspect individual subscriber counters, run `getStreamingStat().subWorkers` in the console or pipe it to `python3 scripts/ddb.py -`.

See **[docs/VALIDATION.md](docs/VALIDATION.md)** for what was actually exercised in this environment.

### Troubleshooting

- **Database never starts:** `docker compose logs dolphindb`; database log also lives at `/data/poc/dolphindb.log` inside the container. Confirm the image's license is valid. To use your own license, bind-mount it read-only at `/data/ddb/server/dolphindb.lic` in an override compose file. No license is committed to this repository.
- **Binance discovery/connection fails:** check outbound HTTPS/WSS and endpoint availability from your location. Use synthetic mode to exercise the database stack independently.
- **No dropdown values:** wait for the collector's `websocket connected` log and inserted rows, then reload the dashboard. Choose `SIM...` in synthetic mode.
- **Latest state moves but historical panels lag:** check subscriber queue depth and failed-message counters. Archive subscribers batch writes separately from computation.
- **Grafana plugin unavailable:** the build downloads the versioned plugin ZIP from GitHub. Verify build-network access. The Compose configuration explicitly enables this upstream unsigned plugin.
- **CPU/memory pressure:** use `docker compose stats`, inspect `getStreamingStat()` and reduce the dashboard time range; the demo uses fixed 1s chart buckets, so very long ranges produce many points. Larger workloads require tuning the caches and license/resource allocation.

## Files

- `src/main.rs`: discovery, WebSocket lifecycle, bounded queue, batching, synthetic feed, shutdown.
- `src/book.rs`: depth validation, typed batch encoding and parser tests.
- `src/db.rs`: authenticated DolphinDB REST client.
- `dolphindb/init.dos`: tables, engine expressions, subscriptions and archive handlers.
- `dolphindb/inspect.dos`, `research.dos`: operational and factor-research queries.
- `grafana/`: versioned plugin image, provisioned datasource and ten-panel dashboard.
- `scripts/`: standard-library REST client and integration checks.

### References

- [Binance partial-book depth streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams)
- [DolphinDB REST API](https://docs.dolphindb.com/en/API/rest_api.html)
- [Unified batch and stream processing](https://docs.dolphindb.com/en/Streaming/unified_batch_and_stream_processing.html)
- [Persistent stream-table semantics](https://docs.dolphindb.com/en/Functions/e/enableTableShareAndPersistence.html)
- [DolphinDB Grafana backend plugin](https://github.com/dolphindb/grafana-datasource-next)
