# Validation record

**2026-10-23 update:** an overnight run exhausted the original 2 GB database limit and suffered archival failures. The limit is now 8 GB. Verification also uncovered an eight-hour error in the container's `gmtime(now())` conversion; current code uses `now()` under `TZ=UTC`, with independent epoch checks. See [the incident record](INCIDENT-2026-10-23.md). Earlier same-clock smoke-test results below must not be read as proof that historical container timing fields were correct.

Executed on **2026-10-22**, Linux amd64, Intel Core i7-12700K, approximately 64 GB host RAM. DolphinDB was configured with `maxMemSize=2`, two query workers and two subscription executors.

## Podman deployment (latest validation)

The stack was subsequently **built and deployed using rootless Podman 5.8.2 and podman-compose 1.5.0** on AlmaLinux 10.2 with SELinux enabled. No Docker daemon or Docker Desktop was installed.

- The Rust collector image built successfully with `cargo build --locked --release` inside its build container.
- The Grafana image built with the actual 3.0.7 DolphinDB backend plugin and a credential-rendering entrypoint.
- All three containers started; the collector discovered and connected to 50 real Binance perpetual streams.
- `python3 scripts/smoke.py` passed against the containerized database: 60 fixture records, raw/factor/latency persistence, correct factors, per-symbol streaming/batch EMA parity, UTC timing and ten database queries.
- `python3 scripts/check_grafana.py` passed against the containerized Grafana: healthy DolphinDB datasource and all ten panel queries returned data frames.
- The observed collector counters showed zero rejected snapshots, stale IDs, queue drops or reconnects.
- Rootless networking, service-name resolution, named-volume writes and SELinux-labeled read-only configuration mounts worked.

Deployment fixes: fully qualified image references, SELinux `Z` labels, a Podman-compatible architecture fallback in the plugin build, and rendering Grafana credentials before provisioning. Grafana's environment expansion otherwise turned the numeric password into a JSON number despite YAML quoting.

The **Podman stack is left running with live ingestion**. Manage it from the project root using `./scripts/compose.sh ps`, `./scripts/compose.sh logs -f collector`, and `./scripts/compose.sh down` (keeps named volumes). With this Compose provider, use `--force-recreate` after rebuilding an existing service image.

This completes the amd64 container build/startup/integration checks. Reboot autostart, containerized crash recovery, volume restore after a full stack recreation and arm64 execution have not been tested.

## Earlier native validation

Before the Podman deployment, Docker Engine was unavailable in this environment. The database, collector and Grafana were exercised as **native processes**, using DolphinDB 3.00.5, Grafana 13.2.2 and the actual DolphinDB Datasource Next 3.0.7 release plugin. Rust used the development profile in this earlier run. The measurements and row counts below refer to that native run.

Docker Compose v2.39.4 also validated `compose.yaml` with `config --quiet`. The pinned Rust, Debian, DolphinDB and Grafana image tags were checked against Docker Registry and all resolved successfully.

## Results

| Check | Result |
|---|---|
| Rust parser/timestamp/invalid-book tests | 2 passed via `cargo test --locked` |
| Rust lint and formatting | `cargo clippy --all-targets --locked -- -D warnings` and `cargo fmt --check` passed |
| Live Binance discovery | Selected 50 active USDT perpetuals by 24h quote volume |
| Live WebSocket | Connected to 50 combined `depth5@100ms` streams; observed roughly 350–440 events/s during the first run |
| Collector counters | No rejected snapshots, stale IDs, queue-overflow drops or reconnects in the observed live runs |
| Synthetic collector | Sustained its configured 500 events/s during a multi-minute functional run |
| Stream/archive subscriptions | All three had zero failed messages and drained to zero queue depth |
| Final drained totals | **424,990 rows in each of `books`, `factors`, and `latency`** |
| Persistence after graceful database restart | Exactly **338,275 rows in each of `books`, `factors`, and `latency`** before and after restart |
| Repeat initialization | Ran `init.dos` twice after restart; both succeeded without duplicating subscriptions |
| Integration smoke test | 60 fixture records persisted in all three tables; spread/OBI/microprice formulas and two-symbol batch/stream EMA parity passed |
| UTC handling | Timing assertions passed on a host whose DolphinDB local clock was UTC+8 |
| Grafana plugin | Datasource health returned `OK`; all ten actual panel queries returned HTTP 200 and Grafana data frames via `/api/ds/query` |
| Grafana symbol variable | Backend variable query returned all 50 live instruments plus the two smoke-test symbols |
| Research SQL | One-second return-label/imbalance correlation query executed against live BTCUSDT history |

The 338,275-row restart check includes synthetic data, live data and smoke fixtures. It verifies **graceful restart persistence**, not crash recovery or exactly-once delivery. Early development records included a timestamp-normalization defect that was corrected before the final smoke tests; those development records are not part of the latency sample below.

The native validation collector, database and Grafana were stopped after the checks. They are not left collecting data or occupying the Compose ports. Downloaded native binaries and validation data remain under `/tmp/opencode/ddb-poc` and `/tmp/opencode/grafana-poc`, outside the project.

## A measured latency sample—not a benchmark claim

After switching the compute subscriber from a 100-row trigger to `batchSize=1, throttle=0.001`, a final-configuration sample covered **11,585 live events** from **08:41:44.883 to 08:42:14.637 UTC**. Synthetic and `TEST...` symbols were excluded. Validation queries and writes were also being exercised around this run.

| Measurement | Observed value |
|---|---:|
| Exchange → Rust receive, p50 | 23.57 ms |
| Exchange → Rust receive, p99 | 24.38 ms |
| Rust receive → DolphinDB ingress, p99 | **379.40 ms** |
| DolphinDB ingress → engine evaluation, p99 | 14.20 ms |

The large ingress p99 is deliberately included: this REST/batching/database path had tail-latency spikes under the combined workload. It should be investigated before claiming a sub-100ms end-to-end SLA. The factor-stage number includes stream persistence and scheduling; it is not pure arithmetic execution time. Percentiles of individual stages must not be added to infer an end-to-end percentile.

An earlier configuration buffered 100 events before computation and showed roughly 286 ms factor-stage p99. This motivated the smaller trigger in the delivered initializer. That comparison was not a controlled benchmark; it illustrates why batching configuration must be measured rather than inferred from the exchange's 100ms update setting.

## Reproduce

On this Podman host, from the project root:

```bash
cp .env.example .env
./scripts/compose.sh up --build -d
./scripts/compose.sh logs -f collector
# After events arrive and Grafana finishes startup, in another terminal:
python3 scripts/smoke.py
python3 scripts/check_grafana.py
python3 scripts/ddb.py dolphindb/inspect.dos
```

For synthetic mode, set `FEED_MODE=synthetic` in `.env` and recreate the collector. Use `CHECK_SYMBOL=SIM000USDT python3 scripts/check_grafana.py` for the Grafana check. The Python tools use the standard library; export database/Grafana credential environment variables if you changed them.

## Remaining evaluation work

- Container volume restore after full stack recreation, reboot autostart and arm64 execution.
- Controlled release-build latency/throughput tests at increasing synthetic rates, including concurrent historical queries and disk-growth measurements.
- Process/OS crash injection, explicit recovery offsets, engine-state restoration, idempotent archival and loss reconciliation.
- Longer live runs including Binance connection rotation and deliberate network/database outages.
- Out-of-sample factor research; the small demonstration correlations are not evidence of trading profitability.
