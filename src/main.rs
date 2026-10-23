mod book;
mod db;

use anyhow::{Context, Result, bail, ensure};
use book::{Book, insert_script, valid_symbol};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc, time};
use tokio_tungstenite::{connect_async, tungstenite::Message};

fn setting(name: &str, default: usize) -> Result<usize> {
    let n = env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<usize>()?;
    ensure!(n > 0, "{name} must be positive");
    Ok(n)
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos() as i64
}

#[derive(Default)]
struct Stats {
    received: AtomicU64,
    rejected: AtomicU64,
    stale: AtomicU64,
    dropped: AtomicU64,
    inserted: AtomicU64,
    reconnects: AtomicU64,
}

fn enqueue(tx: &mpsc::Sender<Book>, b: Book, stats: &Stats) -> Result<()> {
    match tx.try_send(b) {
        Ok(()) => (),
        Err(mpsc::error::TrySendError::Full(_)) => {
            stats.dropped.fetch_add(1, Ordering::Relaxed);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => bail!("writer stopped"),
    }
    Ok(())
}

async fn symbols() -> Result<Vec<String>> {
    let rest = env::var("BINANCE_REST_URL").unwrap_or_else(|_| "https://fapi.binance.com".into());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let info: Value = client
        .get(format!("{rest}/fapi/v1/exchangeInfo"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let eligible: HashSet<String> = info["symbols"]
        .as_array()
        .context("missing symbols")?
        .iter()
        .filter(|s| {
            s["status"] == "TRADING"
                && s["contractType"] == "PERPETUAL"
                && s["quoteAsset"] == "USDT"
        })
        .filter_map(|s| s["symbol"].as_str().map(str::to_owned))
        .collect();
    if let Ok(explicit) = env::var("SYMBOLS")
        && !explicit.trim().is_empty()
    {
        let list: Vec<String> = explicit
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .collect();
        ensure!(
            list.iter().all(|s| eligible.contains(s) && valid_symbol(s)),
            "SYMBOLS contains an invalid/inactive USDT perpetual"
        );
        ensure!(
            list.len() <= 200 && list.iter().collect::<HashSet<_>>().len() == list.len(),
            "use 1..200 unique symbols"
        );
        return Ok(list);
    }
    let tickers: Vec<Value> = client
        .get(format!("{rest}/fapi/v1/ticker/24hr"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let mut ranked: Vec<(String, f64)> = tickers
        .iter()
        .filter_map(|t| {
            let symbol = t["symbol"].as_str()?;
            let volume: f64 = t["quoteVolume"].as_str()?.parse().ok()?;
            (eligible.contains(symbol) && valid_symbol(symbol) && volume.is_finite())
                .then(|| (symbol.to_owned(), volume))
        })
        .collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    let count = setting("SYMBOL_COUNT", 50)?;
    ensure!(
        count <= 200 && ranked.len() >= count,
        "not enough symbols, or SYMBOL_COUNT > 200"
    );
    Ok(ranked.into_iter().take(count).map(|r| r.0).collect())
}

async fn live(tx: mpsc::Sender<Book>, stats: Arc<Stats>) -> Result<()> {
    let symbols = symbols().await.context(
        "Binance discovery failed; synthetic mode is available if the endpoint is unreachable",
    )?;
    tracing::info!(?symbols, "selected perpetuals by 24h quote volume");
    let allowed: HashSet<_> = symbols.iter().cloned().collect();
    let streams = symbols
        .iter()
        .map(|s| format!("{}@depth5@100ms", s.to_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
    let base = env::var("BINANCE_WS_URL").unwrap_or_else(|_| "wss://fstream.binance.com".into());
    let url = format!("{base}/stream?streams={streams}");
    let mut last = HashMap::<String, i64>::new();
    let mut backoff = 1;
    loop {
        let connected = Instant::now();
        let result: Result<()> = async {
            let (mut socket, _) =
                time::timeout(Duration::from_secs(20), connect_async(&url)).await??;
            tracing::info!(count = symbols.len(), "websocket connected");
            loop {
                // Binance pings keep idle connections active; an absent frame for 60s triggers reconnection.
                let message = time::timeout(Duration::from_secs(60), socket.next())
                    .await?
                    .context("websocket ended")??;
                let receive_ns = now_ns(); // First user-space timestamp, before JSON parsing or enqueue.
                match message {
                    Message::Text(text) => {
                        stats.received.fetch_add(1, Ordering::Relaxed);
                        match Book::parse(text.as_ref(), receive_ns) {
                            Ok(b) if allowed.contains(&b.symbol) => {
                                if last.get(&b.symbol).is_some_and(|u| b.update_id <= *u) {
                                    stats.stale.fetch_add(1, Ordering::Relaxed);
                                    continue;
                                }
                                last.insert(b.symbol.clone(), b.update_id);
                                enqueue(&tx, b, &stats)?;
                            }
                            result => {
                                let rejected = stats.rejected.fetch_add(1, Ordering::Relaxed) + 1;
                                if rejected <= 3 {
                                    tracing::warn!(
                                        ?result,
                                        "rejected malformed/unexpected snapshot"
                                    );
                                }
                            }
                        }
                    }
                    Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                    Message::Close(frame) => bail!("websocket closed: {frame:?}"),
                    _ => (),
                }
            }
        }
        .await;
        stats.reconnects.fetch_add(1, Ordering::Relaxed);
        if connected.elapsed() > Duration::from_secs(60) {
            backoff = 1;
        }
        tracing::warn!(error=?result.err(), backoff, "reconnecting; missed snapshots cannot be recovered from this stream");
        time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

async fn synthetic(tx: mpsc::Sender<Book>, stats: Arc<Stats>) -> Result<()> {
    let count = setting("SYMBOL_COUNT", 50)?;
    ensure!(count <= 200, "SYMBOL_COUNT must be <= 200");
    let mut timer = time::interval(Duration::from_millis(
        setting("SYNTHETIC_INTERVAL_MS", 100)? as u64,
    ));
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut seq = now_ns() / 1000;
    let mut tick = 0_u64;
    loop {
        timer.tick().await;
        tick += 1;
        seq += 1;
        for i in 0..count {
            let receive_ns = now_ns();
            let wave = (tick as f64 / 25.0 + i as f64).sin();
            let mid = 100.0 + i as f64 * 10.0 + wave;
            let spread = 0.02 + 0.01 * (1.0 + wave);
            let mut levels = [0.0; 20];
            for level in 0..5 {
                levels[level * 4] = mid - spread / 2.0 - level as f64 * 0.01;
                levels[level * 4 + 1] = 10.0 + 5.0 * wave + level as f64;
                levels[level * 4 + 2] = mid + spread / 2.0 + level as f64 * 0.01;
                levels[level * 4 + 3] = 10.0 - 5.0 * wave + level as f64;
            }
            stats.received.fetch_add(1, Ordering::Relaxed);
            enqueue(
                &tx,
                Book {
                    symbol: format!("SIM{i:03}USDT"),
                    event_ms: receive_ns / 1_000_000 - 8,
                    transaction_ms: receive_ns / 1_000_000 - 10,
                    receive_ns,
                    first_id: seq,
                    update_id: seq,
                    previous_id: seq - 1,
                    levels,
                },
                &stats,
            )?;
        }
    }
}

async fn writer(mut rx: mpsc::Receiver<Book>, db: db::Db, stats: Arc<Stats>) -> Result<()> {
    let max_batch = setting("BATCH_SIZE", 250)?;
    ensure!(max_batch <= 5000, "BATCH_SIZE must be <= 5000");
    let mut timer = time::interval(Duration::from_millis(setting("BATCH_MS", 20)? as u64));
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut rows = Vec::with_capacity(max_batch);
    loop {
        let closed = tokio::select! {
            row = rx.recv() => match row {
                Some(r) => { rows.push(r); false },
                None => true,
            },
            _ = timer.tick() => {
                if rows.is_empty() { continue; }
                // Force a flush by leaving the select.
                flush(&db, &mut rows, &stats).await?;
                continue;
            }
        };
        if rows.len() >= max_batch || closed {
            flush(&db, &mut rows, &stats).await?;
        }
        if closed {
            return Ok(());
        }
    }
}

async fn flush(db: &db::Db, rows: &mut Vec<Book>, stats: &Stats) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let start = Instant::now();
    // A timeout has an ambiguous commit outcome. Never blindly replay it into a stateful engine.
    db.execute(&insert_script(rows))
        .await
        .context("ingest failed; batch outcome may be unknown; no automatic write retry")?;
    stats
        .inserted
        .fetch_add(rows.len() as u64, Ordering::Relaxed);
    tracing::debug!(
        rows = rows.len(),
        request_ms = start.elapsed().as_secs_f64() * 1000.0,
        "stream append acknowledged"
    );
    rows.clear();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let db = db::Db::wait_ready().await?;
    if let Some(path) = env::args().nth(1) {
        let script = std::fs::read_to_string(path)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&db.execute(&script).await?)?
        );
        return Ok(());
    }
    let before_ms = now_ns() / 1_000_000;
    let server_time = db.execute("long(now())").await?;
    let server_ms = server_time[0].as_i64().context("missing database clock")?;
    let after_ms = now_ns() / 1_000_000;
    ensure!(
        (before_ms - 5000..=after_ms + 5000).contains(&server_ms),
        "DolphinDB clock is not UTC/synchronized; run the database with TZ=UTC"
    );
    db.execute(&std::fs::read_to_string("dolphindb/init.dos")?)
        .await
        .context("initializing streaming pipeline")?;
    let capacity = setting("QUEUE_CAPACITY", 10000)?;
    let (tx, rx) = mpsc::channel(capacity);
    let stats = Arc::new(Stats::default());
    let mode = env::var("FEED_MODE").unwrap_or_else(|_| "live".into());
    ensure!(
        mode == "live" || mode == "synthetic",
        "FEED_MODE must be live or synthetic"
    );
    let s = stats.clone();
    let mut producer = tokio::spawn(async move {
        if mode == "live" {
            live(tx, s).await
        } else {
            synthetic(tx, s).await
        }
    });
    let mut consumer = tokio::spawn(writer(rx, db, stats.clone()));
    let mut metrics = time::interval(Duration::from_secs(10));
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = term.recv() => break,
            result = &mut producer => {
                // Producer owns the last sender; drain before reporting its failure.
                consumer.await??;
                return result?;
            }
            result = &mut consumer => {
                producer.abort();
                return result?;
            }
            _ = metrics.tick() => {
                tracing::info!(received=stats.received.load(Ordering::Relaxed),
                    inserted=stats.inserted.load(Ordering::Relaxed), rejected=stats.rejected.load(Ordering::Relaxed),
                    stale=stats.stale.load(Ordering::Relaxed), dropped=stats.dropped.load(Ordering::Relaxed),
                    reconnects=stats.reconnects.load(Ordering::Relaxed), "collector totals (process lifetime)");
            }
        }
    }
    producer.abort();
    let _ = producer.await; // Drops sender so the writer can drain.
    time::timeout(Duration::from_secs(25), consumer).await???;
    tracing::info!("queue drained; collector stopped");
    Ok(())
}
