use anyhow::{Result, ensure};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Depth {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "E")]
    pub event_ms: i64,
    #[serde(rename = "T")]
    pub transaction_ms: i64,
    #[serde(rename = "U")]
    pub first_id: i64,
    #[serde(rename = "u")]
    pub update_id: i64,
    #[serde(rename = "pu")]
    pub previous_id: i64,
    #[serde(rename = "b")]
    pub bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Clone)]
pub struct Book {
    pub symbol: String,
    pub event_ms: i64,
    pub transaction_ms: i64,
    pub receive_ns: i64,
    pub first_id: i64,
    pub update_id: i64,
    pub previous_id: i64,
    // bid price, bid size, ask price, ask size, repeated for five levels
    pub levels: [f64; 20],
}

pub fn valid_symbol(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 32
        && s.bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

impl Book {
    pub fn parse(text: &str, receive_ns: i64) -> Result<Self> {
        let v: serde_json::Value = serde_json::from_str(text)?;
        let d: Depth = serde_json::from_value(v.get("data").unwrap_or(&v).clone())?;
        ensure!(valid_symbol(&d.symbol), "invalid symbol");
        ensure!(
            d.bids.len() == 5 && d.asks.len() == 5,
            "expected a complete depth-5 snapshot"
        );
        ensure!(
            d.event_ms > 0 && d.transaction_ms > 0 && d.update_id >= d.first_id,
            "invalid metadata"
        );
        let mut levels = [0.0; 20];
        for i in 0..5 {
            for (j, s) in [&d.bids[i][0], &d.bids[i][1], &d.asks[i][0], &d.asks[i][1]]
                .iter()
                .enumerate()
            {
                let n: f64 = s.parse()?;
                ensure!(n.is_finite() && n > 0.0, "invalid price/quantity");
                levels[i * 4 + j] = n;
            }
            if i > 0 {
                ensure!(
                    levels[(i - 1) * 4] > levels[i * 4]
                        && levels[(i - 1) * 4 + 2] < levels[i * 4 + 2],
                    "unsorted book"
                );
            }
        }
        ensure!(levels[0] < levels[2], "crossed or locked book");
        Ok(Self {
            symbol: d.symbol,
            event_ms: d.event_ms,
            transaction_ms: d.transaction_ms,
            receive_ns,
            first_id: d.first_id,
            update_id: d.update_id,
            previous_id: d.previous_id,
            levels,
        })
    }
}

// Only validated symbols and parsed finite numbers enter this script. No remote text is executable.
pub fn insert_script(rows: &[Book]) -> String {
    assert!(!rows.is_empty());
    let vector = |f: &dyn Fn(&Book) -> String| rows.iter().map(f).collect::<Vec<_>>().join(",");
    let mut columns = vec![
        format!(
            "symbol([{}]) as symbol",
            vector(&|r| format!("\"{}\"", r.symbol))
        ),
        format!(
            "timestamp(long([{}])) as eventTs",
            vector(&|r| r.event_ms.to_string())
        ),
        format!(
            "timestamp(long([{}])) as transactionTs",
            vector(&|r| r.transaction_ms.to_string())
        ),
        format!(
            "nanotimestamp(long([{}])) as receiveTs",
            vector(&|r| r.receive_ns.to_string())
        ),
        format!("long([{}]) as firstId", vector(&|r| r.first_id.to_string())),
        format!(
            "long([{}]) as updateId",
            vector(&|r| r.update_id.to_string())
        ),
        format!(
            "long([{}]) as previousId",
            vector(&|r| r.previous_id.to_string())
        ),
    ];
    for i in 0..5 {
        for (j, name) in ["bp", "bq", "ap", "aq"].iter().enumerate() {
            columns.push(format!(
                "double([{}]) as {}{}",
                vector(&|r| r.levels[i * 4 + j].to_string()),
                name,
                i + 1
            ));
        }
    }
    format!(
        "incoming=table({}); tableInsert(rawStream, select symbol,date(eventTs) as eventDate,eventTs,transactionTs,receiveTs,take(now(true),size(incoming)) as ingestTs,firstId,updateId,previousId,bp1,bq1,ap1,aq1,bp2,bq2,ap2,aq2,bp3,bq3,ap3,aq3,bp4,bq4,ap4,aq4,bp5,bq5,ap5,aq5 from incoming)",
        columns.join(",")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> String {
        serde_json::json!({"data":{"s":"BTCUSDT","E":1700000000000_i64,"T":1700000000000_i64,
            "U":10,"u":12,"pu":9,"b":[["100","3"],["99","2"],["98","1"],["97","1"],["96","1"]],
            "a":[["102","1"],["103","2"],["104","1"],["105","1"],["106","1"]]}})
        .to_string()
    }
    #[test]
    fn parses_snapshot_and_preserves_clocks_and_levels() {
        let b = Book::parse(&fixture(), 1700000000123456789).unwrap();
        assert_eq!(b.levels[0..4], [100., 3., 102., 1.]);
        assert_eq!(b.levels[16..20], [96., 1., 106., 1.]);
        let sql = insert_script(&[b]);
        assert!(sql.contains("1700000000123456789"));
        assert!(sql.contains("long([12]) as updateId"));
    }
    #[test]
    fn rejects_incomplete_nonfinite_crossed_and_injected_data() {
        for (a, b) in [
            ("\"100\"", "\"NaN\""),
            ("\"100\"", "\"103\""),
            ("BTCUSDT", "BTC\";evil"),
            ("\"3\"", "\"0\""),
        ] {
            assert!(Book::parse(&fixture().replace(a, b), 1).is_err());
        }
        let mut v: serde_json::Value = serde_json::from_str(&fixture()).unwrap();
        v["data"]["b"].as_array_mut().unwrap().pop();
        assert!(Book::parse(&v.to_string(), 1).is_err());
    }
}
