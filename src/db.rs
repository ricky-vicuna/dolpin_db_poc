use anyhow::{Context, Result, bail, ensure};
use reqwest::Client;
use serde_json::{Value, json};
use std::time::Duration;

pub struct Db {
    client: Client,
    url: String,
    token: String,
}

pub fn check_response(v: &Value) -> Result<()> {
    ensure!(
        v["code"] == 0 || v["code"] == "0",
        "DolphinDB: {}",
        v["message"]
    );
    Ok(())
}

impl Db {
    pub async fn connect() -> Result<Self> {
        let client = Client::builder().timeout(Duration::from_secs(15)).build()?;
        let url = std::env::var("DDB_URL").unwrap_or_else(|_| "http://localhost:8848".into());
        let v: Value = client.post(format!("{url}/api/login"))
            .json(&json!({"username": std::env::var("DDB_USER").unwrap_or_else(|_| "admin".into()),
                         "password": std::env::var("DDB_PASSWORD").unwrap_or_else(|_| "123456".into())}))
            .send().await?.error_for_status()?.json().await?;
        check_response(&v)?;
        let token = v["result"][0]
            .as_str()
            .context("missing login token")?
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        Ok(Self { client, url, token })
    }

    pub async fn execute(&self, script: &str) -> Result<Value> {
        let v: Value = self
            .client
            .post(format!("{}/api/execute", self.url))
            .bearer_auth(&self.token)
            .json(&json!({"script": script, "length": 10000}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        check_response(&v)?;
        Ok(v["result"].clone())
    }

    pub async fn wait_ready() -> Result<Self> {
        for attempt in 1..=60 {
            match Self::connect().await {
                Ok(db) => return Ok(db),
                Err(e) => tracing::warn!(attempt, error=%e, "waiting for DolphinDB"),
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        bail!("DolphinDB unavailable; inspect database logs and license")
    }
}
