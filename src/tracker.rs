use crate::trade::signal::TradeSignal;
use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use chrono::Utc;
use futures_util::{stream::SplitSink, stream::SplitStream, SinkExt, StreamExt};
use orca_whirlpools_client::Whirlpool as WhirlpoolAccount;
use orca_whirlpools_core::sqrt_price_to_price;
use reqwest::Client as HttpClient;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;
use spl_token::{solana_program::program_pack::Pack, state::Mint};
use std::{
    collections::HashMap,
    env,
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::{
    sync::{broadcast, mpsc, oneshot, Mutex, Notify},
    time::sleep,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

const ORCA_WHIRLPOOL_PROGRAM_ID: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const OPEN_POSITION_DISCRIMINATOR: [u8; 8] = [135, 128, 47, 77, 15, 152, 240, 49];
const OPEN_POSITION_WITH_METADATA_DISCRIMINATOR: [u8; 8] = [242, 29, 134, 48, 58, 110, 14, 60];
const OPEN_POSITION_WITH_TOKEN_EXTENSIONS_DISCRIMINATOR: [u8; 8] =
    [212, 47, 95, 92, 114, 102, 131, 250];
const INCREASE_LIQUIDITY_DISCRIMINATOR: [u8; 8] = [46, 156, 243, 118, 13, 205, 251, 178];
const INCREASE_LIQUIDITY_V2_DISCRIMINATOR: [u8; 8] = [133, 29, 89, 223, 69, 238, 176, 10];
const WHIRLPOOL_DISCRIMINATOR: [u8; 8] = [63, 149, 209, 12, 225, 128, 99, 9];
const WS_PING_INTERVAL: Duration = Duration::from_secs(15);
const WS_STALE_TIMEOUT: Duration = Duration::from_secs(60);
const BLACKLIST_CHECK_INTERVAL_SECS: u64 = 30 * 60;
const BLACKLIST_WAIT_MS: i64 = 24 * 60 * 60 * 1000;
#[allow(dead_code)]
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
#[allow(dead_code)]
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

#[derive(Clone)]
pub struct Shutdown {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl Shutdown {
    pub fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn trigger(&self) {
        if !self.flag.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }

    pub async fn notified(&self) {
        self.notify.notified().await;
    }
}
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct PoolInfo {
    whirlpool: Pubkey,
    token_mint_a: Pubkey,
    token_mint_b: Pubkey,
    decimals_a: u8,
    decimals_b: u8,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum PlayerState {
    Idle,
    InPool {
        whirlpool: Pubkey,
        other_mint: Pubkey,
        buy_price_wsol_per_token: f64,
        target_price_wsol_per_token: f64,
    },
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct PositionKey {
    player: Pubkey,
    signature: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct TrackedPosition {
    key: PositionKey,
    player: Pubkey,
    signature: String,
    whirlpool: Pubkey,
    other_mint: Pubkey,
    buy_price_wsol_per_token: f64,
    target_price_wsol_per_token: f64,
    profit_pct: f64,
    entry_ts_ms: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct OpenPosition {
    player: Pubkey,
    signature: String,
    whirlpool: Pubkey,
    other_mint: Pubkey,
    entry_ts_ms: i64,
    buy_price_wsol_per_token: f64,
    target_price_wsol_per_token: f64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct PriceHit {
    id: i64,
    ts_ms: i64,
    price: f64,
}

#[derive(Clone)]
struct Db {
    conn: Arc<Mutex<Connection>>,
    event_tx: Option<mpsc::UnboundedSender<TradeSignal>>,
}

impl Db {
    fn now_ms() -> i64 {
        Utc::now().timestamp_millis()
    }

    async fn init(
        path: &str,
        event_tx: Option<mpsc::UnboundedSender<TradeSignal>>,
    ) -> Result<Self> {
        let conn = Connection::open(path).context("open sqlite db")?;
        conn.pragma_update(None, "journal_mode", "WAL").ok(); // WAL если поддерживается

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS events (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ms           INTEGER NOT NULL,
                player          TEXT,
                signature       TEXT,
                action          TEXT NOT NULL,
                whirlpool       TEXT,
                other_mint      TEXT,
                price           REAL,
                target_price    REAL,
                wsol_delta      TEXT,
                token_delta     TEXT,
                details_json    TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts_ms);
            CREATE INDEX IF NOT EXISTS idx_events_player ON events(player);
            CREATE INDEX IF NOT EXISTS idx_events_whirlpool ON events(whirlpool);
        "#,
        )
        .context("create schema")?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            event_tx,
        })
    }

    async fn log_event(
        &self,
        player: Option<&Pubkey>,
        signature: Option<&str>,
        action: &str,
        whirlpool: Option<&Pubkey>,
        other_mint: Option<&Pubkey>,
        price: Option<f64>,
        target_price: Option<f64>,
        wsol_delta: Option<i128>,
        token_delta: Option<i128>,
        details: Value,
    ) -> Result<()> {
        let ts_ms = Self::now_ms();
        let player_s = player.map(|p| p.to_string());
        let whirl_s = whirlpool.map(|p| p.to_string());
        let other_s = other_mint.map(|p| p.to_string());
        let details_s = serde_json::to_string(&details).unwrap_or_else(|_| "{}".to_string());

        let conn = self.conn.lock().await;
        conn.execute(
            r#"
            INSERT INTO events
            (ts_ms, player, signature, action, whirlpool, other_mint, price, target_price, wsol_delta, token_delta, details_json)
            VALUES
            (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            params![
                ts_ms,
                player_s,
                signature,
                action,
                whirl_s,
                other_s,
                price,
                target_price,
                wsol_delta.map(|v| v.to_string()),
                token_delta.map(|v| v.to_string()),
                details_s
            ],
        )
        .with_context(|| format!("insert event action={action}"))?;

        if action == "ENTRY_SIGNAL" || action == "TARGET_HIT" {
            if let Some(tx) = &self.event_tx {
                let id = conn.last_insert_rowid();
                let signal = TradeSignal {
                    id,
                    action: action.to_string(),
                };
                if tx.send(signal).is_err() {
                    eprintln!("trade signal channel closed, action={action}");
                }
            }
        }

        Ok(())
    }

    async fn load_open_positions(&self) -> Result<Vec<OpenPosition>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            r#"
            SELECT
                e.ts_ms,
                e.player,
                e.signature,
                e.whirlpool,
                e.other_mint,
                e.price,
                e.target_price
            FROM events e
            WHERE e.action = 'ENTRY_SIGNAL'
              AND e.player IS NOT NULL
              AND e.signature IS NOT NULL
              AND e.whirlpool IS NOT NULL
              AND e.other_mint IS NOT NULL
              AND e.price IS NOT NULL
              AND e.target_price IS NOT NULL
              AND NOT EXISTS (
                SELECT 1
                FROM events t
                WHERE t.player = e.player
                  AND t.signature = e.signature
                  AND t.action = 'TARGET_HIT'
              )
            ORDER BY e.ts_ms ASC, e.id ASC;
        "#,
        )?;

        let mut out = Vec::new();
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, f64>(5)?,
                row.get::<_, f64>(6)?,
            ))
        })?;

        for row in rows {
            let (ts_ms, player_s, signature, whirlpool_s, other_s, buy_price, target_price) = row?;
            let player = Pubkey::from_str(&player_s).context("bad player pubkey")?;
            let whirlpool = Pubkey::from_str(&whirlpool_s).context("bad whirlpool pubkey")?;
            let other_mint = Pubkey::from_str(&other_s).context("bad other_mint pubkey")?;

            out.push(OpenPosition {
                player,
                signature,
                whirlpool,
                other_mint,
                entry_ts_ms: ts_ms,
                buy_price_wsol_per_token: buy_price,
                target_price_wsol_per_token: target_price,
            });
        }

        Ok(out)
    }

    async fn find_price_hit(
        &self,
        whirlpool: &Pubkey,
        since_ts_ms: i64,
        buy_price: f64,
        target_price: f64,
    ) -> Result<Option<PriceHit>> {
        let conn = self.conn.lock().await;
        // При восстановлении учитываем направление: для падения цена должна быть <= цели.
        let is_short = is_short_position(buy_price, target_price);
        let sql = if is_short {
            r#"
            SELECT id, ts_ms, price
            FROM events
            WHERE whirlpool = ?1
              AND action = 'PRICE_UPDATE'
              AND ts_ms >= ?2
              AND price <= ?3
            ORDER BY ts_ms ASC, id ASC
            LIMIT 1;
        "#
        } else {
            r#"
            SELECT id, ts_ms, price
            FROM events
            WHERE whirlpool = ?1
              AND action = 'PRICE_UPDATE'
              AND ts_ms >= ?2
              AND price >= ?3
            ORDER BY ts_ms ASC, id ASC
            LIMIT 1;
        "#
        };
        let mut stmt = conn.prepare(sql)?;

        let mut rows = stmt.query(params![whirlpool.to_string(), since_ts_ms, target_price])?;
        if let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let ts_ms: i64 = row.get(1)?;
            let price: f64 = row.get(2)?;
            return Ok(Some(PriceHit { id, ts_ms, price }));
        }

        Ok(None)
    }

    async fn count_open_positions(&self, player: &Pubkey) -> Result<u64> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            r#"
            SELECT COUNT(1)
            FROM events e
            WHERE e.action = 'ENTRY_SIGNAL'
              AND e.player = ?1
              AND NOT EXISTS (
                SELECT 1
                FROM events t
                WHERE t.player = e.player
                  AND t.signature = e.signature
                  AND t.action = 'TARGET_HIT'
              )
        "#,
        )?;
        let count: u64 = stmt.query_row(params![player.to_string()], |row| row.get(0))?;
        Ok(count)
    }

    async fn count_profitable(&self, player: &Pubkey) -> Result<u64> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            r#"
            SELECT COUNT(1)
            FROM events
            WHERE action = 'TARGET_HIT'
              AND player = ?1
        "#,
        )?;
        let count: u64 = stmt.query_row(params![player.to_string()], |row| row.get(0))?;
        Ok(count)
    }

    async fn first_entry_ts_ms(&self, player: &Pubkey) -> Result<Option<i64>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            r#"
            SELECT ts_ms
            FROM events
            WHERE action = 'ENTRY_SIGNAL'
              AND player = ?1
            ORDER BY ts_ms ASC, id ASC
            LIMIT 1
        "#,
        )?;
        let res: Option<i64> = stmt
            .query_row(params![player.to_string()], |row| row.get(0))
            .optional()?;
        Ok(res)
    }
}

#[derive(Clone)]
struct PlayerDb {
    path: String,
}

impl PlayerDb {
    async fn init(path: &str) -> Result<Self> {
        Ok(Self {
            path: path.to_string(),
        })
    }

    async fn mark_first_entry(&self, player: &Pubkey, ts_ms: i64) -> Result<bool> {
        let conn = Connection::open(&self.path).context("open players db")?;
        let rows = conn.execute(
            r#"
            UPDATE players
            SET blacklist = 'check',
                first_entry_ts_ms = ?1
            WHERE address = ?2
              AND first_entry_ts_ms IS NULL
        "#,
            params![ts_ms, player.to_string()],
        )?;
        Ok(rows > 0)
    }

    async fn list_ready_checks(&self, cutoff_ts_ms: i64) -> Result<Vec<Pubkey>> {
        let conn = Connection::open(&self.path).context("open players db")?;
        let mut stmt = conn.prepare(
            r#"
            SELECT address
            FROM players
            WHERE blacklist = 'check'
              AND first_entry_ts_ms IS NOT NULL
              AND first_entry_ts_ms <= ?1
        "#,
        )?;
        let rows = stmt.query_map(params![cutoff_ts_ms], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for addr in rows {
            let addr = addr?;
            let pk = Pubkey::from_str(&addr)
                .with_context(|| format!("bad pubkey in players.db: {addr}"))?;
            out.push(pk);
        }
        Ok(out)
    }

    async fn set_blacklist(&self, player: &Pubkey, status: &str) -> Result<()> {
        let conn = Connection::open(&self.path).context("open players db")?;
        conn.execute(
            "UPDATE players SET blacklist = ?1 WHERE address = ?2",
            params![status, player.to_string()],
        )?;
        Ok(())
    }

    async fn list_missing_first_entry(&self) -> Result<Vec<Pubkey>> {
        let conn = Connection::open(&self.path).context("open players db")?;
        let mut stmt = conn.prepare(
            r#"
            SELECT address
            FROM players
            WHERE blacklist = 'check'
              AND first_entry_ts_ms IS NULL
        "#,
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for addr in rows {
            let addr = addr?;
            let pk = Pubkey::from_str(&addr)
                .with_context(|| format!("bad pubkey in players.db: {addr}"))?;
            out.push(pk);
        }
        Ok(out)
    }

    async fn set_first_entry_ts(&self, player: &Pubkey, ts_ms: i64) -> Result<()> {
        let conn = Connection::open(&self.path).context("open players db")?;
        conn.execute(
            "UPDATE players SET first_entry_ts_ms = ?1 WHERE address = ?2",
            params![ts_ms, player.to_string()],
        )?;
        Ok(())
    }
}

#[derive(Clone)]
struct HttpRateLimiter {
    min_interval: Duration,
    last: Arc<Mutex<Instant>>,
}

impl HttpRateLimiter {
    fn new(max_per_sec: u32) -> Self {
        // 9 rps => минимум 111.111ms
        let min_interval = Duration::from_millis((1000u64 / max_per_sec as u64).max(1));
        Self {
            min_interval,
            last: Arc::new(Mutex::new(Instant::now() - min_interval)),
        }
    }

    async fn throttle(&self) {
        let mut last = self.last.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(*last);
        if elapsed < self.min_interval {
            sleep(self.min_interval - elapsed).await;
        }
        *last = Instant::now();
    }
}

#[derive(Clone)]
struct HttpRpc {
    url: String,
    client: HttpClient,
    limiter: HttpRateLimiter,
    id: Arc<AtomicU64>,
}

impl HttpRpc {
    fn new(url: String, limiter: HttpRateLimiter) -> Self {
        Self {
            url,
            client: HttpClient::new(),
            limiter,
            id: Arc::new(AtomicU64::new(1)),
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.limiter.throttle().await;

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let body = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });

        let resp = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("HTTP RPC request {method}"))?;

        let status = resp.status();
        let v: Value = resp.json().await.context("HTTP RPC parse json")?;

        if !status.is_success() {
            return Err(anyhow!("HTTP RPC {method} status={status} body={v}"));
        }

        if let Some(err) = v.get("error") {
            return Err(anyhow!("RPC error on {method}: {err}"));
        }

        v.get("result")
            .cloned()
            .ok_or_else(|| anyhow!("RPC {method} missing result: {v}"))
    }

    async fn get_transaction_json(&self, signature: &str) -> Result<Value> {
        self.call(
            "getTransaction",
            json!([
                signature,
                {
                    "encoding": "json",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0
                }
            ]),
        )
        .await
    }

    async fn get_transaction_json_retry(
        &self,
        signature: &str,
        attempts: usize,
        base_delay_ms: u64,
    ) -> Result<Value> {
        let mut last_err: Option<anyhow::Error> = None;

        for attempt in 0..attempts {
            match self.get_transaction_json(signature).await {
                Ok(v) if !v.is_null() => return Ok(v),
                Ok(_) => {
                    last_err = Some(anyhow!(
                        "getTransaction returned null (attempt {} of {})",
                        attempt + 1,
                        attempts
                    ));
                }
                Err(e) => {
                    last_err = Some(e);
                }
            }

            if attempt + 1 < attempts {
                let delay = Duration::from_millis(base_delay_ms * (attempt as u64 + 1));
                sleep(delay).await;
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("getTransaction failed for {signature}")))
    }

    async fn get_multiple_accounts_base64(&self, keys: &[Pubkey]) -> Result<Vec<Option<Vec<u8>>>> {
        let key_strs: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        let res = self
            .call(
                "getMultipleAccounts",
                json!([
                    key_strs,
                    { "encoding": "base64", "commitment": "confirmed" }
                ]),
            )
            .await?;

        let accounts = res
            .get("value")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("getMultipleAccounts unexpected: {res}"))?;

        let mut out = Vec::with_capacity(accounts.len());
        for acc in accounts {
            if acc.is_null() {
                out.push(None);
                continue;
            }
            let data = acc
                .get("data")
                .ok_or_else(|| anyhow!("account missing data: {acc}"))?;
            let bytes = decode_base64_data_field(data)?;
            out.push(Some(bytes));
        }
        Ok(out)
    }

    async fn get_multiple_accounts_base64_retry(
        &self,
        keys: &[Pubkey],
        attempts: usize,
        base_delay_ms: u64,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let mut last_err: Option<anyhow::Error> = None;

        for attempt in 0..attempts {
            match self.get_multiple_accounts_base64(keys).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last_err = Some(e);
                }
            }

            if attempt + 1 < attempts {
                let delay = Duration::from_millis(base_delay_ms * (attempt as u64 + 1));
                sleep(delay).await;
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("getMultipleAccounts failed")))
    }
}

fn decode_base64_data_field(v: &Value) -> Result<Vec<u8>> {
    // Ожидаем ["....", "base64"] или просто "...."
    if let Some(arr) = v.as_array() {
        let b64 = arr
            .get(0)
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow!("base64 data[0] missing: {v}"))?;
        return Ok(base64::engine::general_purpose::STANDARD
            .decode(b64)
            .context("base64 decode")?);
    }

    if let Some(s) = v.as_str() {
        return Ok(base64::engine::general_purpose::STANDARD
            .decode(s)
            .context("base64 decode")?);
    }

    Err(anyhow!("unexpected base64 field: {v}"))
}

#[derive(Debug, Clone)]
struct LogsNotification {
    signature: String,
    err: Option<Value>,
    logs: Vec<String>,
    slot: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct AccountNotification {
    pubkey: Pubkey,
    data: Vec<u8>,
    slot: Option<u64>,
}

#[derive(Debug, Clone)]
struct IncreaseInfo {
    whirlpool: Pubkey,
    token_vault_a: Pubkey,
    token_vault_b: Pubkey,
}

#[derive(Debug, Clone)]
struct WhirlpoolTxInfo {
    whirlpool: Pubkey,
    has_open: bool,
    increase: Option<IncreaseInfo>,
    tick_range: Option<(i32, i32)>,
}

#[allow(dead_code)]
pub struct ParserHarness {
    http: HttpRpc,
    pool_cache: Arc<Mutex<HashMap<Pubkey, PoolInfo>>>,
}

#[allow(dead_code)]
impl ParserHarness {
    pub async fn new(http_url: Option<String>) -> Result<Self> {
        let url = http_url.unwrap_or_else(|| {
            env::var("SOLANA_RPC_HTTP").unwrap_or_else(|_| {
                "https://mainnet.helius-rpc.com/?api-key=718bdaa4-fdab-4c4b-8ee3-d208c26bd7a1"
                    .to_string()
            })
        });

        let limiter = HttpRateLimiter::new(9);
        let http = HttpRpc::new(url, limiter);
        let pool_cache = Arc::new(Mutex::new(HashMap::new()));

        Ok(Self { http, pool_cache })
    }

    pub async fn eval_signature(&self, signature: &str) -> Result<bool> {
        let (is_match, _) = self.eval_signature_reason(signature).await?;
        Ok(is_match)
    }

    pub async fn eval_signature_reason(&self, signature: &str) -> Result<(bool, String)> {
        let tx = self.http.get_transaction_json(signature).await?;
        if tx.is_null() {
            return Ok((false, "tx null".to_string()));
        }

        let meta = match tx.get("meta") {
            Some(m) => m,
            None => return Ok((false, "meta missing".to_string())),
        };

        if let Some(err) = meta.get("err") {
            if !err.is_null() {
                return Ok((false, "meta err".to_string()));
            }
        }

        let keys = collect_full_keys(&tx);
        let scan = match scan_whirlpool_instructions(&tx, &keys)? {
            Some(s) => s,
            None => return Ok((false, "no whirlpool instructions".to_string())),
        };

        if !scan.has_open {
            return Ok((false, "no open_position".to_string()));
        }

        let increase = match scan.increase {
            Some(v) => v,
            None => return Ok((false, "no increase".to_string())),
        };

        let pool_info = fetch_pool_info_cached(&self.http, &self.pool_cache, scan.whirlpool)
            .await
            .map_err(|e| anyhow!("pool fetch {}", e))?;

        let wsol = Pubkey::from_str(WSOL_MINT)?;
        let contains_wsol = pool_info.token_mint_a == wsol || pool_info.token_mint_b == wsol;
        if !contains_wsol {
            return Ok((false, "pool without wsol".to_string()));
        }

        let (single_sided, wsol_delta, other_delta, wsol_vault, other_vault) =
            is_single_sided_wsol_deposit(meta, &keys, &increase, &pool_info);
        if single_sided {
            Ok((
                true,
                format!(
                    "match wsol_delta={wsol_delta} other_delta={other_delta} wsol_vault={} other_vault={} mint_a={} mint_b={}",
                    wsol_vault, other_vault, pool_info.token_mint_a, pool_info.token_mint_b
                ),
            ))
        } else {
            Ok((
                false,
                format!(
                    "not single-sided wsol (wsol_delta={wsol_delta} other_delta={other_delta} wsol_vault={} other_vault={} mint_a={} mint_b={})",
                    wsol_vault, other_vault, pool_info.token_mint_a, pool_info.token_mint_b
                ),
            ))
        }
    }
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type WsWrite = SplitSink<WsStream, Message>;
type WsRead = SplitStream<WsStream>;

#[derive(Clone)]
struct LogsSub {
    mention: Pubkey,
    tx: mpsc::UnboundedSender<LogsNotification>,
    server_id: Option<u64>,
}

#[derive(Clone)]
struct AcctSub {
    pubkey: Pubkey,
    tx: mpsc::UnboundedSender<AccountNotification>,
    server_id: Option<u64>,
}

#[derive(Clone)]
struct WsRpc {
    ws_url: String,
    write: Arc<Mutex<Option<WsWrite>>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>>,
    logs_subs: Arc<Mutex<HashMap<u64, LogsSub>>>,
    acct_subs: Arc<Mutex<HashMap<u64, AcctSub>>>,
    server_logs: Arc<Mutex<HashMap<u64, u64>>>,
    server_acct: Arc<Mutex<HashMap<u64, u64>>>,
    id: Arc<AtomicU64>,
    connected: Arc<AtomicBool>,
    reconnect_notify: Arc<Notify>,
    last_pong: Arc<Mutex<Instant>>,
    missed_pongs: Arc<AtomicU64>,
}

impl WsRpc {
    async fn new(ws_url: &str) -> Result<Self> {
        let this = Self {
            ws_url: ws_url.to_string(),
            write: Arc::new(Mutex::new(None)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            logs_subs: Arc::new(Mutex::new(HashMap::new())),
            acct_subs: Arc::new(Mutex::new(HashMap::new())),
            server_logs: Arc::new(Mutex::new(HashMap::new())),
            server_acct: Arc::new(Mutex::new(HashMap::new())),
            id: Arc::new(AtomicU64::new(1)),
            connected: Arc::new(AtomicBool::new(false)),
            reconnect_notify: Arc::new(Notify::new()),
            last_pong: Arc::new(Mutex::new(Instant::now())),
            missed_pongs: Arc::new(AtomicU64::new(0)),
        };

        this.start_connection_manager();
        this.ensure_connected().await?;

        Ok(this)
    }

    fn start_connection_manager(&self) {
        let backoffs = [1u64, 2, 5, 10];
        let mut idx = 0usize;
        let manager = self.clone();
        tokio::spawn(async move {
            loop {
                if manager.connected.load(Ordering::Relaxed) {
                    tokio::select! {
                        _ = manager.reconnect_notify.notified() => {}
                        _ = sleep(Duration::from_secs(2)) => {}
                    }
                    continue;
                }

                match manager.try_connect().await {
                    Ok(()) => {
                        idx = 0;
                    }
                    Err(err) => {
                        eprintln!("WS connect error: {err:?}");
                        let delay = backoffs[idx.min(backoffs.len() - 1)];
                        if idx + 1 < backoffs.len() {
                            idx += 1;
                        }
                        sleep(Duration::from_secs(delay)).await;
                    }
                }
            }
        });
    }

    async fn try_connect(&self) -> Result<()> {
        let url = Url::parse(&self.ws_url).context("parse ws url")?;
        let (ws, _) = connect_async(url.as_str())
            .await
            .context("ws connect_async")?;
        let (write, read) = ws.split();

        {
            let mut w = self.write.lock().await;
            *w = Some(write);
        }
        self.connected.store(true, Ordering::Relaxed);
        {
            let mut lp = self.last_pong.lock().await;
            *lp = Instant::now();
        }
        self.missed_pongs.store(0, Ordering::Relaxed);

        println!("WS подключено: {}", self.ws_url);
        self.spawn_read_loop(read);
        self.spawn_ping_loop();
        if let Err(e) = self.wait_ws_ready().await {
            eprintln!("WS health check failed: {e:?}");
            self.handle_disconnect().await;
            return Err(e);
        }
        self.resubscribe_all().await?;

        Ok(())
    }

    fn spawn_read_loop(&self, mut read: WsRead) {
        let reader = self.clone();
        tokio::spawn(async move {
            if let Err(e) = reader.read_loop(&mut read).await {
                eprintln!("WS read_loop error: {e:?}");
            }
            reader.handle_disconnect().await;
        });
    }

    fn spawn_ping_loop(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            let interval = WS_PING_INTERVAL;
            let stale_timeout = WS_STALE_TIMEOUT;
            loop {
                sleep(interval).await;
                if !this.connected.load(Ordering::Relaxed) {
                    break;
                }

                if let Err(e) = this.send_raw(Message::Ping(b"ping".to_vec())).await {
                    eprintln!("WS ping send error: {e:?}");
                    this.handle_disconnect().await;
                    break;
                }

                let last = *this.last_pong.lock().await;
                if last.elapsed() > stale_timeout {
                    let stale = this.missed_pongs.fetch_add(1, Ordering::Relaxed) + 1;
                    eprintln!("WS таймаут тишины, переподключение (stale={stale})");
                    this.handle_disconnect().await;
                    break;
                } else {
                    this.missed_pongs.store(0, Ordering::Relaxed);
                }
            }
        });
    }

    async fn handle_disconnect(&self) {
        let was_connected = self.connected.swap(false, Ordering::Relaxed);
        {
            let mut w = self.write.lock().await;
            *w = None;
        }

        let mut pending = self.pending.lock().await;
        for (_, tx) in pending.drain() {
            let _ = tx.send(Err(anyhow!("ws disconnected")));
        }

        {
            let mut m = self.server_logs.lock().await;
            m.clear();
        }
        {
            let mut m = self.server_acct.lock().await;
            m.clear();
        }
        {
            let mut subs = self.logs_subs.lock().await;
            for sub in subs.values_mut() {
                sub.server_id = None;
            }
        }
        {
            let mut subs = self.acct_subs.lock().await;
            for sub in subs.values_mut() {
                sub.server_id = None;
            }
        }

        eprintln!("WS отключено, инициируем переподключение");
        if was_connected {
            self.reconnect_notify.notify_waiters();
        }
    }

    async fn ensure_connected(&self) -> Result<()> {
        loop {
            if self.connected.load(Ordering::Relaxed) {
                return Ok(());
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn send_raw(&self, msg: Message) -> Result<()> {
        let mut w = self.write.lock().await;
        let writer = w
            .as_mut()
            .ok_or_else(|| anyhow!("ws writer missing (disconnected)"))?;
        writer.send(msg).await.context("ws send")
    }

    async fn ws_call(&self, method: &str, params: Value) -> Result<Value> {
        self.ensure_connected().await?;
        self.ws_call_connected(method, params).await
    }

    async fn ws_call_connected(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel::<Result<Value>>();

        {
            let mut pending = self.pending.lock().await;
            pending.insert(id, tx);
        }

        let req = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });

        let txt = req.to_string();
        if let Err(e) = self
            .send_raw(Message::Text(txt))
            .await
            .with_context(|| format!("ws send {method}"))
        {
            let mut pending = self.pending.lock().await;
            pending.remove(&id);
            return Err(e);
        }

        rx.await.context("ws response dropped")?
    }

    async fn wait_ws_ready(&self) -> Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            return Err(anyhow!("ws not connected during health check"));
        }
        self.send_raw(Message::Ping(b"health".to_vec()))
            .await
            .context("ws ping health send")
    }

    async fn read_loop(&self, read: &mut WsRead) -> Result<()> {
        while let Some(msg) = read.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => return Err(anyhow!("ws read error: {e}")),
            };

            match msg {
                Message::Text(t) => {
                    self.note_activity().await;
                    self.handle_json_message(&t).await?;
                }
                Message::Binary(b) => {
                    self.note_activity().await;
                    let t = String::from_utf8_lossy(&b).to_string();
                    self.handle_json_message(&t).await?;
                }
                Message::Ping(data) => {
                    self.note_activity().await;
                    let _ = self.send_raw(Message::Pong(data)).await;
                }
                Message::Pong(_) => {
                    self.note_activity().await;
                }
                Message::Close(_) => return Err(anyhow!("ws closed by server")),
                _ => {}
            }
        }

        Err(anyhow!("ws stream ended"))
    }

    async fn note_activity(&self) {
        let mut lp = self.last_pong.lock().await;
        *lp = Instant::now();
        self.missed_pongs.store(0, Ordering::Relaxed);
    }

    async fn handle_json_message(&self, txt: &str) -> Result<()> {
        let v: Value = match serde_json::from_str(txt) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        if let Some(id) = v.get("id").and_then(|x| x.as_u64()) {
            let result = if let Some(err) = v.get("error") {
                Err(anyhow!("ws rpc error: {err}"))
            } else {
                Ok(v.get("result").cloned().unwrap_or(Value::Null))
            };

            let mut pending = self.pending.lock().await;
            if let Some(tx) = pending.remove(&id) {
                let _ = tx.send(result);
            }
            return Ok(());
        }

        let method = v.get("method").and_then(|x| x.as_str()).unwrap_or("");
        match method {
            "logsNotification" => {
                if let Some(params) = v.get("params") {
                    if let Some(server_id) = params.get("subscription").and_then(|x| x.as_u64()) {
                        if let Some(res) = params.get("result") {
                            let logical_id = {
                                let map = self.server_logs.lock().await;
                                map.get(&server_id).copied()
                            };
                            let Some(logical_id) = logical_id else {
                                return Ok(());
                            };

                            let value = res.get("value").unwrap_or(&Value::Null);

                            let signature = value
                                .get("signature")
                                .and_then(|x| x.as_str())
                                .unwrap_or("")
                                .to_string();

                            let err = value.get("err").cloned().filter(|e| !e.is_null());

                            let logs = value
                                .get("logs")
                                .and_then(|x| x.as_array())
                                .map(|arr| {
                                    arr.iter()
                                        .filter_map(|z| z.as_str().map(|s| s.to_string()))
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default();

                            let slot = res
                                .get("context")
                                .and_then(|c| c.get("slot"))
                                .and_then(|x| x.as_u64());

                            let note = LogsNotification {
                                signature,
                                err,
                                logs,
                                slot,
                            };

                            let subs = self.logs_subs.lock().await;
                            if let Some(sub) = subs.get(&logical_id) {
                                let _ = sub.tx.send(note);
                            }
                        }
                    }
                }
            }
            "accountNotification" => {
                if let Some(params) = v.get("params") {
                    if let Some(server_id) = params.get("subscription").and_then(|x| x.as_u64()) {
                        if let Some(res) = params.get("result") {
                            let logical_id = {
                                let map = self.server_acct.lock().await;
                                map.get(&server_id).copied()
                            };
                            let Some(logical_id) = logical_id else {
                                return Ok(());
                            };

                            let pubkey_str = res
                                .get("value")
                                .and_then(|vv| vv.get("pubkey"))
                                .and_then(|x| x.as_str())
                                .or_else(|| res.get("pubkey").and_then(|x| x.as_str()))
                                .unwrap_or("");

                            let slot = res
                                .get("context")
                                .and_then(|c| c.get("slot"))
                                .and_then(|x| x.as_u64());

                            let data = res
                                .get("value")
                                .and_then(|vv| vv.get("data"))
                                .or_else(|| res.get("data"))
                                .cloned()
                                .unwrap_or(Value::Null);

                            let bytes = match decode_base64_data_field(&data) {
                                Ok(b) => b,
                                Err(_) => return Ok(()),
                            };

                            let subs = self.acct_subs.lock().await;
                            if let Some(sub) = subs.get(&logical_id) {
                                let pk = if !pubkey_str.is_empty() {
                                    Pubkey::from_str(pubkey_str).unwrap_or(Pubkey::default())
                                } else {
                                    Pubkey::default()
                                };

                                let _ = sub.tx.send(AccountNotification {
                                    pubkey: pk,
                                    data: bytes,
                                    slot,
                                });
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    async fn resubscribe_all(&self) -> Result<()> {
        {
            let mut map = self.server_logs.lock().await;
            map.clear();
        }
        {
            let mut map = self.server_acct.lock().await;
            map.clear();
        }

        let mut logs_pending = {
            let subs = self.logs_subs.lock().await;
            subs.iter()
                .map(|(id, sub)| (*id, sub.mention))
                .collect::<Vec<_>>()
        };
        let mut acct_pending = {
            let subs = self.acct_subs.lock().await;
            subs.iter()
                .map(|(id, sub)| (*id, sub.pubkey))
                .collect::<Vec<_>>()
        };

        // Повторяем подписки, пока соединение живо и всё не восстановлено
        let backoffs_ms = [100u64, 200, 400, 800, 1600, 2000];
        let mut attempt = 0usize;

        while !logs_pending.is_empty() || !acct_pending.is_empty() {
            if !self.connected.load(Ordering::Relaxed) {
                eprintln!("WS переподключаемся, resubscribe прерван из-за отключения");
                return Err(anyhow!("ws disconnected during resubscribe"));
            }

            let mut next_logs = Vec::new();
            for (logical_id, mention) in logs_pending {
                match self.subscribe_logs_on_server(&mention).await {
                    Ok(server_id) => {
                        self.set_logs_server_id(logical_id, server_id).await;
                        println!(
                            "WS resubscribe logs logical={} server={} mention={}",
                            logical_id, server_id, mention
                        );
                    }
                    Err(e) => {
                        eprintln!("logs resubscribe failed: {e:?}");
                        next_logs.push((logical_id, mention));
                    }
                }
            }
            logs_pending = next_logs;

            let mut next_acct = Vec::new();
            for (logical_id, pubkey) in acct_pending {
                match self.subscribe_account_on_server(&pubkey).await {
                    Ok(server_id) => {
                        self.set_acct_server_id(logical_id, server_id).await;
                        println!(
                            "WS resubscribe account logical={} server={} pubkey={}",
                            logical_id, server_id, pubkey
                        );
                    }
                    Err(e) => {
                        eprintln!("account resubscribe failed: {e:?}");
                        next_acct.push((logical_id, pubkey));
                    }
                }
            }
            acct_pending = next_acct;

            if logs_pending.is_empty() && acct_pending.is_empty() {
                break;
            }

            attempt += 1;
            let delay_idx = attempt.min(backoffs_ms.len() - 1);
            sleep(Duration::from_millis(backoffs_ms[delay_idx])).await;
        }

        self.poke_account_subscribers().await;
        Ok(())
    }

    async fn poke_account_subscribers(&self) {
        let subs = self.acct_subs.lock().await;
        for sub in subs.values() {
            let _ = sub.tx.send(AccountNotification {
                pubkey: sub.pubkey,
                data: Vec::new(),
                slot: None,
            });
        }
    }

    async fn subscribe_logs_on_server(&self, mention: &Pubkey) -> Result<u64> {
        let res = self
            .ws_call_connected(
                "logsSubscribe",
                json!([
                    { "mentions": [mention.to_string()] },
                    { "commitment": "confirmed" }
                ]),
            )
            .await?;
        res.as_u64()
            .ok_or_else(|| anyhow!("logsSubscribe bad result: {res}"))
    }

    async fn subscribe_account_on_server(&self, account: &Pubkey) -> Result<u64> {
        let res = self
            .ws_call_connected(
                "accountSubscribe",
                json!([
                    account.to_string(),
                    { "encoding": "base64", "commitment": "confirmed" }
                ]),
            )
            .await?;
        res.as_u64()
            .ok_or_else(|| anyhow!("accountSubscribe bad result: {res}"))
    }

    async fn set_logs_server_id(&self, logical_id: u64, server_id: u64) {
        {
            let mut map = self.server_logs.lock().await;
            map.insert(server_id, logical_id);
        }
        let mut subs = self.logs_subs.lock().await;
        if let Some(sub) = subs.get_mut(&logical_id) {
            sub.server_id = Some(server_id);
        }
    }

    async fn set_acct_server_id(&self, logical_id: u64, server_id: u64) {
        {
            let mut map = self.server_acct.lock().await;
            map.insert(server_id, logical_id);
        }
        let mut subs = self.acct_subs.lock().await;
        if let Some(sub) = subs.get_mut(&logical_id) {
            sub.server_id = Some(server_id);
        }
    }

    async fn logs_subscribe_mentions(
        &self,
        mention: &Pubkey,
    ) -> Result<(u64, mpsc::UnboundedReceiver<LogsNotification>)> {
        self.ensure_connected().await?;
        let logical_id = self.id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();

        {
            let mut subs = self.logs_subs.lock().await;
            subs.insert(
                logical_id,
                LogsSub {
                    mention: *mention,
                    tx: tx.clone(),
                    server_id: None,
                },
            );
        }

        let server_id = self.subscribe_logs_on_server(mention).await?;
        self.set_logs_server_id(logical_id, server_id).await;
        println!(
            "WS logs subscribe logical={} server={} mention={}",
            logical_id, server_id, mention
        );

        Ok((logical_id, rx))
    }

    async fn logs_unsubscribe(&self, logical_id: u64) -> Result<()> {
        let server_id = {
            let mut subs = self.logs_subs.lock().await;
            if let Some(sub) = subs.remove(&logical_id) {
                sub.server_id
            } else {
                None
            }
        };
        if let Some(server_id) = server_id {
            let _ = self.ws_call("logsUnsubscribe", json!([server_id])).await?;
            let mut map = self.server_logs.lock().await;
            map.remove(&server_id);
        }
        Ok(())
    }

    async fn account_subscribe(
        &self,
        account: &Pubkey,
    ) -> Result<(u64, mpsc::UnboundedReceiver<AccountNotification>)> {
        self.ensure_connected().await?;
        let logical_id = self.id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();

        {
            let mut subs = self.acct_subs.lock().await;
            subs.insert(
                logical_id,
                AcctSub {
                    pubkey: *account,
                    tx: tx.clone(),
                    server_id: None,
                },
            );
        }

        let server_id = self.subscribe_account_on_server(account).await?;
        self.set_acct_server_id(logical_id, server_id).await;
        println!(
            "WS account subscribe logical={} server={} pubkey={}",
            logical_id, server_id, account
        );

        Ok((logical_id, rx))
    }

    async fn account_unsubscribe(&self, logical_id: u64) -> Result<()> {
        let server_id = {
            let mut subs = self.acct_subs.lock().await;
            if let Some(sub) = subs.remove(&logical_id) {
                sub.server_id
            } else {
                None
            }
        };
        if let Some(server_id) = server_id {
            let _ = self
                .ws_call("accountUnsubscribe", json!([server_id]))
                .await?;
            let mut map = self.server_acct.lock().await;
            map.remove(&server_id);
        }
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Clone)]
struct PoolTrackerHandle {
    positions: Arc<Mutex<HashMap<PositionKey, TrackedPosition>>>,
    sub_id: Arc<Mutex<Option<u64>>>,
}

#[derive(Clone)]
struct PoolManager {
    ws: WsRpc,
    http: HttpRpc,
    db: Db,
    pool_cache: Arc<Mutex<HashMap<Pubkey, PoolInfo>>>,
    trackers: Arc<Mutex<HashMap<Pubkey, PoolTrackerHandle>>>,
    states: Arc<Mutex<HashMap<Pubkey, PlayerState>>>,
    shutdown: Shutdown,
}

impl PoolManager {
    fn new(
        ws: WsRpc,
        http: HttpRpc,
        db: Db,
        pool_cache: Arc<Mutex<HashMap<Pubkey, PoolInfo>>>,
        states: Arc<Mutex<HashMap<Pubkey, PlayerState>>>,
        shutdown: Shutdown,
    ) -> Self {
        Self {
            ws,
            http,
            db,
            pool_cache,
            trackers: Arc::new(Mutex::new(HashMap::new())),
            states,
            shutdown,
        }
    }

    async fn register_position(&self, pos: TrackedPosition) -> Result<()> {
        let whirlpool = pos.whirlpool;
        let spawn = {
            let mut trackers = self.trackers.lock().await;
            if let Some(handle) = trackers.get(&whirlpool) {
                let mut positions = handle.positions.lock().await;
                if positions.contains_key(&pos.key) {
                    return Ok(());
                }
                positions.insert(pos.key.clone(), pos);
                return Ok(());
            }

            let positions = Arc::new(Mutex::new(HashMap::new()));
            positions.lock().await.insert(pos.key.clone(), pos);
            let sub_id = Arc::new(Mutex::new(None));
            trackers.insert(
                whirlpool,
                PoolTrackerHandle {
                    positions: positions.clone(),
                    sub_id: sub_id.clone(),
                },
            );
            Some((positions, sub_id, whirlpool))
        };

        if let Some((positions, sub_id, whirlpool)) = spawn {
            let tracker = PoolTracker {
                whirlpool,
                positions,
                sub_id,
                ws: self.ws.clone(),
                http: self.http.clone(),
                db: self.db.clone(),
                pool_cache: self.pool_cache.clone(),
                trackers: self.trackers.clone(),
                states: self.states.clone(),
                shutdown: self.shutdown.clone(),
            };
            tokio::spawn(async move {
                if let Err(e) = tracker.run().await {
                    eprintln!("pool tracker error for {}: {e:?}", whirlpool);
                }
            });
        }

        Ok(())
    }
}

struct PoolTracker {
    whirlpool: Pubkey,
    positions: Arc<Mutex<HashMap<PositionKey, TrackedPosition>>>,
    sub_id: Arc<Mutex<Option<u64>>>,
    ws: WsRpc,
    http: HttpRpc,
    db: Db,
    pool_cache: Arc<Mutex<HashMap<Pubkey, PoolInfo>>>,
    trackers: Arc<Mutex<HashMap<Pubkey, PoolTrackerHandle>>>,
    states: Arc<Mutex<HashMap<Pubkey, PlayerState>>>,
    shutdown: Shutdown,
}

impl PoolTracker {
    async fn run(self) -> Result<()> {
        let pool_info = self.ensure_pool_info().await?;
        let wsol = Pubkey::from_str(WSOL_MINT)?;
        let other_mint_pool = if pool_info.token_mint_a == wsol {
            pool_info.token_mint_b
        } else {
            pool_info.token_mint_a
        };

        loop {
            if self.shutdown.is_shutdown() {
                self.cleanup().await?;
                return Ok(());
            }

            if self.positions.lock().await.is_empty() {
                self.cleanup().await?;
                return Ok(());
            }

            let (sub_id, mut rx) = self.subscribe_with_retry().await?;
            {
                let mut guard = self.sub_id.lock().await;
                *guard = Some(sub_id);
            }

            let _ = self
                .db
                .log_event(
                    None,
                    None,
                    "POOL_SUBSCRIBE",
                    Some(&self.whirlpool),
                    Some(&other_mint_pool),
                    None,
                    None,
                    None,
                    None,
                    json!({"pool_sub_id": sub_id}),
                )
                .await;

            if let Err(e) = self.check_current_price(&pool_info, &other_mint_pool).await {
                let _ = self
                    .db
                    .log_event(
                        None,
                        None,
                        "POOL_SNAPSHOT_ERROR",
                        Some(&self.whirlpool),
                        Some(&other_mint_pool),
                        None,
                        None,
                        None,
                        None,
                        json!({ "error": e.to_string() }),
                    )
                    .await;
            }

            loop {
                if self.shutdown.is_shutdown() {
                    break;
                }
                let upd = tokio::select! {
                    upd = rx.recv() => upd,
                    _ = self.shutdown.notified() => {
                        break;
                    }
                };
                let Some(upd) = upd else {
                    break;
                };
                if upd.data.is_empty() {
                    if let Err(e) = self.check_current_price(&pool_info, &other_mint_pool).await {
                        let _ = self
                            .db
                            .log_event(
                                None,
                                None,
                                "POOL_SNAPSHOT_ERROR",
                                Some(&self.whirlpool),
                                Some(&other_mint_pool),
                                None,
                                None,
                                None,
                                None,
                                json!({ "error": e.to_string(), "source": "resubscribe_poke" }),
                            )
                            .await;
                    }
                    continue;
                }

                let whirl_acc = match parse_whirlpool_account(&upd.data) {
                    Ok(x) => x,
                    Err(e) => {
                        let _ = self
                            .db
                            .log_event(
                                None,
                                None,
                                "POOL_ACCOUNT_PARSE_ERROR",
                                Some(&self.whirlpool),
                                Some(&other_mint_pool),
                                None,
                                None,
                                None,
                                None,
                                json!({"error": format!("{e:?}")}),
                            )
                            .await;
                        continue;
                    }
                };

                let (other, current_price) = match compute_wsol_per_token(
                    whirl_acc.sqrt_price,
                    &pool_info.token_mint_a,
                    &pool_info.token_mint_b,
                    pool_info.decimals_a,
                    pool_info.decimals_b,
                ) {
                    Some(v) => v,
                    None => continue,
                };

                if other != other_mint_pool {
                    continue;
                }

                self.process_price_update(current_price, upd.slot, "ws")
                    .await?;

                if self.positions.lock().await.is_empty() {
                    break;
                }
            }

            if self.shutdown.is_shutdown() {
                self.cleanup().await?;
                return Ok(());
            }

            if self.positions.lock().await.is_empty() {
                continue;
            }

            let _ = self
                .db
                .log_event(
                    None,
                    None,
                    "POOL_SUB_STREAM_CLOSED",
                    Some(&self.whirlpool),
                    Some(&other_mint_pool),
                    None,
                    None,
                    None,
                    None,
                    json!({"pool_sub_id": sub_id}),
                )
                .await;
        }
    }

    async fn ensure_pool_info(&self) -> Result<PoolInfo> {
        loop {
            match fetch_pool_info_cached(&self.http, &self.pool_cache, self.whirlpool).await {
                Ok(info) => return Ok(info),
                Err(e) => {
                    let _ = self
                        .db
                        .log_event(
                            None,
                            None,
                            "POOL_INFO_FETCH_FAILED",
                            Some(&self.whirlpool),
                            None,
                            None,
                            None,
                            None,
                            None,
                            json!({ "error": e.to_string() }),
                        )
                        .await;
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    async fn subscribe_with_retry(
        &self,
    ) -> Result<(u64, mpsc::UnboundedReceiver<AccountNotification>)> {
        loop {
            match self.ws.account_subscribe(&self.whirlpool).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    let _ = self
                        .db
                        .log_event(
                            None,
                            None,
                            "POOL_SUBSCRIBE_FAILED",
                            Some(&self.whirlpool),
                            None,
                            None,
                            None,
                            None,
                            None,
                            json!({ "error": e.to_string() }),
                        )
                        .await;
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    async fn check_current_price(
        &self,
        pool_info: &PoolInfo,
        other_mint_pool: &Pubkey,
    ) -> Result<()> {
        let accs = self
            .http
            .get_multiple_accounts_base64_retry(&[self.whirlpool], 5, 200)
            .await?;
        let whirl_data = accs
            .get(0)
            .and_then(|x| x.as_ref())
            .ok_or_else(|| anyhow!("whirlpool account missing for snapshot price"))?;
        let whirl_acc = parse_whirlpool_account(whirl_data)?;

        let (other, current_price) = compute_wsol_per_token(
            whirl_acc.sqrt_price,
            &pool_info.token_mint_a,
            &pool_info.token_mint_b,
            pool_info.decimals_a,
            pool_info.decimals_b,
        )
        .ok_or_else(|| anyhow!("snapshot price computation failed"))?;

        if &other != other_mint_pool {
            return Ok(());
        }

        self.process_price_update(current_price, None, "http_snapshot")
            .await
    }

    async fn process_price_update(
        &self,
        current_price: f64,
        slot: Option<u64>,
        source: &str,
    ) -> Result<()> {
        let positions_snapshot = {
            let positions = self.positions.lock().await;
            positions.values().cloned().collect::<Vec<_>>()
        };

        let mut to_close = Vec::new();
        for pos in positions_snapshot {
            self.db
                .log_event(
                    Some(&pos.player),
                    Some(&pos.signature),
                    "PRICE_UPDATE",
                    Some(&pos.whirlpool),
                    Some(&pos.other_mint),
                    Some(current_price),
                    Some(pos.target_price_wsol_per_token),
                    None,
                    None,
                    json!({
                        "slot": slot,
                        "buy_price": pos.buy_price_wsol_per_token,
                        "source": source
                    }),
                )
                .await?;

            // Для позиции на падение цель ниже входа, поэтому условие достижения обратное.
            let is_short = is_short_position(
                pos.buy_price_wsol_per_token,
                pos.target_price_wsol_per_token,
            );
            let hit = if is_short {
                current_price <= pos.target_price_wsol_per_token
            } else {
                current_price >= pos.target_price_wsol_per_token
            };
            if hit {
                to_close.push(pos);
            }
        }

        for pos in to_close {
            let removed = {
                let mut positions = self.positions.lock().await;
                positions.remove(&pos.key)
            };
            if removed.is_some() {
                self.db
                    .log_event(
                        Some(&pos.player),
                        Some(&pos.signature),
                        "TARGET_HIT",
                        Some(&pos.whirlpool),
                        Some(&pos.other_mint),
                        Some(current_price),
                        Some(pos.target_price_wsol_per_token),
                        None,
                        None,
                        json!({
                            "profit_pct": pos.profit_pct,
                            "source": source
                        }),
                    )
                    .await?;

                let mut st = self.states.lock().await;
                st.insert(pos.player, PlayerState::Idle);
            }
        }

        Ok(())
    }

    async fn cleanup(&self) -> Result<()> {
        let sub_id = {
            let mut guard = self.sub_id.lock().await;
            guard.take()
        };
        if let Some(id) = sub_id {
            let _ = self.ws.account_unsubscribe(id).await;
            let _ = self
                .db
                .log_event(
                    None,
                    None,
                    "POOL_UNSUBSCRIBE",
                    Some(&self.whirlpool),
                    None,
                    None,
                    None,
                    None,
                    None,
                    json!({"pool_sub_id": id, "reason": "no_positions"}),
                )
                .await;
        }

        let mut trackers = self.trackers.lock().await;
        if let Some(handle) = trackers.get(&self.whirlpool) {
            if Arc::ptr_eq(&handle.positions, &self.positions) {
                trackers.remove(&self.whirlpool);
            }
        }

        Ok(())
    }
}

fn discriminator_matches(data_b58: &str, disc: &[u8; 8]) -> bool {
    let bytes = match bs58::decode(data_b58).into_vec() {
        Ok(b) => b,
        Err(_) => return false,
    };
    bytes.len() >= 8 && &bytes[..8] == disc
}

fn extract_instruction_accounts(ix: &Value) -> Option<Vec<usize>> {
    ix.get("accounts")?.as_array().map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_u64().map(|u| u as usize))
            .collect::<Vec<_>>()
    })
}

fn extract_program_id_index(ix: &Value) -> Option<usize> {
    ix.get("programIdIndex")?.as_u64().map(|u| u as usize)
}

fn extract_data_b58(ix: &Value) -> Option<String> {
    ix.get("data")?.as_str().map(|s| s.to_string())
}

fn sum_owner_mint_delta(meta: &Value, owner: &Pubkey, mint: &Pubkey) -> Result<i128> {
    let pre = meta
        .get("preTokenBalances")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let post = meta
        .get("postTokenBalances")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();

    let mut pre_sum: i128 = 0;
    for b in pre {
        let o = b.get("owner").and_then(|x| x.as_str()).unwrap_or("");
        let m = b.get("mint").and_then(|x| x.as_str()).unwrap_or("");
        if o == owner.to_string() && m == mint.to_string() {
            let amt = b
                .get("uiTokenAmount")
                .and_then(|u| u.get("amount"))
                .and_then(|a| a.as_str())
                .unwrap_or("0");
            pre_sum += amt.parse::<i128>().unwrap_or(0);
        }
    }

    let mut post_sum: i128 = 0;
    for b in post {
        let o = b.get("owner").and_then(|x| x.as_str()).unwrap_or("");
        let m = b.get("mint").and_then(|x| x.as_str()).unwrap_or("");
        if o == owner.to_string() && m == mint.to_string() {
            let amt = b
                .get("uiTokenAmount")
                .and_then(|u| u.get("amount"))
                .and_then(|a| a.as_str())
                .unwrap_or("0");
            post_sum += amt.parse::<i128>().unwrap_or(0);
        }
    }

    Ok(post_sum - pre_sum)
}

fn owner_lamport_delta(meta: &Value, msg: &Value, owner: &Pubkey) -> Result<Option<i128>> {
    let account_keys = match msg.get("accountKeys").and_then(|k| k.as_array()) {
        Some(k) => k,
        None => return Ok(None),
    };

    let owner_str = owner.to_string();
    let mut owner_idx: Option<usize> = None;
    for (i, v) in account_keys.iter().enumerate() {
        let s = v
            .as_str()
            .or_else(|| v.get("pubkey").and_then(|p| p.as_str()))
            .unwrap_or("");
        if s == owner_str {
            owner_idx = Some(i);
            break;
        }
    }
    let idx = match owner_idx {
        Some(i) => i,
        None => return Ok(None),
    };

    let pre_balances = match meta.get("preBalances").and_then(|b| b.as_array()) {
        Some(v) => v,
        None => return Ok(None),
    };
    let post_balances = match meta.get("postBalances").and_then(|b| b.as_array()) {
        Some(v) => v,
        None => return Ok(None),
    };

    let pre = match pre_balances.get(idx).and_then(|v| v.as_u64()) {
        Some(v) => v as i128,
        None => return Ok(None),
    };
    let post = match post_balances.get(idx).and_then(|v| v.as_u64()) {
        Some(v) => v as i128,
        None => return Ok(None),
    };

    Ok(Some(post - pre))
}

fn parse_whirlpool_account(data: &[u8]) -> Result<WhirlpoolAccount> {
    if data.len() < WhirlpoolAccount::LEN {
        return Err(anyhow!("whirlpool account data too short"));
    }

    if &data[..8] != WHIRLPOOL_DISCRIMINATOR {
        return Err(anyhow!("whirlpool discriminator mismatch"));
    }

    let acc = WhirlpoolAccount::from_bytes(&data[..WhirlpoolAccount::LEN])
        .context("borsh deserialize whirlpool")?;
    Ok(acc)
}

fn load_mint_decimals(mint_data: &[u8]) -> Result<u8> {
    let mint = Mint::unpack(mint_data).context("spl-token Mint unpack")?;
    Ok(mint.decimals)
}

fn compute_wsol_per_token(
    sqrt_price_x64: u128,
    token_mint_a: &Pubkey,
    token_mint_b: &Pubkey,
    decimals_a: u8,
    decimals_b: u8,
) -> Option<(Pubkey, f64)> {
    let wsol = Pubkey::from_str(WSOL_MINT).ok()?;
    let price_b_per_a = sqrt_price_to_price(sqrt_price_x64, decimals_a, decimals_b); // tokenB/tokenA

    if *token_mint_b == wsol {
        // price = WSOL per tokenA
        return Some((*token_mint_a, price_b_per_a));
    }
    if *token_mint_a == wsol {
        // price = tokenB per WSOL, значит WSOL per tokenB = 1/price
        if price_b_per_a == 0.0 {
            return None;
        }
        return Some((*token_mint_b, 1.0 / price_b_per_a));
    }
    None
}

async fn fetch_pool_info_cached(
    http: &HttpRpc,
    cache: &Arc<Mutex<HashMap<Pubkey, PoolInfo>>>,
    whirlpool: Pubkey,
) -> Result<PoolInfo> {
    {
        let c = cache.lock().await;
        if let Some(info) = c.get(&whirlpool) {
            return Ok(info.clone());
        }
    }

    // getMultipleAccounts: whirlpool + mintA + mintB
    let keys = vec![whirlpool];
    let mut accs = http
        .get_multiple_accounts_base64_retry(&keys, 5, 200)
        .await?;
    let whirl_data = accs
        .remove(0)
        .ok_or_else(|| anyhow!("whirlpool account not found: {whirlpool}"))?;

    let whirl_acc = parse_whirlpool_account(&whirl_data)?;
    let mint_a = whirl_acc.token_mint_a;
    let mint_b = whirl_acc.token_mint_b;

    let keys2 = vec![mint_a, mint_b];
    let accs2 = http
        .get_multiple_accounts_base64_retry(&keys2, 5, 200)
        .await?;

    let mint_a_data = accs2
        .get(0)
        .and_then(|x| x.as_ref())
        .ok_or_else(|| anyhow!("mint A account not found"))?;
    let mint_b_data = accs2
        .get(1)
        .and_then(|x| x.as_ref())
        .ok_or_else(|| anyhow!("mint B account not found"))?;

    let decimals_a = load_mint_decimals(mint_a_data)?;
    let decimals_b = load_mint_decimals(mint_b_data)?;

    let info = PoolInfo {
        whirlpool,
        token_mint_a: mint_a,
        token_mint_b: mint_b,
        decimals_a,
        decimals_b,
    };

    {
        let mut c = cache.lock().await;
        c.insert(whirlpool, info.clone());
    }

    Ok(info)
}

fn tx_contains_orca_whirlpool_logs(logs: &[String]) -> bool {
    logs.iter().any(|l| l.contains(ORCA_WHIRLPOOL_PROGRAM_ID))
}

fn collect_full_keys(tx: &Value) -> Vec<String> {
    let mut out = Vec::new();
    let msg = tx
        .get("transaction")
        .and_then(|t| t.get("message"))
        .unwrap_or(&Value::Null);

    if let Some(arr) = msg.get("accountKeys").and_then(|k| k.as_array()) {
        for v in arr {
            if let Some(s) = v.as_str() {
                out.push(s.to_string());
            } else if let Some(pk) = v.get("pubkey").and_then(|x| x.as_str()) {
                out.push(pk.to_string());
            }
        }
    }

    if let Some(loaded) = tx.get("meta").and_then(|m| m.get("loadedAddresses")) {
        for k in ["writable", "readonly"] {
            if let Some(arr) = loaded.get(k).and_then(|x| x.as_array()) {
                for v in arr {
                    if let Some(s) = v.as_str() {
                        out.push(s.to_string());
                    }
                }
            }
        }
    }

    out
}

fn parse_increase_accounts(
    disc: &[u8; 8],
    ix_accounts: &[usize],
    keys: &[String],
) -> Option<IncreaseInfo> {
    let (whirlpool_idx, vault_a_idx, vault_b_idx) = if disc == &INCREASE_LIQUIDITY_V2_DISCRIMINATOR
    {
        // Account order (v2): whirlpool, token_program_a, token_program_b, memo_program,
        // position_authority, position, position_token_account, token_mint_a, token_mint_b,
        // token_owner_account_a, token_owner_account_b, token_vault_a, token_vault_b, ...
        if ix_accounts.len() < 13 {
            return None;
        }
        (0, 11, 12)
    } else if disc == &INCREASE_LIQUIDITY_DISCRIMINATOR {
        // Legacy order: whirlpool, token_program, token_owner, position_authority, position,
        // token_owner_account_a, token_owner_account_b, token_vault_a, token_vault_b, ...
        if ix_accounts.len() < 9 {
            return None;
        }
        (0, 7, 8)
    } else {
        return None;
    };

    let whirlpool = keys
        .get(*ix_accounts.get(whirlpool_idx)?)
        .and_then(|k| Pubkey::from_str(k).ok())?;
    let token_vault_a = keys
        .get(*ix_accounts.get(vault_a_idx)?)
        .and_then(|k| Pubkey::from_str(k).ok())?;
    let token_vault_b = keys
        .get(*ix_accounts.get(vault_b_idx)?)
        .and_then(|k| Pubkey::from_str(k).ok())?;

    Some(IncreaseInfo {
        whirlpool,
        token_vault_a,
        token_vault_b,
    })
}

fn parse_open_position_ticks(disc: &[u8; 8], data_bytes: &[u8]) -> Option<(i32, i32)> {
    // Anchor layouts (little-endian):
    // openPosition: discriminator + u8 bump + i32 tick_lower + i32 tick_upper
    // openPositionWithMetadata: discriminator + u8 bump + u8 metadata_bump + i32 + i32
    // openPositionWithTokenExtensions: discriminator + i32 + i32 + u8 with_metadata_extension
    let (tick_lower_off, tick_upper_off) = if disc == &OPEN_POSITION_DISCRIMINATOR {
        (8 + 1, 8 + 1 + 4)
    } else if disc == &OPEN_POSITION_WITH_METADATA_DISCRIMINATOR {
        (8 + 2, 8 + 2 + 4)
    } else if disc == &OPEN_POSITION_WITH_TOKEN_EXTENSIONS_DISCRIMINATOR {
        (8, 8 + 4)
    } else {
        return None;
    };

    let lower = data_bytes
        .get(tick_lower_off..tick_lower_off + 4)?
        .try_into()
        .ok()
        .map(i32::from_le_bytes)?;
    let upper = data_bytes
        .get(tick_upper_off..tick_upper_off + 4)?
        .try_into()
        .ok()
        .map(i32::from_le_bytes)?;

    Some((lower, upper))
}

fn scan_whirlpool_instructions(tx: &Value, keys: &[String]) -> Result<Option<WhirlpoolTxInfo>> {
    let msg = tx
        .get("transaction")
        .and_then(|t| t.get("message"))
        .ok_or_else(|| anyhow!("bad tx: missing transaction.message"))?;

    let instructions_outer = msg
        .get("instructions")
        .and_then(|i| i.as_array())
        .ok_or_else(|| anyhow!("bad tx: missing instructions"))?;

    // Собираем все инструкции: верхнего уровня и внутренние
    let mut all_instructions: Vec<&Value> = Vec::new();
    for ix in instructions_outer {
        all_instructions.push(ix);
    }
    if let Some(inner_sets) = tx
        .get("meta")
        .and_then(|m| m.get("innerInstructions"))
        .and_then(|v| v.as_array())
    {
        for set in inner_sets {
            if let Some(inner) = set.get("instructions").and_then(|x| x.as_array()) {
                for ix in inner {
                    all_instructions.push(ix);
                }
            }
        }
    }

    let mut whirlpool_opt: Option<Pubkey> = None;
    let mut has_open = false;
    let mut increase: Option<IncreaseInfo> = None;
    let mut tick_range: Option<(i32, i32)> = None;

    for ix in all_instructions {
        let pid_idx = match extract_program_id_index(ix) {
            Some(x) => x,
            None => continue,
        };
        let program_id = keys.get(pid_idx).cloned().unwrap_or_default();
        if program_id != ORCA_WHIRLPOOL_PROGRAM_ID {
            continue;
        }

        let data_b58 = match extract_data_b58(ix) {
            Some(d) => d,
            None => continue,
        };
        let accounts = match extract_instruction_accounts(ix) {
            Some(a) => a,
            None => continue,
        };
        let data_bytes = match bs58::decode(&data_b58).into_vec() {
            Ok(b) => b,
            Err(_) => continue,
        };
        if data_bytes.len() < 8 {
            continue;
        }
        let disc: [u8; 8] = data_bytes[..8].try_into().unwrap_or([0u8; 8]);

        // openPosition* (IDL: whirlpool на accounts[5])
        if discriminator_matches(&data_b58, &OPEN_POSITION_DISCRIMINATOR)
            || discriminator_matches(&data_b58, &OPEN_POSITION_WITH_METADATA_DISCRIMINATOR)
            || discriminator_matches(
                &data_b58,
                &OPEN_POSITION_WITH_TOKEN_EXTENSIONS_DISCRIMINATOR,
            )
        {
            has_open = true;
            if accounts.len() > 5 {
                if let Some(k) = keys.get(accounts[5]) {
                    if let Ok(w) = Pubkey::from_str(k) {
                        whirlpool_opt = Some(w);
                    }
                }
            }
            if tick_range.is_none() {
                tick_range = parse_open_position_ticks(&disc, &data_bytes);
            }
        }

        // increaseLiquidity (оба варианта)
        if disc == INCREASE_LIQUIDITY_DISCRIMINATOR || disc == INCREASE_LIQUIDITY_V2_DISCRIMINATOR {
            if let Some(info) = parse_increase_accounts(&disc, &accounts, keys) {
                whirlpool_opt = Some(info.whirlpool);
                increase = Some(info);
            }
        }
    }

    if whirlpool_opt.is_none() {
        return Ok(None);
    }

    Ok(Some(WhirlpoolTxInfo {
        whirlpool: whirlpool_opt.unwrap(),
        has_open,
        increase,
        tick_range,
    }))
}

fn pct_to_edge_by_tick(
    tick_current: i32,
    tick_lower: i32,
    tick_upper: i32,
    direction: &str,
) -> Option<f64> {
    match direction {
        "long" => {
            if tick_upper < tick_current {
                return None;
            }
            let delta = tick_upper - tick_current;
            let ratio = 1.0001f64.powi(delta);
            Some((ratio - 1.0) * 100.0)
        }
        "short" => {
            if tick_lower > tick_current {
                return None;
            }
            let delta = tick_current - tick_lower;
            let ratio = 1.0001f64.powi(delta);
            Some((ratio - 1.0) / ratio * 100.0)
        }
        _ => None,
    }
}

fn is_single_sided_wsol_deposit(
    meta: &Value,
    keys: &[String],
    increase: &IncreaseInfo,
    pool_info: &PoolInfo,
) -> (bool, i128, i128, Pubkey, Pubkey) {
    let wsol = match Pubkey::from_str(WSOL_MINT) {
        Ok(pk) => pk,
        Err(_) => return (false, 0, 0, Pubkey::default(), Pubkey::default()),
    };

    let (wsol_vault, other_vault) = if pool_info.token_mint_a == wsol {
        (increase.token_vault_a, increase.token_vault_b)
    } else {
        (increase.token_vault_b, increase.token_vault_a)
    };

    let wsol_delta = token_balance_delta(meta, keys, &wsol_vault);
    let other_delta = token_balance_delta(meta, keys, &other_vault);

    let single_sided = (wsol_delta > 0 && other_delta == 0) || (other_delta > 0 && wsol_delta == 0);

    (
        single_sided,
        wsol_delta,
        other_delta,
        wsol_vault,
        other_vault,
    )
}

fn token_balance_delta(meta: &Value, keys: &[String], account: &Pubkey) -> i128 {
    let pre = token_balance_amount(meta, "preTokenBalances", keys, account).unwrap_or(0);
    let post = token_balance_amount(meta, "postTokenBalances", keys, account).unwrap_or(0);
    post as i128 - pre as i128
}

fn token_balance_amount(
    meta: &Value,
    field: &str,
    keys: &[String],
    account: &Pubkey,
) -> Option<u128> {
    let balances = meta.get(field)?.as_array()?;
    for bal in balances {
        let idx = bal.get("accountIndex")?.as_u64()? as usize;
        let key = keys.get(idx)?;
        if key != &account.to_string() {
            continue;
        }
        let amount_str = bal
            .get("uiTokenAmount")?
            .get("amount")?
            .as_str()
            .unwrap_or("0");
        if let Ok(v) = amount_str.parse::<u128>() {
            return Some(v);
        }
    }
    None
}

async fn log_reject(
    _db: &Db,
    player: &Pubkey,
    signature: &str,
    whirlpool: Option<&Pubkey>,
    other_mint: Option<&Pubkey>,
    reason: &str,
    details: Value,
) -> Result<()> {
    println!(
        "ENTRY_REJECT player={} sig={} whirlpool={:?} other_mint={:?} reason={} details={}",
        player, signature, whirlpool, other_mint, reason, details
    );
    Ok(())
}

fn profit_pct_from_prices(buy_price: f64, target_price: f64) -> f64 {
    if buy_price <= 0.0 {
        return 0.0;
    }
    let raw = (target_price / buy_price) - 1.0;
    raw.abs()
}

fn is_short_position(buy_price: f64, target_price: f64) -> bool {
    target_price < buy_price
}

async fn restore_open_positions(
    db: &Db,
    pool_manager: &PoolManager,
    states: &Arc<Mutex<HashMap<Pubkey, PlayerState>>>,
) -> Result<()> {
    let open_positions = db.load_open_positions().await?;
    if open_positions.is_empty() {
        return Ok(());
    }

    let mut to_track = Vec::new();
    for pos in open_positions {
        if pos.buy_price_wsol_per_token <= 0.0 || pos.target_price_wsol_per_token <= 0.0 {
            bail!(
                "invalid open position prices for player={} signature={}",
                pos.player,
                pos.signature
            );
        }

        if let Some(hit) = db
            .find_price_hit(
                &pos.whirlpool,
                pos.entry_ts_ms,
                pos.buy_price_wsol_per_token,
                pos.target_price_wsol_per_token,
            )
            .await?
        {
            let profit_pct = profit_pct_from_prices(
                pos.buy_price_wsol_per_token,
                pos.target_price_wsol_per_token,
            );
            db.log_event(
                Some(&pos.player),
                Some(&pos.signature),
                "TARGET_HIT",
                Some(&pos.whirlpool),
                Some(&pos.other_mint),
                Some(hit.price),
                Some(pos.target_price_wsol_per_token),
                None,
                None,
                json!({
                    "source": "db_catchup",
                    "price_event_id": hit.id,
                    "price_event_ts_ms": hit.ts_ms,
                    "profit_pct": profit_pct
                }),
            )
            .await?;
            continue;
        }

        to_track.push(pos);
    }

    for pos in to_track {
        let profit_pct = profit_pct_from_prices(
            pos.buy_price_wsol_per_token,
            pos.target_price_wsol_per_token,
        );
        let tracked = TrackedPosition {
            key: PositionKey {
                player: pos.player,
                signature: pos.signature.clone(),
            },
            player: pos.player,
            signature: pos.signature.clone(),
            whirlpool: pos.whirlpool,
            other_mint: pos.other_mint,
            buy_price_wsol_per_token: pos.buy_price_wsol_per_token,
            target_price_wsol_per_token: pos.target_price_wsol_per_token,
            profit_pct,
            entry_ts_ms: pos.entry_ts_ms,
        };

        pool_manager.register_position(tracked).await?;

        {
            let mut st = states.lock().await;
            st.insert(
                pos.player,
                PlayerState::InPool {
                    whirlpool: pos.whirlpool,
                    other_mint: pos.other_mint,
                    buy_price_wsol_per_token: pos.buy_price_wsol_per_token,
                    target_price_wsol_per_token: pos.target_price_wsol_per_token,
                },
            );
        }

        db.log_event(
            Some(&pos.player),
            Some(&pos.signature),
            "POSITION_RECOVERED",
            Some(&pos.whirlpool),
            Some(&pos.other_mint),
            Some(pos.buy_price_wsol_per_token),
            Some(pos.target_price_wsol_per_token),
            None,
            None,
            json!({ "entry_ts_ms": pos.entry_ts_ms }),
        )
        .await?;
    }

    Ok(())
}

async fn evaluate_blacklist(
    player_db: &PlayerDb,
    db: &Db,
    blacklist_tx: &broadcast::Sender<Pubkey>,
) -> Result<()> {
    let missing = player_db.list_missing_first_entry().await?;
    for player in missing {
        if let Some(ts_ms) = db.first_entry_ts_ms(&player).await? {
            let _ = player_db.set_first_entry_ts(&player, ts_ms).await;
        }
    }

    let cutoff = Db::now_ms() - BLACKLIST_WAIT_MS;
    let players = player_db.list_ready_checks(cutoff).await?;
    if players.is_empty() {
        return Ok(());
    }

    for player in players {
        let open_positions = db.count_open_positions(&player).await?;
        let profitable = db.count_profitable(&player).await?;
        let good = (open_positions == 0 && profitable > 1)
            || (open_positions > 0 && profitable > open_positions);

        if good {
            player_db.set_blacklist(&player, "good").await?;
            println!(
                "✅ Игрок {} проверен: good (profit={}, open={})",
                player, profitable, open_positions
            );
        } else {
            player_db.set_blacklist(&player, "black").await?;
            let _ = blacklist_tx.send(player);
            println!(
                "⛔ Игрок {} проверен: black (profit={}, open={})",
                player, profitable, open_positions
            );
        }
    }

    Ok(())
}

async fn player_loop(
    player: Pubkey,
    ws: WsRpc,
    http: HttpRpc,
    db: Db,
    player_db: PlayerDb,
    pool_cache: Arc<Mutex<HashMap<Pubkey, PoolInfo>>>,
    states: Arc<Mutex<HashMap<Pubkey, PlayerState>>>,
    pool_manager: PoolManager,
    profit_pct: f64,
    mut blacklist_rx: broadcast::Receiver<Pubkey>,
    shutdown: Shutdown,
) -> Result<()> {
    db.log_event(
        Some(&player),
        None,
        "PLAYER_START",
        None,
        None,
        None,
        None,
        None,
        None,
        json!({"player": player.to_string()}),
    )
    .await?;

    let (logs_sub_id, mut logs_rx) = loop {
        if shutdown.is_shutdown() {
            return Ok(());
        }
        match ws.logs_subscribe_mentions(&player).await {
            Ok(v) => break v,
            Err(e) => {
                let _ = db
                    .log_event(
                        Some(&player),
                        None,
                        "LOGS_SUBSCRIBE_FAILED",
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        json!({ "error": e.to_string() }),
                    )
                    .await;
                tokio::select! {
                    _ = sleep(Duration::from_millis(500)) => {}
                    _ = shutdown.notified() => {
                        return Ok(());
                    }
                }
                continue;
            }
        }
    };
    println!(
        "Игрок {}: подписка на логи logical_id={}",
        player, logs_sub_id
    );
    db.log_event(
        Some(&player),
        None,
        "LOGS_SUBSCRIBE",
        None,
        None,
        None,
        None,
        None,
        None,
        json!({"logs_sub_id": logs_sub_id}),
    )
    .await?;

    loop {
        if shutdown.is_shutdown() {
            break;
        }
        tokio::select! {
            note = logs_rx.recv() => {
                let Some(note) = note else {
                    break;
                };
        // Игнорируем ошибки транзакций
        if note.err.is_some() {
            log_reject(
                &db,
                &player,
                &note.signature,
                None,
                None,
                "tx error in logs notification",
                json!({ "err": note.err }),
            )
            .await?;
            continue;
        }

        // Быстрый pre-filter: только Orca Whirlpool
        if !tx_contains_orca_whirlpool_logs(&note.logs) {
            log_reject(
                &db,
                &player,
                &note.signature,
                None,
                None,
                "logs without orca whirlpool markers",
                json!({ "logs_sample": note.logs }),
            )
            .await?;
            continue;
        }

        // Забираем транзакцию (HTTP, лимит 9 rps)
        let tx = match http
            .get_transaction_json_retry(&note.signature, 6, 200)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                db.log_event(
                    Some(&player),
                    Some(&note.signature),
                    "TX_FETCH_ERROR",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    json!({"error": format!("{e:?}")}),
                )
                .await?;
                eprintln!("TX fetch error for {}: {e:?}", note.signature);
                continue;
            }
        };

        let msg = match tx.get("transaction").and_then(|t| t.get("message")) {
            Some(m) => m,
            None => {
                log_reject(
                    &db,
                    &player,
                    &note.signature,
                    None,
                    None,
                    "transaction.message missing",
                    json!({}),
                )
                .await?;
                continue;
            }
        };

        let full_keys = collect_full_keys(&tx);

        // Должны быть open_position* + increase_liquidity* в одной tx
        let scan = match scan_whirlpool_instructions(&tx, &full_keys)? {
            Some(s) => s,
            None => {
                log_reject(
                    &db,
                    &player,
                    &note.signature,
                    None,
                    None,
                    "no whirlpool open/increase instructions",
                    json!({}),
                )
                .await?;
                continue;
            }
        };

        if !scan.has_open {
            log_reject(
                &db,
                &player,
                &note.signature,
                Some(&scan.whirlpool),
                None,
                "open_position missing",
                json!({ "whirlpool": scan.whirlpool }),
            )
            .await?;
            continue;
        }

        let increase = match scan.increase {
            Some(v) => v,
            None => {
                log_reject(
                    &db,
                    &player,
                    &note.signature,
                    Some(&scan.whirlpool),
                    None,
                    "increase_liquidity missing",
                    json!({ "whirlpool": scan.whirlpool }),
                )
                .await?;
                continue;
            }
        };
        let whirlpool = scan.whirlpool;

        // Получаем мету чтобы проверить "односторонний вклад WSOL"
        let meta = match tx.get("meta") {
            Some(m) => m,
            None => {
                log_reject(
                    &db,
                    &player,
                    &note.signature,
                    Some(&whirlpool),
                    None,
                    "meta missing",
                    json!({ "whirlpool": whirlpool }),
                )
                .await?;
                continue;
            }
        };

        // Подтягиваем pool info + decimals
        let pool_info = match fetch_pool_info_cached(&http, &pool_cache, whirlpool).await {
            Ok(v) => v,
            Err(e) => {
                let _ = db
                    .log_event(
                        Some(&player),
                        Some(&note.signature),
                        "POOL_INFO_FETCH_FAILED",
                        Some(&whirlpool),
                        None,
                        None,
                        None,
                        None,
                        None,
                        json!({ "error": e.to_string() }),
                    )
                    .await;
                continue;
            }
        };

        // Проверяем что пул содержит WSOL
        let wsol = Pubkey::from_str(WSOL_MINT)?;
        let contains_wsol = pool_info.token_mint_a == wsol || pool_info.token_mint_b == wsol;
        if !contains_wsol {
            log_reject(
                &db,
                &player,
                &note.signature,
                Some(&whirlpool),
                None,
                "pool without wsol",
                json!({
                    "mint_a": pool_info.token_mint_a,
                    "mint_b": pool_info.token_mint_b
                }),
            )
            .await?;
            continue;
        }

        // Определяем "other mint"
        let other_mint = if pool_info.token_mint_a == wsol {
            pool_info.token_mint_b
        } else {
            pool_info.token_mint_a
        };

        // Проверяем “односторонний WSOL” по фактической дельте на vault'ах
        let (single_sided, wsol_delta_vault, other_delta_vault, wv, ov) =
            is_single_sided_wsol_deposit(meta, &full_keys, &increase, &pool_info);
        if !single_sided {
            log_reject(
                &db,
                &player,
                &note.signature,
                Some(&whirlpool),
                Some(&other_mint),
                "not single-sided deposit",
                json!({
                    "wsol_vault_delta": wsol_delta_vault,
                    "other_vault_delta": other_delta_vault,
                    "wsol_vault": wv,
                    "other_vault": ov,
                    "mint_a": pool_info.token_mint_a,
                    "mint_b": pool_info.token_mint_b
                }),
            )
            .await?;
            continue;
        }

        // Для логов фиксируем дельты по owner (могут быть нулевыми в оборачивающих схемах)
        let wsol_delta = sum_owner_mint_delta(meta, &player, &wsol).unwrap_or(0);
        let token_delta = sum_owner_mint_delta(meta, &player, &other_mint).unwrap_or(0);
        let lamport_delta = owner_lamport_delta(meta, msg, &player).unwrap_or(None);

        // Берём текущую цену из whirlpool аккаунта (через getMultipleAccounts уже взяли кеш,
        // но нужна sqrt_price на момент сигнала; корректнее быстро взять ещё раз сам whirlpool)
        let accs = match http
            .get_multiple_accounts_base64_retry(&[whirlpool], 5, 200)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                let _ = log_reject(
                    &db,
                    &player,
                    &note.signature,
                    Some(&whirlpool),
                    Some(&other_mint),
                    "getMultipleAccounts failed (temporary network)",
                    json!({ "error": e.to_string() }),
                )
                .await;
                continue;
            }
        };
        let whirl_data = accs
            .get(0)
            .and_then(|x| x.as_ref())
            .ok_or_else(|| anyhow!("whirlpool account missing for price"))?;
        let whirl_acc = parse_whirlpool_account(whirl_data)?;

        // Цена WSOL per token
        let (other_from_price, buy_price) = match compute_wsol_per_token(
            whirl_acc.sqrt_price,
            &pool_info.token_mint_a,
            &pool_info.token_mint_b,
            pool_info.decimals_a,
            pool_info.decimals_b,
        ) {
            Some(v) => v,
            None => {
                log_reject(
                    &db,
                    &player,
                    &note.signature,
                    Some(&whirlpool),
                    Some(&other_mint),
                    "price computation failed",
                    json!({
                        "sqrt_price": whirl_acc.sqrt_price,
                        "mint_a": pool_info.token_mint_a,
                        "mint_b": pool_info.token_mint_b
                    }),
                )
                .await?;
                continue;
            }
        };

        if other_from_price != other_mint {
            log_reject(
                &db,
                &player,
                &note.signature,
                Some(&whirlpool),
                Some(&other_mint),
                "price computed for another mint",
                json!({
                    "expected": other_mint,
                    "computed": other_from_price
                }),
            )
            .await?;
            continue;
        }

        // Направление сигнала определяем по одностороннему вкладу в хранилища пула.
        // Рост WSOL-хранилища = ставка на рост цены токена (direction="long"),
        // рост другого хранилища = ставка на падение (direction="short").
        let (direction, target_price) = if wsol_delta_vault > 0 && other_delta_vault == 0 {
            ("long", buy_price * (1.0 + profit_pct))
        } else if other_delta_vault > 0 && wsol_delta_vault == 0 {
            ("short", buy_price * (1.0 - profit_pct))
        } else {
            log_reject(
                &db,
                &player,
                &note.signature,
                Some(&whirlpool),
                Some(&other_mint),
                "unknown direction by vault deltas",
                json!({
                    "wsol_vault_delta": wsol_delta_vault,
                    "other_vault_delta": other_delta_vault
                }),
            )
            .await?;
            continue;
        };
        println!(
            "ENTRY_SIGNAL player={} whirlpool={} direction={} buy={:.8} target={:.8} sig={} ts_ms={}",
            player,
            whirlpool,
            direction,
            buy_price,
            target_price,
            note.signature,
            Db::now_ms()
        );

        let pct_to_edge = scan
            .tick_range
            .and_then(|(t_low, t_up)| {
                pct_to_edge_by_tick(whirl_acc.tick_current_index, t_low, t_up, direction)
            });

        db.log_event(
            Some(&player),
            Some(&note.signature),
            "SIGNAL_SEEN",
            Some(&whirlpool),
            Some(&other_mint),
            Some(buy_price),
            Some(target_price),
            None,
            None,
            json!({
                "slot": note.slot,
                "logs_sub_id": logs_sub_id
            }),
        )
        .await?;

        db.log_event(
            Some(&player),
            Some(&note.signature),
            "ENTRY_SIGNAL",
            Some(&whirlpool),
            Some(&other_mint),
            Some(buy_price),
            Some(target_price),
            Some(wsol_delta),
            Some(token_delta),
            json!({
                "rule": "open_position + increase_liquidity атомарно, односторонний вклад в один токен",
                "direction": direction,
                "slot": note.slot,
                "logs_sub_id": logs_sub_id,
                "lamport_delta": lamport_delta,
                "tick_current": whirl_acc.tick_current_index,
                "tick_lower": scan.tick_range.map(|(l, _)| l),
                "tick_upper": scan.tick_range.map(|(_, u)| u),
                "pct_to_edge": pct_to_edge
            }),
        )
            .await?;

        let entry_ts_ms = Db::now_ms();
        match player_db.mark_first_entry(&player, entry_ts_ms).await {
            Ok(true) => {
                println!("🕒 Игрок {} переведён в check (первый вход)", player);
            }
            Ok(false) => {}
            Err(e) => {
                eprintln!("⚠️ Не удалось обновить players.db для {}: {e:?}", player);
            }
        }

        let tracked = TrackedPosition {
            key: PositionKey {
                player,
                signature: note.signature.clone(),
            },
            player,
            signature: note.signature.clone(),
            whirlpool,
            other_mint,
            buy_price_wsol_per_token: buy_price,
            target_price_wsol_per_token: target_price,
            profit_pct,
            entry_ts_ms,
        };

        pool_manager.register_position(tracked).await?;

        {
            let mut st = states.lock().await;
            st.insert(
                player,
                PlayerState::InPool {
                    whirlpool,
                    other_mint,
                    buy_price_wsol_per_token: buy_price,
                    target_price_wsol_per_token: target_price,
                },
            );
        }
            }
            msg = blacklist_rx.recv() => {
                match msg {
                    Ok(pk) => {
                        if pk == player {
                            println!("⛔ Игрок {} в black, отписка", player);
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
            _ = shutdown.notified() => {
                break;
            }
        }
    }

    // Если цикл завершился — подчистим подписку логов
    let _ = ws.logs_unsubscribe(logs_sub_id).await;

    Ok(())
}

pub async fn run(
    profit_pct: f64,
    players: Vec<Pubkey>,
    mut new_players_rx: UnboundedReceiver<Pubkey>,
    shutdown: Shutdown,
    trade_signal_tx: Option<mpsc::UnboundedSender<TradeSignal>>,
) -> Result<()> {
    if profit_pct <= 0.0 {
        bail!("profit_pct must be positive");
    }

    if players.is_empty() {
        bail!("players list must not be empty");
    }

    let http_url = env::var("SOLANA_RPC_HTTP").unwrap_or_else(|_| {
        "https://mainnet.helius-rpc.com/?api-key=718bdaa4-fdab-4c4b-8ee3-d208c26bd7a1".to_string()
    });

    let ws_url = env::var("SOLANA_RPC_WS").unwrap_or_else(|_| {
        "wss://mainnet.helius-rpc.com/?api-key=718bdaa4-fdab-4c4b-8ee3-d208c26bd7a1".to_string()
    });

    let db = Db::init("tracker.db", trade_signal_tx).await?;
    log_tracker_db_path();

    db.log_event(
        None,
        None,
        "APP_START",
        None,
        None,
        None,
        None,
        None,
        None,
        json!({
            "http": http_url,
            "ws": ws_url,
            "orca_program": ORCA_WHIRLPOOL_PROGRAM_ID,
            "wsol_mint": WSOL_MINT,
            "players": players.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
            "profit_pct": profit_pct
        }),
    )
    .await?;

    // Глобальный лимит 9 HTTP RPC / sec (Helius)
    let limiter = HttpRateLimiter::new(9);
    let http = HttpRpc::new(http_url, limiter);

    let ws = WsRpc::new(&ws_url).await?;
    let player_db = PlayerDb::init("players.db").await?;
    let (blacklist_tx, _) = broadcast::channel::<Pubkey>(1024);

    let pool_cache: Arc<Mutex<HashMap<Pubkey, PoolInfo>>> = Arc::new(Mutex::new(HashMap::new()));
    let states: Arc<Mutex<HashMap<Pubkey, PlayerState>>> = Arc::new(Mutex::new(HashMap::new()));
    let pool_manager = PoolManager::new(
        ws.clone(),
        http.clone(),
        db.clone(),
        pool_cache.clone(),
        states.clone(),
        shutdown.clone(),
    );

    // Инициализируем состояния игроков
    {
        let mut st = states.lock().await;
        for player in players.iter() {
            st.insert(*player, PlayerState::Idle);
        }
    }

    restore_open_positions(&db, &pool_manager, &states).await?;

    let mut handles = Vec::new();

    {
        let player_db_check = player_db.clone();
        let db_check = db.clone();
        let blacklist_tx_check = blacklist_tx.clone();
        let shutdown_check = shutdown.clone();
        let handle = tokio::spawn(async move {
            loop {
                if shutdown_check.is_shutdown() {
                    break;
                }
                tokio::select! {
                    _ = sleep(Duration::from_secs(BLACKLIST_CHECK_INTERVAL_SECS)) => {}
                    _ = shutdown_check.notified() => {
                        break;
                    }
                }
                if shutdown_check.is_shutdown() {
                    break;
                }
                if let Err(e) =
                    evaluate_blacklist(&player_db_check, &db_check, &blacklist_tx_check).await
                {
                    eprintln!("⚠️ Ошибка проверки blacklist: {e:?}");
                }
            }
        });
        handles.push(handle);
    }

    // Запускаем 10 независимых loops
    for player in players {
        let ws2 = ws.clone();
        let http2 = http.clone();
        let db2 = db.clone();
        let player_db2 = player_db.clone();
        let cache2 = pool_cache.clone();
        let states2 = states.clone();
        let pool_manager2 = pool_manager.clone();
        let profit_pct2 = profit_pct;
        let blacklist_rx = blacklist_tx.subscribe();
        let shutdown_player = shutdown.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = player_loop(
                player,
                ws2,
                http2,
                db2,
                player_db2,
                cache2,
                states2,
                pool_manager2,
                profit_pct2,
                blacklist_rx,
                shutdown_player,
            )
            .await
            {
                eprintln!("player_loop error for {player}: {e:?}");
            }
        });
        handles.push(handle);
    }

    // Приём новых игроков для динамического мониторинга
    {
        let ws_new = ws.clone();
        let http_new = http.clone();
        let db_new = db.clone();
        let player_db_new = player_db.clone();
        let cache_new = pool_cache.clone();
        let states_new = states.clone();
        let pool_manager_new = pool_manager.clone();
        let blacklist_tx_new = blacklist_tx.clone();
        let shutdown_new = shutdown.clone();
        let handle = tokio::spawn(async move {
            loop {
                if shutdown_new.is_shutdown() {
                    break;
                }
                let next_player = tokio::select! {
                    p = new_players_rx.recv() => p,
                    _ = shutdown_new.notified() => {
                        break;
                    }
                };
                let Some(player) = next_player else {
                    break;
                };
                let exists = {
                    let st = states_new.lock().await;
                    st.contains_key(&player)
                };
                if exists {
                    println!("ℹ️ Игрок {} уже мониторится, пропуск", player);
                    continue;
                }

                {
                    let mut st = states_new.lock().await;
                    st.insert(player, PlayerState::Idle);
                }

                println!("🚀 Подписываем нового игрока: {}", player);
                let ws2 = ws_new.clone();
                let http2 = http_new.clone();
                let db2 = db_new.clone();
                let player_db2 = player_db_new.clone();
                let cache2 = cache_new.clone();
                let states2 = states_new.clone();
                let pool_manager2 = pool_manager_new.clone();
                let blacklist_rx = blacklist_tx_new.subscribe();
                let shutdown_player = shutdown_new.clone();

                tokio::spawn(async move {
                    if let Err(e) = player_loop(
                        player,
                        ws2,
                        http2,
                        db2,
                        player_db2,
                        cache2,
                        states2,
                        pool_manager2,
                        profit_pct,
                        blacklist_rx,
                        shutdown_player,
                    )
                    .await
                    {
                        eprintln!("player_loop error for {player}: {e:?}");
                    }
                });
            }
        });
        handles.push(handle);
    }

    if !shutdown.is_shutdown() {
        shutdown.notified().await;
    }

    for handle in handles {
        let _ = handle.await;
    }

    db.log_event(
        None,
        None,
        "APP_STOP",
        None,
        None,
        None,
        None,
        None,
        None,
        json!({}),
    )
    .await?;

    Ok(())
}

fn log_tracker_db_path() {
    let path = std::path::Path::new("tracker.db");
    let abs = std::fs::canonicalize(path).unwrap_or_else(|_| {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    });
    println!("🗄️ tracker.db путь: {}", abs.display());
}
