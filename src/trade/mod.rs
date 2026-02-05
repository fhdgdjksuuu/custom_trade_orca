pub mod executor;
pub mod signal;
pub mod swap;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use orca_whirlpools::{
    set_funder, set_native_mint_wrapping_strategy, set_whirlpools_config_address,
    swap_instructions, NativeMintWrappingStrategy, SwapInstructions, SwapQuote, SwapType,
    WhirlpoolsConfigInput,
};
use orca_whirlpools_client::Whirlpool;
use reqwest::Client as HttpClient;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::{json, Value};
use solana_account::Account;
use solana_account_decoder::UiAccountEncoding;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_instruction::Instruction;
use solana_keypair::Keypair;
use solana_program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_rpc_client::http_sender::HttpSender;
use solana_rpc_client::rpc_client::RpcClientConfig;
use solana_rpc_client::rpc_sender::{RpcSender, RpcTransportStats};
use solana_rpc_client_api::client_error::Result as RpcResult;
use solana_rpc_client_api::config::{
    RpcSendTransactionConfig, RpcSimulateTransactionAccountsConfig, RpcSimulateTransactionConfig,
};
use solana_rpc_client_api::request::RpcRequest;
use solana_rpc_client_api::request::TokenAccountsFilter;
use solana_rpc_client_api::response::RpcSimulateTransactionResult;
use solana_signature::Signature;
use solana_signer::Signer;
use solana_transaction::Transaction;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token;
use spl_token_2022;
use std::{
    collections::{HashSet, VecDeque},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const SIMULATION_FEE_LAMPORTS: u64 = 5000;
const RPC_RATE_LIMIT_PER_SEC: usize = 9;
const RPC_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(1);
const SWAP_INSTRUCTIONS_RETRY_DELAY_MS: u64 = 500;
const GET_TRANSACTION_RETRY_ATTEMPTS: usize = 3;
const GET_TRANSACTION_RETRY_DELAY_MS: u64 = 300;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecMode {
    Simulate,
    Live,
}

pub struct Trader {
    pub rpc: RpcClient,
    pub payer: Keypair,
    pub mode: ExecMode,
    pub db: TradeDb,
    rpc_limiter: Arc<RpcRateLimiter>,
}

struct TradeDb {
    path: PathBuf,
}

impl ExecMode {
    fn as_db_str(self) -> &'static str {
        match self {
            ExecMode::Live => "LIVE",
            ExecMode::Simulate => "SIM",
        }
    }
}

fn trade_commitment() -> CommitmentConfig {
    CommitmentConfig::confirmed()
}

struct RpcRateLimiter {
    max_per_window: usize,
    window: Duration,
    state: Mutex<VecDeque<Instant>>,
}

impl RpcRateLimiter {
    fn new(max_per_window: usize, window: Duration) -> Self {
        Self {
            max_per_window,
            window,
            state: Mutex::new(VecDeque::new()),
        }
    }

    async fn acquire(&self) {
        loop {
            let now = Instant::now();
            let mut guard = self.state.lock().await;
            while let Some(front) = guard.front().copied() {
                if now.duration_since(front) >= self.window {
                    guard.pop_front();
                } else {
                    break;
                }
            }
            if guard.len() < self.max_per_window {
                guard.push_back(now);
                return;
            }
            let wait = guard
                .front()
                .map(|t| self.window.saturating_sub(now.duration_since(*t)))
                .unwrap_or(Duration::from_millis(0));
            drop(guard);
            tokio::time::sleep(wait).await;
        }
    }
}

struct RateLimitedRpcSender {
    inner: HttpSender,
    limiter: Arc<RpcRateLimiter>,
}

impl RateLimitedRpcSender {
    fn new(url: String, limiter: Arc<RpcRateLimiter>) -> Self {
        Self {
            inner: HttpSender::new(url),
            limiter,
        }
    }
}

#[async_trait::async_trait]
impl RpcSender for RateLimitedRpcSender {
    async fn send(&self, request: RpcRequest, params: serde_json::Value) -> RpcResult<Value> {
        self.limiter.acquire().await;
        self.inner.send(request, params).await
    }

    fn get_transport_stats(&self) -> RpcTransportStats {
        self.inner.get_transport_stats()
    }

    fn url(&self) -> String {
        self.inner.url()
    }
}

fn build_rate_limited_rpc_client(url: &str, limiter: Arc<RpcRateLimiter>) -> RpcClient {
    let sender = RateLimitedRpcSender::new(url.to_string(), limiter);
    RpcClient::new_sender(sender, RpcClientConfig::with_commitment(trade_commitment()))
}

impl TradeDb {
    fn new(path: &Path) -> Result<Self> {
        let db = Self {
            path: path.to_path_buf(),
        };
        db.ensure_schema()?;
        Ok(db)
    }

    fn open_conn(&self) -> Result<Connection> {
        let conn = Connection::open(&self.path).context("open trade.db")?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("pragma journal_mode")?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("pragma foreign_keys")?;
        conn.busy_timeout(Duration::from_millis(5000))
            .context("pragma busy_timeout")?;
        Ok(conn)
    }

    fn ensure_schema(&self) -> Result<()> {
        let conn = self.open_conn()?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payer TEXT NOT NULL,
                pool TEXT NOT NULL,
                side TEXT NOT NULL CHECK(side IN ('LONG','SHORT')),
                state TEXT NOT NULL CHECK(state IN ('OPENING','OPEN','CLOSING','CLOSED','FAILED')),
                mode TEXT NOT NULL CHECK(mode IN ('LIVE','SIM')),
                slippage_bps INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                reserved_usdc INTEGER NOT NULL DEFAULT 0,
                reserved_sol_lamports INTEGER NOT NULL DEFAULT 0,
                sol_position_lamports INTEGER NOT NULL DEFAULT 0,
                usdc_position INTEGER NOT NULL DEFAULT 0,
                open_sig TEXT,
                close_sig TEXT,
                open_last_valid_block_height INTEGER,
                close_last_valid_block_height INTEGER,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS trade_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id INTEGER NOT NULL REFERENCES positions(id),
                ts_ms INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                details_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sim_wallet (
                payer TEXT PRIMARY KEY,
                usdc_balance INTEGER NOT NULL,
                sol_lamports INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_positions_state ON positions(state);
            CREATE INDEX IF NOT EXISTS idx_positions_payer ON positions(payer);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_positions_payer_open_sig ON positions(payer, open_sig);
            "#,
        )
        .context("init trade db schema")?;
        Self::ensure_positions_columns(&conn)?;
        Ok(())
    }

    fn ensure_positions_columns(conn: &Connection) -> Result<()> {
        let mut stmt = conn.prepare("PRAGMA table_info(positions)")?;
        let mut cols = HashSet::new();
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        for col in rows {
            cols.insert(col?);
        }

        if !cols.contains("entry_price") {
            conn.execute("ALTER TABLE positions ADD COLUMN entry_price REAL", [])?;
        }
        if !cols.contains("exit_price") {
            conn.execute("ALTER TABLE positions ADD COLUMN exit_price REAL", [])?;
        }
        if !cols.contains("profit_pct") {
            conn.execute("ALTER TABLE positions ADD COLUMN profit_pct REAL", [])?;
        }
        Ok(())
    }

    fn now_ms() -> i64 {
        Utc::now().timestamp_millis()
    }

    fn insert_event(
        conn: &Connection,
        position_id: i64,
        event_type: &str,
        details: &Value,
    ) -> Result<()> {
        let ts_ms = Self::now_ms();
        let details_json = serde_json::to_string(details).unwrap_or_else(|_| "{}".to_string());
        conn.execute(
            r#"
            INSERT INTO trade_events (position_id, ts_ms, event_type, details_json)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![position_id, ts_ms, event_type, details_json],
        )
        .with_context(|| format!("insert trade_event {event_type}"))?;
        Ok(())
    }

    fn reserved_totals(conn: &Connection, payer: &Pubkey) -> Result<(u64, u64)> {
        let mut stmt = conn.prepare(
            r#"
            SELECT COALESCE(SUM(reserved_usdc),0), COALESCE(SUM(reserved_sol_lamports),0)
            FROM positions
            WHERE payer = ?1 AND state IN ('OPENING','OPEN','CLOSING')
            "#,
        )?;
        let (usdc, sol): (i64, i64) = stmt.query_row(params![payer.to_string()], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
        Ok((
            u64::try_from(usdc).unwrap_or(0),
            u64::try_from(sol).unwrap_or(0),
        ))
    }

    fn reserved_totals_excluding(
        conn: &Connection,
        payer: &Pubkey,
        exclude_id: i64,
    ) -> Result<(u64, u64)> {
        let mut stmt = conn.prepare(
            r#"
            SELECT COALESCE(SUM(reserved_usdc),0), COALESCE(SUM(reserved_sol_lamports),0)
            FROM positions
            WHERE payer = ?1 AND state IN ('OPENING','OPEN','CLOSING') AND id <> ?2
            "#,
        )?;
        let (usdc, sol): (i64, i64) = stmt
            .query_row(params![payer.to_string(), exclude_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;
        Ok((
            u64::try_from(usdc).unwrap_or(0),
            u64::try_from(sol).unwrap_or(0),
        ))
    }

    fn get_sim_wallet(conn: &Connection, payer: &Pubkey) -> Result<Option<(u64, u64)>> {
        let mut stmt = conn.prepare(
            r#"
            SELECT usdc_balance, sol_lamports
            FROM sim_wallet
            WHERE payer = ?1
            "#,
        )?;
        let row: Option<(i64, i64)> = stmt
            .query_row(params![payer.to_string()], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .optional()?;
        Ok(row.map(|(u, s)| (u64::try_from(u).unwrap_or(0), u64::try_from(s).unwrap_or(0))))
    }

    fn upsert_sim_wallet(conn: &Connection, payer: &Pubkey, usdc: u64, sol: u64) -> Result<()> {
        let ts_ms = Self::now_ms();
        conn.execute(
            r#"
            INSERT INTO sim_wallet (payer, usdc_balance, sol_lamports, updated_at_ms)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(payer) DO UPDATE SET
                usdc_balance = excluded.usdc_balance,
                sol_lamports = excluded.sol_lamports,
                updated_at_ms = excluded.updated_at_ms
            "#,
            params![payer.to_string(), usdc as i64, sol as i64, ts_ms],
        )
        .context("upsert sim_wallet")?;
        Ok(())
    }

    async fn reconcile_pending(
        &self,
        rpc: &RpcClient,
        mode: ExecMode,
        limiter: &Arc<RpcRateLimiter>,
    ) -> Result<()> {
        let http = HttpRpc::new(rpc.url(), limiter.clone());
        let conn = self.open_conn()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT id, state, side, payer, pool, open_sig, close_sig,
                   open_last_valid_block_height, close_last_valid_block_height,
                   reserved_usdc, reserved_sol_lamports, sol_position_lamports, usdc_position
            FROM positions
            WHERE state IN ('OPENING','CLOSING')
            "#,
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, i64>(11)?,
                    row.get::<_, i64>(12)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        if rows.is_empty() {
            return Ok(());
        }

        match mode {
            ExecMode::Simulate => {
                let mut conn = self.open_conn()?;
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                for (id, _, _, _, _, _, _, _, _, _, _, _, _) in &rows {
                    tx.execute(
                        r#"
                        UPDATE positions
                        SET state='FAILED',
                            last_error=?1,
                            reserved_usdc=0,
                            reserved_sol_lamports=0,
                            updated_at_ms=?2
                        WHERE id=?3
                        "#,
                        params!["simulate pending", Self::now_ms(), id],
                    )?;
                    TradeDb::insert_event(
                        &tx,
                        *id,
                        "RECONCILE",
                        &json!({"reason": "SIMULATE_PENDING", "state": "FAILED"}),
                    )?;
                }
                tx.commit()?;
            }
            ExecMode::Live => {
                let current_height = rpc.get_block_height().await? as i64;
                for (
                    id,
                    state,
                    side,
                    payer_s,
                    pool_s,
                    open_sig,
                    close_sig,
                    open_lvb,
                    close_lvb,
                    reserved_usdc_i64,
                    reserved_sol_i64,
                    sol_pos_i64,
                    usdc_pos_i64,
                ) in rows
                {
                    let payer = Pubkey::from_str(&payer_s).unwrap_or_default();
                    let pool = Pubkey::from_str(&pool_s).unwrap_or_default();
                    let sig_str_opt = if state == "OPENING" {
                        open_sig
                    } else {
                        close_sig
                    };
                    let last_valid = if state == "OPENING" {
                        open_lvb
                    } else {
                        close_lvb
                    };

                    let Some(sig_str) = sig_str_opt else {
                        self.fail_position(id, "missing signature", "MISSING_SIGNATURE")?;
                        continue;
                    };
                    let sig = match Signature::from_str(&sig_str) {
                        Ok(s) => s,
                        Err(_) => {
                            self.fail_position(id, "bad signature", "BAD_SIGNATURE")?;
                            continue;
                        }
                    };

                    let statuses = rpc.get_signature_statuses_with_history(&[sig]).await?.value;
                    let status = statuses.get(0).cloned().unwrap_or(None);

                    if let Some(status) = status {
                        if status.err.is_none() {
                            if state == "OPENING" {
                                self.finalize_open_confirmed(
                                    rpc,
                                    &http,
                                    id,
                                    &payer,
                                    &pool,
                                    &side,
                                    reserved_usdc_i64,
                                    reserved_sol_i64,
                                    sol_pos_i64,
                                    usdc_pos_i64,
                                    sig,
                                )
                                .await?;
                            } else {
                                self.finalize_close_confirmed(id, &side, sig)?;
                            }
                        } else {
                            self.fail_position(
                                id,
                                &format!("tx err: {:?}", status.err),
                                "FAILED_ERR",
                            )?;
                        }
                        continue;
                    }

                    if let Some(last_valid) = last_valid {
                        if current_height > last_valid {
                            self.fail_position(id, "blockhash expired", "FAILED_EXPIRED")?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn fail_position(&self, id: i64, err: &str, reason: &str) -> Result<()> {
        let mut conn = self.open_conn()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            r#"
            UPDATE positions
            SET state='FAILED',
                last_error=?1,
                reserved_usdc=0,
                reserved_sol_lamports=0,
                updated_at_ms=?2
            WHERE id=?3
            "#,
            params![err, Self::now_ms(), id],
        )?;
        TradeDb::insert_event(
            &tx,
            id,
            "RECONCILE",
            &json!({"reason": reason, "state": "FAILED"}),
        )?;
        tx.commit()?;
        Ok(())
    }

    async fn finalize_open_confirmed(
        &self,
        rpc: &RpcClient,
        http: &HttpRpc,
        id: i64,
        payer: &Pubkey,
        pool: &Pubkey,
        side: &str,
        reserved_usdc_i64: i64,
        reserved_sol_i64: i64,
        sol_position_i64: i64,
        usdc_position_i64: i64,
        sig: Signature,
    ) -> Result<()> {
        let (tx_json_opt, err_opt) = try_get_transaction_json(http, &sig).await;
        let (mint_a, mint_b) = resolve_pool_mints(rpc, pool).await?;
        let (usdc_mint, _wsol_mint) = ensure_pool_is_wsol_pair(mint_a, mint_b)?;
        let mut tx_meta_error: Option<String> = None;
        let mut sol_delta: i128 = 0;
        let mut usdc_delta: i128 = 0;
        if let Some(tx_json) = tx_json_opt {
            let parsed = (|| -> Result<(i128, i128)> {
                let meta = tx_json
                    .get("meta")
                    .ok_or_else(|| anyhow!("getTransaction missing meta"))?;
                let msg = tx_json
                    .get("transaction")
                    .and_then(|t| t.get("message"))
                    .unwrap_or(&Value::Null);
                let sol_delta = owner_lamport_delta(meta, msg, payer)?
                    .ok_or_else(|| anyhow!("payer not found in account keys"))?;
                let usdc_delta = sum_owner_mint_delta(meta, payer, &usdc_mint)?;
                Ok((sol_delta, usdc_delta))
            })();
            match parsed {
                Ok((sol, usdc)) => {
                    sol_delta = sol;
                    usdc_delta = usdc;
                }
                Err(err) => {
                    tx_meta_error = Some(err.to_string());
                }
            }
        } else {
            tx_meta_error = err_opt;
        }

        let reserved_sol_budget = u64::try_from(reserved_sol_i64).unwrap_or(0);
        let _reserved_usdc_budget = u64::try_from(reserved_usdc_i64).unwrap_or(0);
        let stored_sol_position = u64::try_from(sol_position_i64).unwrap_or(0);
        let stored_usdc_position = u64::try_from(usdc_position_i64).unwrap_or(0);
        let now = Self::now_ms();

        let mut conn = self.open_conn()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        match side {
            "LONG" => {
                let sol_position = if sol_delta > 0 {
                    u64::try_from(sol_delta).unwrap_or(stored_sol_position)
                } else {
                    stored_sol_position
                };
                let new_reserved_sol = sol_position
                    .checked_add(reserved_sol_budget)
                    .ok_or_else(|| anyhow!("reserved sol overflow"))?;
                tx.execute(
                    r#"
                    UPDATE positions
                    SET state='OPEN',
                        reserved_usdc=0,
                        reserved_sol_lamports=?1,
                        sol_position_lamports=?2,
                        updated_at_ms=?3,
                        last_error=NULL
                    WHERE id=?4 AND state='OPENING'
                    "#,
                    params![
                        u64_to_i64(new_reserved_sol, "reserved_sol_lamports")?,
                        u64_to_i64(sol_position, "sol_position_lamports")?,
                        now,
                        id
                    ],
                )?;
                TradeDb::insert_event(
                    &tx,
                    id,
                    "OPEN_COMMITTED",
                    &json!({
                        "reconcile": true,
                        "delta_sol": sol_delta.to_string(),
                        "delta_usdc": usdc_delta.to_string(),
                        "reserved_sol_lamports": new_reserved_sol,
                        "tx_meta_error": tx_meta_error
                    }),
                )?;
            }
            "SHORT" => {
                let sol_spent = if sol_delta < 0 {
                    u64::try_from(-sol_delta).unwrap_or(stored_sol_position)
                } else {
                    stored_sol_position
                };
                let usdc_received = if usdc_delta > 0 {
                    u64::try_from(usdc_delta).unwrap_or(stored_usdc_position)
                } else {
                    stored_usdc_position
                };
                tx.execute(
                    r#"
                    UPDATE positions
                    SET state='OPEN',
                        reserved_sol_lamports=?1,
                        reserved_usdc=?2,
                        sol_position_lamports=?3,
                        usdc_position=?4,
                        updated_at_ms=?5,
                        last_error=NULL
                    WHERE id=?6 AND state='OPENING'
                    "#,
                    params![
                        u64_to_i64(reserved_sol_budget, "reserved_sol_lamports")?,
                        u64_to_i64(usdc_received, "reserved_usdc")?,
                        u64_to_i64(sol_spent, "sol_position_lamports")?,
                        u64_to_i64(usdc_received, "usdc_position")?,
                        now,
                        id
                    ],
                )?;
                TradeDb::insert_event(
                    &tx,
                    id,
                    "OPEN_COMMITTED",
                    &json!({
                        "reconcile": true,
                        "delta_sol": sol_delta.to_string(),
                        "delta_usdc": usdc_delta.to_string(),
                        "reserved_usdc": usdc_received,
                        "reserved_sol_lamports": reserved_sol_budget,
                        "tx_meta_error": tx_meta_error
                    }),
                )?;
            }
            _ => {
                return Err(anyhow!("unknown side {side}"));
            }
        }

        tx.commit()?;
        Ok(())
    }

    fn finalize_close_confirmed(&self, id: i64, side: &str, sig: Signature) -> Result<()> {
        let mut conn = self.open_conn()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = Self::now_ms();
        tx.execute(
            r#"
            UPDATE positions
            SET state='CLOSED',
                close_sig=?1,
                reserved_usdc=0,
                reserved_sol_lamports=0,
                updated_at_ms=?2,
                last_error=NULL
            WHERE id=?3 AND state='CLOSING'
            "#,
            params![sig.to_string(), now, id],
        )?;
        let evt = match side {
            "LONG" => {
                json!({"reconcile": true, "close_sig": sig.to_string(), "reserved_sol_lamports": 0})
            }
            "SHORT" => json!({"reconcile": true, "close_sig": sig.to_string(), "reserved_usdc": 0}),
            _ => json!({"reconcile": true, "close_sig": sig.to_string()}),
        };
        TradeDb::insert_event(&tx, id, "CLOSE_COMMITTED", &evt)?;
        tx.commit()?;
        Ok(())
    }
}
fn token_program_for_mint_owner(mint_owner: &Pubkey) -> Result<TokenProgramFlavor> {
    if *mint_owner == spl_token::id() {
        Ok(TokenProgramFlavor::Token)
    } else if *mint_owner == spl_token_2022::id() {
        Ok(TokenProgramFlavor::Token2022)
    } else {
        Err(anyhow!(
            "Mint owner {} is not SPL Token or Token-2022 program",
            mint_owner
        ))
    }
}

#[derive(Debug, Clone, Copy)]
enum TokenProgramFlavor {
    Token,
    Token2022,
}

impl TokenProgramFlavor {
    fn program_id(&self) -> Pubkey {
        match self {
            TokenProgramFlavor::Token => spl_token::id(),
            TokenProgramFlavor::Token2022 => spl_token_2022::id(),
        }
    }

    fn sync_native_ix(&self, account: &Pubkey) -> Result<Instruction> {
        let pid = self.program_id();
        match self {
            TokenProgramFlavor::Token => Ok(spl_token::instruction::sync_native(&pid, account)?),
            TokenProgramFlavor::Token2022 => {
                Ok(spl_token_2022::instruction::sync_native(&pid, account)?)
            }
        }
    }

    fn close_account_ix(
        &self,
        account: &Pubkey,
        destination: &Pubkey,
        owner: &Pubkey,
    ) -> Result<Instruction> {
        let pid = self.program_id();
        match self {
            TokenProgramFlavor::Token => Ok(spl_token::instruction::close_account(
                &pid,
                account,
                destination,
                owner,
                &[],
            )?),
            TokenProgramFlavor::Token2022 => Ok(spl_token_2022::instruction::close_account(
                &pid,
                account,
                destination,
                owner,
                &[],
            )?),
        }
    }

    fn unpack_token_account(&self, data: &[u8]) -> Result<(Pubkey, u64)> {
        match self {
            TokenProgramFlavor::Token => {
                let acc = spl_token::state::Account::unpack_from_slice(data)
                    .context("Failed to unpack SPL Token account")?;
                Ok((acc.mint, acc.amount))
            }
            TokenProgramFlavor::Token2022 => {
                let acc = spl_token_2022::state::Account::unpack_from_slice(data)
                    .context("Failed to unpack SPL Token-2022 account")?;
                Ok((acc.mint, acc.amount))
            }
        }
    }

    fn unpack_token_account_amount(&self, data: &[u8]) -> Result<u64> {
        Ok(self.unpack_token_account(data)?.1)
    }

    fn unpack_mint_decimals(&self, data: &[u8]) -> Result<u8> {
        match self {
            TokenProgramFlavor::Token => {
                let mint = spl_token::state::Mint::unpack_from_slice(data)
                    .context("Failed to unpack SPL Token mint")?;
                Ok(mint.decimals)
            }
            TokenProgramFlavor::Token2022 => {
                let mint = spl_token_2022::state::Mint::unpack_from_slice(data)
                    .context("Failed to unpack SPL Token-2022 mint")?;
                Ok(mint.decimals)
            }
        }
    }
}

struct HttpRpc {
    url: String,
    client: HttpClient,
    limiter: Arc<RpcRateLimiter>,
}

impl HttpRpc {
    fn new(url: String, limiter: Arc<RpcRateLimiter>) -> Self {
        Self {
            url,
            client: HttpClient::new(),
            limiter,
        }
    }

    async fn get_transaction_json(&self, signature: &Signature) -> Result<Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature.to_string(),
                {
                    "encoding": "json",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        });
        self.limiter.acquire().await;
        let resp = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .context("getTransaction http request")?;
        let status = resp.status();
        let v: Value = resp.json().await.context("getTransaction json")?;
        if !status.is_success() {
            return Err(anyhow!("getTransaction http status={status} body={v}"));
        }
        if let Some(err) = v.get("error") {
            return Err(anyhow!("getTransaction rpc error: {err}"));
        }
        v.get("result")
            .cloned()
            .ok_or_else(|| anyhow!("getTransaction missing result"))
    }
}

async fn try_get_transaction_json(
    http: &HttpRpc,
    signature: &Signature,
) -> (Option<Value>, Option<String>) {
    let mut last_error: Option<String> = None;
    for attempt in 1..=GET_TRANSACTION_RETRY_ATTEMPTS {
        match http.get_transaction_json(signature).await {
            Ok(value) => return (Some(value), None),
            Err(err) => {
                last_error = Some(err.to_string());
                if attempt < GET_TRANSACTION_RETRY_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(
                        GET_TRANSACTION_RETRY_DELAY_MS,
                    ))
                    .await;
                }
            }
        }
    }
    (None, last_error)
}

fn configure_orca(payer: &Pubkey) -> Result<()> {
    set_whirlpools_config_address(WhirlpoolsConfigInput::SolanaMainnet)
        .map_err(|e| anyhow!(e.to_string()))?;
    set_funder(*payer).map_err(|e| anyhow!(e.to_string()))?;
    set_native_mint_wrapping_strategy(NativeMintWrappingStrategy::Ata)
        .map_err(|e| anyhow!(e.to_string()))?;
    Ok(())
}

async fn resolve_pool_mints(rpc: &RpcClient, whirlpool: &Pubkey) -> Result<(Pubkey, Pubkey)> {
    let account = rpc
        .get_account(whirlpool)
        .await
        .context("get whirlpool account")?;
    let decoded = Whirlpool::from_bytes(&account.data).context("decode whirlpool")?;
    Ok((decoded.token_mint_a, decoded.token_mint_b))
}

async fn mint_owner_program(rpc: &RpcClient, mint: &Pubkey) -> Result<Pubkey> {
    let acc = rpc
        .get_account(mint)
        .await
        .with_context(|| format!("Failed to fetch mint account {mint}"))?;
    Ok(acc.owner)
}

async fn load_mint_decimals(
    rpc: &RpcClient,
    mint: &Pubkey,
    program: TokenProgramFlavor,
) -> Result<u8> {
    let acc = rpc
        .get_account(mint)
        .await
        .with_context(|| format!("Failed to fetch mint account {mint}"))?;
    program.unpack_mint_decimals(&acc.data)
}

async fn get_account_optional(rpc: &RpcClient, address: &Pubkey) -> Result<Option<Account>> {
    let resp = rpc
        .get_account_with_commitment(address, CommitmentConfig::confirmed())
        .await
        .with_context(|| format!("get_account_with_commitment failed for {address}"))?;
    Ok(resp.value)
}

async fn ensure_wsol_ata_is_safe_to_use_and_close(
    rpc: &RpcClient,
    wsol_ata: &Pubkey,
    wsol_token_program: TokenProgramFlavor,
) -> Result<bool> {
    if let Some(acc) = get_account_optional(rpc, wsol_ata).await? {
        let amount = wsol_token_program.unpack_token_account_amount(&acc.data)?;
        if amount != 0 {
            return Err(anyhow!(
                "WSOL ATA {} already exists and has non-zero balance ({}). Refusing to proceed because we must close this ATA.",
                wsol_ata,
                amount
            ));
        }
        return Ok(true);
    }
    Ok(false)
}

fn ui_to_amount(ui: &str, decimals: u8) -> Result<u64> {
    let ui = ui.trim();
    if ui.is_empty() {
        return Err(anyhow!("empty amount"));
    }
    let mut parts = ui.split('.');
    let whole = parts.next().unwrap_or("");
    let frac = parts.next();
    if parts.next().is_some() {
        return Err(anyhow!("invalid amount format"));
    }
    let whole_val: u64 = if whole.is_empty() { 0 } else { whole.parse()? };
    let decimals_usize = decimals as usize;
    let frac_str = frac.unwrap_or("");
    if frac_str.len() > decimals_usize {
        return Err(anyhow!("too many decimal places"));
    }
    let mut frac_val: u64 = if frac_str.is_empty() {
        0
    } else {
        frac_str.parse()?
    };
    let pad = decimals_usize - frac_str.len();
    if pad > 0 {
        let pow = 10u64
            .checked_pow(pad as u32)
            .ok_or_else(|| anyhow!("amount overflow"))?;
        frac_val = frac_val
            .checked_mul(pow)
            .ok_or_else(|| anyhow!("amount overflow"))?;
    }
    let base = 10u64
        .checked_pow(decimals as u32)
        .ok_or_else(|| anyhow!("amount overflow"))?;
    let total = whole_val
        .checked_mul(base)
        .and_then(|v| v.checked_add(frac_val))
        .ok_or_else(|| anyhow!("amount overflow"))?;
    Ok(total)
}

async fn sum_token_accounts_by_owner(
    rpc: &RpcClient,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Result<u64> {
    let accounts = rpc
        .get_token_accounts_by_owner(owner, TokenAccountsFilter::Mint(*mint))
        .await
        .context("get_token_accounts_by_owner")?;
    let mint_str = mint.to_string();
    let mut total: u64 = 0;
    for acc in accounts {
        let data = serde_json::to_value(&acc.account.data).context("serialize ui account data")?;
        let parsed = data
            .get("parsed")
            .ok_or_else(|| anyhow!("token account missing parsed data"))?;
        let info = parsed
            .get("info")
            .ok_or_else(|| anyhow!("token account missing info"))?;
        let mint_val = info.get("mint").and_then(|v| v.as_str()).unwrap_or("");
        if mint_val != mint_str {
            continue;
        }
        let amount_str = info
            .get("tokenAmount")
            .and_then(|v| v.get("amount"))
            .and_then(|v| v.as_str())
            .unwrap_or("0");
        let amount: u64 = amount_str.parse().unwrap_or(0);
        total = total
            .checked_add(amount)
            .ok_or_else(|| anyhow!("token balance overflow"))?;
    }
    Ok(total)
}

async fn get_effective_wallet_balances(
    _db: &TradeDb,
    conn: &Connection,
    rpc: &RpcClient,
    payer: &Pubkey,
    mode: ExecMode,
    usdc_mint: &Pubkey,
) -> Result<(u64, u64)> {
    match mode {
        ExecMode::Live => {
            let usdc = sum_token_accounts_by_owner(rpc, payer, usdc_mint).await?;
            let sol = rpc.get_balance(payer).await?;
            Ok((usdc, sol))
        }
        ExecMode::Simulate => {
            if let Some((usdc, sol)) = TradeDb::get_sim_wallet(conn, payer)? {
                return Ok((usdc, sol));
            }
            let usdc = sum_token_accounts_by_owner(rpc, payer, usdc_mint).await?;
            let sol = rpc.get_balance(payer).await?;
            TradeDb::upsert_sim_wallet(conn, payer, usdc, sol)?;
            Ok((usdc, sol))
        }
    }
}

async fn list_owner_token_accounts(
    rpc: &RpcClient,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Result<Vec<Pubkey>> {
    let accounts = rpc
        .get_token_accounts_by_owner(owner, TokenAccountsFilter::Mint(*mint))
        .await
        .context("get_token_accounts_by_owner")?;
    let mut out = Vec::new();
    for acc in accounts {
        let pk = Pubkey::from_str(&acc.pubkey)
            .with_context(|| format!("bad token account pubkey {}", acc.pubkey))?;
        out.push(pk);
    }
    Ok(out)
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

async fn build_transaction_with_fee(
    rpc: &RpcClient,
    payer: &Keypair,
    instructions: &[Instruction],
    extra_signers: &[Keypair],
    mode: ExecMode,
) -> Result<(Transaction, u64, u64)> {
    let (blockhash, last_valid_block_height) = rpc
        .get_latest_blockhash_with_commitment(trade_commitment())
        .await
        .context("get_latest_blockhash_with_commitment")?;

    let mut signer_refs: Vec<&dyn Signer> = Vec::with_capacity(1 + extra_signers.len());
    signer_refs.push(payer);
    for kp in extra_signers {
        signer_refs.push(kp);
    }

    let tx = Transaction::new_signed_with_payer(
        instructions,
        Some(&payer.pubkey()),
        &signer_refs,
        blockhash,
    );
    let fee = match mode {
        ExecMode::Simulate => SIMULATION_FEE_LAMPORTS,
        ExecMode::Live => rpc
            .get_fee_for_message(tx.message())
            .await
            .context("get_fee_for_message")?,
    };

    Ok((tx, last_valid_block_height, fee))
}

struct TxExecution {
    signature: Signature,
    simulated: Option<RpcSimulateTransactionResult>,
}

async fn execute_tx(
    rpc: &RpcClient,
    tx: &Transaction,
    mode: ExecMode,
    simulate_addresses: &[Pubkey],
    last_valid_block_height: u64,
) -> Result<TxExecution> {
    let commitment = trade_commitment();
    let addresses: Vec<String> = simulate_addresses.iter().map(|p| p.to_string()).collect();
    let sim_cfg = RpcSimulateTransactionConfig {
        sig_verify: true,
        commitment: Some(commitment),
        replace_recent_blockhash: false,
        accounts: Some(RpcSimulateTransactionAccountsConfig {
            encoding: Some(UiAccountEncoding::Base64),
            addresses,
        }),
        ..RpcSimulateTransactionConfig::default()
    };
    let sim = rpc
        .simulate_transaction_with_config(tx, sim_cfg)
        .await
        .context("simulate_transaction_with_config")?;
    if let Some(err) = sim.value.err.clone() {
        eprintln!("❌ Симуляция транзакции: ошибка {err:?}");
        if let Some(units) = sim.value.units_consumed {
            eprintln!("⚙️ Потрачено вычислительных единиц: {units}");
        }
        match &sim.value.logs {
            Some(logs) if !logs.is_empty() => {
                eprintln!("📑 Логи инструкций симуляции:");
                for line in logs {
                    eprintln!("📑 {line}");
                }
            }
            _ => {
                eprintln!("⚠️ Логи симуляции отсутствуют.");
            }
        }
        return Err(anyhow!("simulateTransaction error: {err:?}"));
    }
    println!("🧪 Симуляция транзакции: успешно.");
    if let Some(units) = sim.value.units_consumed {
        println!("⚙️ Потрачено вычислительных единиц: {units}");
    }
    match &sim.value.logs {
        Some(logs) if !logs.is_empty() => {
            println!("📑 Логи инструкций симуляции:");
            for line in logs {
                println!("📑 {line}");
            }
        }
        _ => {
            println!("⚠️ Логи симуляции отсутствуют.");
        }
    }

    let sim_sig = tx.signatures.get(0).cloned().unwrap_or_default();

    if mode == ExecMode::Simulate {
        return Ok(TxExecution {
            signature: sim_sig,
            simulated: Some(sim.value),
        });
    }

    let send_cfg = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: Some(commitment.commitment),
        ..RpcSendTransactionConfig::default()
    };
    let sig = rpc
        .send_transaction_with_config(tx, send_cfg)
        .await
        .context("send_transaction_with_config")?;

    loop {
        let status = rpc
            .get_signature_statuses_with_history(&[sig])
            .await
            .context("get_signature_statuses_with_history")?
            .value
            .get(0)
            .cloned()
            .unwrap_or(None);
        if let Some(status) = status {
            if let Some(err) = status.err {
                return Err(anyhow!("tx err: {err:?}"));
            }
        }
        let confirmed = rpc
            .confirm_transaction_with_commitment(&sig, commitment)
            .await
            .context("confirm_transaction_with_commitment")?
            .value;
        if confirmed {
            break;
        }
        let current_height = rpc.get_block_height().await.context("get_block_height")?;
        if current_height > last_valid_block_height {
            return Err(anyhow!(
                "tx not confirmed before last_valid_block_height={last_valid_block_height}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Ok(TxExecution {
        signature: sig,
        simulated: Some(sim.value),
    })
}

async fn rent_budget_for_atas(rpc: &RpcClient, atas: &[Pubkey]) -> Result<u64> {
    let rent = rpc
        .get_minimum_balance_for_rent_exemption(spl_token::state::Account::LEN)
        .await
        .context("get_minimum_balance_for_rent_exemption")?;
    let mut total = 0u64;
    for ata in atas {
        if get_account_optional(rpc, ata).await?.is_none() {
            total = total
                .checked_add(rent)
                .ok_or_else(|| anyhow!("rent overflow"))?;
        }
    }
    Ok(total)
}

fn find_reqwest_error<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> Option<&'a reqwest::Error> {
    if let Some(req) = err.downcast_ref::<reqwest::Error>() {
        return Some(req);
    }
    let mut source = err.source();
    while let Some(src) = source {
        if let Some(req) = src.downcast_ref::<reqwest::Error>() {
            return Some(req);
        }
        source = src.source();
    }
    None
}

fn format_orca_swap_error(err: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = Vec::new();
    parts.push(format!("{err:?}"));
    let mut source = err.source();
    while let Some(src) = source {
        parts.push(format!("{src:?}"));
        source = src.source();
    }
    if let Some(req) = find_reqwest_error(err) {
        let mut extra = Vec::new();
        if let Some(status) = req.status() {
            extra.push(format!("status={status}"));
        }
        if let Some(url) = req.url() {
            let mut clean = url.clone();
            clean.set_query(None);
            extra.push(format!("url={clean}"));
        }
        extra.push(format!("is_decode={}", req.is_decode()));
        extra.push(format!("is_timeout={}", req.is_timeout()));
        extra.push(format!("is_connect={}", req.is_connect()));
        if !extra.is_empty() {
            parts.push(extra.join(", "));
        }
    }
    parts.join(" | ")
}

async fn swap_instructions_with_retry(
    rpc: &RpcClient,
    whirlpool: Pubkey,
    amount: u64,
    specified_mint: Pubkey,
    swap_type: SwapType,
    slippage_bps: u16,
    signer: Pubkey,
) -> Result<SwapInstructions> {
    let first = swap_instructions(
        rpc,
        whirlpool,
        amount,
        specified_mint,
        swap_type.clone(),
        Some(slippage_bps),
        Some(signer),
    )
    .await;
    match first {
        Ok(swap) => Ok(swap),
        Err(first_err) => {
            tokio::time::sleep(Duration::from_millis(SWAP_INSTRUCTIONS_RETRY_DELAY_MS)).await;
            let second = swap_instructions(
                rpc,
                whirlpool,
                amount,
                specified_mint,
                swap_type,
                Some(slippage_bps),
                Some(signer),
            )
            .await;
            match second {
                Ok(swap) => Ok(swap),
                Err(second_err) => Err(anyhow!(
                    "orca swap_instructions failed after retry: {} | {}",
                    format_orca_swap_error(first_err.as_ref()),
                    format_orca_swap_error(second_err.as_ref())
                )),
            }
        }
    }
}

async fn build_swap_instructions(
    rpc: &RpcClient,
    payer: &Keypair,
    whirlpool: Pubkey,
    swap_type: SwapType,
    specified_mint: Pubkey,
    amount: u64,
    slippage_bps: u16,
) -> Result<SwapInstructions> {
    configure_orca(&payer.pubkey())?;
    let swap = swap_instructions_with_retry(
        rpc,
        whirlpool,
        amount,
        specified_mint,
        swap_type,
        slippage_bps,
        payer.pubkey(),
    )
    .await?;
    Ok(swap)
}

fn exact_out_max_in(quote: &SwapQuote) -> Result<u64> {
    match quote {
        SwapQuote::ExactOut(q) => Ok(q.token_max_in),
        _ => Err(anyhow!("expected ExactOut quote")),
    }
}

fn exact_out_est_in(quote: &SwapQuote) -> Result<u64> {
    match quote {
        SwapQuote::ExactOut(q) => Ok(q.token_est_in),
        _ => Err(anyhow!("expected ExactOut quote")),
    }
}

fn exact_in_min_out(quote: &SwapQuote) -> Result<u64> {
    match quote {
        SwapQuote::ExactIn(q) => Ok(q.token_min_out),
        _ => Err(anyhow!("expected ExactIn quote")),
    }
}

fn exact_in_est_out(quote: &SwapQuote) -> Result<u64> {
    match quote {
        SwapQuote::ExactIn(q) => Ok(q.token_est_out),
        _ => Err(anyhow!("expected ExactIn quote")),
    }
}

fn build_simulation_addresses(
    payer: &Pubkey,
    token_accounts: &[Pubkey],
) -> (Vec<Pubkey>, HashSet<Pubkey>) {
    let set: HashSet<Pubkey> = token_accounts.iter().copied().collect();
    let mut list: Vec<Pubkey> = set.iter().copied().collect();
    list.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
    let mut addresses = Vec::with_capacity(1 + list.len());
    addresses.push(*payer);
    for pk in &list {
        addresses.push(*pk);
    }
    (addresses, set)
}

fn post_balances_from_simulation(
    simulated: &RpcSimulateTransactionResult,
    addresses: &[Pubkey],
    payer: &Pubkey,
    token_accounts: &HashSet<Pubkey>,
    usdc_mint: &Pubkey,
) -> Result<(u64, u64)> {
    let accounts = simulated
        .accounts
        .as_ref()
        .ok_or_else(|| anyhow!("simulateTransaction missing accounts"))?;
    if accounts.len() != addresses.len() {
        return Err(anyhow!("simulateTransaction accounts length mismatch"));
    }
    let mut post_sol = 0u64;
    let mut post_usdc = 0u64;
    for (idx, addr) in addresses.iter().enumerate() {
        let acct_opt = accounts.get(idx).and_then(|a| a.as_ref());
        if addr == payer {
            post_sol = acct_opt.map(|a| a.lamports).unwrap_or(0);
        }
        if token_accounts.contains(addr) {
            let Some(acct) = acct_opt else {
                continue;
            };
            let owner = Pubkey::from_str(&acct.owner)
                .with_context(|| format!("bad token account owner {}", acct.owner))?;
            let program = token_program_for_mint_owner(&owner)?;
            let data = acct
                .data
                .decode()
                .ok_or_else(|| anyhow!("unable to decode token account data"))?;
            let (mint, amount) = program.unpack_token_account(&data)?;
            if mint == *usdc_mint {
                post_usdc = post_usdc
                    .checked_add(amount)
                    .ok_or_else(|| anyhow!("post usdc overflow"))?;
            }
        }
    }
    Ok((post_usdc, post_sol))
}

fn ensure_pool_is_wsol_pair(mint_a: Pubkey, mint_b: Pubkey) -> Result<(Pubkey, Pubkey)> {
    let wsol_mint = Pubkey::from_str(WSOL_MINT)?;
    if mint_a == wsol_mint {
        Ok((mint_b, mint_a))
    } else if mint_b == wsol_mint {
        Ok((mint_a, mint_b))
    } else {
        Err(anyhow!(
            "Provided whirlpool is not a SOL (WSOL native mint) pair. mints: {} / {}",
            mint_a,
            mint_b
        ))
    }
}

#[allow(dead_code)]
async fn swap_core(
    rpc_url: &str,
    payer: &Keypair,
    whirlpool: Pubkey,
    swap_type: SwapType,
    specified_mint: Pubkey,
    amount: u64,
    slippage_bps: u16,
    pre_instructions: Vec<Instruction>,
    post_instructions: Vec<Instruction>,
) -> Result<Signature> {
    configure_orca(&payer.pubkey())?;

    let limiter = Arc::new(RpcRateLimiter::new(
        RPC_RATE_LIMIT_PER_SEC,
        RPC_RATE_LIMIT_WINDOW,
    ));
    let rpc = build_rate_limited_rpc_client(rpc_url, limiter);

    let swap = swap_instructions_with_retry(
        &rpc,
        whirlpool,
        amount,
        specified_mint,
        swap_type,
        slippage_bps,
        payer.pubkey(),
    )
    .await?;

    let mut ixs = Vec::with_capacity(
        pre_instructions.len() + swap.instructions.len() + post_instructions.len(),
    );
    ixs.extend(pre_instructions);
    ixs.extend(swap.instructions);
    ixs.extend(post_instructions);

    let mut signer_refs: Vec<&dyn Signer> = Vec::with_capacity(1 + swap.additional_signers.len());
    signer_refs.push(payer);
    for kp in &swap.additional_signers {
        signer_refs.push(kp);
    }

    let (recent_blockhash, last_valid_block_height) = rpc
        .get_latest_blockhash_with_commitment(trade_commitment())
        .await?;
    let tx = Transaction::new_signed_with_payer(
        &ixs,
        Some(&payer.pubkey()),
        &signer_refs,
        recent_blockhash,
    );

    let exec = execute_tx(&rpc, &tx, ExecMode::Live, &[], last_valid_block_height).await?;
    Ok(exec.signature)
}

fn u64_to_i64(value: u64, label: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{label} overflow"))
}

fn checked_free_amount(effective: u64, reserved: u64, label: &str) -> Result<u64> {
    effective.checked_sub(reserved).ok_or_else(|| {
        anyhow!(
            "reserved {label} exceeds effective {label}: reserved={reserved} effective={effective}"
        )
    })
}

fn mark_failed(db: &TradeDb, position_id: i64, stage: &str, error: &str) -> Result<()> {
    let mut conn = db.open_conn()?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute(
        r#"
        UPDATE positions
        SET state='FAILED',
            last_error=?1,
            reserved_usdc=0,
            reserved_sol_lamports=0,
            updated_at_ms=?2
        WHERE id=?3
        "#,
        params![error, TradeDb::now_ms(), position_id],
    )
    .context("update position FAILED")?;
    TradeDb::insert_event(
        &tx,
        position_id,
        "ERROR",
        &json!({ "stage": stage, "error": error }),
    )?;
    tx.commit()?;
    Ok(())
}

fn mark_exec_error(db: &TradeDb, position_id: i64, error: &str) -> Result<()> {
    let mut conn = db.open_conn()?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute(
        r#"
        UPDATE positions
        SET last_error=?1,
            updated_at_ms=?2
        WHERE id=?3
        "#,
        params![error, TradeDb::now_ms(), position_id],
    )
    .context("update position EXEC_ERROR")?;
    TradeDb::insert_event(&tx, position_id, "EXEC_ERROR", &json!({ "error": error }))?;
    tx.commit()?;
    Ok(())
}

impl Trader {
    pub fn new(
        rpc_url: &str,
        payer: Keypair,
        mode: ExecMode,
        db_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let limiter = Arc::new(RpcRateLimiter::new(
            RPC_RATE_LIMIT_PER_SEC,
            RPC_RATE_LIMIT_WINDOW,
        ));
        let rpc = build_rate_limited_rpc_client(rpc_url, limiter.clone());
        let db = TradeDb::new(db_path.as_ref())?;
        Ok(Self {
            rpc,
            payer,
            mode,
            db,
            rpc_limiter: limiter,
        })
    }

    pub async fn open_long(
        &self,
        pool: &str,
        usdc_ui: &str,
        slippage_bps: u16,
    ) -> Result<(i64, Signature)> {
        let db = &self.db;
        let rpc = &self.rpc;
        let http = if self.mode == ExecMode::Live {
            Some(HttpRpc::new(self.rpc.url(), self.rpc_limiter.clone()))
        } else {
            None
        };

        db.reconcile_pending(rpc, self.mode, &self.rpc_limiter)
            .await?;

        let whirlpool = Pubkey::from_str(pool).context("bad pool pubkey")?;
        let (mint_a, mint_b) = resolve_pool_mints(rpc, &whirlpool).await?;
        let (usdc_mint, wsol_mint) = ensure_pool_is_wsol_pair(mint_a, mint_b)?;
        let usdc_owner = mint_owner_program(rpc, &usdc_mint).await?;
        let usdc_program = token_program_for_mint_owner(&usdc_owner)?;
        let wsol_owner = mint_owner_program(rpc, &wsol_mint).await?;
        let wsol_program = token_program_for_mint_owner(&wsol_owner)?;
        let usdc_decimals = load_mint_decimals(rpc, &usdc_mint, usdc_program).await?;
        let usdc_amount_base = ui_to_amount(usdc_ui, usdc_decimals)?;

        let payer_pubkey = self.payer.pubkey();
        let usdc_ata = get_associated_token_address_with_program_id(
            &payer_pubkey,
            &usdc_mint,
            &usdc_program.program_id(),
        );
        let wsol_ata = get_associated_token_address_with_program_id(
            &payer_pubkey,
            &wsol_mint,
            &wsol_program.program_id(),
        );

        let wsol_ata_exists =
            ensure_wsol_ata_is_safe_to_use_and_close(rpc, &wsol_ata, wsol_program).await?;

        let swap = swap::open_long(
            rpc,
            &self.payer,
            whirlpool,
            usdc_mint,
            wsol_mint,
            usdc_amount_base,
            slippage_bps,
            wsol_ata_exists,
        )
        .await?;
        let SwapInstructions {
            instructions: swap_instructions,
            quote,
            additional_signers,
            ..
        } = swap;
        let min_out = exact_in_min_out(&quote)?;
        let est_out = exact_in_est_out(&quote)?;

        let mut instructions =
            Vec::with_capacity(swap_instructions.len() + if wsol_ata_exists { 1 } else { 0 });
        instructions.extend(swap_instructions);
        if wsol_ata_exists {
            instructions.push(wsol_program.close_account_ix(
                &wsol_ata,
                &payer_pubkey,
                &payer_pubkey,
            )?);
        }

        let rent_budget = rent_budget_for_atas(rpc, &[usdc_ata, wsol_ata]).await?;
        let (transaction, last_valid_block_height, fee) = build_transaction_with_fee(
            rpc,
            &self.payer,
            &instructions,
            &additional_signers,
            self.mode,
        )
        .await?;
        let tx_sig = transaction.signatures.get(0).cloned().unwrap_or_default();
        let fee_plus_rent = fee
            .checked_add(rent_budget)
            .ok_or_else(|| anyhow!("fee+rent overflow"))?;
        let last_valid_i64 = u64_to_i64(last_valid_block_height, "open_last_valid_block_height")?;

        let mut position_id = 0i64;
        let mut effective_usdc = 0u64;
        let mut effective_sol = 0u64;
        let mut reserved_usdc_total = 0u64;
        let mut reserved_sol_total = 0u64;
        let mut free_usdc = 0u64;
        let mut free_sol = 0u64;

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (eff_usdc, eff_sol) = get_effective_wallet_balances(
                db,
                &db_tx,
                rpc,
                &payer_pubkey,
                self.mode,
                &usdc_mint,
            )
            .await?;
            let (res_usdc, res_sol) = TradeDb::reserved_totals(&db_tx, &payer_pubkey)?;
            free_usdc = checked_free_amount(eff_usdc, res_usdc, "usdc")?;
            free_sol = checked_free_amount(eff_sol, res_sol, "sol")?;

            if free_usdc < usdc_amount_base {
                return Err(anyhow!(
                    "insufficient free usdc: free={free_usdc} required={usdc_amount_base}"
                ));
            }
            if free_sol < fee_plus_rent {
                return Err(anyhow!(
                    "insufficient free sol: free={free_sol} required={fee_plus_rent}"
                ));
            }

            effective_usdc = eff_usdc;
            effective_sol = eff_sol;
            reserved_usdc_total = res_usdc;
            reserved_sol_total = res_sol;

            let now = TradeDb::now_ms();
            let reserved_usdc_i64 = u64_to_i64(usdc_amount_base, "reserved_usdc")?;
            let reserved_sol_i64 = u64_to_i64(fee_plus_rent, "reserved_sol_lamports")?;
            db_tx
                .execute(
                    r#"
                INSERT INTO positions
                (payer, pool, side, state, mode, slippage_bps, created_at_ms, updated_at_ms,
                 reserved_usdc, reserved_sol_lamports, sol_position_lamports, usdc_position,
                 open_sig, open_last_valid_block_height)
                VALUES
                (?1, ?2, 'LONG', 'OPENING', ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11)
                "#,
                    params![
                        payer_pubkey.to_string(),
                        whirlpool.to_string(),
                        self.mode.as_db_str(),
                        i64::from(slippage_bps),
                        now,
                        now,
                        reserved_usdc_i64,
                        reserved_sol_i64,
                        u64_to_i64(est_out, "sol_position_lamports")?,
                        tx_sig.to_string(),
                        last_valid_i64
                    ],
                )
                .context("insert position OPENING")?;
            position_id = db_tx.last_insert_rowid();

            TradeDb::insert_event(
                &db_tx,
                position_id,
                "PRECHECK_OK",
                &json!({
                    "effective_usdc": effective_usdc,
                    "effective_sol": effective_sol,
                    "reserved_usdc_total": reserved_usdc_total,
                    "reserved_sol_total": reserved_sol_total,
                    "free_usdc": free_usdc,
                    "free_sol": free_sol,
                    "required_usdc": usdc_amount_base,
                    "fee": fee,
                    "rent_budget": rent_budget,
                    "required_sol": fee_plus_rent
                }),
            )?;
            TradeDb::insert_event(
                &db_tx,
                position_id,
                "RESERVE_OK",
                &json!({
                    "reserved_usdc": usdc_amount_base,
                    "reserved_sol_lamports": fee_plus_rent
                }),
            )?;

            db_tx.commit()?;
        }

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            TradeDb::insert_event(
                &db_tx,
                position_id,
                "TX_BUILT",
                &json!({
                    "payer": payer_pubkey.to_string(),
                    "pool": whirlpool.to_string(),
                    "usdc_ata": usdc_ata.to_string(),
                    "wsol_ata": wsol_ata.to_string(),
                    "swap_type": "ExactIn",
                    "amount": usdc_amount_base,
                    "slippage_bps": slippage_bps,
                    "quote_min_out": min_out,
                    "quote_est_out": est_out
                }),
            )?;
            db_tx.commit()?;
        }

        let (simulate_addresses, simulate_set) = if self.mode == ExecMode::Simulate {
            let mut token_accounts =
                list_owner_token_accounts(rpc, &payer_pubkey, &usdc_mint).await?;
            if !token_accounts.contains(&usdc_ata) {
                token_accounts.push(usdc_ata);
            }
            build_simulation_addresses(&payer_pubkey, &token_accounts)
        } else {
            (Vec::new(), HashSet::new())
        };

        let exec = match execute_tx(
            rpc,
            &transaction,
            self.mode,
            &simulate_addresses,
            last_valid_block_height,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                let err_msg = e.to_string();
                if self.mode == ExecMode::Live {
                    mark_exec_error(db, position_id, &err_msg)?;
                } else {
                    mark_failed(db, position_id, "EXECUTE_TX", &err_msg)?;
                }
                return Err(e);
            }
        };

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            match self.mode {
                ExecMode::Live => {
                    TradeDb::insert_event(
                        &db_tx,
                        position_id,
                        "TX_SENT",
                        &json!({
                            "signature": exec.signature.to_string(),
                            "last_valid_block_height": last_valid_block_height
                        }),
                    )?;
                }
                ExecMode::Simulate => {
                    let simulated = exec
                        .simulated
                        .as_ref()
                        .ok_or_else(|| anyhow!("simulate result missing"))?;
                    TradeDb::insert_event(
                        &db_tx,
                        position_id,
                        "TX_SIMULATED_OK",
                        &json!({
                            "signature": exec.signature.to_string(),
                            "units_consumed": simulated.units_consumed,
                            "logs": simulated.logs
                        }),
                    )?;
                }
            }
            db_tx.commit()?;
        }

        let mut delta_sol: i128 = 0;
        let mut delta_usdc: i128 = 0;
        let mut post_usdc: Option<u64> = None;
        let mut post_sol: Option<u64> = None;
        let mut tx_meta_error: Option<String> = None;

        match self.mode {
            ExecMode::Live => {
                let http = http
                    .as_ref()
                    .ok_or_else(|| anyhow!("http client missing"))?;
                let (tx_json_opt, err_opt) =
                    try_get_transaction_json(http, &exec.signature).await;
                if let Some(tx_json) = tx_json_opt {
                    let parsed = (|| -> Result<(i128, i128)> {
                        let meta = tx_json
                            .get("meta")
                            .ok_or_else(|| anyhow!("getTransaction missing meta"))?;
                        let msg = tx_json
                            .get("transaction")
                            .and_then(|t| t.get("message"))
                            .unwrap_or(&Value::Null);
                        let sol_delta = owner_lamport_delta(meta, msg, &payer_pubkey)?
                            .ok_or_else(|| anyhow!("payer not found in account keys"))?;
                        let usdc_delta = sum_owner_mint_delta(meta, &payer_pubkey, &usdc_mint)?;
                        Ok((sol_delta, usdc_delta))
                    })();
                    match parsed {
                        Ok((sol_delta, usdc_delta)) => {
                            delta_sol = sol_delta;
                            delta_usdc = usdc_delta;
                        }
                        Err(err) => {
                            tx_meta_error = Some(err.to_string());
                        }
                    }
                } else {
                    tx_meta_error = err_opt;
                }
            }
            ExecMode::Simulate => {
                let simulated = exec
                    .simulated
                    .as_ref()
                    .ok_or_else(|| anyhow!("simulate result missing"))?;
                let (post_usdc_val, post_sol_val) = post_balances_from_simulation(
                    simulated,
                    &simulate_addresses,
                    &payer_pubkey,
                    &simulate_set,
                    &usdc_mint,
                )?;
                post_usdc = Some(post_usdc_val);
                post_sol = Some(post_sol_val);
                delta_sol = post_sol_val as i128 - effective_sol as i128;
                delta_usdc = post_usdc_val as i128 - effective_usdc as i128;
            }
        }

        let sol_position = est_out;
        let reserved_sol_after_open = sol_position
            .checked_add(fee_plus_rent)
            .ok_or_else(|| anyhow!("reserved_sol_lamports overflow"))?;

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let now = TradeDb::now_ms();
            let sol_position_i64 = u64_to_i64(sol_position, "sol_position_lamports")?;
            let reserved_sol_after_open_i64 =
                u64_to_i64(reserved_sol_after_open, "reserved_sol_lamports")?;
            let rows = db_tx.execute(
                r#"
                UPDATE positions
                SET state='OPEN',
                    open_sig=?1,
                    open_last_valid_block_height=?2,
                    reserved_usdc=0,
                    reserved_sol_lamports = ?3,
                    sol_position_lamports = ?4,
                    updated_at_ms=?5
                WHERE id=?6 AND state='OPENING'
                "#,
                params![
                    exec.signature.to_string(),
                    last_valid_i64,
                    reserved_sol_after_open_i64,
                    sol_position_i64,
                    now,
                    position_id
                ],
            )?;
            if rows == 0 {
                return Err(anyhow!("position not in OPENING state"));
            }

            if let (Some(u), Some(s)) = (post_usdc, post_sol) {
                TradeDb::upsert_sim_wallet(&db_tx, &payer_pubkey, u, s)?;
            }

            TradeDb::insert_event(
                &db_tx,
                position_id,
                "OPEN_COMMITTED",
                &json!({
                    "delta_sol": delta_sol.to_string(),
                    "delta_usdc": delta_usdc.to_string(),
                    "sol_position_lamports": sol_position,
                    "reserved_usdc": 0,
                    "reserved_sol_lamports": reserved_sol_after_open,
                    "tx_meta_error": tx_meta_error
                }),
            )?;
            db_tx.commit()?;
        }

        Ok((position_id, exec.signature))
    }

    pub async fn close_long(&self, position_id: i64) -> Result<Signature> {
        let db = &self.db;
        let rpc = &self.rpc;
        let http = if self.mode == ExecMode::Live {
            Some(HttpRpc::new(self.rpc.url(), self.rpc_limiter.clone()))
        } else {
            None
        };

        db.reconcile_pending(rpc, self.mode, &self.rpc_limiter)
            .await?;

        let payer_pubkey = self.payer.pubkey();
        let conn = db.open_conn()?;
        let row: Option<(String, i64, i64)> = conn
            .query_row(
                r#"
                SELECT pool, slippage_bps, sol_position_lamports
                FROM positions
                WHERE id=?1 AND payer=?2 AND side='LONG' AND state='OPEN'
                "#,
                params![position_id, payer_pubkey.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let (pool_s, slippage_i64, sol_position_i64) =
            row.ok_or_else(|| anyhow!("position not found or not open long: {position_id}"))?;
        let slippage_bps = u16::try_from(slippage_i64).context("slippage_bps overflow")?;
        let sol_position =
            u64::try_from(sol_position_i64).context("sol_position_lamports overflow")?;

        let whirlpool = Pubkey::from_str(&pool_s).context("bad pool pubkey")?;
        let (mint_a, mint_b) = resolve_pool_mints(rpc, &whirlpool).await?;
        let (usdc_mint, wsol_mint) = ensure_pool_is_wsol_pair(mint_a, mint_b)?;
        let usdc_owner = mint_owner_program(rpc, &usdc_mint).await?;
        let usdc_program = token_program_for_mint_owner(&usdc_owner)?;
        let wsol_owner = mint_owner_program(rpc, &wsol_mint).await?;
        let wsol_program = token_program_for_mint_owner(&wsol_owner)?;

        let usdc_ata = get_associated_token_address_with_program_id(
            &payer_pubkey,
            &usdc_mint,
            &usdc_program.program_id(),
        );
        let wsol_ata = get_associated_token_address_with_program_id(
            &payer_pubkey,
            &wsol_mint,
            &wsol_program.program_id(),
        );

        let wsol_ata_exists =
            ensure_wsol_ata_is_safe_to_use_and_close(rpc, &wsol_ata, wsol_program).await?;

        let swap = swap::close_long(
            rpc,
            &self.payer,
            whirlpool,
            usdc_mint,
            wsol_mint,
            sol_position,
            slippage_bps,
            wsol_ata_exists,
        )
        .await?;
        let SwapInstructions {
            instructions: swap_instructions,
            quote,
            additional_signers,
            ..
        } = swap;
        let min_out = exact_in_min_out(&quote)?;

        let mut instructions =
            Vec::with_capacity(swap_instructions.len() + if wsol_ata_exists { 1 } else { 0 });
        instructions.extend(swap_instructions);
        if wsol_ata_exists {
            instructions.push(wsol_program.close_account_ix(
                &wsol_ata,
                &payer_pubkey,
                &payer_pubkey,
            )?);
        }

        let rent_budget = rent_budget_for_atas(rpc, &[usdc_ata, wsol_ata]).await?;
        let (transaction, last_valid_block_height, fee) = build_transaction_with_fee(
            rpc,
            &self.payer,
            &instructions,
            &additional_signers,
            self.mode,
        )
        .await?;
        let tx_sig = transaction.signatures.get(0).cloned().unwrap_or_default();
        let fee_plus_rent = fee
            .checked_add(rent_budget)
            .ok_or_else(|| anyhow!("fee+rent overflow"))?;
        let last_valid_i64 = u64_to_i64(last_valid_block_height, "close_last_valid_block_height")?;

        let mut effective_usdc = 0u64;
        let mut effective_sol = 0u64;
        let mut reserved_sol_total = 0u64;
        let mut free_sol = 0u64;

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (eff_usdc, eff_sol) = get_effective_wallet_balances(
                db,
                &db_tx,
                rpc,
                &payer_pubkey,
                self.mode,
                &usdc_mint,
            )
            .await?;
            let (_res_usdc, res_sol) =
                TradeDb::reserved_totals_excluding(&db_tx, &payer_pubkey, position_id)?;
            free_sol = checked_free_amount(eff_sol, res_sol, "sol")?;
            if free_sol < fee_plus_rent {
                return Err(anyhow!(
                    "insufficient free sol: free={free_sol} required={fee_plus_rent}"
                ));
            }

            effective_usdc = eff_usdc;
            effective_sol = eff_sol;
            reserved_sol_total = res_sol;

            let now = TradeDb::now_ms();
            let rows = db_tx.execute(
                r#"
                UPDATE positions
                SET state='CLOSING',
                    close_sig=?1,
                    close_last_valid_block_height=?2,
                    updated_at_ms=?3
                WHERE id=?4 AND payer=?5 AND side='LONG' AND state='OPEN'
                "#,
                params![
                    tx_sig.to_string(),
                    last_valid_i64,
                    now,
                    position_id,
                    payer_pubkey.to_string()
                ],
            )?;
            if rows == 0 {
                return Err(anyhow!("position not in OPEN state"));
            }

            TradeDb::insert_event(
                &db_tx,
                position_id,
                "PRECHECK_OK",
                &json!({
                    "effective_usdc": effective_usdc,
                    "effective_sol": effective_sol,
                    "reserved_sol_total": reserved_sol_total,
                    "free_sol": free_sol,
                    "sol_position_lamports": sol_position,
                    "fee": fee,
                    "rent_budget": rent_budget,
                    "required_sol": fee_plus_rent
                }),
            )?;
            TradeDb::insert_event(
                &db_tx,
                position_id,
                "STATE_CHANGE",
                &json!({ "state": "CLOSING" }),
            )?;
            TradeDb::insert_event(
                &db_tx,
                position_id,
                "TX_BUILT",
                &json!({
                    "payer": payer_pubkey.to_string(),
                    "pool": whirlpool.to_string(),
                    "usdc_ata": usdc_ata.to_string(),
                    "wsol_ata": wsol_ata.to_string(),
                    "swap_type": "ExactIn",
                    "amount": sol_position,
                    "slippage_bps": slippage_bps,
                    "quote_min_out": min_out
                }),
            )?;

            db_tx.commit()?;
        }

        let (simulate_addresses, simulate_set) = if self.mode == ExecMode::Simulate {
            let mut token_accounts =
                list_owner_token_accounts(rpc, &payer_pubkey, &usdc_mint).await?;
            if !token_accounts.contains(&usdc_ata) {
                token_accounts.push(usdc_ata);
            }
            build_simulation_addresses(&payer_pubkey, &token_accounts)
        } else {
            (Vec::new(), HashSet::new())
        };

        let exec = match execute_tx(
            rpc,
            &transaction,
            self.mode,
            &simulate_addresses,
            last_valid_block_height,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                let err_msg = e.to_string();
                if self.mode == ExecMode::Live {
                    mark_exec_error(db, position_id, &err_msg)?;
                } else {
                    mark_failed(db, position_id, "EXECUTE_TX", &err_msg)?;
                }
                return Err(e);
            }
        };

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            match self.mode {
                ExecMode::Live => {
                    TradeDb::insert_event(
                        &db_tx,
                        position_id,
                        "TX_SENT",
                        &json!({
                            "signature": exec.signature.to_string(),
                            "last_valid_block_height": last_valid_block_height
                        }),
                    )?;
                }
                ExecMode::Simulate => {
                    let simulated = exec
                        .simulated
                        .as_ref()
                        .ok_or_else(|| anyhow!("simulate result missing"))?;
                    TradeDb::insert_event(
                        &db_tx,
                        position_id,
                        "TX_SIMULATED_OK",
                        &json!({
                            "signature": exec.signature.to_string(),
                            "units_consumed": simulated.units_consumed,
                            "logs": simulated.logs
                        }),
                    )?;
                }
            }
            db_tx.commit()?;
        }

        let mut delta_sol: i128 = 0;
        let mut delta_usdc: i128 = 0;
        let mut post_usdc: Option<u64> = None;
        let mut post_sol: Option<u64> = None;
        let mut tx_meta_error: Option<String> = None;

        match self.mode {
            ExecMode::Live => {
                let http = http
                    .as_ref()
                    .ok_or_else(|| anyhow!("http client missing"))?;
                let (tx_json_opt, err_opt) =
                    try_get_transaction_json(http, &exec.signature).await;
                if let Some(tx_json) = tx_json_opt {
                    let parsed = (|| -> Result<(i128, i128)> {
                        let meta = tx_json
                            .get("meta")
                            .ok_or_else(|| anyhow!("getTransaction missing meta"))?;
                        let msg = tx_json
                            .get("transaction")
                            .and_then(|t| t.get("message"))
                            .unwrap_or(&Value::Null);
                        let sol_delta = owner_lamport_delta(meta, msg, &payer_pubkey)?
                            .ok_or_else(|| anyhow!("payer not found in account keys"))?;
                        let usdc_delta = sum_owner_mint_delta(meta, &payer_pubkey, &usdc_mint)?;
                        Ok((sol_delta, usdc_delta))
                    })();
                    match parsed {
                        Ok((sol_delta, usdc_delta)) => {
                            delta_sol = sol_delta;
                            delta_usdc = usdc_delta;
                        }
                        Err(err) => {
                            tx_meta_error = Some(err.to_string());
                        }
                    }
                } else {
                    tx_meta_error = err_opt;
                }
            }
            ExecMode::Simulate => {
                let simulated = exec
                    .simulated
                    .as_ref()
                    .ok_or_else(|| anyhow!("simulate result missing"))?;
                let (post_usdc_val, post_sol_val) = post_balances_from_simulation(
                    simulated,
                    &simulate_addresses,
                    &payer_pubkey,
                    &simulate_set,
                    &usdc_mint,
                )?;
                post_usdc = Some(post_usdc_val);
                post_sol = Some(post_sol_val);
                delta_sol = post_sol_val as i128 - effective_sol as i128;
                delta_usdc = post_usdc_val as i128 - effective_usdc as i128;
            }
        }

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let now = TradeDb::now_ms();
            let rows = db_tx.execute(
                r#"
                UPDATE positions
                SET state='CLOSED',
                    close_sig=?1,
                    close_last_valid_block_height=?2,
                    reserved_sol_lamports=0,
                    updated_at_ms=?3
                WHERE id=?4 AND state='CLOSING'
                "#,
                params![exec.signature.to_string(), last_valid_i64, now, position_id],
            )?;
            if rows == 0 {
                return Err(anyhow!("position not in CLOSING state"));
            }

            if let (Some(u), Some(s)) = (post_usdc, post_sol) {
                TradeDb::upsert_sim_wallet(&db_tx, &payer_pubkey, u, s)?;
            }

            TradeDb::insert_event(
                &db_tx,
                position_id,
                "CLOSE_COMMITTED",
                &json!({
                    "delta_sol": delta_sol.to_string(),
                    "delta_usdc": delta_usdc.to_string(),
                    "reserved_sol_lamports": 0,
                    "tx_meta_error": tx_meta_error
                }),
            )?;
            db_tx.commit()?;
        }

        Ok(exec.signature)
    }

    pub async fn open_short(
        &self,
        pool: &str,
        usdc_ui: &str,
        slippage_bps: u16,
    ) -> Result<(i64, Signature)> {
        let db = &self.db;
        let rpc = &self.rpc;
        let http = if self.mode == ExecMode::Live {
            Some(HttpRpc::new(self.rpc.url(), self.rpc_limiter.clone()))
        } else {
            None
        };

        db.reconcile_pending(rpc, self.mode, &self.rpc_limiter)
            .await?;

        let whirlpool = Pubkey::from_str(pool).context("bad pool pubkey")?;
        let (mint_a, mint_b) = resolve_pool_mints(rpc, &whirlpool).await?;
        let (usdc_mint, wsol_mint) = ensure_pool_is_wsol_pair(mint_a, mint_b)?;
        let usdc_owner = mint_owner_program(rpc, &usdc_mint).await?;
        let usdc_program = token_program_for_mint_owner(&usdc_owner)?;
        let wsol_owner = mint_owner_program(rpc, &wsol_mint).await?;
        let wsol_program = token_program_for_mint_owner(&wsol_owner)?;
        let usdc_decimals = load_mint_decimals(rpc, &usdc_mint, usdc_program).await?;
        let usdc_out_base = ui_to_amount(usdc_ui, usdc_decimals)?;

        let payer_pubkey = self.payer.pubkey();
        let usdc_ata = get_associated_token_address_with_program_id(
            &payer_pubkey,
            &usdc_mint,
            &usdc_program.program_id(),
        );
        let wsol_ata = get_associated_token_address_with_program_id(
            &payer_pubkey,
            &wsol_mint,
            &wsol_program.program_id(),
        );

        let wsol_ata_exists =
            ensure_wsol_ata_is_safe_to_use_and_close(rpc, &wsol_ata, wsol_program).await?;

        let swap = swap::open_short(
            rpc,
            &self.payer,
            whirlpool,
            usdc_mint,
            wsol_mint,
            usdc_out_base,
            slippage_bps,
            wsol_ata_exists,
        )
        .await?;
        let SwapInstructions {
            instructions: swap_instructions,
            quote,
            additional_signers,
            ..
        } = swap;
        let max_sol_in = exact_out_max_in(&quote)?;
        let est_sol_in = exact_out_est_in(&quote)?;

        let mut instructions =
            Vec::with_capacity(swap_instructions.len() + if wsol_ata_exists { 1 } else { 0 });
        instructions.extend(swap_instructions);
        if wsol_ata_exists {
            instructions.push(wsol_program.close_account_ix(
                &wsol_ata,
                &payer_pubkey,
                &payer_pubkey,
            )?);
        }

        let rent_budget = rent_budget_for_atas(rpc, &[usdc_ata, wsol_ata]).await?;
        let (transaction, last_valid_block_height, fee) = build_transaction_with_fee(
            rpc,
            &self.payer,
            &instructions,
            &additional_signers,
            self.mode,
        )
        .await?;
        let tx_sig = transaction.signatures.get(0).cloned().unwrap_or_default();
        let fee_plus_rent = fee
            .checked_add(rent_budget)
            .ok_or_else(|| anyhow!("fee+rent overflow"))?;
        let required_sol = max_sol_in
            .checked_add(fee_plus_rent)
            .ok_or_else(|| anyhow!("required sol overflow"))?;
        let last_valid_i64 = u64_to_i64(last_valid_block_height, "open_last_valid_block_height")?;

        let mut position_id = 0i64;
        let mut effective_usdc = 0u64;
        let mut effective_sol = 0u64;
        let mut reserved_sol_total = 0u64;
        let mut free_sol = 0u64;

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (eff_usdc, eff_sol) = get_effective_wallet_balances(
                db,
                &db_tx,
                rpc,
                &payer_pubkey,
                self.mode,
                &usdc_mint,
            )
            .await?;
            let (_res_usdc, res_sol) = TradeDb::reserved_totals(&db_tx, &payer_pubkey)?;
            free_sol = checked_free_amount(eff_sol, res_sol, "sol")?;
            if free_sol < required_sol {
                return Err(anyhow!(
                    "insufficient free sol: free={free_sol} required={required_sol}"
                ));
            }

            effective_usdc = eff_usdc;
            effective_sol = eff_sol;
            reserved_sol_total = res_sol;

            let now = TradeDb::now_ms();
            let reserved_sol_i64 = u64_to_i64(required_sol, "reserved_sol_lamports")?;
            let usdc_out_i64 = u64_to_i64(usdc_out_base, "usdc_position")?;
            db_tx
                .execute(
                    r#"
                INSERT INTO positions
                (payer, pool, side, state, mode, slippage_bps, created_at_ms, updated_at_ms,
                 reserved_usdc, reserved_sol_lamports, sol_position_lamports, usdc_position,
                 open_sig, open_last_valid_block_height)
                VALUES
                (?1, ?2, 'SHORT', 'OPENING', ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9, ?10, ?11)
                "#,
                    params![
                        payer_pubkey.to_string(),
                        whirlpool.to_string(),
                        self.mode.as_db_str(),
                        i64::from(slippage_bps),
                        now,
                        now,
                        reserved_sol_i64,
                        u64_to_i64(est_sol_in, "sol_position_lamports")?,
                        usdc_out_i64,
                        tx_sig.to_string(),
                        last_valid_i64
                    ],
                )
                .context("insert position OPENING")?;
            position_id = db_tx.last_insert_rowid();

            TradeDb::insert_event(
                &db_tx,
                position_id,
                "PRECHECK_OK",
                &json!({
                    "effective_usdc": effective_usdc,
                    "effective_sol": effective_sol,
                    "reserved_sol_total": reserved_sol_total,
                    "free_sol": free_sol,
                    "required_sol": required_sol,
                    "fee": fee,
                    "rent_budget": rent_budget,
                    "max_sol_in": max_sol_in
                }),
            )?;
            TradeDb::insert_event(
                &db_tx,
                position_id,
                "RESERVE_OK",
                &json!({
                    "reserved_sol_lamports": required_sol,
                    "usdc_position": usdc_out_base
                }),
            )?;
            db_tx.commit()?;
        }

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            TradeDb::insert_event(
                &db_tx,
                position_id,
                "TX_BUILT",
                &json!({
                    "payer": payer_pubkey.to_string(),
                    "pool": whirlpool.to_string(),
                    "usdc_ata": usdc_ata.to_string(),
                    "wsol_ata": wsol_ata.to_string(),
                    "swap_type": "ExactOut",
                    "amount": usdc_out_base,
                    "slippage_bps": slippage_bps,
                    "quote_max_in": max_sol_in,
                    "quote_est_in": est_sol_in
                }),
            )?;
            db_tx.commit()?;
        }

        let (simulate_addresses, simulate_set) = if self.mode == ExecMode::Simulate {
            let mut token_accounts =
                list_owner_token_accounts(rpc, &payer_pubkey, &usdc_mint).await?;
            if !token_accounts.contains(&usdc_ata) {
                token_accounts.push(usdc_ata);
            }
            build_simulation_addresses(&payer_pubkey, &token_accounts)
        } else {
            (Vec::new(), HashSet::new())
        };

        let exec = match execute_tx(
            rpc,
            &transaction,
            self.mode,
            &simulate_addresses,
            last_valid_block_height,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                let err_msg = e.to_string();
                if self.mode == ExecMode::Live {
                    mark_exec_error(db, position_id, &err_msg)?;
                } else {
                    mark_failed(db, position_id, "EXECUTE_TX", &err_msg)?;
                }
                return Err(e);
            }
        };

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            match self.mode {
                ExecMode::Live => {
                    TradeDb::insert_event(
                        &db_tx,
                        position_id,
                        "TX_SENT",
                        &json!({
                            "signature": exec.signature.to_string(),
                            "last_valid_block_height": last_valid_block_height
                        }),
                    )?;
                }
                ExecMode::Simulate => {
                    let simulated = exec
                        .simulated
                        .as_ref()
                        .ok_or_else(|| anyhow!("simulate result missing"))?;
                    TradeDb::insert_event(
                        &db_tx,
                        position_id,
                        "TX_SIMULATED_OK",
                        &json!({
                            "signature": exec.signature.to_string(),
                            "units_consumed": simulated.units_consumed,
                            "logs": simulated.logs
                        }),
                    )?;
                }
            }
            db_tx.commit()?;
        }

        let mut delta_sol: i128 = 0;
        let mut delta_usdc: i128 = 0;
        let mut post_usdc: Option<u64> = None;
        let mut post_sol: Option<u64> = None;
        let mut tx_meta_error: Option<String> = None;

        match self.mode {
            ExecMode::Live => {
                let http = http
                    .as_ref()
                    .ok_or_else(|| anyhow!("http client missing"))?;
                let (tx_json_opt, err_opt) =
                    try_get_transaction_json(http, &exec.signature).await;
                if let Some(tx_json) = tx_json_opt {
                    let parsed = (|| -> Result<(i128, i128)> {
                        let meta = tx_json
                            .get("meta")
                            .ok_or_else(|| anyhow!("getTransaction missing meta"))?;
                        let msg = tx_json
                            .get("transaction")
                            .and_then(|t| t.get("message"))
                            .unwrap_or(&Value::Null);
                        let sol_delta = owner_lamport_delta(meta, msg, &payer_pubkey)?
                            .ok_or_else(|| anyhow!("payer not found in account keys"))?;
                        let usdc_delta = sum_owner_mint_delta(meta, &payer_pubkey, &usdc_mint)?;
                        Ok((sol_delta, usdc_delta))
                    })();
                    match parsed {
                        Ok((sol_delta, usdc_delta)) => {
                            delta_sol = sol_delta;
                            delta_usdc = usdc_delta;
                        }
                        Err(err) => {
                            tx_meta_error = Some(err.to_string());
                        }
                    }
                } else {
                    tx_meta_error = err_opt;
                }
            }
            ExecMode::Simulate => {
                let simulated = exec
                    .simulated
                    .as_ref()
                    .ok_or_else(|| anyhow!("simulate result missing"))?;
                let (post_usdc_val, post_sol_val) = post_balances_from_simulation(
                    simulated,
                    &simulate_addresses,
                    &payer_pubkey,
                    &simulate_set,
                    &usdc_mint,
                )?;
                post_usdc = Some(post_usdc_val);
                post_sol = Some(post_sol_val);
                delta_sol = post_sol_val as i128 - effective_sol as i128;
                delta_usdc = post_usdc_val as i128 - effective_usdc as i128;
            }
        }

        let sol_spent = est_sol_in;
        let reserved_sol_after_open = fee_plus_rent;

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let now = TradeDb::now_ms();
            let sol_position_i64 = u64_to_i64(sol_spent, "sol_position_lamports")?;
            let usdc_position_i64 = u64_to_i64(usdc_out_base, "usdc_position")?;
            let reserved_usdc_i64 = u64_to_i64(usdc_out_base, "reserved_usdc")?;
            let reserved_sol_after_open_i64 =
                u64_to_i64(reserved_sol_after_open, "reserved_sol_lamports")?;
            let rows = db_tx.execute(
                r#"
                UPDATE positions
                SET state='OPEN',
                    open_sig=?1,
                    open_last_valid_block_height=?2,
                    reserved_sol_lamports=?3,
                    reserved_usdc=?4,
                    sol_position_lamports=?5,
                    usdc_position=?6,
                    updated_at_ms=?7
                WHERE id=?8 AND state='OPENING'
                "#,
                params![
                    exec.signature.to_string(),
                    last_valid_i64,
                    reserved_sol_after_open_i64,
                    reserved_usdc_i64,
                    sol_position_i64,
                    usdc_position_i64,
                    now,
                    position_id
                ],
            )?;
            if rows == 0 {
                return Err(anyhow!("position not in OPENING state"));
            }

            if let (Some(u), Some(s)) = (post_usdc, post_sol) {
                TradeDb::upsert_sim_wallet(&db_tx, &payer_pubkey, u, s)?;
            }

            TradeDb::insert_event(
                &db_tx,
                position_id,
                "OPEN_COMMITTED",
                &json!({
                    "delta_sol": delta_sol.to_string(),
                    "delta_usdc": delta_usdc.to_string(),
                    "sol_position_lamports": sol_spent,
                    "usdc_position": usdc_out_base,
                    "reserved_usdc": usdc_out_base,
                    "reserved_sol_lamports": reserved_sol_after_open,
                    "tx_meta_error": tx_meta_error
                }),
            )?;
            db_tx.commit()?;
        }

        Ok((position_id, exec.signature))
    }

    pub async fn close_short(&self, position_id: i64) -> Result<Signature> {
        let db = &self.db;
        let rpc = &self.rpc;
        let http = if self.mode == ExecMode::Live {
            Some(HttpRpc::new(self.rpc.url(), self.rpc_limiter.clone()))
        } else {
            None
        };

        db.reconcile_pending(rpc, self.mode, &self.rpc_limiter)
            .await?;

        let payer_pubkey = self.payer.pubkey();
        let conn = db.open_conn()?;
        let row: Option<(String, i64, i64, i64)> = conn
            .query_row(
                r#"
                SELECT pool, slippage_bps, sol_position_lamports, reserved_usdc
                FROM positions
                WHERE id=?1 AND payer=?2 AND side='SHORT' AND state='OPEN'
                "#,
                params![position_id, payer_pubkey.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let (pool_s, slippage_i64, sol_position_i64, reserved_usdc_i64) =
            row.ok_or_else(|| anyhow!("position not found or not open short: {position_id}"))?;
        let slippage_bps = u16::try_from(slippage_i64).context("slippage_bps overflow")?;
        let sol_position =
            u64::try_from(sol_position_i64).context("sol_position_lamports overflow")?;
        let reserved_usdc = u64::try_from(reserved_usdc_i64).context("reserved_usdc overflow")?;

        let whirlpool = Pubkey::from_str(&pool_s).context("bad pool pubkey")?;
        let (mint_a, mint_b) = resolve_pool_mints(rpc, &whirlpool).await?;
        let (usdc_mint, wsol_mint) = ensure_pool_is_wsol_pair(mint_a, mint_b)?;
        let usdc_owner = mint_owner_program(rpc, &usdc_mint).await?;
        let usdc_program = token_program_for_mint_owner(&usdc_owner)?;
        let wsol_owner = mint_owner_program(rpc, &wsol_mint).await?;
        let wsol_program = token_program_for_mint_owner(&wsol_owner)?;

        let usdc_ata = get_associated_token_address_with_program_id(
            &payer_pubkey,
            &usdc_mint,
            &usdc_program.program_id(),
        );
        let wsol_ata = get_associated_token_address_with_program_id(
            &payer_pubkey,
            &wsol_mint,
            &wsol_program.program_id(),
        );

        let wsol_ata_exists =
            ensure_wsol_ata_is_safe_to_use_and_close(rpc, &wsol_ata, wsol_program).await?;

        let swap = swap::close_short(
            rpc,
            &self.payer,
            whirlpool,
            usdc_mint,
            wsol_mint,
            sol_position,
            slippage_bps,
            wsol_ata_exists,
        )
        .await?;
        let SwapInstructions {
            instructions: swap_instructions,
            quote,
            additional_signers,
            ..
        } = swap;
        let max_usdc_in = exact_out_max_in(&quote)?;
        let est_usdc_in = exact_out_est_in(&quote)?;

        let mut instructions =
            Vec::with_capacity(swap_instructions.len() + if wsol_ata_exists { 1 } else { 0 });
        instructions.extend(swap_instructions);
        if wsol_ata_exists {
            instructions.push(wsol_program.close_account_ix(
                &wsol_ata,
                &payer_pubkey,
                &payer_pubkey,
            )?);
        }

        let rent_budget = rent_budget_for_atas(rpc, &[usdc_ata, wsol_ata]).await?;
        let (transaction, last_valid_block_height, fee) = build_transaction_with_fee(
            rpc,
            &self.payer,
            &instructions,
            &additional_signers,
            self.mode,
        )
        .await?;
        let tx_sig = transaction.signatures.get(0).cloned().unwrap_or_default();
        let fee_plus_rent = fee
            .checked_add(rent_budget)
            .ok_or_else(|| anyhow!("fee+rent overflow"))?;
        let last_valid_i64 = u64_to_i64(last_valid_block_height, "close_last_valid_block_height")?;

        let mut effective_usdc = 0u64;
        let mut effective_sol = 0u64;
        let mut reserved_sol_total = 0u64;
        let mut free_sol = 0u64;

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (eff_usdc, eff_sol) = get_effective_wallet_balances(
                db,
                &db_tx,
                rpc,
                &payer_pubkey,
                self.mode,
                &usdc_mint,
            )
            .await?;
            let (_res_usdc, res_sol) =
                TradeDb::reserved_totals_excluding(&db_tx, &payer_pubkey, position_id)?;
            free_sol = eff_sol.saturating_sub(res_sol);
            if reserved_usdc < max_usdc_in {
                return Err(anyhow!(
                    "reserved_usdc below max_usdc_in: reserved={reserved_usdc} max_usdc_in={max_usdc_in}"
                ));
            }
            if free_sol < fee_plus_rent {
                return Err(anyhow!(
                    "insufficient free sol: free={free_sol} required={fee_plus_rent}"
                ));
            }

            effective_usdc = eff_usdc;
            effective_sol = eff_sol;
            reserved_sol_total = res_sol;

            let now = TradeDb::now_ms();
            let rows = db_tx.execute(
                r#"
                UPDATE positions
                SET state='CLOSING',
                    close_sig=?1,
                    close_last_valid_block_height=?2,
                    updated_at_ms=?3
                WHERE id=?4 AND payer=?5 AND side='SHORT' AND state='OPEN'
                "#,
                params![
                    tx_sig.to_string(),
                    last_valid_i64,
                    now,
                    position_id,
                    payer_pubkey.to_string()
                ],
            )?;
            if rows == 0 {
                return Err(anyhow!("position not in OPEN state"));
            }

            TradeDb::insert_event(
                &db_tx,
                position_id,
                "PRECHECK_OK",
                &json!({
                    "effective_usdc": effective_usdc,
                    "effective_sol": effective_sol,
                    "reserved_sol_total": reserved_sol_total,
                    "free_sol": free_sol,
                    "reserved_usdc": reserved_usdc,
                    "max_usdc_in": max_usdc_in,
                    "fee": fee,
                    "rent_budget": rent_budget,
                    "required_sol": fee_plus_rent
                }),
            )?;
            TradeDb::insert_event(
                &db_tx,
                position_id,
                "STATE_CHANGE",
                &json!({ "state": "CLOSING" }),
            )?;
            TradeDb::insert_event(
                &db_tx,
                position_id,
                "TX_BUILT",
                &json!({
                    "payer": payer_pubkey.to_string(),
                    "pool": whirlpool.to_string(),
                    "usdc_ata": usdc_ata.to_string(),
                    "wsol_ata": wsol_ata.to_string(),
                    "swap_type": "ExactOut",
                    "amount": sol_position,
                    "slippage_bps": slippage_bps,
                    "quote_max_in": max_usdc_in,
                    "quote_est_in": est_usdc_in
                }),
            )?;

            db_tx.commit()?;
        }

        let (simulate_addresses, simulate_set) = if self.mode == ExecMode::Simulate {
            let mut token_accounts =
                list_owner_token_accounts(rpc, &payer_pubkey, &usdc_mint).await?;
            if !token_accounts.contains(&usdc_ata) {
                token_accounts.push(usdc_ata);
            }
            build_simulation_addresses(&payer_pubkey, &token_accounts)
        } else {
            (Vec::new(), HashSet::new())
        };

        let exec = match execute_tx(
            rpc,
            &transaction,
            self.mode,
            &simulate_addresses,
            last_valid_block_height,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                let err_msg = e.to_string();
                if self.mode == ExecMode::Live {
                    mark_exec_error(db, position_id, &err_msg)?;
                } else {
                    mark_failed(db, position_id, "EXECUTE_TX", &err_msg)?;
                }
                return Err(e);
            }
        };

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            match self.mode {
                ExecMode::Live => {
                    TradeDb::insert_event(
                        &db_tx,
                        position_id,
                        "TX_SENT",
                        &json!({
                            "signature": exec.signature.to_string(),
                            "last_valid_block_height": last_valid_block_height
                        }),
                    )?;
                }
                ExecMode::Simulate => {
                    let simulated = exec
                        .simulated
                        .as_ref()
                        .ok_or_else(|| anyhow!("simulate result missing"))?;
                    TradeDb::insert_event(
                        &db_tx,
                        position_id,
                        "TX_SIMULATED_OK",
                        &json!({
                            "signature": exec.signature.to_string(),
                            "units_consumed": simulated.units_consumed,
                            "logs": simulated.logs
                        }),
                    )?;
                }
            }
            db_tx.commit()?;
        }

        let mut delta_sol: i128 = 0;
        let mut delta_usdc: i128 = 0;
        let mut post_usdc: Option<u64> = None;
        let mut post_sol: Option<u64> = None;
        let mut tx_meta_error: Option<String> = None;

        match self.mode {
            ExecMode::Live => {
                let http = http
                    .as_ref()
                    .ok_or_else(|| anyhow!("http client missing"))?;
                let (tx_json_opt, err_opt) =
                    try_get_transaction_json(http, &exec.signature).await;
                if let Some(tx_json) = tx_json_opt {
                    let parsed = (|| -> Result<(i128, i128)> {
                        let meta = tx_json
                            .get("meta")
                            .ok_or_else(|| anyhow!("getTransaction missing meta"))?;
                        let msg = tx_json
                            .get("transaction")
                            .and_then(|t| t.get("message"))
                            .unwrap_or(&Value::Null);
                        let sol_delta = owner_lamport_delta(meta, msg, &payer_pubkey)?
                            .ok_or_else(|| anyhow!("payer not found in account keys"))?;
                        let usdc_delta = sum_owner_mint_delta(meta, &payer_pubkey, &usdc_mint)?;
                        Ok((sol_delta, usdc_delta))
                    })();
                    match parsed {
                        Ok((sol_delta, usdc_delta)) => {
                            delta_sol = sol_delta;
                            delta_usdc = usdc_delta;
                        }
                        Err(err) => {
                            tx_meta_error = Some(err.to_string());
                        }
                    }
                } else {
                    tx_meta_error = err_opt;
                }
            }
            ExecMode::Simulate => {
                let simulated = exec
                    .simulated
                    .as_ref()
                    .ok_or_else(|| anyhow!("simulate result missing"))?;
                let (post_usdc_val, post_sol_val) = post_balances_from_simulation(
                    simulated,
                    &simulate_addresses,
                    &payer_pubkey,
                    &simulate_set,
                    &usdc_mint,
                )?;
                post_usdc = Some(post_usdc_val);
                post_sol = Some(post_sol_val);
                delta_sol = post_sol_val as i128 - effective_sol as i128;
                delta_usdc = post_usdc_val as i128 - effective_usdc as i128;
            }
        }

        {
            let mut conn = db.open_conn()?;
            let db_tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let now = TradeDb::now_ms();
            let rows = db_tx.execute(
                r#"
                UPDATE positions
                SET state='CLOSED',
                    close_sig=?1,
                    close_last_valid_block_height=?2,
                    reserved_usdc=0,
                    updated_at_ms=?3
                WHERE id=?4 AND state='CLOSING'
                "#,
                params![exec.signature.to_string(), last_valid_i64, now, position_id],
            )?;
            if rows == 0 {
                return Err(anyhow!("position not in CLOSING state"));
            }

            if let (Some(u), Some(s)) = (post_usdc, post_sol) {
                TradeDb::upsert_sim_wallet(&db_tx, &payer_pubkey, u, s)?;
            }

            TradeDb::insert_event(
                &db_tx,
                position_id,
                "CLOSE_COMMITTED",
                &json!({
                    "delta_sol": delta_sol.to_string(),
                    "delta_usdc": delta_usdc.to_string(),
                    "reserved_usdc": 0,
                    "tx_meta_error": tx_meta_error
                }),
            )?;
            db_tx.commit()?;
        }

        Ok(exec.signature)
    }

    pub async fn open_long_on_pool(
        &self,
        pool: &str,
        usdc_ui: &str,
        slippage_bps: u16,
    ) -> Result<(i64, Signature)> {
        self.open_long(pool, usdc_ui, slippage_bps).await
    }

    pub async fn close_long_on_pool(&self, position_id: i64) -> Result<Signature> {
        self.close_long(position_id).await
    }

    pub async fn open_short_on_pool(
        &self,
        pool: &str,
        usdc_ui: &str,
        slippage_bps: u16,
    ) -> Result<(i64, Signature)> {
        self.open_short(pool, usdc_ui, slippage_bps).await
    }

    pub async fn close_short_on_pool(&self, position_id: i64) -> Result<Signature> {
        self.close_short(position_id).await
    }
}
