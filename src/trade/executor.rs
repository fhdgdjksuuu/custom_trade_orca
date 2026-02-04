use crate::trade;
use crate::trade::signal::TradeSignal;
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use solana_keypair::{read_keypair_file, Keypair};
use std::{env, time::Duration};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::sleep;

const DEFAULT_TRACKER_DB: &str = "tracker.db";
const DEFAULT_TRADE_DB: &str = "trade.db";
const DEFAULT_PLAYERS_DB: &str = "players.db";
const DEFAULT_POOL: &str = "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE";
const DEFAULT_USDC_UI: &str = "1";
const DEFAULT_SLIPPAGE_BPS: u16 = 10_000;
const DEFAULT_POLL_MS: u64 = 1000;
const EVENT_BATCH_LIMIT: i64 = 200;

const DEFAULT_TRADE_RPC_URL: &str =
    "https://mainnet.helius-rpc.com/?api-key=718bdaa4-fdab-4c4b-8ee3-d208c26bd7a1";

#[derive(Debug, Clone)]
pub struct Config {
    tracker_db: String,
    trade_db: String,
    players_db: String,
    rpc_url: String,
    pool: String,
    usdc_ui: String,
    slippage_bps: u16,
    keypair_path: String,
    dry_run: bool,
    once: bool,
    poll_ms: u64,
}

#[derive(Debug, Clone)]
struct TrackerEvent {
    id: i64,
    action: String,
    player: String,
    signature: String,
    whirlpool: String,
    price: Option<f64>,
    target_price: Option<f64>,
}

fn default_config() -> Config {
    Config {
        tracker_db: DEFAULT_TRACKER_DB.to_string(),
        trade_db: DEFAULT_TRADE_DB.to_string(),
        players_db: env::var("PLAYERS_DB").unwrap_or_else(|_| DEFAULT_PLAYERS_DB.to_string()),
        rpc_url: env::var("SOLANA_TRADE_RPC_HTTP")
            .unwrap_or_else(|_| DEFAULT_TRADE_RPC_URL.to_string()),
        pool: DEFAULT_POOL.to_string(),
        usdc_ui: DEFAULT_USDC_UI.to_string(),
        slippage_bps: DEFAULT_SLIPPAGE_BPS,
        keypair_path: env::var("SOLANA_PAYER_KEYPAIR").unwrap_or_default(),
        dry_run: false,
        once: false,
        poll_ms: DEFAULT_POLL_MS,
    }
}

fn validate_config(cfg: &Config) -> Result<()> {
    if cfg.keypair_path.is_empty() && !cfg.dry_run {
        return Err(anyhow!(
            "missing keypair path. set --keypair or SOLANA_PAYER_KEYPAIR"
        ));
    }
    if !is_one_usdc(&cfg.usdc_ui) {
        return Err(anyhow!("usdc amount must be 1"));
    }
    Ok(())
}

pub fn config_from_env() -> Result<Config> {
    let cfg = default_config();
    validate_config(&cfg)?;
    Ok(cfg)
}

pub fn parse_args() -> Result<Config> {
    let mut cfg = default_config();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--tracker-db" => {
                cfg.tracker_db = args.next().ok_or_else(|| anyhow!("missing --tracker-db"))?;
            }
            "--trade-db" => {
                cfg.trade_db = args.next().ok_or_else(|| anyhow!("missing --trade-db"))?;
            }
            "--players-db" => {
                cfg.players_db = args.next().ok_or_else(|| anyhow!("missing --players-db"))?;
            }
            "--rpc-url" => {
                cfg.rpc_url = args.next().ok_or_else(|| anyhow!("missing --rpc-url"))?;
            }
            "--pool" => {
                cfg.pool = args.next().ok_or_else(|| anyhow!("missing --pool"))?;
            }
            "--usdc" => {
                cfg.usdc_ui = args.next().ok_or_else(|| anyhow!("missing --usdc"))?;
            }
            "--slippage-bps" => {
                let raw = args
                    .next()
                    .ok_or_else(|| anyhow!("missing --slippage-bps"))?;
                cfg.slippage_bps = raw
                    .parse::<u16>()
                    .map_err(|_| anyhow!("bad --slippage-bps"))?;
            }
            "--keypair" => {
                cfg.keypair_path = args.next().ok_or_else(|| anyhow!("missing --keypair"))?;
            }
            "--dry-run" => {
                cfg.dry_run = true;
            }
            "--once" => {
                cfg.once = true;
            }
            "--poll-ms" => {
                let raw = args.next().ok_or_else(|| anyhow!("missing --poll-ms"))?;
                cfg.poll_ms = raw.parse::<u64>().map_err(|_| anyhow!("bad --poll-ms"))?;
            }
            "--help" => {
                print_usage();
                std::process::exit(0);
            }
            other => {
                return Err(anyhow!("unknown argument: {other}"));
            }
        }
    }

    validate_config(&cfg)?;
    Ok(cfg)
}

fn is_one_usdc(raw: &str) -> bool {
    let s = raw.trim();
    if s.is_empty() {
        return false;
    }
    if let Some((int_part, frac_part)) = s.split_once('.') {
        if int_part != "1" || frac_part.is_empty() {
            return false;
        }
        return frac_part.chars().all(|c| c == '0');
    }
    s == "1"
}

fn print_usage() {
    println!(
        "\
Usage: trade_executor [options]

Options:
  --tracker-db PATH       Path to tracker.db (default: tracker.db)
  --trade-db PATH         Path to trade.db (default: trade.db)
  --players-db PATH       Path to players.db (default: players.db)
  --rpc-url URL           RPC URL (default: env SOLANA_TRADE_RPC_HTTP or built-in)
  --pool PUBKEY           Whirlpool pool (default: {pool})
  --usdc AMOUNT           USDC amount as UI string (default: {usdc}, must be 1)
  --slippage-bps N         Slippage in bps (default: {slip})
  --keypair PATH          Payer keypair file (or env SOLANA_PAYER_KEYPAIR)
  --dry-run               Check mode without simulate calls
  --once                  Process current events and exit
  --poll-ms N             Poll interval in ms (default: {poll})
  --help                  Show help
",
        pool = DEFAULT_POOL,
        usdc = DEFAULT_USDC_UI,
        slip = DEFAULT_SLIPPAGE_BPS,
        poll = DEFAULT_POLL_MS
    );
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn open_db(path: &str) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("open db {path}"))?;
    conn.pragma_update(None, "journal_mode", "WAL").ok();
    conn.busy_timeout(Duration::from_millis(5000)).ok();
    Ok(conn)
}

fn ensure_trade_links_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS trade_links (
            player        TEXT NOT NULL,
            signature     TEXT NOT NULL,
            whirlpool     TEXT NOT NULL,
            side          TEXT NOT NULL CHECK(side IN ('LONG','SHORT')),
            position_id   INTEGER,
            status        TEXT NOT NULL CHECK(status IN ('OPENING','OPEN','CLOSING','CLOSED','FAILED','PENDING_CLOSE')),
            entry_event_id INTEGER NOT NULL,
            exit_event_id  INTEGER,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            last_error    TEXT,
            PRIMARY KEY (player, signature)
        );

        CREATE TABLE IF NOT EXISTS executor_state (
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL
        );
        "#,
    )
    .context("init trade_links schema")?;
    Ok(())
}

fn ensure_players_schema(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(players)")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let mut has_address = false;
    let mut has_blacklist = false;
    for r in rows {
        let name = r?;
        if name == "address" {
            has_address = true;
        } else if name == "blacklist" {
            has_blacklist = true;
        }
    }
    if !has_address || !has_blacklist {
        return Err(anyhow!("players.db missing required columns"));
    }
    Ok(())
}

fn is_player_good(conn: &Connection, player: &str) -> Result<bool> {
    let status: Option<String> = conn
        .query_row(
            "SELECT blacklist FROM players WHERE address = ?1",
            params![player],
            |row| row.get(0),
        )
        .optional()?;
    Ok(status
        .map(|v| v.trim().eq_ignore_ascii_case("good"))
        .unwrap_or(false))
}

fn get_last_event_id(conn: &Connection) -> Result<i64> {
    let val: Option<i64> = conn
        .query_row(
            "SELECT value FROM executor_state WHERE key = 'last_event_id'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(val.unwrap_or(0))
}

fn set_last_event_id(conn: &Connection, id: i64) -> Result<()> {
    conn.execute(
        r#"
        INSERT INTO executor_state (key, value)
        VALUES ('last_event_id', ?1)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        "#,
        params![id],
    )?;
    Ok(())
}

fn fetch_events(conn: &Connection, last_id: i64) -> Result<Vec<TrackerEvent>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, action, player, signature, whirlpool, price, target_price
        FROM events
        WHERE id > ?1
        ORDER BY id ASC
        LIMIT ?2
        "#,
    )?;
    let rows = stmt.query_map(params![last_id, EVENT_BATCH_LIMIT], |row| {
        Ok(TrackerEvent {
            id: row.get(0)?,
            action: row.get(1)?,
            player: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            signature: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
            whirlpool: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
            price: row.get(5)?,
            target_price: row.get(6)?,
        })
    })?;

    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

fn find_entry_event(
    conn: &Connection,
    player: &str,
    signature: &str,
) -> Result<Option<TrackerEvent>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, action, player, signature, whirlpool, price, target_price
        FROM events
        WHERE action = 'ENTRY_SIGNAL'
          AND player = ?1
          AND signature = ?2
        ORDER BY id ASC
        LIMIT 1
        "#,
    )?;
    let row = stmt
        .query_row(params![player, signature], |row| {
            Ok(TrackerEvent {
                id: row.get(0)?,
                action: row.get(1)?,
                player: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                signature: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                whirlpool: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                price: row.get(5)?,
                target_price: row.get(6)?,
            })
        })
        .optional()?;
    Ok(row)
}

fn compute_side(entry: &TrackerEvent) -> Result<&'static str> {
    let price = entry.price.ok_or_else(|| anyhow!("missing entry price"))?;
    let target = entry
        .target_price
        .ok_or_else(|| anyhow!("missing target price"))?;
    if target > price {
        Ok("LONG")
    } else if target < price {
        Ok("SHORT")
    } else {
        Err(anyhow!("target equals entry price"))
    }
}

fn compute_profit_pct(side: &str, entry_price: f64, exit_price: f64) -> Result<f64> {
    if entry_price <= 0.0 {
        return Err(anyhow!("entry price must be positive"));
    }
    match side {
        "LONG" => Ok((exit_price - entry_price) / entry_price * 100.0),
        "SHORT" => Ok((entry_price - exit_price) / entry_price * 100.0),
        _ => Err(anyhow!("unknown side {side}")),
    }
}

fn side_human(side: &str) -> &'static str {
    match side {
        "LONG" => "на рост",
        "SHORT" => "на падение",
        _ => "неизвестно",
    }
}

fn rpc_base_url(url: &str) -> &str {
    url.split('?').next().unwrap_or(url)
}

async fn open_position(
    cfg: &Config,
    trader: &trade::Trader,
    tracker_conn: &Connection,
    trade_conn: &Connection,
    entry: &TrackerEvent,
    pending_exit_id: Option<i64>,
    dry_run: bool,
) -> Result<()> {
    if entry.whirlpool != cfg.pool {
        println!(
            "⏭️ Пропуск входа: другой пул ликвидности, player={} sig={} пул={}",
            entry.player, entry.signature, entry.whirlpool
        );
        return Ok(());
    }
    let side = compute_side(entry)?;
    let now = now_ms();
    println!(
        "🟢 Запрос на открытие позиции {}: player={} sig={} цена={:.8} цель={:.8}",
        side_human(side),
        entry.player,
        entry.signature,
        entry.price.unwrap_or_default(),
        entry.target_price.unwrap_or_default()
    );

    let existing: Option<(String, Option<i64>, Option<i64>)> = trade_conn
        .query_row(
            r#"
            SELECT status, position_id, exit_event_id
            FROM trade_links
            WHERE player = ?1 AND signature = ?2
            "#,
            params![entry.player, entry.signature],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;

    if let Some((status, _, _)) = &existing {
        if status == "OPEN" || status == "CLOSED" || status == "OPENING" || status == "CLOSING" {
            println!(
                "⏭️ Пропуск открытия: позиция уже в состоянии {} (player={} sig={})",
                status, entry.player, entry.signature
            );
            return Ok(());
        }
    }

    if dry_run {
        println!(
            "🧪 Проверка входа: player={} sig={} сторона={} пул={}",
            entry.player,
            entry.signature,
            side_human(side),
            entry.whirlpool
        );
        return Ok(());
    }

    if existing.is_none() {
        trade_conn.execute(
            r#"
            INSERT INTO trade_links
            (player, signature, whirlpool, side, status, entry_event_id, created_at_ms, updated_at_ms)
            VALUES (?1, ?2, ?3, ?4, 'OPENING', ?5, ?6, ?6)
            "#,
            params![
                entry.player,
                entry.signature,
                entry.whirlpool,
                side,
                entry.id,
                now
            ],
        )?;
    } else {
        trade_conn.execute(
            r#"
            UPDATE trade_links
            SET status = 'OPENING',
                side = ?1,
                entry_event_id = ?2,
                updated_at_ms = ?3,
                last_error = NULL
            WHERE player = ?4 AND signature = ?5
            "#,
            params![side, entry.id, now, entry.player, entry.signature],
        )?;
    }

    let (position_id, _) = match side {
        "LONG" => {
            trader
                .open_long(&entry.whirlpool, &cfg.usdc_ui, cfg.slippage_bps)
                .await?
        }
        "SHORT" => {
            trader
                .open_short(&entry.whirlpool, &cfg.usdc_ui, cfg.slippage_bps)
                .await?
        }
        _ => return Err(anyhow!("unknown side {side}")),
    };

    trade_conn.execute(
        r#"
        UPDATE trade_links
        SET status = 'OPEN',
            position_id = ?1,
            updated_at_ms = ?2,
            last_error = NULL
        WHERE player = ?3 AND signature = ?4
        "#,
        params![position_id, now_ms(), entry.player, entry.signature],
    )?;

    if let Some(entry_price) = entry.price {
        trade_conn.execute(
            r#"
            UPDATE positions
            SET entry_price = ?1
            WHERE id = ?2
            "#,
            params![entry_price, position_id],
        )?;
    }
    println!(
        "✅ Открыта позиция {}: player={} sig={} position_id={}",
        side_human(side),
        entry.player,
        entry.signature,
        position_id
    );

    let exit_id = pending_exit_id.or_else(|| existing.and_then(|x| x.2));
    if let Some(exit_event_id) = exit_id {
        println!(
            "🔁 Найден ожидающий выход: player={} sig={} событие_выхода={}",
            entry.player, entry.signature, exit_event_id
        );
        close_position(
            tracker_conn,
            trader,
            trade_conn,
            &entry.player,
            &entry.signature,
            exit_event_id,
            dry_run,
        )
        .await?;
    }

    Ok(())
}

async fn close_position(
    tracker_conn: &Connection,
    trader: &trade::Trader,
    trade_conn: &Connection,
    player: &str,
    signature: &str,
    exit_event_id: i64,
    dry_run: bool,
) -> Result<()> {
    let row: Option<(String, Option<i64>)> = trade_conn
        .query_row(
            r#"
            SELECT side, position_id
            FROM trade_links
            WHERE player = ?1 AND signature = ?2
            "#,
            params![player, signature],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    let Some((side, position_id)) = row else {
        println!(
            "⚠️ Выход без записи: player={} sig={} событие_выхода={}",
            player, signature, exit_event_id
        );
        return Ok(());
    };

    let Some(position_id) = position_id else {
        trade_conn.execute(
            r#"
            UPDATE trade_links
            SET status = 'PENDING_CLOSE',
                exit_event_id = ?1,
                updated_at_ms = ?2
            WHERE player = ?3 AND signature = ?4
            "#,
            params![exit_event_id, now_ms(), player, signature],
        )?;
        println!(
            "⏸️ Выход отложен: позиция ещё не создана (player={} sig={})",
            player, signature
        );
        return Ok(());
    };

    if dry_run {
        println!(
            "🧪 Проверка выхода: player={} sig={} сторона={} position_id={}",
            player,
            signature,
            side_human(&side),
            position_id
        );
        return Ok(());
    }

    println!(
        "🔵 Закрытие позиции {}: player={} sig={} position_id={}",
        side_human(&side),
        player,
        signature,
        position_id
    );

    trade_conn.execute(
        r#"
        UPDATE trade_links
        SET status = 'CLOSING',
            exit_event_id = ?1,
            updated_at_ms = ?2,
            last_error = NULL
        WHERE player = ?3 AND signature = ?4
        "#,
        params![exit_event_id, now_ms(), player, signature],
    )?;

    match side.as_str() {
        "LONG" => {
            trader.close_long(position_id).await?;
        }
        "SHORT" => {
            trader.close_short(position_id).await?;
        }
        _ => return Err(anyhow!("unknown side {side}")),
    }

    trade_conn.execute(
        r#"
        UPDATE trade_links
        SET status = 'CLOSED',
            updated_at_ms = ?1,
            last_error = NULL
        WHERE player = ?2 AND signature = ?3
        "#,
        params![now_ms(), player, signature],
    )?;

    let profit_result = record_profit(
        tracker_conn,
        trade_conn,
        position_id,
        &side,
        player,
        signature,
        exit_event_id,
    );
    match profit_result {
        Ok(Some(pct)) => {
            println!(
                "💹 Профит по позиции: player={} sig={} сторона={} profit={:.4}%",
                player,
                signature,
                side_human(&side),
                pct
            );
        }
        Ok(None) => {
            println!(
                "⚠️ Профит не рассчитан: отсутствуют цены (player={} sig={})",
                player, signature
            );
        }
        Err(err) => {
            println!(
                "⚠️ Ошибка расчёта профита: player={} sig={} err={}",
                player, signature, err
            );
        }
    }
    println!(
        "✅ Позиция закрыта {}: player={} sig={} position_id={}",
        side_human(&side),
        player,
        signature,
        position_id
    );

    Ok(())
}

fn record_profit(
    tracker_conn: &Connection,
    trade_conn: &Connection,
    position_id: i64,
    side: &str,
    player: &str,
    signature: &str,
    exit_event_id: i64,
) -> Result<Option<f64>> {
    let entry = find_entry_event(tracker_conn, player, signature)?;
    let Some(entry) = entry else {
        return Ok(None);
    };
    let exit = fetch_event_by_id(tracker_conn, exit_event_id)?;
    let Some(exit) = exit else {
        return Ok(None);
    };

    let entry_price = match entry.price {
        Some(v) => v,
        None => return Ok(None),
    };
    let exit_price = match exit.price {
        Some(v) => v,
        None => return Ok(None),
    };

    let profit_pct = compute_profit_pct(side, entry_price, exit_price)?;
    let now = now_ms();

    trade_conn.execute(
        r#"
        UPDATE positions
        SET entry_price = ?1,
            exit_price = ?2,
            profit_pct = ?3,
            updated_at_ms = ?4
        WHERE id = ?5
        "#,
        params![entry_price, exit_price, profit_pct, now, position_id],
    )?;

    let details_json = serde_json::to_string(&serde_json::json!({
        "entry_price": entry_price,
        "exit_price": exit_price,
        "profit_pct": profit_pct,
        "side": side,
        "player": player,
        "signature": signature,
        "entry_event_id": entry.id,
        "exit_event_id": exit_event_id
    }))
    .unwrap_or_else(|_| "{}".to_string());
    trade_conn.execute(
        r#"
        INSERT INTO trade_events (position_id, ts_ms, event_type, details_json)
        VALUES (?1, ?2, 'PROFIT', ?3)
        "#,
        params![position_id, now, details_json],
    )?;

    Ok(Some(profit_pct))
}

async fn handle_entry_event(
    cfg: &Config,
    trader: &trade::Trader,
    tracker_conn: &Connection,
    players_conn: &Connection,
    trade_conn: &Connection,
    entry: &TrackerEvent,
    pending_exit_id: Option<i64>,
    dry_run: bool,
) -> Result<()> {
    if entry.player.is_empty() || entry.signature.is_empty() || entry.whirlpool.is_empty() {
        println!(
            "⚠️ Пропуск входа: пустые поля player/signature/whirlpool (id={})",
            entry.id
        );
        return Ok(());
    }
    if !is_player_good(players_conn, &entry.player)? {
        println!(
            "⛔ Пропуск входа: игрок не допущен (player={} sig={})",
            entry.player, entry.signature
        );
        return Ok(());
    }
    open_position(
        cfg,
        trader,
        tracker_conn,
        trade_conn,
        entry,
        pending_exit_id,
        dry_run,
    )
    .await
}

async fn handle_target_event(
    cfg: &Config,
    trader: &trade::Trader,
    tracker_conn: &Connection,
    players_conn: &Connection,
    trade_conn: &Connection,
    event: &TrackerEvent,
    dry_run: bool,
) -> Result<()> {
    if event.player.is_empty() || event.signature.is_empty() {
        println!(
            "⚠️ Пропуск выхода: пустые поля player/signature (id={})",
            event.id
        );
        return Ok(());
    }
    let existing: Option<String> = trade_conn
        .query_row(
            r#"
            SELECT status
            FROM trade_links
            WHERE player = ?1 AND signature = ?2
            "#,
            params![event.player, event.signature],
            |row| row.get(0),
        )
        .optional()?;

    if let Some(status) = existing {
        if status == "OPEN" {
            return close_position(
                tracker_conn,
                trader,
                trade_conn,
                &event.player,
                &event.signature,
                event.id,
                dry_run,
            )
            .await;
        }
        if status == "OPENING" || status == "PENDING_CLOSE" {
            if dry_run {
                println!(
                    "🧪 Проверка выхода: ожидание открытия (player={} sig={})",
                    event.player, event.signature
                );
                return Ok(());
            }
            trade_conn.execute(
                r#"
                UPDATE trade_links
                SET status = 'PENDING_CLOSE',
                    exit_event_id = ?1,
                    updated_at_ms = ?2
                WHERE player = ?3 AND signature = ?4
                "#,
                params![event.id, now_ms(), event.player, event.signature],
            )?;
            println!(
                "⏸️ Выход отложен до открытия: player={} sig={}",
                event.player, event.signature
            );
            return Ok(());
        }
        println!(
            "⏭️ Пропуск выхода: неподходящий статус {} (player={} sig={})",
            status, event.player, event.signature
        );
        return Ok(());
    }

    let entry = find_entry_event(tracker_conn, &event.player, &event.signature)?;
    let Some(entry) = entry else {
        println!(
            "⚠️ Выход без входа: не найден ENTRY_SIGNAL (player={} sig={})",
            event.player, event.signature
        );
        return Ok(());
    };

    handle_entry_event(
        cfg,
        trader,
        tracker_conn,
        players_conn,
        trade_conn,
        &entry,
        Some(event.id),
        dry_run,
    )
    .await
}

fn build_trader(cfg: &Config) -> Result<trade::Trader> {
    let payer = if cfg.keypair_path.is_empty() {
        Keypair::new()
    } else {
        read_keypair_file(&cfg.keypair_path).map_err(|e| anyhow!("read keypair: {e}"))?
    };
    trade::Trader::new(
        &cfg.rpc_url,
        payer,
        trade::ExecMode::Simulate,
        &cfg.trade_db,
    )
}

fn fetch_event_by_id(conn: &Connection, id: i64) -> Result<Option<TrackerEvent>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, action, player, signature, whirlpool, price, target_price
        FROM events
        WHERE id = ?1
        LIMIT 1
        "#,
    )?;
    let row = stmt
        .query_row(params![id], |row| {
            Ok(TrackerEvent {
                id: row.get(0)?,
                action: row.get(1)?,
                player: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                signature: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                whirlpool: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                price: row.get(5)?,
                target_price: row.get(6)?,
            })
        })
        .optional()?;
    Ok(row)
}

pub async fn run_polling(cfg: Config) -> Result<()> {
    println!("🚦 Исполнитель торговли запущен (опрос базы).");
    println!(
        "🗂️ БД: tracker={}, trade={}, players={}",
        cfg.tracker_db, cfg.trade_db, cfg.players_db
    );
    println!("🌊 Пул ликвидности: {}", cfg.pool);
    println!(
        "💵 Сумма сделки: {} USDC, проскальзывание: {} б.п.",
        cfg.usdc_ui, cfg.slippage_bps
    );
    println!(
        "🧪 Режим проверки без симуляции: {}",
        if cfg.dry_run { "да" } else { "нет" }
    );
    println!("🔌 Узел торговли: {}", rpc_base_url(&cfg.rpc_url));

    let trader = build_trader(&cfg)?;
    let tracker_conn = open_db(&cfg.tracker_db)?;
    let trade_conn = open_db(&cfg.trade_db)?;
    let players_conn = open_db(&cfg.players_db)?;
    ensure_trade_links_schema(&trade_conn)?;
    ensure_players_schema(&players_conn)?;

    let mut last_event_id = if cfg.dry_run {
        0
    } else {
        get_last_event_id(&trade_conn)?
    };

    loop {
        let events = fetch_events(&tracker_conn, last_event_id)?;
        if events.is_empty() {
            if cfg.once {
                println!("🏁 Опрос завершён: новых событий нет.");
                break;
            }
            sleep(Duration::from_millis(cfg.poll_ms)).await;
            continue;
        }

        for event in events {
            println!(
                "📨 Событие: action={} id={} player={} sig={}",
                event.action, event.id, event.player, event.signature
            );
            let res = match event.action.as_str() {
                "ENTRY_SIGNAL" => {
                    handle_entry_event(
                        &cfg,
                        &trader,
                        &tracker_conn,
                        &players_conn,
                        &trade_conn,
                        &event,
                        None,
                        cfg.dry_run,
                    )
                    .await
                }
                "TARGET_HIT" => {
                    handle_target_event(
                        &cfg,
                        &trader,
                        &tracker_conn,
                        &players_conn,
                        &trade_conn,
                        &event,
                        cfg.dry_run,
                    )
                    .await
                }
                _ => Ok(()),
            };

            if let Err(err) = res {
                if !cfg.dry_run && !event.player.is_empty() && !event.signature.is_empty() {
                    let updated = trade_conn
                        .execute(
                            r#"
                            UPDATE trade_links
                            SET status = 'FAILED',
                                updated_at_ms = ?1,
                                last_error = ?2
                            WHERE player = ?3 AND signature = ?4
                            "#,
                            params![now_ms(), err.to_string(), event.player, event.signature],
                        )
                        .unwrap_or(0);
                    if updated == 0 {
                        eprintln!(
                            "❌ Ошибка без записи: player={} sig={} err={}",
                            event.player, event.signature, err
                        );
                    } else {
                        eprintln!(
                            "❌ Ошибка обработки: player={} sig={} err={}",
                            event.player, event.signature, err
                        );
                    }
                } else {
                    eprintln!("❌ Ошибка обработки события: {:?}", err);
                }
            }

            last_event_id = event.id;
            if !cfg.dry_run {
                set_last_event_id(&trade_conn, last_event_id)?;
            }
        }
    }

    Ok(())
}

pub async fn run_from_signals(
    cfg: Config,
    mut signals: UnboundedReceiver<TradeSignal>,
) -> Result<()> {
    println!("🚦 Исполнитель торговли запущен (реакция на сигналы).");
    println!(
        "🗂️ БД: tracker={}, trade={}, players={}",
        cfg.tracker_db, cfg.trade_db, cfg.players_db
    );
    println!("🌊 Пул ликвидности: {}", cfg.pool);
    println!(
        "💵 Сумма сделки: {} USDC, проскальзывание: {} б.п.",
        cfg.usdc_ui, cfg.slippage_bps
    );
    println!(
        "🧪 Режим проверки без симуляции: {}",
        if cfg.dry_run { "да" } else { "нет" }
    );
    println!("🔌 Узел торговли: {}", rpc_base_url(&cfg.rpc_url));

    let trader = build_trader(&cfg)?;
    let tracker_conn = open_db(&cfg.tracker_db)?;
    let trade_conn = open_db(&cfg.trade_db)?;
    let players_conn = open_db(&cfg.players_db)?;
    ensure_trade_links_schema(&trade_conn)?;
    ensure_players_schema(&players_conn)?;

    while let Some(signal) = signals.recv().await {
        println!("📨 Сигнал: action={} id={}", signal.action, signal.id);
        if signal.action != "ENTRY_SIGNAL" && signal.action != "TARGET_HIT" {
            println!("⏭️ Пропуск сигнала: неизвестное действие {}", signal.action);
            continue;
        }
        let Some(event) = fetch_event_by_id(&tracker_conn, signal.id)? else {
            println!("⚠️ Событие не найдено в tracker.db: id={}", signal.id);
            continue;
        };
        println!(
            "🧭 Событие: action={} id={} player={} sig={}",
            event.action, event.id, event.player, event.signature
        );
        let res = match event.action.as_str() {
            "ENTRY_SIGNAL" => {
                handle_entry_event(
                    &cfg,
                    &trader,
                    &tracker_conn,
                    &players_conn,
                    &trade_conn,
                    &event,
                    None,
                    cfg.dry_run,
                )
                .await
            }
            "TARGET_HIT" => {
                handle_target_event(
                    &cfg,
                    &trader,
                    &tracker_conn,
                    &players_conn,
                    &trade_conn,
                    &event,
                    cfg.dry_run,
                )
                .await
            }
            _ => Ok(()),
        };

        if let Err(err) = res {
            if !cfg.dry_run && !event.player.is_empty() && !event.signature.is_empty() {
                let updated = trade_conn
                    .execute(
                        r#"
                        UPDATE trade_links
                        SET status = 'FAILED',
                            updated_at_ms = ?1,
                            last_error = ?2
                        WHERE player = ?3 AND signature = ?4
                        "#,
                        params![now_ms(), err.to_string(), event.player, event.signature],
                    )
                    .unwrap_or(0);
                if updated == 0 {
                    eprintln!(
                        "❌ Ошибка без записи: player={} sig={} err={}",
                        event.player, event.signature, err
                    );
                } else {
                    eprintln!(
                        "❌ Ошибка обработки: player={} sig={} err={}",
                        event.player, event.signature, err
                    );
                }
            } else {
                eprintln!("❌ Ошибка обработки события: {:?}", err);
            }
        }
    }

    println!("🛑 Исполнитель торговли завершил работу.");
    Ok(())
}
