mod tracker;
#[path = "trade/mod.rs"]
mod trade;

use anyhow::{anyhow, Context, Result};
use rusqlite::{params, Connection};
use serde_json::Value;
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::{BTreeSet, HashSet},
    fs,
    str::FromStr,
};
use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

const TARGET_PROFIT_PCT: f64 = 0.004; // 0.4% над ценой входа
const STANDARD_JSON_PATH: &str = r"/home/user/DB/custom.json";
const PLAYER_REFRESH_INTERVAL_SECS: u64 = 300;
const PAYER_KEYPAIR_PATH: &str = "/home/user/DB/id.json";
const SOLANA_RPC_HTTP_URL: &str =
    "https://mainnet.helius-rpc.com/?api-key=ef131ba9-5495-460f-9d12-06515001f5ed";
const SOLANA_RPC_WS_URL: &str =
    "wss://mainnet.helius-rpc.com/?api-key=ef131ba9-5495-460f-9d12-06515001f5ed";
const SOLANA_TRADE_RPC_HTTP_URL: &str =
    "https://mainnet.helius-rpc.com/?api-key=d83804d1-9c0c-4de0-8d42-f85c8e39f897";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("SOLANA_PAYER_KEYPAIR").is_err() {
        std::env::set_var("SOLANA_PAYER_KEYPAIR", PAYER_KEYPAIR_PATH);
    }
    if std::env::var("SOLANA_RPC_HTTP").is_err() {
        std::env::set_var("SOLANA_RPC_HTTP", SOLANA_RPC_HTTP_URL);
    }
    if std::env::var("SOLANA_RPC_WS").is_err() {
        std::env::set_var("SOLANA_RPC_WS", SOLANA_RPC_WS_URL);
    }
    if std::env::var("SOLANA_TRADE_RPC_HTTP").is_err() {
        std::env::set_var("SOLANA_TRADE_RPC_HTTP", SOLANA_TRADE_RPC_HTTP_URL);
    }

    let players = load_players_from_standard(STANDARD_JSON_PATH)?;
    println!("👥 Загружено игроков для мониторинга: {}", players.len());

    let (tx, rx) = mpsc::unbounded_channel::<Pubkey>();
    let (trade_tx, trade_rx) = mpsc::unbounded_channel::<trade::signal::TradeSignal>();
    let mut known_players: HashSet<Pubkey> = players.iter().cloned().collect();
    let shutdown = tracker::Shutdown::new();
    let path = STANDARD_JSON_PATH.to_string();
    let tx_cloned = tx.clone();
    let shutdown_ctrl = shutdown.clone();

    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        shutdown_ctrl.trigger();
    });

    let shutdown_refresh = shutdown.clone();
    tokio::spawn(async move {
        loop {
            if shutdown_refresh.is_shutdown() {
                break;
            }
            tokio::select! {
                _ = sleep(Duration::from_secs(PLAYER_REFRESH_INTERVAL_SECS)) => {}
                _ = shutdown_refresh.notified() => {
                    break;
                }
            }
            match load_players_from_standard(&path) {
                Ok(all_players) => {
                    let mut new_added = 0usize;
                    for p in all_players {
                        if known_players.insert(p) {
                            new_added += 1;
                            if tx_cloned.send(p).is_err() {
                                eprintln!(
                                    "❌ Канал добавления игроков закрыт, остановка обновлений"
                                );
                                return;
                            }
                        }
                    }
                    if new_added > 0 {
                        println!("➕ Добавлены новые игроки из файла: {}", new_added);
                    } else {
                        println!("⏳ Новых игроков не найдено при обновлении");
                    }
                }
                Err(err) => eprintln!("⚠️ Ошибка обновления списка игроков: {err:?}"),
            }
        }
    });

    let trade_cfg = trade::executor::config_from_env()?;
    let exec_fut = trade::executor::run_from_signals(trade_cfg, trade_rx);
    let tracker_fut = tracker::run(TARGET_PROFIT_PCT, players, rx, shutdown, Some(trade_tx));
    tokio::try_join!(exec_fut, tracker_fut).map(|_| ())
}

fn load_players_from_standard(path: &str) -> Result<Vec<Pubkey>> {
    let data = fs::read_to_string(path).with_context(|| format!("чтение файла {path}"))?;
    let json: Value = serde_json::from_str(&data).context("парсинг standard.json")?;

    let rows = json
        .get("rows")
        .and_then(|r| r.as_array())
        .ok_or_else(|| anyhow!("в standard.json отсутствует массив rows"))?;

    let mut uniq = BTreeSet::new();
    for row in rows {
        if let Some(player) = row.get("player").and_then(|p| p.as_str()) {
            uniq.insert(player.to_string());
        }
    }

    if uniq.is_empty() {
        return Err(anyhow!("не найдено ни одного player в {path}"));
    }

    let mut conn = Connection::open("players.db").context("открытие/создание players.db")?;
    ensure_players_schema(&conn)?;

    {
        let tx = conn
            .transaction()
            .context("start transaction for inserting players")?;
        let mut inserted = 0usize;
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        for player in &uniq {
            let pk = Pubkey::from_str(player)
                .with_context(|| format!("некорректный pubkey в standard.json: {player}"))?;
            let rows = tx
                .execute(
                    "INSERT OR IGNORE INTO players(address, created_at_utc) VALUES (?1, ?2)",
                    params![pk.to_string(), now],
                )
                .with_context(|| format!("добавление игрока {pk} в players.db"))?;
            if rows > 0 {
                inserted += 1;
            }
        }
        tx.commit().context("commit players insert")?;
        println!(
            "🗄️  Загрузка игроков: всего уникальных в файле — {}, новых добавлено в БД — {}",
            uniq.len(),
            inserted
        );
    }

    let mut stmt = conn
        .prepare("SELECT address FROM players WHERE blacklist != 'black' ORDER BY address ASC")
        .context("prepare select players")?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .context("query players")?;

    let mut players = Vec::new();
    for addr in rows {
        let addr = addr?;
        let pk = Pubkey::from_str(&addr)
            .with_context(|| format!("некорректный pubkey в players.db: {addr}"))?;
        players.push(pk);
    }

    println!(
        "📦 Итоговый список игроков в БД: {} адресов (включая ранее добавленных)",
        players.len()
    );

    Ok(players)
}

fn ensure_players_schema(conn: &Connection) -> Result<()> {
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS players(
            address   TEXT PRIMARY KEY,
            created_at_utc TEXT NOT NULL,
            blacklist TEXT NOT NULL DEFAULT 'good',
            first_entry_ts_ms INTEGER
        )
        "#,
        [],
    )
    .context("создание таблицы players")?;

    let mut stmt = conn
        .prepare("PRAGMA table_info(players)")
        .context("получение схемы players")?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .context("чтение колонок players")?;

    let mut has_blacklist = false;
    let mut has_first_entry = false;
    for name in rows {
        let name = name?;
        if name == "blacklist" {
            has_blacklist = true;
        } else if name == "first_entry_ts_ms" {
            has_first_entry = true;
        }
    }

    if !has_blacklist {
        conn.execute(
            "ALTER TABLE players ADD COLUMN blacklist TEXT NOT NULL DEFAULT 'good'",
            [],
        )
        .context("добавление колонки blacklist в players")?;
    }
    if !has_first_entry {
        conn.execute(
            "ALTER TABLE players ADD COLUMN first_entry_ts_ms INTEGER",
            [],
        )
        .context("добавление колонки first_entry_ts_ms в players")?;
    }

    Ok(())
}
