#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "sqlite3 не найден. Установите его и повторите."
  exit 1
fi

TRACKER_DB="${TRACKER_DB:-tracker.db}"
TRADE_DB="${TRADE_DB:-trade.db}"
PLAYERS_DB="${PLAYERS_DB:-players.db}"
EXPECTED_POOL="${EXPECTED_POOL:-}"

if [ ! -f "$TRACKER_DB" ]; then
  echo "Нет файла $TRACKER_DB"
  exit 1
fi
if [ ! -f "$TRADE_DB" ]; then
  echo "Нет файла $TRADE_DB"
  exit 1
fi
if [ ! -f "$PLAYERS_DB" ]; then
  echo "Нет файла $PLAYERS_DB"
  exit 1
fi

run_sql() {
  local db="$1"
  local sql="$2"
  sqlite3 -readonly "$db" "$sql"
}

echo "== Файлы БД =="
ls -lh "$TRACKER_DB" "$TRADE_DB" "$PLAYERS_DB"
echo

echo "== Сводка сигналов (tracker.db) =="
entry_count=$(run_sql "$TRACKER_DB" "SELECT COUNT(*) FROM events WHERE action='ENTRY_SIGNAL';")
target_count=$(run_sql "$TRACKER_DB" "SELECT COUNT(*) FROM events WHERE action='TARGET_HIT';")
open_by_signal=$(run_sql "$TRACKER_DB" "SELECT COUNT(1) FROM events e WHERE e.action='ENTRY_SIGNAL' AND NOT EXISTS (SELECT 1 FROM events t WHERE t.player=e.player AND t.signature=e.signature AND t.action='TARGET_HIT');")
last_tracker_ts=$(run_sql "$TRACKER_DB" "SELECT COALESCE(MAX(ts_ms),0) FROM events;")
last_tracker_ts_h=$(run_sql "$TRACKER_DB" "SELECT COALESCE(datetime(MAX(ts_ms)/1000,'unixepoch'), 'нет') FROM events;")

if [ -n "$EXPECTED_POOL" ]; then
  entry_expected=$(run_sql "$TRACKER_DB" "SELECT COUNT(*) FROM events WHERE action='ENTRY_SIGNAL' AND whirlpool='$EXPECTED_POOL';")
fi

echo "ENTRY_SIGNAL: $entry_count"
echo "TARGET_HIT:   $target_count"
echo "Открытых по сигналам: $open_by_signal"
if [ -n "$EXPECTED_POOL" ]; then
  echo "ENTRY_SIGNAL по ожидаемому пулу: $entry_expected"
fi
echo "Последнее событие: $last_tracker_ts_h (ts_ms=$last_tracker_ts)"

last_entry_pool=$(run_sql "$TRACKER_DB" "SELECT whirlpool FROM events WHERE action='ENTRY_SIGNAL' ORDER BY ts_ms DESC LIMIT 1;")
if [ -n "$last_entry_pool" ]; then
  echo "Последний ENTRY_SIGNAL пул: $last_entry_pool"
  if [ -n "$EXPECTED_POOL" ] && [ "$last_entry_pool" != "$EXPECTED_POOL" ]; then
    echo "⚠️ Пул не совпадает с ожидаемым."
  fi
else
  echo "ENTRY_SIGNAL отсутствуют."
fi

if [ "$entry_count" -gt 0 ] || [ "$target_count" -gt 0 ]; then
  echo
  echo "Последние 10 сигналов вход/выход:"
  run_sql "$TRACKER_DB" "SELECT ts_ms, action, player, signature, whirlpool FROM events WHERE action IN ('ENTRY_SIGNAL','TARGET_HIT') ORDER BY ts_ms DESC LIMIT 10;"
else
  echo "Сигналов входа/выхода пока нет."
fi

echo

echo "== Входы по пулам (tracker.db) =="
if [ "$entry_count" -gt 0 ]; then
  run_sql "$TRACKER_DB" "SELECT whirlpool, COUNT(*) FROM events WHERE action='ENTRY_SIGNAL' GROUP BY whirlpool;"
else
  echo "Нет данных."
fi

echo

echo "== Статусы игроков по входам (players.db) =="
if [ "$entry_count" -gt 0 ]; then
  run_sql "$TRACKER_DB" "ATTACH '$PLAYERS_DB' AS p; SELECT e.player, p.players.blacklist, COUNT(*) FROM events e JOIN p.players ON p.players.address=e.player WHERE e.action='ENTRY_SIGNAL' GROUP BY e.player, p.players.blacklist;"
else
  echo "Нет данных."
fi

echo

echo "== Отказы по входам (tracker.db) =="
reject_count=$(run_sql "$TRACKER_DB" "SELECT COUNT(*) FROM events WHERE action='ENTRY_REJECT';")
if [ "$reject_count" -gt 0 ]; then
  run_sql "$TRACKER_DB" "SELECT action, COUNT(*) FROM events WHERE action='ENTRY_REJECT' GROUP BY action;"
  echo "Последние 5 отказов:"
  run_sql "$TRACKER_DB" "SELECT ts_ms, action, player, signature, details_json FROM events WHERE action='ENTRY_REJECT' ORDER BY ts_ms DESC LIMIT 5;"
else
  echo "Нет данных."
fi

echo

echo "== Сводка торговли (trade.db) =="
trade_links_count=$(run_sql "$TRADE_DB" "SELECT COUNT(*) FROM trade_links;")
positions_count=$(run_sql "$TRADE_DB" "SELECT COUNT(*) FROM positions;")
trade_events_count=$(run_sql "$TRADE_DB" "SELECT COUNT(*) FROM trade_events;")
sim_wallet_count=$(run_sql "$TRADE_DB" "SELECT COUNT(*) FROM sim_wallet;")
last_trade_event_ts=$(run_sql "$TRADE_DB" "SELECT COALESCE(MAX(ts_ms),0) FROM trade_events;")
last_trade_event_ts_h=$(run_sql "$TRADE_DB" "SELECT COALESCE(datetime(MAX(ts_ms)/1000,'unixepoch'), 'нет') FROM trade_events;")

echo "Записей trade_links:  $trade_links_count"
echo "Записей positions:    $positions_count"
echo "Записей trade_events: $trade_events_count"
echo "Записей sim_wallet:   $sim_wallet_count"
echo "Последнее событие торговли: $last_trade_event_ts_h (ts_ms=$last_trade_event_ts)"

echo

echo "== Состояния связей торговли (trade.db) =="
run_sql "$TRADE_DB" "WITH statuses(s) AS (VALUES('OPENING'),('OPEN'),('CLOSING'),('CLOSED'),('FAILED'),('PENDING_CLOSE')) SELECT s AS status, COALESCE(c.cnt,0) FROM statuses s LEFT JOIN (SELECT status, COUNT(*) cnt FROM trade_links GROUP BY status) c ON c.status=s;"

echo

echo "== Состояния позиций (trade.db) =="
run_sql "$TRADE_DB" "WITH states(s) AS (VALUES('OPENING'),('OPEN'),('CLOSING'),('CLOSED'),('FAILED')) SELECT s AS state, COALESCE(c.cnt,0) FROM states s LEFT JOIN (SELECT state, COUNT(*) cnt FROM positions GROUP BY state) c ON c.state=s;"

echo

echo "== Последние связи торговли (trade.db) =="
if [ "$trade_links_count" -gt 0 ]; then
  run_sql "$TRADE_DB" "SELECT player, signature, status, position_id, datetime(updated_at_ms/1000,'unixepoch') FROM trade_links ORDER BY updated_at_ms DESC LIMIT 10;"
else
  echo "Нет данных."
fi

echo

echo "== Ошибки исполнителя (trade.db) =="
if [ "$trade_links_count" -gt 0 ]; then
  run_sql "$TRADE_DB" "SELECT player, signature, status, last_error, entry_event_id, datetime(updated_at_ms/1000,'unixepoch') FROM trade_links WHERE status='FAILED' ORDER BY updated_at_ms DESC LIMIT 20;"
else
  echo "Нет данных."
fi

echo

echo "== Последние события по сделкам (trade.db) =="
if [ "$trade_events_count" -gt 0 ]; then
  run_sql "$TRADE_DB" "SELECT e.ts_ms, p.side, p.state, e.event_type FROM trade_events e JOIN positions p ON p.id=e.position_id ORDER BY e.ts_ms DESC LIMIT 20;"
else
  echo "Нет данных."
fi

echo

echo "== Баланс симуляции (trade.db) =="
if [ "$sim_wallet_count" -gt 0 ]; then
  run_sql "$TRADE_DB" "SELECT payer, usdc_balance, sol_lamports, datetime(updated_at_ms/1000,'unixepoch') FROM sim_wallet;"
else
  echo "Нет данных."
fi
