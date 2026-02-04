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
USDC_PER_TRADE="${USDC_PER_TRADE:-1}"
LAMPORTS_PER_SOL=1000000000

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

has_column() {
  local db="$1"
  local table="$2"
  local col="$3"
  local val
  val=$(run_sql "$db" "SELECT 1 FROM pragma_table_info('$table') WHERE name='$col' LIMIT 1;")
  [ -n "$val" ]
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

echo "== Сделки и прибыль (trade.db) =="
if has_column "$TRADE_DB" "positions" "profit_pct"; then
  closed_positions=$(run_sql "$TRADE_DB" "SELECT COUNT(*) FROM positions WHERE state='CLOSED';")
  open_positions=$(run_sql "$TRADE_DB" "SELECT COUNT(*) FROM positions WHERE state='OPEN';")
  open_committed=$(run_sql "$TRADE_DB" "SELECT COUNT(*) FROM trade_events WHERE event_type='OPEN_COMMITTED';")
  close_committed=$(run_sql "$TRADE_DB" "SELECT COUNT(*) FROM trade_events WHERE event_type='CLOSE_COMMITTED';")
  sum_profit_usdc=$(run_sql "$TRADE_DB" "SELECT COALESCE(SUM(profit_pct * $USDC_PER_TRADE / 100.0),0) FROM positions WHERE state='CLOSED' AND profit_pct IS NOT NULL;")
  sum_profit_sol=$(run_sql "$TRADE_DB" "SELECT COALESCE(SUM((profit_pct * $USDC_PER_TRADE / 100.0) * exit_price),0) FROM positions WHERE state='CLOSED' AND profit_pct IS NOT NULL AND exit_price IS NOT NULL;")
  spent_usdc=$(run_sql "$TRADE_DB" "SELECT COALESCE(COUNT(*),0) * $USDC_PER_TRADE FROM positions WHERE state='CLOSED';")
  echo "Сделок закрыто (positions): $closed_positions"
  echo "Сделок открыто (positions): $open_positions"
  echo "OPEN_COMMITTED (trade_events): $open_committed"
  echo "CLOSE_COMMITTED (trade_events): $close_committed"
  echo "Потрачено USDC (закрытые): $spent_usdc"
  echo "Заработано USDC (сумма profit_pct): $sum_profit_usdc"
  echo "Заработано SOL (по exit_price): $sum_profit_sol"
  echo "USDC на сделку (настройка): $USDC_PER_TRADE"
else
  echo "Колонка profit_pct отсутствует."
fi

echo

echo "== Состояния связей торговли (trade.db) =="
run_sql "$TRADE_DB" "WITH statuses(s) AS (VALUES('OPENING'),('OPEN'),('CLOSING'),('CLOSED'),('FAILED'),('PENDING_CLOSE')) SELECT s AS status, COALESCE(c.cnt,0) FROM statuses s LEFT JOIN (SELECT status, COUNT(*) cnt FROM trade_links GROUP BY status) c ON c.status=s;"

echo

echo "== Состояния позиций (trade.db) =="
run_sql "$TRADE_DB" "WITH states(s) AS (VALUES('OPENING'),('OPEN'),('CLOSING'),('CLOSED'),('FAILED')) SELECT s AS state, COALESCE(c.cnt,0) FROM states s LEFT JOIN (SELECT state, COUNT(*) cnt FROM positions GROUP BY state) c ON c.state=s;"

echo

echo "== Последние связи торговли (trade.db) =="
if [ "$trade_links_count" -gt 0 ]; then
  if has_column "$TRADE_DB" "positions" "profit_pct"; then
    run_sql "$TRADE_DB" "SELECT t.player, t.signature, t.status, t.position_id, p.side, p.state, p.entry_price, p.exit_price, p.profit_pct, datetime(t.updated_at_ms/1000,'unixepoch') FROM trade_links t LEFT JOIN positions p ON p.id = t.position_id ORDER BY t.updated_at_ms DESC LIMIT 10;"
  else
    run_sql "$TRADE_DB" "SELECT player, signature, status, position_id, datetime(updated_at_ms/1000,'unixepoch') FROM trade_links ORDER BY updated_at_ms DESC LIMIT 10;"
  fi
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

echo "== Битые транзакции по пулам (trade.db + tracker.db) =="
has_events=$(run_sql "$TRACKER_DB" "SELECT name FROM sqlite_master WHERE type='table' AND name='events';")
if [ "$trade_links_count" -gt 0 ] && [ "$has_events" = "events" ]; then
  run_sql "$TRADE_DB" "ATTACH '$TRACKER_DB' AS t; SELECT COALESCE(e.whirlpool,'(unknown)') AS pool, COUNT(*) AS cnt FROM trade_links tl LEFT JOIN t.events e ON e.id=tl.entry_event_id WHERE tl.status='FAILED' GROUP BY pool ORDER BY cnt DESC;"
  echo "Причины (top 30):"
  run_sql "$TRADE_DB" "ATTACH '$TRACKER_DB' AS t; SELECT COALESCE(e.whirlpool,'(unknown)') AS pool, COALESCE(tl.last_error,'') AS err, COUNT(*) AS cnt FROM trade_links tl LEFT JOIN t.events e ON e.id=tl.entry_event_id WHERE tl.status='FAILED' GROUP BY pool, err ORDER BY cnt DESC LIMIT 30;"
else
  echo "Нет данных (либо нет FAILED, либо нет таблицы events в tracker.db)."
fi

echo

echo "== Последние события по сделкам (trade.db) =="
if [ "$trade_events_count" -gt 0 ]; then
  run_sql "$TRADE_DB" "SELECT e.ts_ms, p.side, p.state, e.event_type FROM trade_events e JOIN positions p ON p.id=e.position_id ORDER BY e.ts_ms DESC LIMIT 20;"
else
  echo "Нет данных."
fi

echo

echo "== Профит по позициям (trade.db) =="
if has_column "$TRADE_DB" "positions" "profit_pct"; then
  profit_count=$(run_sql "$TRADE_DB" "SELECT COUNT(*) FROM positions WHERE profit_pct IS NOT NULL;")
  closed_profit_count=$(run_sql "$TRADE_DB" "SELECT COUNT(*) FROM positions WHERE state='CLOSED' AND profit_pct IS NOT NULL;")
  avg_profit=$(run_sql "$TRADE_DB" "SELECT COALESCE(AVG(profit_pct),0) FROM positions WHERE state='CLOSED' AND profit_pct IS NOT NULL;")
  echo "Позиции с расчётом profit_pct: $profit_count"
  echo "Закрытые с profit_pct: $closed_profit_count"
  echo "Средний profit_pct (CLOSED): $avg_profit"
  echo "Последние 10 позиций с profit_pct:"
  run_sql "$TRADE_DB" "SELECT id, side, state, entry_price, exit_price, profit_pct, datetime(updated_at_ms/1000,'unixepoch') FROM positions WHERE profit_pct IS NOT NULL ORDER BY updated_at_ms DESC LIMIT 10;"
else
  echo "Колонка profit_pct отсутствует."
fi

echo

echo "== Последние события PROFIT (trade.db) =="
if [ "$trade_events_count" -gt 0 ]; then
  run_sql "$TRADE_DB" "SELECT ts_ms, position_id, event_type, details_json FROM trade_events WHERE event_type='PROFIT' ORDER BY ts_ms DESC LIMIT 10;"
else
  echo "Нет данных."
fi

echo

echo "== Баланс симуляции (trade.db) =="
if [ "$sim_wallet_count" -gt 0 ]; then
  run_sql "$TRADE_DB" "SELECT payer, usdc_balance, sol_lamports, datetime(updated_at_ms/1000,'unixepoch') FROM sim_wallet;"
  reserved_sol=$(run_sql "$TRADE_DB" "SELECT COALESCE(SUM(reserved_sol_lamports),0) FROM positions WHERE state IN ('OPENING','OPEN','CLOSING');")
  sim_sol=$(run_sql "$TRADE_DB" "SELECT COALESCE(SUM(sol_lamports),0) FROM sim_wallet;")
  free_sol=$((sim_sol - reserved_sol))
  free_sol_ui=$(run_sql "$TRADE_DB" "SELECT ($free_sol / 1000000000.0);")
  reserved_sol_ui=$(run_sql "$TRADE_DB" "SELECT ($reserved_sol / 1000000000.0);")
  sim_sol_ui=$(run_sql "$TRADE_DB" "SELECT ($sim_sol / 1000000000.0);")
  echo
  echo "== Свободный SOL (симуляция) =="
  echo "SOL всего: $sim_sol_ui"
  echo "SOL в резервах: $reserved_sol_ui"
  echo "SOL свободно: $free_sol_ui"
else
  echo "Нет данных."
fi
