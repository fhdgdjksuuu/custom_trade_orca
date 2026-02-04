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

echo "== Сигналы входа/выхода (tracker.db) =="
sqlite3 -readonly "$TRACKER_DB" "SELECT action, COUNT(*) FROM events WHERE action IN ('ENTRY_SIGNAL','TARGET_HIT') GROUP BY action;"
echo

echo "== Входы без выхода (tracker.db) =="
sqlite3 -readonly "$TRACKER_DB" "SELECT COUNT(1) FROM events e WHERE e.action='ENTRY_SIGNAL' AND NOT EXISTS (SELECT 1 FROM events t WHERE t.player=e.player AND t.signature=e.signature AND t.action='TARGET_HIT');"
echo

echo "== Входы по пулам (tracker.db) =="
sqlite3 -readonly "$TRACKER_DB" "SELECT whirlpool, COUNT(*) FROM events WHERE action='ENTRY_SIGNAL' GROUP BY whirlpool;"
echo

echo "== Статусы игроков по входам (players.db) =="
sqlite3 -readonly "$TRACKER_DB" "ATTACH '$PLAYERS_DB' AS p; SELECT e.player, p.players.blacklist, COUNT(*) FROM events e JOIN p.players ON p.players.address=e.player WHERE e.action='ENTRY_SIGNAL' GROUP BY e.player, p.players.blacklist;"
echo

echo "== Состояния связей торговли (trade.db) =="
sqlite3 -readonly "$TRADE_DB" "SELECT status, COUNT(*) FROM trade_links GROUP BY status;"
echo

echo "== Ошибки исполнителя (trade.db) =="
sqlite3 -readonly "$TRADE_DB" "SELECT player, signature, status, last_error, entry_event_id, datetime(updated_at_ms/1000,'unixepoch') FROM trade_links WHERE status='FAILED' ORDER BY updated_at_ms DESC LIMIT 20;"
echo

echo "== Последние события по сделкам (trade.db) =="
sqlite3 -readonly "$TRADE_DB" "SELECT e.ts_ms, p.side, p.state, e.event_type FROM trade_events e JOIN positions p ON p.id=e.position_id ORDER BY e.ts_ms DESC LIMIT 20;"
echo

echo "== Баланс симуляции (trade.db) =="
sqlite3 -readonly "$TRADE_DB" "SELECT payer, usdc_balance, sol_lamports, datetime(updated_at_ms/1000,'unixepoch') FROM sim_wallet;"
