#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import datetime as dt
import html
import json
import os
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

TRADE_DB = os.environ.get("TRADE_DB", "trade.db")
TRACKER_DB = os.environ.get("TRACKER_DB", "tracker.db")
OUTPUT_HTML = os.environ.get("TRADE_REPORT_HTML", "trade_flow_report.html")
USDC_PER_TRADE = float(os.environ.get("USDC_PER_TRADE", "1"))
LAMPORTS_PER_SOL = 1_000_000_000


def _esc(v: Any) -> str:
    return "" if v is None else html.escape(str(v), quote=True)


def _ts_utc(ts_ms: Optional[int]) -> str:
    if ts_ms is None:
        return "нет"
    try:
        return dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc).isoformat()
    except Exception:
        return str(ts_ms)


def _fmt_float(v: Optional[float], digits: int = 12) -> str:
    if v is None:
        return ""
    return f"{v:.{digits}g}"


def _safe_json_loads(raw: Optional[str]) -> Any:
    if raw is None:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {"_raw": raw}


def _truncate(s: str, max_len: int) -> str:
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def _summarize_details(details: Any, max_log_lines: int, max_str: int) -> Tuple[str, List[str]]:
    if details is None:
        return "", []
    if not isinstance(details, dict):
        return _truncate(str(details), max_str), []

    parts: List[str] = []
    logs_lines: List[str] = []

    for key in ["error", "err", "reason", "last_error"]:
        if key in details and details[key]:
            parts.append(f"{key}={_truncate(str(details[key]), max_str)}")

    if "units_consumed" in details:
        parts.append(f"units={details['units_consumed']}")

    if "signature" in details:
        parts.append(f"sig={details['signature']}")

    if "profit_pct" in details:
        parts.append(f"profit_pct={details['profit_pct']}")

    if "entry_price" in details:
        parts.append(f"entry_price={details['entry_price']}")
    if "exit_price" in details:
        parts.append(f"exit_price={details['exit_price']}")

    if "logs" in details and isinstance(details["logs"], list):
        logs = details["logs"]
        parts.append(f"logs={len(logs)}")
        for line in logs[:max_log_lines]:
            logs_lines.append(str(line))

    if not parts:
        for k, v in list(details.items())[:8]:
            parts.append(f"{k}={_truncate(str(v), max_str)}")

    return "; ".join(parts), logs_lines


@dataclass
class PositionRow:
    id: int
    payer: str
    pool: str
    side: str
    state: str
    mode: str
    slippage_bps: int
    created_at_ms: int
    updated_at_ms: int
    reserved_usdc: int
    reserved_sol_lamports: int
    sol_position_lamports: int
    usdc_position: int
    open_sig: Optional[str]
    close_sig: Optional[str]
    open_last_valid_block_height: Optional[int]
    close_last_valid_block_height: Optional[int]
    last_error: Optional[str]
    entry_price: Optional[float]
    exit_price: Optional[float]
    profit_pct: Optional[float]
    link_player: Optional[str]
    link_signature: Optional[str]
    link_status: Optional[str]
    link_entry_event_id: Optional[int]
    link_exit_event_id: Optional[int]
    link_error: Optional[str]


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _load_positions(conn: sqlite3.Connection) -> List[PositionRow]:
    sql = """
    SELECT
        p.id, p.payer, p.pool, p.side, p.state, p.mode, p.slippage_bps,
        p.created_at_ms, p.updated_at_ms,
        p.reserved_usdc, p.reserved_sol_lamports,
        p.sol_position_lamports, p.usdc_position,
        p.open_sig, p.close_sig,
        p.open_last_valid_block_height, p.close_last_valid_block_height,
        p.last_error,
        p.entry_price, p.exit_price, p.profit_pct,
        l.player, l.signature, l.status, l.entry_event_id, l.exit_event_id, l.last_error
    FROM positions p
    LEFT JOIN trade_links l ON l.position_id = p.id
    ORDER BY p.updated_at_ms DESC, p.id DESC
    """
    rows = conn.execute(sql).fetchall()
    out: List[PositionRow] = []
    for r in rows:
        out.append(
            PositionRow(
                id=r[0],
                payer=r[1],
                pool=r[2],
                side=r[3],
                state=r[4],
                mode=r[5],
                slippage_bps=r[6],
                created_at_ms=r[7],
                updated_at_ms=r[8],
                reserved_usdc=r[9],
                reserved_sol_lamports=r[10],
                sol_position_lamports=r[11],
                usdc_position=r[12],
                open_sig=r[13],
                close_sig=r[14],
                open_last_valid_block_height=r[15],
                close_last_valid_block_height=r[16],
                last_error=r[17],
                entry_price=r[18],
                exit_price=r[19],
                profit_pct=r[20],
                link_player=r[21],
                link_signature=r[22],
                link_status=r[23],
                link_entry_event_id=r[24],
                link_exit_event_id=r[25],
                link_error=r[26],
            )
        )
    return out


def _fetch_events(conn: sqlite3.Connection, position_id: int) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT id, ts_ms, event_type, details_json
        FROM trade_events
        WHERE position_id = ?
        ORDER BY ts_ms ASC, id ASC
        """,
        (position_id,),
    ).fetchall()


def _fetch_tracker_event(conn: sqlite3.Connection, event_id: int) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT id, action, player, signature, whirlpool, price, target_price, ts_ms
        FROM events
        WHERE id = ?
        """,
        (event_id,),
    ).fetchone()


def _count(conn: sqlite3.Connection, sql: str) -> int:
    return int(conn.execute(sql).fetchone()[0])


def _sum_float(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> float:
    row = conn.execute(sql, params).fetchone()
    return float(row[0] or 0.0)


def _build_summary(conn: sqlite3.Connection) -> Dict[str, Any]:
    summary: Dict[str, Any] = {}
    summary["positions"] = _count(conn, "SELECT COUNT(*) FROM positions")
    summary["trade_links"] = _count(conn, "SELECT COUNT(*) FROM trade_links")
    summary["trade_events"] = _count(conn, "SELECT COUNT(*) FROM trade_events")
    summary["sim_wallet"] = _count(conn, "SELECT COUNT(*) FROM sim_wallet")
    summary["last_trade_ts"] = int(
        conn.execute("SELECT COALESCE(MAX(ts_ms),0) FROM trade_events").fetchone()[0]
    )

    summary["open_committed"] = _count(
        conn, "SELECT COUNT(*) FROM trade_events WHERE event_type='OPEN_COMMITTED'"
    )
    summary["close_committed"] = _count(
        conn, "SELECT COUNT(*) FROM trade_events WHERE event_type='CLOSE_COMMITTED'"
    )
    summary["profit_events"] = _count(
        conn, "SELECT COUNT(*) FROM trade_events WHERE event_type='PROFIT'"
    )

    summary["closed_positions"] = _count(
        conn, "SELECT COUNT(*) FROM positions WHERE state='CLOSED'"
    )
    summary["open_positions"] = _count(
        conn, "SELECT COUNT(*) FROM positions WHERE state='OPEN'"
    )

    summary["profit_positions"] = _count(
        conn, "SELECT COUNT(*) FROM positions WHERE profit_pct IS NOT NULL"
    )

    summary["sum_profit_usdc"] = _sum_float(
        conn,
        "SELECT COALESCE(SUM(profit_pct * ? / 100.0),0) FROM positions WHERE state='CLOSED' AND profit_pct IS NOT NULL",
        (USDC_PER_TRADE,),
    )

    summary["sum_profit_sol"] = _sum_float(
        conn,
        "SELECT COALESCE(SUM((profit_pct * ? / 100.0) * exit_price),0) FROM positions WHERE state='CLOSED' AND profit_pct IS NOT NULL AND exit_price IS NOT NULL",
        (USDC_PER_TRADE,),
    )

    return summary


def _states(conn: sqlite3.Connection, table: str, col: str) -> List[Tuple[str, int]]:
    rows = conn.execute(
        f"SELECT {col}, COUNT(*) FROM {table} GROUP BY {col} ORDER BY {col}"
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


def _sim_wallet_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not _table_exists(conn, "sim_wallet"):
        return out
    row = conn.execute(
        "SELECT payer, usdc_balance, sol_lamports, updated_at_ms FROM sim_wallet LIMIT 1"
    ).fetchone()
    if not row:
        return out

    reserved_sol = int(
        conn.execute(
            "SELECT COALESCE(SUM(reserved_sol_lamports),0) FROM positions WHERE state IN ('OPENING','OPEN','CLOSING')"
        ).fetchone()[0]
    )

    out["payer"] = row[0]
    out["usdc_balance"] = int(row[1])
    out["sol_lamports"] = int(row[2])
    out["updated_at_ms"] = int(row[3])
    out["reserved_sol_lamports"] = reserved_sol
    out["free_sol_lamports"] = int(row[2]) - reserved_sol
    return out


def _render_html(
    summary: Dict[str, Any],
    positions: List[PositionRow],
    trade_conn: sqlite3.Connection,
    tracker_conn: Optional[sqlite3.Connection],
    sim_wallet: Dict[str, Any],
) -> str:
    now_utc = dt.datetime.now(tz=dt.timezone.utc).isoformat()

    css = """
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; margin: 24px; }
    h1 { margin-top: 0; }
    .muted { color: #666; }
    table { border-collapse: collapse; width: 100%; margin: 12px 0 24px; }
    th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; }
    th { background: #f6f6f6; text-align: left; }
    .card { border: 1px solid #ddd; border-radius: 8px; padding: 12px; margin: 12px 0; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 6px 16px; }
    .k { color: #444; font-weight: 600; }
    .v { word-break: break-all; }
    .pill { display: inline-block; padding: 2px 8px; border: 1px solid #ccc; border-radius: 999px; font-size: 12px; margin-right: 6px; }
    details { margin: 8px 0; }
    pre { background: #f6f6f6; padding: 10px; overflow-x: auto; }
    .events li { margin: 6px 0; }
    """

    summary_rows = "".join(
        f"<tr><td>{_esc(k)}</td><td>{_esc(v)}</td></tr>"
        for k, v in [
            ("positions", summary.get("positions")),
            ("trade_links", summary.get("trade_links")),
            ("trade_events", summary.get("trade_events")),
            ("sim_wallet", summary.get("sim_wallet")),
            ("open_committed", summary.get("open_committed")),
            ("close_committed", summary.get("close_committed")),
            ("profit_events", summary.get("profit_events")),
            ("closed_positions", summary.get("closed_positions")),
            ("open_positions", summary.get("open_positions")),
            ("profit_positions", summary.get("profit_positions")),
        ]
    )

    states_positions = "".join(
        f"<tr><td>{_esc(s)}</td><td>{c}</td></tr>" for s, c in _states(trade_conn, "positions", "state")
    )
    states_links = "".join(
        f"<tr><td>{_esc(s)}</td><td>{c}</td></tr>" for s, c in _states(trade_conn, "trade_links", "status")
    )

    sim_wallet_block = "<div class='muted'>Нет данных</div>"
    if sim_wallet:
        sol_total = sim_wallet["sol_lamports"] / LAMPORTS_PER_SOL
        sol_reserved = sim_wallet["reserved_sol_lamports"] / LAMPORTS_PER_SOL
        sol_free = sim_wallet["free_sol_lamports"] / LAMPORTS_PER_SOL
        sim_wallet_block = f"""
        <div class="grid">
          <div><span class="k">payer:</span> <span class="v">{_esc(sim_wallet['payer'])}</span></div>
          <div><span class="k">updated:</span> <span class="v">{_esc(_ts_utc(sim_wallet['updated_at_ms']))}</span></div>
          <div><span class="k">usdc_balance:</span> <span class="v">{sim_wallet['usdc_balance']}</span></div>
          <div><span class="k">sol_lamports:</span> <span class="v">{sim_wallet['sol_lamports']}</span></div>
          <div><span class="k">sol_total:</span> <span class="v">{sol_total:.9f}</span></div>
          <div><span class="k">sol_reserved:</span> <span class="v">{sol_reserved:.9f}</span></div>
          <div><span class="k">sol_free:</span> <span class="v">{sol_free:.9f}</span></div>
        </div>
        """

    profit_block = f"""
    <div class="grid">
      <div><span class="k">USDC per trade:</span> <span class="v">{USDC_PER_TRADE}</span></div>
      <div><span class="k">Profit USDC (sum):</span> <span class="v">{summary.get('sum_profit_usdc', 0):.12g}</span></div>
      <div><span class="k">Profit SOL (sum):</span> <span class="v">{summary.get('sum_profit_sol', 0):.12g}</span></div>
      <div><span class="k">Last trade event:</span> <span class="v">{_esc(_ts_utc(summary.get('last_trade_ts')))}</span></div>
    </div>
    """

    position_blocks: List[str] = []
    for pos in positions:
        link_info = ""
        if pos.link_player or pos.link_signature:
            link_info = f"""
            <div class="grid">
              <div><span class="k">link player:</span> <span class="v">{_esc(pos.link_player)}</span></div>
              <div><span class="k">link signature:</span> <span class="v">{_esc(pos.link_signature)}</span></div>
              <div><span class="k">link status:</span> <span class="v">{_esc(pos.link_status)}</span></div>
              <div><span class="k">entry_event_id:</span> <span class="v">{_esc(pos.link_entry_event_id)}</span></div>
              <div><span class="k">exit_event_id:</span> <span class="v">{_esc(pos.link_exit_event_id)}</span></div>
            </div>
            """

        entry_block = ""
        if tracker_conn and pos.link_entry_event_id:
            ev = _fetch_tracker_event(tracker_conn, pos.link_entry_event_id)
            if ev is not None:
                entry_block = f"""
                <div class="card">
                  <div class="k">ENTRY_SIGNAL</div>
                  <div class="grid">
                    <div><span class="k">id:</span> <span class="v">{ev['id']}</span></div>
                    <div><span class="k">ts:</span> <span class="v">{_esc(_ts_utc(ev['ts_ms']))}</span></div>
                    <div><span class="k">player:</span> <span class="v">{_esc(ev['player'])}</span></div>
                    <div><span class="k">sig:</span> <span class="v">{_esc(ev['signature'])}</span></div>
                    <div><span class="k">whirlpool:</span> <span class="v">{_esc(ev['whirlpool'])}</span></div>
                    <div><span class="k">price:</span> <span class="v">{_fmt_float(ev['price'])}</span></div>
                    <div><span class="k">target_price:</span> <span class="v">{_fmt_float(ev['target_price'])}</span></div>
                  </div>
                </div>
                """

        exit_block = ""
        if tracker_conn and pos.link_exit_event_id:
            ev = _fetch_tracker_event(tracker_conn, pos.link_exit_event_id)
            if ev is not None:
                exit_block = f"""
                <div class="card">
                  <div class="k">TARGET_HIT</div>
                  <div class="grid">
                    <div><span class="k">id:</span> <span class="v">{ev['id']}</span></div>
                    <div><span class="k">ts:</span> <span class="v">{_esc(_ts_utc(ev['ts_ms']))}</span></div>
                    <div><span class="k">player:</span> <span class="v">{_esc(ev['player'])}</span></div>
                    <div><span class="k">sig:</span> <span class="v">{_esc(ev['signature'])}</span></div>
                    <div><span class="k">whirlpool:</span> <span class="v">{_esc(ev['whirlpool'])}</span></div>
                    <div><span class="k">price:</span> <span class="v">{_fmt_float(ev['price'])}</span></div>
                    <div><span class="k">target_price:</span> <span class="v">{_fmt_float(ev['target_price'])}</span></div>
                  </div>
                </div>
                """

        events = _fetch_events(trade_conn, pos.id)
        ev_lines: List[str] = []
        for ev in events:
            details = _safe_json_loads(ev["details_json"])
            summary, logs_lines = _summarize_details(details, 6, 240)
            details_pre = _esc(json.dumps(details, ensure_ascii=False, indent=2))
            logs_pre = "\n".join(_esc(_truncate(l, 400)) for l in logs_lines)
            logs_block = ""
            if logs_pre:
                logs_block = f"<details><summary>логи</summary><pre>{logs_pre}</pre></details>"
            ev_lines.append(
                f"<li><span class='pill'>{_esc(ev['event_type'])}</span> {_esc(_ts_utc(ev['ts_ms']))} { _esc(summary)}"
                f"<details><summary>детали</summary><pre>{details_pre}</pre></details>{logs_block}</li>"
            )

        errors_block = ""
        if pos.last_error:
            errors_block += f"<div class='card'><div class='k'>position_error</div><div class='v'>{_esc(pos.last_error)}</div></div>"
        if pos.link_error:
            errors_block += f"<div class='card'><div class='k'>link_error</div><div class='v'>{_esc(pos.link_error)}</div></div>"

        position_blocks.append(
            f"""
            <details class="card">
              <summary>
                <span class="pill">#{pos.id}</span>
                <span class="pill">{_esc(pos.side)}</span>
                <span class="pill">{_esc(pos.state)}</span>
                <span class="pill">profit_pct={_fmt_float(pos.profit_pct, 6)}</span>
                <span class="pill">player={_esc(pos.link_player)}</span>
              </summary>
              <div class="grid">
                <div><span class="k">payer:</span> <span class="v">{_esc(pos.payer)}</span></div>
                <div><span class="k">pool:</span> <span class="v">{_esc(pos.pool)}</span></div>
                <div><span class="k">mode:</span> <span class="v">{_esc(pos.mode)}</span></div>
                <div><span class="k">slippage_bps:</span> <span class="v">{pos.slippage_bps}</span></div>
                <div><span class="k">created:</span> <span class="v">{_esc(_ts_utc(pos.created_at_ms))}</span></div>
                <div><span class="k">updated:</span> <span class="v">{_esc(_ts_utc(pos.updated_at_ms))}</span></div>
                <div><span class="k">reserved_usdc:</span> <span class="v">{pos.reserved_usdc}</span></div>
                <div><span class="k">reserved_sol_lamports:</span> <span class="v">{pos.reserved_sol_lamports}</span></div>
                <div><span class="k">sol_position_lamports:</span> <span class="v">{pos.sol_position_lamports}</span></div>
                <div><span class="k">usdc_position:</span> <span class="v">{pos.usdc_position}</span></div>
                <div><span class="k">entry_price:</span> <span class="v">{_fmt_float(pos.entry_price)}</span></div>
                <div><span class="k">exit_price:</span> <span class="v">{_fmt_float(pos.exit_price)}</span></div>
                <div><span class="k">open_sig:</span> <span class="v">{_esc(pos.open_sig)}</span></div>
                <div><span class="k">close_sig:</span> <span class="v">{_esc(pos.close_sig)}</span></div>
                <div><span class="k">open_lvb:</span> <span class="v">{_esc(pos.open_last_valid_block_height)}</span></div>
                <div><span class="k">close_lvb:</span> <span class="v">{_esc(pos.close_last_valid_block_height)}</span></div>
              </div>
              {errors_block}
              {link_info}
              {entry_block}
              {exit_block}
              <div class="card">
                <div class="k">События торговли</div>
                <ul class="events">
                  {''.join(ev_lines) if ev_lines else '<li>нет событий</li>'}
                </ul>
              </div>
            </details>
            """
        )

    html_doc = f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Trade Flow Report</title>
  <style>{css}</style>
</head>
<body>
  <h1>Отчёт по торговой части</h1>
  <div class="muted">Сгенерировано: {_esc(now_utc)} (UTC)</div>
  <div class="muted">БД: trade={_esc(TRADE_DB)} tracker={_esc(TRACKER_DB)}</div>

  <h2>Сводка</h2>
  <table>
    <tbody>
      {summary_rows}
    </tbody>
  </table>

  <h2>Профит</h2>
  {profit_block}

  <h2>Состояния позиций</h2>
  <table>
    <thead><tr><th>state</th><th>count</th></tr></thead>
    <tbody>{states_positions}</tbody>
  </table>

  <h2>Статусы связей</h2>
  <table>
    <thead><tr><th>status</th><th>count</th></tr></thead>
    <tbody>{states_links}</tbody>
  </table>

  <h2>Баланс симуляции</h2>
  {sim_wallet_block}

  <h2>Позиции (детально)</h2>
  {''.join(position_blocks) if position_blocks else '<div class="muted">Нет позиций</div>'}
</body>
</html>
"""
    return html_doc


def main() -> None:
    if not os.path.exists(TRADE_DB):
        raise SystemExit(f"Не найден файл: {TRADE_DB}")

    trade_conn = sqlite3.connect(TRADE_DB)
    trade_conn.row_factory = sqlite3.Row
    tracker_conn: Optional[sqlite3.Connection] = None
    try:
        if not _table_exists(trade_conn, "positions"):
            raise SystemExit("В trade.db нет таблицы positions")
        if not _table_exists(trade_conn, "trade_events"):
            raise SystemExit("В trade.db нет таблицы trade_events")

        if os.path.exists(TRACKER_DB):
            tracker_conn = sqlite3.connect(TRACKER_DB)

        summary = _build_summary(trade_conn)
        positions = _load_positions(trade_conn)
        sim_wallet = _sim_wallet_stats(trade_conn)

        html_doc = _render_html(summary, positions, trade_conn, tracker_conn, sim_wallet)
        with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
            f.write(html_doc)

        print(f"OK: wrote {OUTPUT_HTML} (positions={len(positions)})")
    finally:
        trade_conn.close()
        if tracker_conn is not None:
            tracker_conn.close()


if __name__ == "__main__":
    main()
