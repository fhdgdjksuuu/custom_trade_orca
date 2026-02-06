#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Generate an HTML report of trades from tracker.db.

Reads: SQLite database created by tracker.rs (table: events)

Trade model (based on tracker.rs logger):
- ENTRY_SIGNAL  => position entry (buy_price stored in column `price`, target in `target_price`)
- TARGET_HIT    => take-profit hit (exit price stored in column `price`)

A trade is considered profitable if:
  (player, signature) has both ENTRY_SIGNAL and TARGET_HIT.

Output:
- Single self-contained HTML report with:
  - Summary (counts per player)
  - Profitable trades (ENTRY_SIGNAL + TARGET_HIT)
  - Open trades (ENTRY_SIGNAL without TARGET_HIT)
  - Full timeline of events for each trade

Usage:
  python profit_report_long_short.py --db tracker.db --out profit_report.html

Optional:
  python profit_report_long_short.py --db tracker.db --out profit_report.html --min-profit-pct 0.05
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _fmt_duration_ms(ms: int) -> str:
    """Human readable duration for report cards."""
    if ms < 0:
        ms = 0
    total_s = ms // 1000
    h = total_s // 3600
    m = (total_s % 3600) // 60
    s = total_s % 60
    if h > 0:
        return f"{h:d}h {m:02d}m {s:02d}s"
    if m > 0:
        return f"{m:d}m {s:02d}s"
    return f"{s:d}s"


@dataclass
class Event:
    id: int
    ts_ms: int
    player: Optional[str]
    signature: Optional[str]
    action: str
    whirlpool: Optional[str]
    other_mint: Optional[str]
    price: Optional[float]
    target_price: Optional[float]
    wsol_delta: Optional[str]
    token_delta: Optional[str]
    details_json: Optional[str]

    def ts_utc(self) -> str:
        try:
            return dt.datetime.fromtimestamp(self.ts_ms / 1000, tz=dt.timezone.utc).isoformat()
        except Exception:
            return str(self.ts_ms)

    def details_obj(self) -> Dict[str, Any]:
        if not self.details_json:
            return {}
        try:
            return json.loads(self.details_json)
        except Exception:
            return {"_raw": self.details_json}


@dataclass
class Trade:
    player: str
    signature: str
    whirlpool: Optional[str]
    other_mint: Optional[str]
    entry_ts_ms: int
    exit_ts_ms: int
    entry_price: float
    target_price: Optional[float]
    exit_price: float
    direction: str
    wsol_delta: Optional[str]
    token_delta: Optional[str]
    entry_details: Dict[str, Any]
    exit_details: Dict[str, Any]
    events: List[Event]

    def profit_pct(self) -> float:
        if self.entry_price == 0:
            return 0.0
        delta = self.exit_price - self.entry_price
        if delta >= 0:
            return delta / self.entry_price * 100.0
        return (-delta) / self.entry_price * 100.0

    def pct_to_edge(self) -> Optional[float]:
        """Процент хода до границы диапазона на момент входа (если был записан в details_json)."""
        v = self.entry_details.get("pct_to_edge")
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    def entry_ts_utc(self) -> str:
        return dt.datetime.fromtimestamp(self.entry_ts_ms / 1000, tz=dt.timezone.utc).isoformat()

    def exit_ts_utc(self) -> str:
        return dt.datetime.fromtimestamp(self.exit_ts_ms / 1000, tz=dt.timezone.utc).isoformat()

    def time_to_profit_ms(self) -> int:
        return int(self.exit_ts_ms) - int(self.entry_ts_ms)

    def time_to_profit_human(self) -> str:
        return _fmt_duration_ms(self.time_to_profit_ms())


@dataclass
class OpenTrade:
    """ENTRY_SIGNAL exists but TARGET_HIT hasn't happened (yet)."""

    player: str
    signature: str
    whirlpool: Optional[str]
    other_mint: Optional[str]
    entry_ts_ms: int
    entry_price: float
    target_price: Optional[float]
    direction: str
    wsol_delta: Optional[str]
    token_delta: Optional[str]
    entry_details: Dict[str, Any]
    last_ts_ms: int
    last_action: Optional[str]
    last_price: Optional[float]
    events: List[Event]

    def entry_ts_utc(self) -> str:
        return dt.datetime.fromtimestamp(self.entry_ts_ms / 1000, tz=dt.timezone.utc).isoformat()

    def last_ts_utc(self) -> str:
        return dt.datetime.fromtimestamp(self.last_ts_ms / 1000, tz=dt.timezone.utc).isoformat()

    def age_ms(self, reference_ts_ms: int) -> int:
        return int(reference_ts_ms) - int(self.entry_ts_ms)

    def age_human(self, reference_ts_ms: int) -> str:
        return _fmt_duration_ms(self.age_ms(reference_ts_ms))

    def pct_to_edge(self) -> Optional[float]:
        v = self.entry_details.get("pct_to_edge")
        try:
            return float(v) if v is not None else None
        except Exception:
            return None


def _esc(x: Any) -> str:
    if x is None:
        return ""
    return html.escape(str(x), quote=True)


def _json_pretty(x: Any) -> str:
    try:
        return json.dumps(x, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return str(x)


def _detect_direction(
    entry_price: float,
    target_price: Optional[float],
    entry_details: Dict[str, Any],
) -> str:
    raw = entry_details.get("direction")
    if isinstance(raw, str):
        val = raw.strip().lower()
        if val in {"long", "short"}:
            return val

    if not target_price or entry_price == 0:
        return "unknown"
    if target_price < entry_price:
        return "long"
    if target_price > entry_price:
        return "short"
    return "unknown"


def fetch_profitable_trades(conn: sqlite3.Connection) -> List[Trade]:
    """Load profitable trades (ENTRY_SIGNAL + TARGET_HIT) and attach full event timelines."""

    conn.row_factory = sqlite3.Row

    # Pick the earliest TARGET_HIT per (player, signature)
    profitable_sql = r"""
    SELECT
        e.player             AS player,
        e.signature          AS signature,
        e.whirlpool          AS whirlpool,
        e.other_mint         AS other_mint,
        e.ts_ms              AS entry_ts_ms,
        e.price              AS entry_price,
        e.target_price       AS target_price,
        e.wsol_delta         AS wsol_delta,
        e.token_delta        AS token_delta,
        e.details_json       AS entry_details_json,
        t.ts_ms              AS exit_ts_ms,
        t.price              AS exit_price,
        t.details_json       AS exit_details_json
    FROM events e
    JOIN events t
      ON t.id = (
        SELECT id
        FROM events
        WHERE player = e.player
          AND signature = e.signature
          AND action = 'TARGET_HIT'
        ORDER BY ts_ms ASC
        LIMIT 1
      )
    WHERE e.action = 'ENTRY_SIGNAL'
      AND e.player IS NOT NULL
      AND e.signature IS NOT NULL
    ORDER BY e.ts_ms ASC;
    """

    rows = conn.execute(profitable_sql).fetchall()

    trades: List[Trade] = []

    for r in rows:
        player = r["player"]
        signature = r["signature"]

        # Full timeline for this (player, signature)
        ev_rows = conn.execute(
            """
            SELECT id, ts_ms, player, signature, action, whirlpool, other_mint,
                   price, target_price, wsol_delta, token_delta, details_json
            FROM events
            WHERE player = ?
              AND signature = ?
            ORDER BY ts_ms ASC, id ASC;
            """,
            (player, signature),
        ).fetchall()

        events: List[Event] = []
        for er in ev_rows:
            events.append(
                Event(
                    id=er["id"],
                    ts_ms=er["ts_ms"],
                    player=er["player"],
                    signature=er["signature"],
                    action=er["action"],
                    whirlpool=er["whirlpool"],
                    other_mint=er["other_mint"],
                    price=er["price"],
                    target_price=er["target_price"],
                    wsol_delta=er["wsol_delta"],
                    token_delta=er["token_delta"],
                    details_json=er["details_json"],
                )
            )

        entry_details = {}
        try:
            entry_details = json.loads(r["entry_details_json"] or "{}")
        except Exception:
            entry_details = {"_raw": r["entry_details_json"]}

        exit_details = {}
        try:
            exit_details = json.loads(r["exit_details_json"] or "{}")
        except Exception:
            exit_details = {"_raw": r["exit_details_json"]}

        trades.append(
            Trade(
                player=player,
                signature=signature,
                whirlpool=r["whirlpool"],
                other_mint=r["other_mint"],
                entry_ts_ms=int(r["entry_ts_ms"]),
                exit_ts_ms=int(r["exit_ts_ms"]),
                entry_price=float(r["entry_price"] or 0.0),
                target_price=(float(r["target_price"]) if r["target_price"] is not None else None),
                exit_price=float(r["exit_price"] or 0.0),
                direction=_detect_direction(
                    entry_price=float(r["entry_price"] or 0.0),
                    target_price=(
                        float(r["target_price"]) if r["target_price"] is not None else None
                    ),
                    entry_details=entry_details,
                ),
                wsol_delta=r["wsol_delta"],
                token_delta=r["token_delta"],
                entry_details=entry_details,
                exit_details=exit_details,
                events=events,
            )
        )

    return trades


def fetch_open_trades(conn: sqlite3.Connection) -> List[OpenTrade]:
    """Load open trades (ENTRY_SIGNAL exists, but TARGET_HIT missing) and attach timelines."""

    conn.row_factory = sqlite3.Row

    open_sql = r"""
    SELECT
        e.player             AS player,
        e.signature          AS signature,
        e.whirlpool          AS whirlpool,
        e.other_mint         AS other_mint,
        e.ts_ms              AS entry_ts_ms,
        e.price              AS entry_price,
        e.target_price       AS target_price,
        e.wsol_delta         AS wsol_delta,
        e.token_delta        AS token_delta,
        e.details_json       AS entry_details_json
    FROM events e
    WHERE e.action = 'ENTRY_SIGNAL'
      AND e.player IS NOT NULL
      AND e.signature IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM events t
        WHERE t.player = e.player
          AND t.signature = e.signature
          AND t.action = 'TARGET_HIT'
      )
    ORDER BY e.ts_ms ASC;
    """

    rows = conn.execute(open_sql).fetchall()
    opens: List[OpenTrade] = []

    for r in rows:
        player = r["player"]
        signature = r["signature"]

        ev_rows = conn.execute(
            """
            SELECT id, ts_ms, player, signature, action, whirlpool, other_mint,
                   price, target_price, wsol_delta, token_delta, details_json
            FROM events
            WHERE player = ?
              AND signature = ?
            ORDER BY ts_ms ASC, id ASC;
            """,
            (player, signature),
        ).fetchall()

        events: List[Event] = [
            Event(
                id=er["id"],
                ts_ms=er["ts_ms"],
                player=er["player"],
                signature=er["signature"],
                action=er["action"],
                whirlpool=er["whirlpool"],
                other_mint=er["other_mint"],
                price=er["price"],
                target_price=er["target_price"],
                wsol_delta=er["wsol_delta"],
                token_delta=er["token_delta"],
                details_json=er["details_json"],
            )
            for er in ev_rows
        ]

        last_ev = events[-1] if events else None
        last_action = last_ev.action if last_ev else None
        last_price = last_ev.price if last_ev else None
        last_ts_ms = last_ev.ts_ms if last_ev else int(r["entry_ts_ms"])

        entry_details = {}
        try:
            entry_details = json.loads(r["entry_details_json"] or "{}")
        except Exception:
            entry_details = {"_raw": r["entry_details_json"]}

        opens.append(
            OpenTrade(
                player=player,
                signature=signature,
                whirlpool=r["whirlpool"],
                other_mint=r["other_mint"],
                entry_ts_ms=int(r["entry_ts_ms"]),
                entry_price=float(r["entry_price"] or 0.0),
                target_price=(float(r["target_price"]) if r["target_price"] is not None else None),
                direction=_detect_direction(
                    entry_price=float(r["entry_price"] or 0.0),
                    target_price=(
                        float(r["target_price"]) if r["target_price"] is not None else None
                    ),
                    entry_details=entry_details,
                ),
                wsol_delta=r["wsol_delta"],
                token_delta=r["token_delta"],
                entry_details=entry_details,
                last_ts_ms=int(last_ts_ms),
                last_action=last_action,
                last_price=last_price,
                events=events,
            )
        )

    return opens


def fetch_db_last_ts_ms(conn: sqlite3.Connection) -> int:
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT MAX(ts_ms) AS m FROM events").fetchone()
    if not row or row["m"] is None:
        return int(dt.datetime.now(tz=dt.timezone.utc).timestamp() * 1000)
    return int(row["m"])

def fetch_blacklist_map(players_db_path: Path) -> Dict[str, str]:
    """Load blacklist status from players.db (if present)."""
    if not players_db_path.exists():
        return {}

    conn = sqlite3.connect(str(players_db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Ensure table/column exist
        cur = conn.execute("PRAGMA table_info(players)")
        cols = {row["name"] for row in cur.fetchall()}
        if "address" not in cols or "blacklist" not in cols:
            return {}

        rows = conn.execute("SELECT address, blacklist FROM players").fetchall()
        out: Dict[str, str] = {}
        for r in rows:
            addr = r["address"]
            status = r["blacklist"]
            if addr is not None:
                out[str(addr)] = "" if status is None else str(status)
        return out
    except Exception:
        return {}
    finally:
        conn.close()


def fetch_blacklisted_rows(players_db_path: Path) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    """Load all columns for blacklisted players from players.db."""
    if not players_db_path.exists():
        return [], []

    conn = sqlite3.connect(str(players_db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute("PRAGMA table_info(players)")
        cols = [row["name"] for row in cur.fetchall()]
        if "blacklist" not in cols:
            return [], []

        rows = conn.execute(
            "SELECT * FROM players WHERE blacklist = 'black' ORDER BY address ASC"
        ).fetchall()
        out_rows: List[Tuple[Any, ...]] = []
        for r in rows:
            out_rows.append(tuple(r[c] for c in cols))
        return cols, out_rows
    except Exception:
        return [], []
    finally:
        conn.close()

def _is_black(status: str) -> bool:
    return status.strip().lower() == "black"

def build_summary_row(
    player: str,
    prof: List[Trade],
    opens: List[OpenTrade],
    reference_ts_ms: int,
    blacklist: str,
) -> str:
    avg_profit = (sum(x.profit_pct() for x in prof) / len(prof)) if prof else 0.0
    prof_long = sum(1 for x in prof if x.direction == "long")
    prof_short = sum(1 for x in prof if x.direction == "short")
    open_long = sum(1 for x in opens if x.direction == "long")
    open_short = sum(1 for x in opens if x.direction == "short")
    oldest_open = max((x.age_ms(reference_ts_ms) for x in opens), default=0)
    return (
        "<tr>"
        f"<td>{_esc(player)}</td>"
        f"<td>{_esc(blacklist)}</td>"
        f"<td class='right'>{len(prof)}</td>"
        f"<td class='right'>{avg_profit:.4f}%</td>"
        f"<td class='right'>{prof_long}/{prof_short}</td>"
        f"<td class='right'>{len(opens)}</td>"
        f"<td class='right'>{open_long}/{open_short}</td>"
        f"<td class='right'>{_esc(_fmt_duration_ms(oldest_open)) if opens else ''}</td>"
        "</tr>"
    )


def render_html(
    profitable_trades: List[Trade],
    open_trades: List[OpenTrade],
    min_profit_pct: float,
    reference_ts_ms: int,
    blacklist_by_player: Dict[str, str],
    black_summary_rows: List[str],
) -> str:
    """Render combined report."""

    # Filter profitable trades by min profit
    prof_filtered = [t for t in profitable_trades if t.profit_pct() >= min_profit_pct]

    prof_by_player: Dict[str, List[Trade]] = defaultdict(list)
    for t in prof_filtered:
        prof_by_player[t.player].append(t)

    open_by_player: Dict[str, List[OpenTrade]] = defaultdict(list)
    for t in open_trades:
        open_by_player[t.player].append(t)

    # Summary rows
    all_players = sorted(set(list(prof_by_player.keys()) + list(open_by_player.keys())))
    summary_rows: List[str] = []
    for player in sorted(
        all_players,
        key=lambda p: (len(prof_by_player.get(p, [])) + len(open_by_player.get(p, []))),
        reverse=True,
    ):
        prof = prof_by_player.get(player, [])
        opens = open_by_player.get(player, [])
        blacklist = blacklist_by_player.get(player, "")
        summary_rows.append(
            build_summary_row(player, prof, opens, reference_ts_ms, blacklist)
        )

    now_utc = dt.datetime.now(tz=dt.timezone.utc).isoformat()

    css = """
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; margin: 24px; }
    h1 { margin-top: 0; }
    .muted { color: #666; }
    table { border-collapse: collapse; width: 100%; margin: 12px 0 24px; }
    th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; }
    th { background: #f6f6f6; text-align: left; }
    .trade { border: 1px solid #ddd; padding: 12px; border-radius: 8px; margin: 16px 0; }
    .trade-header { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 16px; }
    .k { color: #444; font-weight: 600; }
    .v { word-break: break-all; }
    .pill { display: inline-block; padding: 2px 8px; border: 1px solid #ccc; border-radius: 999px; font-size: 12px; }
    details { margin-top: 12px; }
    pre { background: #f6f6f6; padding: 10px; overflow-x: auto; }
    .search { width: 100%; max-width: 620px; padding: 8px 10px; border: 1px solid #ccc; border-radius: 6px; }
    .right { text-align: right; }
    """

    js = """
    function filterTrades() {
      const q = (document.getElementById('searchBox').value || '').toLowerCase();
      const cards = document.querySelectorAll('.trade');
      cards.forEach(c => {
        const hay = (c.getAttribute('data-hay') || '').toLowerCase();
        c.style.display = hay.includes(q) ? 'block' : 'none';
      });
    }
    """

    prof_blocks: List[str] = []
    for i, t in enumerate(prof_filtered, start=1):
        solscan_tx = f"https://solscan.io/tx/{t.signature}"
        solscan_whirlpool = f"https://solscan.io/account/{t.whirlpool}" if t.whirlpool else ""
        profit = t.profit_pct()
        pct_to_edge = t.pct_to_edge()

        prof_blocks.append(
            f"""
            <div class="trade" id="trade-{i}" data-hay="{_esc(t.player)} { _esc(t.signature)} { _esc(t.whirlpool)} { _esc(t.other_mint)} { _esc(t.direction)}">
              <div class="trade-header">
                <div>
                  <span class="k">#{i}</span>
                  <span class="pill">profit {profit:.4f}%</span>
                  <span class="pill">took { _esc(t.time_to_profit_human()) }</span>
                  <span class="pill">{_esc(t.direction)}</span>
                </div>
                <div class="muted">entry: {_esc(t.entry_ts_utc())} | exit: {_esc(t.exit_ts_utc())}</div>

                <div><span class="k">player:</span> <span class="v">{_esc(t.player)}</span></div>
                <div><span class="k">signature:</span> <span class="v"><a href="{_esc(solscan_tx)}" target="_blank" rel="noreferrer">{_esc(t.signature)}</a></span></div>

                <div><span class="k">whirlpool:</span> <span class="v">{('' if not t.whirlpool else f'<a href="{_esc(solscan_whirlpool)}" target="_blank" rel="noreferrer">{_esc(t.whirlpool)}</a>')}</span></div>
                <div><span class="k">other_mint:</span> <span class="v">{_esc(t.other_mint)}</span></div>

                <div><span class="k">entry_price (WSOL/token):</span> <span class="v">{t.entry_price:.12g}</span></div>
                <div><span class="k">exit_price (WSOL/token):</span> <span class="v">{t.exit_price:.12g}</span></div>

                <div><span class="k">direction:</span> <span class="v">{_esc(t.direction)}</span></div>
                <div><span class="k">target_price:</span> <span class="v">{'' if t.target_price is None else f'{t.target_price:.12g}'}</span></div>
                <div><span class="k">wsol_delta / token_delta:</span> <span class="v">{_esc(t.wsol_delta)} / {_esc(t.token_delta)}</span></div>
                <div><span class="k">pct_to_edge (вход→граница):</span> <span class="v">{'' if pct_to_edge is None else f'{pct_to_edge:.4f}%'}</span></div>
              </div>

            </div>
            """
        )

    open_blocks: List[str] = []
    for j, ot in enumerate(open_trades, start=1):
        solscan_tx = f"https://solscan.io/tx/{ot.signature}"
        solscan_whirlpool = f"https://solscan.io/account/{ot.whirlpool}" if ot.whirlpool else ""
        seen_ms = int(ot.last_ts_ms) - int(ot.entry_ts_ms)
        pct_to_edge = ot.pct_to_edge()

        open_blocks.append(
            f"""
            <div class="trade" id="open-{j}" data-hay="{_esc(ot.player)} { _esc(ot.signature)} { _esc(ot.whirlpool)} { _esc(ot.other_mint)} { _esc(ot.direction)}">
              <div class="trade-header">
                <div>
                  <span class="k">OPEN #{j}</span>
                  <span class="pill">seen { _esc(_fmt_duration_ms(seen_ms)) }</span>
                  <span class="pill">until DB end { _esc(ot.age_human(reference_ts_ms)) }</span>
                  <span class="pill">{_esc(ot.direction)}</span>
                </div>
                <div class="muted">entry: {_esc(ot.entry_ts_utc())} | last: {_esc(ot.last_ts_utc())}</div>

                <div><span class="k">player:</span> <span class="v">{_esc(ot.player)}</span></div>
                <div><span class="k">signature:</span> <span class="v"><a href="{_esc(solscan_tx)}" target="_blank" rel="noreferrer">{_esc(ot.signature)}</a></span></div>

                <div><span class="k">whirlpool:</span> <span class="v">{('' if not ot.whirlpool else f'<a href="{_esc(solscan_whirlpool)}" target="_blank" rel="noreferrer">{_esc(ot.whirlpool)}</a>')}</span></div>
                <div><span class="k">other_mint:</span> <span class="v">{_esc(ot.other_mint)}</span></div>

                <div><span class="k">entry_price (WSOL/token):</span> <span class="v">{ot.entry_price:.12g}</span></div>
                <div><span class="k">target_price:</span> <span class="v">{'' if ot.target_price is None else f'{ot.target_price:.12g}'}</span></div>

                <div><span class="k">last_action:</span> <span class="v">{_esc(ot.last_action)}</span></div>
                <div><span class="k">last_price:</span> <span class="v">{'' if ot.last_price is None else f'{ot.last_price:.12g}'}</span></div>

                <div><span class="k">direction:</span> <span class="v">{_esc(ot.direction)}</span></div>
                <div><span class="k">wsol_delta / token_delta:</span> <span class="v">{_esc(ot.wsol_delta)} / {_esc(ot.token_delta)}</span></div>
                <div><span class="k">age (from entry to DB end):</span> <span class="v">{_esc(ot.age_human(reference_ts_ms))}</span></div>
                <div><span class="k">pct_to_edge (вход→граница):</span> <span class="v">{'' if pct_to_edge is None else f'{pct_to_edge:.4f}%'}</span></div>
              </div>

            </div>
            """
        )

    html_doc = f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Trades Report</title>
  <style>{css}</style>
</head>
<body>
  <h1>Отчёт по сделкам</h1>
  <div class="muted">Сгенерировано: { _esc(now_utc) } (UTC). Последний ts в БД: {_esc(dt.datetime.fromtimestamp(reference_ts_ms/1000, tz=dt.timezone.utc).isoformat())}</div>

  <h2>Логика</h2>
  <div class="muted">
    <div><b>Profit:</b> ENTRY_SIGNAL + TARGET_HIT по одному (player, signature)</div>
    <div><b>Open:</b> ENTRY_SIGNAL есть, а TARGET_HIT в БД не найден</div>
    <div><b>Direction:</b> берётся из ENTRY_SIGNAL details_json.direction, иначе вычисляется по target_price относительно entry_price</div>
  </div>
  <div style="margin: 8px 0 16px;">
    <input id="searchBox" class="search" placeholder="Поиск по player / signature / whirlpool / mint" oninput="filterTrades()" />
  </div>

  <h2>Сводка по игрокам</h2>
  <table>
    <thead>
      <tr>
        <th>player</th>
        <th>blacklist</th>
        <th class="right">profitable</th>
        <th class="right">avg profit</th>
        <th class="right">prof L/S</th>
        <th class="right">open</th>
        <th class="right">open L/S</th>
        <th class="right">oldest open</th>
      </tr>
    </thead>
    <tbody>
      {''.join(summary_rows) if summary_rows else '<tr><td colspan="8">Нет сделок, попадающих под критерии</td></tr>'}
    </tbody>
  </table>

  <details>
    <summary>Исключённые black-игроки: {len(black_summary_rows)}</summary>
    <div class="muted">Эти игроки исключены из расчётов profitable/open.</div>
    {(''
      if not black_summary_rows
      else '<table><thead><tr><th>player</th><th>blacklist</th><th class="right">profitable</th>'
           '<th class="right">avg profit</th><th class="right">prof L/S</th>'
           '<th class="right">open</th><th class="right">open L/S</th>'
           '<th class="right">oldest open</th></tr></thead><tbody>' +
           ''.join(black_summary_rows) +
           '</tbody></table>'
     )}
  </details>

  <h2>Профитные сделки (детально)</h2>
  {''.join(prof_blocks) if prof_blocks else '<div class="muted">Нет профитных сделок, попадающих под критерии</div>'}

  <h2>Вход был, но профит не дождались (OPEN)</h2>
  {''.join(open_blocks) if open_blocks else '<div class="muted">Нет open-сделок</div>'}

  <script>{js}</script>
</body>
</html>
"""

    return html_doc


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract trades from tracker.db and build an HTML report")
    parser.add_argument("--db", default="tracker.db", help="Path to tracker.db")
    parser.add_argument("--out", default="profit_report.html", help="Path to output HTML")
    parser.add_argument(
        "--min-profit-pct",
        type=float,
        default=0.0,
        help="Minimum profit percent to include (e.g. 0.3 means 0.3%)",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        profitable_trades = fetch_profitable_trades(conn)
        open_trades = fetch_open_trades(conn)
        reference_ts_ms = fetch_db_last_ts_ms(conn)
    finally:
        conn.close()

    players_db_path = db_path.with_name("players.db")
    blacklist_by_player = fetch_blacklist_map(players_db_path)
    if blacklist_by_player:
        black_players = {p for p, s in blacklist_by_player.items() if _is_black(s)}
    else:
        black_players = set()

    prof_by_player_all: Dict[str, List[Trade]] = defaultdict(list)
    for t in profitable_trades:
        prof_by_player_all[t.player].append(t)

    open_by_player_all: Dict[str, List[OpenTrade]] = defaultdict(list)
    for t in open_trades:
        open_by_player_all[t.player].append(t)

    black_summary_rows: List[str] = []
    for player in sorted(black_players):
        prof = prof_by_player_all.get(player, [])
        opens = open_by_player_all.get(player, [])
        status = blacklist_by_player.get(player, "")
        black_summary_rows.append(
            build_summary_row(player, prof, opens, reference_ts_ms, status)
        )

    if black_players:
        profitable_trades = [t for t in profitable_trades if t.player not in black_players]
        open_trades = [t for t in open_trades if t.player not in black_players]

    html_doc = render_html(
        profitable_trades=profitable_trades,
        open_trades=open_trades,
        min_profit_pct=args.min_profit_pct,
        reference_ts_ms=reference_ts_ms,
        blacklist_by_player=blacklist_by_player,
        black_summary_rows=black_summary_rows,
    )

    out_path = Path(args.out)
    out_path.write_text(html_doc, encoding="utf-8")
    print(
        f"OK: wrote {out_path.resolve()} (profitable={len(profitable_trades)}, open={len(open_trades)})"
    )


if __name__ == "__main__":
    main()
