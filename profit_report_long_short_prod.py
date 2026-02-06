#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Readable, production-style analytics HTML report for trade.db (SQLite).

- NO CLI args (paths are configured in RUN CONFIG below).
- Works with schema:
  - positions
  - trade_links
  - trade_events
  - sim_wallet
  - executor_state

Main goals:
- No text overlap: no sticky table headers, robust word wrapping (overflow-wrap:anywhere).
- Analyst-friendly layout: clear sections, anchors, collapsible raw previews per table.
"""

from __future__ import annotations

import datetime as dt
import html
import json
import os
import sqlite3
import statistics
from pathlib import Path
from typing import Any, Dict, List, Tuple

# =========================
# RUN CONFIG (edit here)
# =========================
DB_PATH = "trade.db"
OUT_HTML = "profit_report_readable.html"

TZ_NAME = "Europe/Berlin"
MAX_ROWS_PREVIEW = 500          # per raw table preview
MAX_FAILED_PREVIEW = 2000       # failed links preview in the "FAILED" section
MAX_EVENTS_PREVIEW = 200        # last events preview

MAX_DEALS_PREVIEW = 2000       # deals (positions) preview rows

# Unit conventions used by this DB:
LAMPORTS_PER_SOL = 1_000_000_000
MICROUSDC_PER_USDC = 1_000_000


def _ms_to_dt_str(ms: Any) -> str:
    if ms is None:
        return ""
    try:
        ms_i = int(ms)
    except Exception:
        return str(ms)

    dt_utc = dt.datetime.fromtimestamp(ms_i / 1000.0, tz=dt.timezone.utc)
    try:
        from zoneinfo import ZoneInfo  # py3.9+
        return dt_utc.astimezone(ZoneInfo(TZ_NAME)).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return dt_utc.isoformat()


def _fmt_num(x: Any, nd: int = 6) -> str:
    if x is None:
        return ""
    try:
        if isinstance(x, (int, float)):
            return f"{x:,.{nd}f}"
        return str(x)
    except Exception:
        return str(x)


def _esc(s: Any) -> str:
    if s is None:
        return ""
    return html.escape(str(s), quote=True)


def _json_pretty(j: Any) -> str:
    if j is None:
        return ""
    try:
        if isinstance(j, str):
            obj = json.loads(j)
        else:
            obj = j
        return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return str(j)


def _fetchall_dict(con: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    con.row_factory = sqlite3.Row
    cur = con.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def _render_table(headers: List[str], rows: List[List[Any]]) -> str:
    thead = "".join(f"<th>{_esc(h)}</th>" for h in headers)

    if not rows:
        body = f"<tr><td class='muted' colspan='{len(headers)}'>Нет данных</td></tr>"
    else:
        body_parts = []
        for r in rows:
            tds = []
            for cell in r:
                s = "" if cell is None else str(cell)
                tds.append(f"<td><div class='cell'>{_esc(s)}</div></td>")
            body_parts.append("<tr>" + "".join(tds) + "</tr>")
        body = "".join(body_parts)

    return f"""
<div class="table-scroll">
  <table>
    <thead><tr>{thead}</tr></thead>
    <tbody>{body}</tbody>
  </table>
</div>
"""


def _table_info(con: sqlite3.Connection) -> List[Tuple[str, List[Dict[str, Any]], str]]:
    rows = _fetchall_dict(con, "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name;")
    out = []
    for r in rows:
        name = r["name"]
        if name == "sqlite_sequence":
            continue
        cols = _fetchall_dict(con, f"PRAGMA table_info({name});")
        out.append((name, cols, r.get("sql") or ""))
    return out


def generate_report(db_path: str, out_html: str) -> str:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys=ON;")

    # Tables list and row counts
    tables = [
        r["name"]
        for r in _fetchall_dict(con, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        if r["name"] != "sqlite_sequence"
    ]
    counts = {t: _fetchall_dict(con, f"SELECT COUNT(*) AS n FROM {t};")[0]["n"] for t in tables}

    # Aggregations
    links_status = _fetchall_dict(con, "SELECT status, COUNT(*) n FROM trade_links GROUP BY status ORDER BY n DESC;")
    pos_state = _fetchall_dict(con, "SELECT state, side, COUNT(*) n FROM positions GROUP BY state, side ORDER BY n DESC;")
    events_type = _fetchall_dict(con, "SELECT event_type, COUNT(*) n FROM trade_events GROUP BY event_type ORDER BY n DESC;")

    by_pool = _fetchall_dict(con, """
        SELECT pool, side,
               COUNT(*) AS n_positions,
               SUM(CASE WHEN state='CLOSED' THEN 1 ELSE 0 END) AS closed,
               SUM(CASE WHEN state IN ('OPENING','OPEN','CLOSING') THEN 1 ELSE 0 END) AS open,
               AVG(CASE WHEN state='CLOSED' THEN profit_pct END) AS avg_profit_pct,
               SUM(CASE WHEN state='CLOSED' AND profit_pct>0 THEN 1 ELSE 0 END) AS win,
               SUM(CASE WHEN state='CLOSED' AND profit_pct<=0 THEN 1 ELSE 0 END) AS lose
        FROM positions
        GROUP BY pool, side
        ORDER BY closed DESC, n_positions DESC;
    """)

    closed_total = sum(r["n"] for r in pos_state if r.get("state") == "CLOSED")

    closed_pp = _fetchall_dict(con, "SELECT profit_pct FROM positions WHERE state='CLOSED' AND profit_pct IS NOT NULL;")
    profs = [r["profit_pct"] for r in closed_pp if r.get("profit_pct") is not None]
    profs_sorted = sorted(profs)

    def _pct(p: float) -> float | None:
        if not profs_sorted:
            return None
        k = int(round((len(profs_sorted) - 1) * p))
        return profs_sorted[max(0, min(len(profs_sorted) - 1, k))]

    profit_stats = {
        "closed_total": closed_total,
        "closed_positions": len(profs),
        "avg_profit_pct": (sum(profs) / len(profs)) if profs else None,
        "median_profit_pct": statistics.median(profs) if profs else None,
        "p10_profit_pct": _pct(0.10),
        "p90_profit_pct": _pct(0.90),
        "min_profit_pct": min(profs) if profs else None,
        "max_profit_pct": max(profs) if profs else None,
        "wins": sum(1 for x in profs if x > 0),
        "losses": sum(1 for x in profs if x <= 0),
    }

    # Failed / broken transactions
    failed_links = _fetchall_dict(con, """
        SELECT player, signature, whirlpool, side, position_id, status,
               created_at_ms, updated_at_ms, last_error
        FROM trade_links
        WHERE status='FAILED'
           OR (position_id IS NULL AND last_error IS NOT NULL AND last_error!='')
        ORDER BY updated_at_ms DESC;
    """)

    # Integrity checks (orphans)
    orphan_links = _fetchall_dict(con, """
        SELECT COUNT(*) AS n
        FROM trade_links tl
        LEFT JOIN positions p ON p.id=tl.position_id
        WHERE tl.position_id IS NOT NULL AND p.id IS NULL;
    """)[0]["n"]

    orphan_events = _fetchall_dict(con, """
        SELECT COUNT(*) AS n
        FROM trade_events te
        LEFT JOIN positions p ON p.id=te.position_id
        WHERE te.position_id IS NOT NULL AND p.id IS NULL;
    """)[0]["n"]

    # Recent events
    last_events = _fetchall_dict(con, f"""
        SELECT id, position_id, ts_ms, event_type, details_json
        FROM trade_events
        ORDER BY ts_ms DESC
        LIMIT {MAX_EVENTS_PREVIEW};
    """)

    # Wallet + executor_state
    wallet = _fetchall_dict(con, "SELECT payer, usdc_balance, sol_lamports, updated_at_ms FROM sim_wallet ORDER BY updated_at_ms DESC;")
    exec_state = _fetchall_dict(con, "SELECT key, value FROM executor_state ORDER BY key;")

    # Raw table previews
    raw_sections = []
    for t in tables:
        cols = [r["name"] for r in _fetchall_dict(con, f"PRAGMA table_info({t});")]
        rows = _fetchall_dict(con, f"SELECT * FROM {t} ORDER BY rowid DESC LIMIT {MAX_ROWS_PREVIEW};")
        formatted = []
        for r in rows:
            rr = []
            for c in cols:
                v = r.get(c)
                if c.endswith("_ms") and isinstance(v, (int, float)):
                    rr.append(_ms_to_dt_str(v))
                elif c.endswith("_lamports") and isinstance(v, (int, float)):
                    rr.append(f"{v} ({v / LAMPORTS_PER_SOL:.9f} SOL)")
                elif c in ("reserved_usdc", "usdc_position", "usdc_balance") and isinstance(v, (int, float)):
                    rr.append(f"{v} ({v / MICROUSDC_PER_USDC:.6f} USDC)")
                elif c == "details_json":
                    rr.append(_json_pretty(v)[:1500])
                else:
                    rr.append(v)
            formatted.append(rr)
        raw_sections.append((t, cols, formatted, counts.get(t, 0)))

    schema_rows = []
    for name, cols, sql in _table_info(con):
        col_desc = ", ".join([f"{c['name']}:{c['type']}" for c in cols])
        schema_rows.append([name, str(len(cols)), col_desc, (sql or "")[:800]])

    # Deals data for report (positions with tx signatures)
    deals_all = _fetchall_dict(con, f'''
        SELECT id, payer, pool, side, mode, state, slippage_bps,
               created_at_ms, updated_at_ms,
               entry_price, exit_price, profit_pct,
               open_sig, close_sig
        FROM positions
        ORDER BY updated_at_ms DESC
        LIMIT {MAX_DEALS_PREVIEW};
    ''')


    con.close()

    # =========================
    # HTML + CSS (readable)
    # =========================
    gen_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    CSS = """
:root{
  --bg:#ffffff;
  --panel:#ffffff;
  --text:#12161f;
  --muted:#5b677a;
  --border:#d9e0ea;
  --accent:#1565d8;
  --good:#0f8a5f;
  --bad:#c62828;
  --warn:#b26a00;
  --codebg:#f6f8fb;
}
*{box-sizing:border-box}
html,body{margin:0;padding:0;background:var(--bg);color:var(--text);font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial;line-height:1.5}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
.wrap{max-width:1400px;margin:0 auto;padding:20px 18px 80px}
.topbar{
  position:sticky; top:0; z-index:20;
  background:rgba(255,255,255,0.92);
  backdrop-filter: blur(8px);
  border-bottom:1px solid var(--border);
}
.topbar .inner{max-width:1400px;margin:0 auto;padding:14px 18px;display:flex;gap:12px;flex-wrap:wrap;align-items:center;justify-content:space-between}
.h1{font-size:18px;font-weight:800}
.meta{color:var(--muted);font-size:13px}
.nav{display:flex;gap:10px;flex-wrap:wrap}
.nav a{font-size:13px;padding:6px 10px;border:1px solid var(--border);border-radius:999px;background:#fff}
.section{margin-top:18px}
.card{
  background:var(--panel);
  border:1px solid var(--border);
  border-radius:16px;
  padding:14px 14px;
  box-shadow:0 1px 0 rgba(16,24,40,0.02);
}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
.kpi .label{color:var(--muted);font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}
.kpi .value{font-size:20px;font-weight:800;margin-top:6px}
.kpi .sub{color:var(--muted);font-size:13px;margin-top:4px}
.badge{display:inline-flex;gap:6px;align-items:center;font-size:12px;font-weight:800;border-radius:999px;padding:4px 10px;border:1px solid var(--border);background:#fff;color:var(--muted)}
.badge.good{color:var(--good);border-color:rgba(15,138,95,.25);background:rgba(15,138,95,.06)}
.badge.bad{color:var(--bad);border-color:rgba(198,40,40,.25);background:rgba(198,40,40,.06)}
.badge.warn{color:var(--warn);border-color:rgba(178,106,0,.25);background:rgba(178,106,0,.06)}
.h2{font-size:16px;font-weight:900;margin:0 0 10px}
.h3{font-size:14px;font-weight:900;margin:0 0 8px}
.p{color:var(--muted);margin:0 0 10px;font-size:13.5px}
.table-scroll{overflow:auto;border:1px solid var(--border);border-radius:14px}
table{width:100%;border-collapse:separate;border-spacing:0;font-size:12.8px}
th,td{padding:10px 12px;border-bottom:1px solid var(--border);vertical-align:top;text-align:left}
th{background:#fbfcfe;font-size:12px;font-weight:900}
tr:nth-child(even) td{background:#fcfdff}
tr:hover td{background:#f5f9ff}
.cell{
  white-space:pre-wrap;
  overflow-wrap:anywhere;
  word-break:break-word;
}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace}
.small{font-size:12px}
.codeblock{
  background:var(--codebg);
  border:1px solid var(--border);
  border-radius:14px;
  padding:10px 12px;
  overflow:auto;
}
details{
  border:1px solid var(--border);
  border-radius:16px;
  padding:10px 12px;
  background:#fff;
  margin-top:12px;
}
summary{
  cursor:pointer;
  font-weight:900;
  list-style:none;
}
summary::-webkit-details-marker{display:none}
summary .hint{color:var(--muted);font-weight:700;font-size:12px;margin-left:8px}
.foot{color:var(--muted);font-size:12px;margin-top:18px}
"""

    def kpi_card(title: str, value: str, sub: str | None = None, badge: Tuple[str, str] | None = None) -> str:
        b = ""
        if badge:
            cls, txt = badge
            b = f"<span class='badge {cls}'>{_esc(txt)}</span>"
        return f"""
<div class="card kpi">
  <div class="label">{_esc(title)} {b}</div>
  <div class="value">{_esc(value)}</div>
  {f"<div class='sub'>{_esc(sub)}</div>" if sub else ""}
</div>
"""

    kpis_html = ""
    kpis_html += kpi_card("Таблиц в БД", str(len(tables)), sub=", ".join(tables))
    kpis_html += kpi_card("Всего positions", str(counts.get("positions", 0)))
    kpis_html += kpi_card("Всего trade_links", str(counts.get("trade_links", 0)))
    kpis_html += kpi_card(
        "FAILED / битые link'и",
        str(len(failed_links)),
        sub="status='FAILED' или без position_id + last_error",
        badge=("bad" if len(failed_links) > 0 else "good", "ALERT" if len(failed_links) > 0 else "OK"),
    )

    closed_total_i = int(profit_stats.get("closed_total") or 0)
    known_i = int(profit_stats.get("closed_positions") or 0)
    missing_i = max(0, closed_total_i - known_i)

    if closed_total_i > 0:
        if known_i > 0:
            winrate = profit_stats["wins"] / known_i * 100.0
            sub = f"Win-rate: {winrate:.1f}%"
            if missing_i > 0:
                sub += f" • profit_pct: {known_i}/{closed_total_i} (missing: {missing_i})"
            kpis_html += kpi_card("CLOSED позиций", str(closed_total_i), sub=sub)
            kpis_html += kpi_card(
                "Profit% avg / median",
                f"{profit_stats['avg_profit_pct']:.6f} / {profit_stats['median_profit_pct']:.6f}",
                sub=(
                    f"min={profit_stats['min_profit_pct']:.6f}, "
                    f"p10={profit_stats['p10_profit_pct']:.6f}, "
                    f"p90={profit_stats['p90_profit_pct']:.6f}, "
                    f"max={profit_stats['max_profit_pct']:.6f}"
                ),
            )
        else:
            kpis_html += kpi_card("CLOSED позиций", str(closed_total_i), sub="profit_pct отсутствует для всех CLOSED позиций")
    else:
        kpis_html += kpi_card("CLOSED позиций", "0", sub="Нет закрытых позиций")

    links_status_tbl = _render_table(["status", "count"], [[r["status"], r["n"]] for r in links_status])
    pos_state_tbl = _render_table(["state", "side", "count"], [[r["state"], r["side"], r["n"]] for r in pos_state])
    events_type_tbl = _render_table(["event_type", "count"], [[r["event_type"], r["n"]] for r in events_type])

    by_pool_tbl = _render_table(
        ["pool", "side", "positions", "closed", "open", "win", "lose", "avg_profit_pct(CLOSED)"],
        [
            [
                r["pool"],
                r["side"],
                r["n_positions"],
                r["closed"],
                r["open"],
                r["win"],
                r["lose"],
                "" if r["avg_profit_pct"] is None else f"{r['avg_profit_pct']:.6f}",
            ]
            for r in by_pool
        ],
    )


    # Deals tables (positions) with tx signatures for open/close (data fetched earlier as deals_all)
    def _deal_rows(rows: List[Dict[str, Any]]) -> List[List[Any]]:
        out: List[List[Any]] = []
        for r in rows:
            out.append([
                r.get("id"),
                r.get("payer"),
                r.get("pool"),
                r.get("side"),
                r.get("mode"),
                r.get("state"),
                r.get("slippage_bps"),
                _ms_to_dt_str(r.get("created_at_ms")),
                _ms_to_dt_str(r.get("updated_at_ms")),
                "" if r.get("entry_price") is None else _fmt_num(r.get("entry_price"), 12),
                "" if r.get("exit_price") is None else _fmt_num(r.get("exit_price"), 12),
                "" if r.get("profit_pct") is None else _fmt_num(r.get("profit_pct"), 6),
                r.get("open_sig") or "",
                r.get("close_sig") or "",
            ])
        return out

    deals_closed = [r for r in deals_all if r.get("state") == "CLOSED"]
    deals_open = [r for r in deals_all if r.get("state") in ("OPENING", "OPEN", "CLOSING", "PENDING_CLOSE")]
    deals_failed = [r for r in deals_all if r.get("state") == "FAILED"]

    deals_headers = [
        "id", "payer", "pool", "side", "mode", "state", "slippage_bps",
        "created", "updated",
        "entry_price", "exit_price", "profit_pct",
        "open_sig (tx)", "close_sig (tx)"
    ]

    deals_closed_tbl = _render_table(deals_headers, _deal_rows(deals_closed))
    deals_open_tbl = _render_table(deals_headers, _deal_rows(deals_open))
    deals_failed_tbl = _render_table(deals_headers, _deal_rows(deals_failed))


    failed_preview_rows = []
    for r in failed_links[:MAX_FAILED_PREVIEW]:
        failed_preview_rows.append(
            [
                r.get("status"),
                r.get("side"),
                r.get("player"),
                r.get("whirlpool"),
                r.get("signature"),
                r.get("position_id"),
                _ms_to_dt_str(r.get("created_at_ms")),
                _ms_to_dt_str(r.get("updated_at_ms")),
                (r.get("last_error") or "")[:500],
            ]
        )

    failed_tbl = _render_table(
        ["status", "side", "player", "whirlpool", "signature", "position_id", "created", "updated", "last_error (trunc)"],
        failed_preview_rows,
    )

    last_events_rows = []
    for r in last_events:
        last_events_rows.append(
            [
                r.get("id"),
                r.get("position_id"),
                _ms_to_dt_str(r.get("ts_ms")),
                r.get("event_type"),
                (r.get("details_json") or "")[:300],
            ]
        )
    last_events_tbl = _render_table(["id", "position_id", "ts", "event_type", "details_json (trunc)"], last_events_rows)

    wallet_rows = []
    for r in wallet:
        sol = (r.get("sol_lamports") or 0) / LAMPORTS_PER_SOL
        wallet_rows.append([r.get("payer"), _fmt_num((r.get("usdc_balance") or 0) / MICROUSDC_PER_USDC, 6), _fmt_num(sol, 9), _ms_to_dt_str(r.get("updated_at_ms"))])
    wallet_tbl = _render_table(["payer", "usdc_balance", "sol", "updated"], wallet_rows)

    exec_tbl = _render_table(["key", "value"], [[r.get("key"), r.get("value")] for r in exec_state])

    schema_tbl = _render_table(["table", "cols", "columns(type)", "create_sql (trunc)"], schema_rows)

    raw_blocks = []
    for t, cols, formatted, nrows in raw_sections:
        raw_blocks.append(
            f"""
<details>
  <summary>{_esc(t)} <span class="hint">(строки: {nrows}, предпросмотр: {min(nrows, MAX_ROWS_PREVIEW)})</span></summary>
  <div class="section">{_render_table(cols, formatted)}</div>
</details>
"""
        )
    raw_all = "\n".join(raw_blocks)


    min_len_closed = min(len(deals_closed), MAX_DEALS_PREVIEW)
    min_len_open = min(len(deals_open), MAX_DEALS_PREVIEW)
    min_len_failed = min(len(deals_failed), MAX_DEALS_PREVIEW)

    html_doc = f"""<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>trade.db analytics report</title>
<style>{CSS}</style>
</head>
<body>
<div class="topbar">
  <div class="inner">
    <div>
      <div class="h1">trade.db — аналитический отчёт</div>
      <div class="meta">Сгенерировано: {_esc(gen_time)} • БД: {_esc(os.path.basename(db_path))} • TZ: {_esc(TZ_NAME)}</div>
    </div>
    <div class="nav">
      <a href="#kpi">Сводка</a>
      <a href="#statuses">Статусы</a>
      <a href="#profit">Профит</a>
      <a href="#positions">Сделки</a>
      <a href="#failed">Битые/FAILED</a>
      <a href="#integrity">Целостность</a>
      <a href="#events">События</a>
      <a href="#wallet">Wallet/State</a>
      <a href="#schema">Схема</a>
      <a href="#raw">Все данные</a>
    </div>
  </div>
</div>

<div class="wrap">

<section id="kpi" class="section">
  <div class="card">
    <div class="h2">Сводка</div>
    <div class="grid">{kpis_html}</div>
  </div>
</section>

<section id="statuses" class="section">
  <div class="card">
    <div class="h2">Статусы (trade_links / positions / trade_events)</div>
    <div class="grid">
      <div>
        <div class="h3">trade_links по status</div>
        {links_status_tbl}
      </div>
      <div>
        <div class="h3">positions по state и side</div>
        {pos_state_tbl}
      </div>
      <div>
        <div class="h3">trade_events по event_type</div>
        {events_type_tbl}
      </div>
    </div>
  </div>
</section>

<section id="profit" class="section">
  <div class="card">
    <div class="h2">Сводка по пулам (positions)</div>
    <p class="p">Средний профит считается только по <span class="mono">state='CLOSED'</span>.</p>
    {by_pool_tbl}
  </div>
</section>

<section id="positions" class="section">
  <div class="card">
    <div class="h2">Сделки (positions) с сигнатурами транзакций</div>
    <p class="p">Колонки <span class="mono">open_sig</span> и <span class="mono">close_sig</span> — подписи (signature / txhash) Solana-транзакций открытия и закрытия позиции.</p>

    <div class="section">
      <div class="h3">CLOSED</div>
      {deals_closed_tbl}
      <div class="foot">Показаны первые {min_len_closed} записей (лимит: {MAX_DEALS_PREVIEW}).</div>
    </div>

    <div class="section">
      <div class="h3">OPEN / OPENING / CLOSING</div>
      {deals_open_tbl}
      <div class="foot">Показаны первые {min_len_open} записей (лимит: {MAX_DEALS_PREVIEW}).</div>
    </div>

    <div class="section">
      <div class="h3">FAILED</div>
      {deals_failed_tbl}
      <div class="foot">Показаны первые {min_len_failed} записей (лимит: {MAX_DEALS_PREVIEW}).</div>
    </div>
  </div>
</section>


<section id="failed" class="section">
  <div class="card">
    <div class="h2">Битые транзакции / FAILED</div>
    <p class="p">Сюда попадают: <span class="mono">trade_links.status='FAILED'</span> ИЛИ записи без <span class="mono">position_id</span>, но с <span class="mono">last_error</span>.</p>
    {failed_tbl}
    <div class="foot">Показаны первые {min(len(failed_links), MAX_FAILED_PREVIEW)} записей.</div>
  </div>
</section>

<section id="integrity" class="section">
  <div class="card">
    <div class="h2">Проверки целостности</div>
    <div class="grid">
      <div class="card">
        <div class="h3">Orphan trade_links → positions</div>
        <div class="kpi value mono">{orphan_links}</div>
        <div class="p">trade_links.position_id задан, но positions.id не найден.</div>
      </div>
      <div class="card">
        <div class="h3">Orphan trade_events → positions</div>
        <div class="kpi value mono">{orphan_events}</div>
        <div class="p">trade_events.position_id задан, но positions.id не найден.</div>
      </div>
    </div>
  </div>
</section>

<section id="events" class="section">
  <div class="card">
    <div class="h2">Последние события (trade_events)</div>
    <p class="p">Показаны последние {MAX_EVENTS_PREVIEW} событий по времени <span class="mono">ts_ms</span>.</p>
    {last_events_tbl}
  </div>
</section>

<section id="wallet" class="section">
  <div class="card">
    <div class="h2">Состояние кошелька и исполнителя</div>
    <div class="grid">
      <div>
        <div class="h3">sim_wallet</div>
        {wallet_tbl}
      </div>
      <div>
        <div class="h3">executor_state</div>
        {exec_tbl}
      </div>
    </div>
  </div>
</section>

<section id="schema" class="section">
  <div class="card">
    <div class="h2">Схема БД (SQLite)</div>
    {schema_tbl}
  </div>
</section>

<section id="raw" class="section">
  <div class="card">
    <div class="h2">Все данные (raw preview)</div>
    <p class="p">Каждая таблица — отдельный раскрывающийся блок. Длинные поля переносятся через <span class="mono">overflow-wrap:anywhere</span>.</p>
    {raw_all}
  </div>
</section>

<div class="foot">Отчёт самодостаточный (один HTML). Если данные содержат экстремально длинные строки, они не ломают верстку: включены переносы и горизонтальный скролл таблиц.</div>

</div>
</body>
</html>
"""

    Path(out_html).write_text(html_doc, encoding="utf-8")
    return out_html


def main() -> None:
    # Resolve paths relative to this script directory
    base = Path(__file__).resolve().parent
    db_path = str((base / DB_PATH).resolve())
    out_html = str((base / OUT_HTML).resolve())

    if not Path(db_path).exists():
        raise SystemExit(f"DB not found: {db_path}")

    out = generate_report(db_path, out_html)
    print(out)


if __name__ == "__main__":
    main()
