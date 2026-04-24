#!/usr/bin/env python3
"""Mezmo Deploy Duty — Deterministic Error Analysis.

Calls the Mezmo MCP server directly (no LLM needed) to compare today's
errors against the previous week for Apify apps on staging & production,
then generates an HTML dashboard and opens it in the browser.

Usage:
    MEZMO_MCP_TOKEN=sts_xxx python3 deploy_duty.py [--debug]
"""

import json
import os
import re
import subprocess
import sys
import platform
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from html import escape
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Missing dependency. Run: pip install requests")

# ── Configuration ───────────────────────────────────────────────────────────

MCP_URL = os.environ.get("MEZMO_MCP_URL", "https://mcp.mezmo.com/mcp")
MCP_TOKEN = os.environ.get("MEZMO_MCP_TOKEN", "")
DEBUG = "--debug" in sys.argv
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = SCRIPT_DIR / "mezmo-dashboard.html"
MAX_RESOLVED_SHOWN = 30

APPS = [
    "apify-app-daemons", "apify-api", "apify-app-daemons-new",
    "apify-app-ui-backend", "apify-infinite-daemons", "apify-daemons-new",
]
APP_Q = " OR ".join(f"app:{a}" for a in APPS)
LVL_Q = "(level:error OR level:fatal OR level:critical)"
STAGING_Q = f"{LVL_Q} tag:staging ({APP_Q})"
PROD_Q = f"{LVL_Q} (tag:production OR tag:prod) ({APP_Q})"


# ── MCP Client ──────────────────────────────────────────────────────────────

class MCPClient:
    """Minimal MCP Streamable-HTTP client (JSON-RPC 2.0)."""

    def __init__(self, url: str, token: str):
        self.url = url
        self.token = token
        self.session_id: str | None = None
        self._id = 0
        self._lock = threading.Lock()

    def _next_id(self) -> int:
        with self._lock:
            self._id += 1
            return self._id

    def _headers(self) -> dict:
        h = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self.session_id:
            h["Mcp-Session"] = self.session_id
        return h

    def _post(self, payload: dict, timeout: int = 180) -> dict:
        resp = requests.post(
            self.url, headers=self._headers(), json=payload,
            stream=True, timeout=timeout,
        )
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        # Capture session id from any response
        sid = resp.headers.get("Mcp-Session")
        if sid:
            self.session_id = sid
        if "text/event-stream" in ct:
            return self._read_sse(resp)
        return resp.json()

    @staticmethod
    def _read_sse(resp) -> dict:
        """Parse SSE stream, return last JSON-RPC result/error."""
        result = None
        buf = ""
        for raw in resp.iter_lines(decode_unicode=True):
            if raw and raw.startswith("data: "):
                buf = raw[6:]
            elif raw and buf:
                # Continuation of a multi-line data field
                buf += raw
            elif not raw and buf:
                # Blank line = end of SSE event, try to parse buffered data
                try:
                    parsed = json.loads(buf)
                    if "result" in parsed or "error" in parsed:
                        result = parsed
                except json.JSONDecodeError:
                    pass
                buf = ""
        # Handle final buffered data if stream ends without trailing blank line
        if buf:
            try:
                parsed = json.loads(buf)
                if "result" in parsed or "error" in parsed:
                    result = parsed
            except json.JSONDecodeError:
                pass
        if result is None:
            return {"error": {"code": -1, "message": "No result in SSE stream"}}
        return result

    def initialize(self) -> dict:
        payload = {
            "jsonrpc": "2.0", "id": self._next_id(),
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "deploy-duty", "version": "1.0"},
            },
        }
        data = self._post(payload, timeout=30)
        # Send initialized notification (fire-and-forget)
        try:
            requests.post(
                self.url, headers=self._headers(),
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
                timeout=10,
            )
        except Exception:
            pass
        return data

    def call_tool(self, name: str, arguments: dict, timeout: int = 300) -> dict:
        payload = {
            "jsonrpc": "2.0", "id": self._next_id(),
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
        return self._post(payload, timeout=timeout)


# ── Response parsing ────────────────────────────────────────────────────────

def extract_text(mcp_resp: dict) -> str:
    """Pull all text content from an MCP tools/call response."""
    if not mcp_resp:
        return ""
    if "error" in mcp_resp:
        err = mcp_resp["error"]
        print(f"  MCP error: {err.get('message', err)}", file=sys.stderr)
        return ""
    content = mcp_resp.get("result", {}).get("content", [])
    return "\n".join(c["text"] for c in content if c.get("type") == "text")


def parse_entries(text: str) -> list[dict]:
    """Parse log entries from MCP dedup response text."""
    if not text.strip():
        return []
    # Try as JSON array or object with a list field
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("lines", "entries", "results", "data", "logs"):
                if isinstance(data.get(key), list):
                    return data[key]
            return [data]
    except json.JSONDecodeError:
        pass
    # Try JSONL
    entries = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return entries


# ── Normalization ───────────────────────────────────────────────────────────

def normalize(msg: str) -> str:
    """Normalize a log message by stripping variable parts."""
    # UUIDs (v1–v5)
    msg = re.sub(
        r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-'
        r'[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', '<UUID>', msg)
    # Hex hashes: MongoDB ObjectIDs (24), short SHAs (7-12), long SHAs (40)
    msg = re.sub(r'\b[0-9a-fA-F]{24}\b', '<HEX>', msg)
    msg = re.sub(r'\b[0-9a-fA-F]{40}\b', '<HEX>', msg)
    msg = re.sub(r'\b[0-9a-f]{7,12}\b', '<HEX>', msg)
    # Memory addresses
    msg = re.sub(r'0x[0-9a-fA-F]+', '<ADDR>', msg)
    # IP addresses with optional port
    msg = re.sub(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d+)?', '<IP>', msg)
    # ISO timestamps
    msg = re.sub(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.\dZ+:\-]*', '<TS>', msg)
    # Durations with units (e.g., 1523ms, 3.2s, 500µs)
    msg = re.sub(r'\b\d+(\.\d+)?\s*(ms|µs|us|ns|s|sec|seconds|minutes|min)\b', '<DUR>', msg)
    # Large numeric IDs (6+ digits)
    msg = re.sub(r'\b\d{6,}\b', '<ID>', msg)
    # Labeled IDs (e.g., "request ID: 3", "id: 42")
    msg = re.sub(r'(?i)\b(id|ID|Id)[:\s]+\d+\b', r'\1: <ID>', msg)
    # URL paths with numeric segments (e.g., /users/42/orders)
    msg = re.sub(r'(/[a-zA-Z_-]+)/\d+', r'\1/<ID>', msg)
    # Kubernetes pod suffixes
    msg = re.sub(r'-[a-z0-9]{5,10}-[a-z0-9]{5}\b', '-<POD>', msg)
    # Collapse whitespace
    msg = re.sub(r'\s+', ' ', msg).strip()
    return msg


def short_app(app: str) -> str:
    """Shorten app names for display."""
    for pfx in ("apify-infinite-daemons-", "apify-app-daemons-"):
        if app.startswith(pfx) and len(app) > len(pfx):
            return app[len(pfx):]
    return app


# ── Processing ──────────────────────────────────────────────────────────────

def process_entries(entries: list[dict]) -> tuple[dict, int]:
    """Normalize, group, and count log entries.

    Returns (groups_dict, total_error_count).
    groups_dict keys are "{app}::{normalized_msg}" and values are dicts with
    count, app, message, detail.
    """
    groups: dict[str, dict] = {}
    total = 0

    for entry in entries:
        # Unwrap {"log": {...}, "count": N} structure from MCP
        if "log" in entry and isinstance(entry["log"], dict):
            log = entry["log"]
            if "count" not in log and "count" in entry:
                log["count"] = entry["count"]
            entry = log

        count = entry.get("count", entry.get("_count", 1))
        if isinstance(count, str):
            try:
                count = int(count)
            except ValueError:
                count = 1
        total += count

        # Parse _line
        raw = entry.get("_line", entry.get("line", ""))
        if isinstance(raw, str) and raw.strip():
            try:
                ld = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                ld = {"msg": raw}
        elif isinstance(raw, dict):
            ld = raw
        else:
            ld = {}

        # Fall back to entry-level fields
        if not ld:
            ld = entry

        msg = (ld.get("msg") or ld.get("message") or ld.get("error")
               or str(ld)[:200])
        app = (entry.get("_app") or entry.get("app")
               or ld.get("app") or "unknown")

        # Extract exception info for grouping and display
        exc_raw = ld.get("exception", "")
        if isinstance(exc_raw, dict):
            exc_name = exc_raw.get("name") or exc_raw.get("type") or ""
            exc_message = exc_raw.get("message") or ""
        else:
            exc_name = str(exc_raw) if exc_raw else ""
            exc_message = ""

        # Extract detail info — include exception message, not just name
        details = []
        if exc_name:
            details.append(f"exception: {exc_name}")
        if exc_message:
            details.append(f"message: {exc_message[:300]}")

        # Additional useful fields
        action_key = ld.get("body", {}).get("actionKey") if isinstance(ld.get("body"), dict) else None
        if action_key:
            details.append(f"actionKey: {action_key}")

        for f in ("errorMessage", "error_message", "collection",
                   "statusCode", "status_code", "errorCode", "error_code"):
            v = ld.get(f)
            if v and str(v) != str(msg):
                if isinstance(v, dict):
                    v = v.get("name") or v.get("message") or json.dumps(v)
                details.append(f"{f}: {v}")

        # Build grouping key — always include exception message when present
        # so that same top-level msg with different underlying errors stay separate
        norm = normalize(str(msg))
        if exc_message:
            key = f"{app}::{norm}::{normalize(exc_message)}"
        elif exc_name:
            key = f"{app}::{norm}::{normalize(exc_name)}"
        else:
            key = f"{app}::{norm}"

        if key not in groups:
            # Store full raw _line as sample for context
            if isinstance(raw, str) and raw.strip():
                sample = raw
            elif isinstance(raw, dict):
                sample = json.dumps(raw)
            else:
                sample = json.dumps(ld) if ld else None
            groups[key] = {
                "count": 0,
                "app": app,
                "message": str(msg)[:300],
                "detail": "; ".join(details)[:500] if details else None,
                "sample": sample,
            }
        groups[key]["count"] += count

    return groups, total


# ── Classification ──────────────────────────────────────────────────────────

def classify(today_g: dict, week_g: dict, days_back: int = 7, min_count: int = 1):
    """Classify error patterns into new, recurring, resolved."""
    tk, wk = set(today_g), set(week_g)

    new = sorted(
        [{"today": today_g[k]["count"], "app_tag": short_app(today_g[k]["app"]),
          "message": today_g[k]["message"], "detail": today_g[k]["detail"],
          "sample": today_g[k].get("sample")}
         for k in tk - wk if today_g[k]["count"] >= min_count],
        key=lambda x: -x["today"],
    )

    recurring = []
    for k in sorted(tk & wk, key=lambda k: -today_g[k]["count"]):
        tc, wc = today_g[k]["count"], week_g[k]["count"]
        if tc < min_count:
            continue
        avg = wc / days_back
        if tc > avg * 2:
            trend = "spike"
        elif tc < avg * 0.5:
            trend = "lower"
        else:
            trend = "normal"
        recurring.append({
            "today": tc, "lastweek": wc, "trend": trend,
            "app_tag": short_app(today_g[k]["app"]),
            "message": today_g[k]["message"],
            "detail": today_g[k]["detail"],
            "sample": today_g[k].get("sample"),
        })

    resolved = sorted(
        [{"lastweek": week_g[k]["count"], "app_tag": short_app(week_g[k]["app"]),
          "message": week_g[k]["message"], "detail": week_g[k]["detail"],
          "sample": week_g[k].get("sample")}
         for k in wk - tk if week_g[k]["count"] >= min_count],
        key=lambda x: -x["lastweek"],
    )

    return new, recurring, resolved


# ── HTML Generation ─────────────────────────────────────────────────────────

def _f(n: int) -> str:
    """Format number with commas."""
    return f"{n:,}"


def _e(s) -> str:
    """HTML-escape."""
    return escape(str(s)) if s else ""


CSS = """\
  :root {
    --bg: #0f1117; --surface: #161922; --surface2: #1c1f2e;
    --border: #2a2d3e; --text: #e1e4ed; --text-dim: #8b8fa3;
    --accent: #6c72ff; --accent-dim: #6c72ff22;
    --red: #ff5c5c; --red-dim: #ff5c5c18;
    --orange: #ff9f43; --orange-dim: #ff9f4318;
    --green: #2ed573; --green-dim: #2ed57318;
    --yellow: #ffc312; --yellow-dim: #ffc31218;
    --blue: #54a0ff; --blue-dim: #54a0ff18;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif;
    background: var(--bg); color: var(--text); line-height: 1.6; padding: 24px;
  }
  .header { display:flex; align-items:center; justify-content:space-between; margin-bottom:32px; flex-wrap:wrap; gap:16px; }
  .header h1 { font-size:24px; font-weight:700; letter-spacing:-0.5px; }
  .header h1 span { color: var(--accent); }
  .header-meta { display:flex; gap:16px; align-items:center; flex-wrap:wrap; }
  .badge { display:inline-flex; align-items:center; gap:6px; padding:5px 12px; border-radius:20px; font-size:12px; font-weight:600; text-transform:uppercase; letter-spacing:0.5px; }
  .badge-staging { background:var(--blue-dim); color:var(--blue); border:1px solid var(--blue); }
  .badge-prod { background:var(--orange-dim); color:var(--orange); border:1px solid var(--orange); }
  .badge-new { background:var(--red-dim); color:var(--red); border:1px solid var(--red); }
  .badge-resolved { background:var(--green-dim); color:var(--green); border:1px solid var(--green); }
  .badge-recurring { background:var(--yellow-dim); color:var(--yellow); border:1px solid var(--yellow); }
  .badge-dot { width:6px; height:6px; border-radius:50%; display:inline-block; }
  .badge-staging .badge-dot { background:var(--blue); }
  .badge-prod .badge-dot { background:var(--orange); }
  .stats-row { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:16px; margin-bottom:32px; }
  .stat-card { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:20px; }
  .stat-card .label { font-size:12px; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.5px; margin-bottom:6px; }
  .stat-card .value { font-size:32px; font-weight:700; letter-spacing:-1px; }
  .stat-card .sub { font-size:12px; color:var(--text-dim); margin-top:2px; }
  .stat-card.red .value { color:var(--red); }
  .stat-card.green .value { color:var(--green); }
  .stat-card.orange .value { color:var(--orange); }
  .stat-card.blue .value { color:var(--blue); }
  .stat-card.accent .value { color:var(--accent); }
  .tabs { display:flex; gap:0; margin-bottom:0; border-bottom:1px solid var(--border); }
  .tab { padding:12px 24px; cursor:pointer; font-size:14px; font-weight:600; color:var(--text-dim); border-bottom:2px solid transparent; transition:all 0.2s; user-select:none; }
  .tab:hover { color:var(--text); }
  .tab.active { color:var(--accent); border-bottom-color:var(--accent); }
  .tab-content { display:none; }
  .tab-content.active { display:block; }
  .section { margin-top:28px; margin-bottom:32px; }
  .section-header { display:flex; align-items:center; gap:12px; margin-bottom:16px; }
  .section-header h2 { font-size:18px; font-weight:700; }
  .section-header .count { background:var(--surface2); border:1px solid var(--border); padding:2px 10px; border-radius:12px; font-size:12px; font-weight:600; color:var(--text-dim); }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  thead th { text-align:left; padding:10px 14px; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--text-dim); background:var(--surface); border-bottom:1px solid var(--border); position:sticky; top:0; z-index:1; }
  thead th:first-child { border-radius:8px 0 0 0; }
  thead th:last-child { border-radius:0 8px 0 0; }
  tbody td { padding:12px 14px; border-bottom:1px solid var(--border); vertical-align:top; }
  tbody tr:hover { background:var(--surface); }
  tbody tr:last-child td { border-bottom:none; }
  .table-wrap { background:var(--surface); border:1px solid var(--border); border-radius:12px; overflow:hidden; }
  .table-wrap table thead th { background:var(--surface2); }
  .app-tag { display:inline-block; padding:2px 8px; border-radius:4px; font-size:11px; font-weight:600; font-family:'SF Mono','Fira Code',monospace; background:var(--accent-dim); color:var(--accent); white-space:nowrap; }
  .msg-cell { font-family:'SF Mono','Fira Code',monospace; font-size:12px; line-height:1.5; word-break:break-word; max-width:600px; }
  .msg-cell .detail { color:var(--text-dim); font-size:11px; margin-top:4px; }
  .count-cell { font-weight:700; font-variant-numeric:tabular-nums; white-space:nowrap; }
  .count-today { color:var(--text); }
  .count-week { color:var(--text-dim); }
  .trend-up { color:var(--red); }
  .trend-down { color:var(--green); }
  .trend-same { color:var(--text-dim); }
  .trend-new { color:var(--red); font-weight:700; }
  .empty-state { text-align:center; padding:48px 24px; color:var(--text-dim); }
  .empty-state .icon { font-size:40px; margin-bottom:12px; }
  .empty-state p { font-size:14px; }
  .callout { background:var(--surface); border:1px solid var(--border); border-left:3px solid var(--accent); border-radius:8px; padding:16px 20px; margin-bottom:20px; font-size:13px; line-height:1.7; }
  .callout.warning { border-left-color:var(--orange); }
  .callout.danger { border-left-color:var(--red); }
  .callout.success { border-left-color:var(--green); }
  .callout strong { color:var(--text); }
  .filter-bar { display:flex; align-items:center; gap:12px; margin-bottom:20px; flex-wrap:wrap; }
  .filter-bar input { background:var(--surface); border:1px solid var(--border); border-radius:8px; padding:8px 14px; color:var(--text); font-size:13px; width:300px; outline:none; transition:border-color 0.2s; }
  .filter-bar input:focus { border-color:var(--accent); }
  .filter-bar input::placeholder { color:var(--text-dim); }
  @media (max-width:768px) { body{padding:12px;} .stats-row{grid-template-columns:repeat(2,1fr);} .msg-cell{max-width:300px;} .filter-bar input{width:100%;} }
"""

JS = """\
function switchTab(tab) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.getElementById('tab-' + tab).classList.add('active');
  document.querySelectorAll('.tab').forEach(t => {
    if ((tab === 'staging' && t.textContent === 'Staging') ||
        (tab === 'production' && t.textContent === 'Production')) {
      t.classList.add('active');
    }
  });
}
function filterTable(tableId, query) {
  const table = document.getElementById(tableId);
  if (!table) return;
  const rows = table.querySelectorAll('tbody tr');
  const q = query.toLowerCase();
  rows.forEach(row => {
    const text = row.textContent.toLowerCase();
    row.style.display = text.includes(q) ? '' : 'none';
  });
}
"""


def _detail_html(detail: str | None, style: str = "") -> str:
    if not detail:
        return ""
    s = f' style="{style}"' if style else ""
    return f'\n              <div class="detail"{s}>{_e(detail)}</div>'


def _new_section(env: str, items: list[dict], day_label: str = "Today", env_label: str | None = None) -> str:
    day_lower = day_label.lower()
    if env_label is None:
        env_label = "staging" if env == "staging" else "production"
    count = len(items)
    header = f"""
  <div class="section">
    <div class="section-header">
      <h2>New Errors</h2>
      <span class="badge badge-new">{count} patterns</span>
    </div>"""

    if count == 0:
        return header + f"""
    <div class="empty-state">
      <div class="icon">&#10004;</div>
      <p>No new error patterns{f" on {env_label}" if env_label else ""} {day_lower}.</p>
    </div>
  </div>"""

    rows = ""
    for e in items:
        rows += f"""
          <tr style="background:var(--red-dim);">
            <td class="count-cell trend-new">{_f(e["today"])}</td>
            <td><span class="app-tag">{_e(e["app_tag"])}</span></td>
            <td class="msg-cell">{_e(e["message"])}{_detail_html(e["detail"])}</td>
            <td><span class="badge" style="background:var(--red-dim);color:var(--red);border:1px solid var(--red);font-size:11px;">ERROR</span></td>
          </tr>"""

    return header + f"""
    <div class="callout danger">
      <strong>{count} new error pattern{"s" if count != 1 else ""}</strong> appeared{f" in {env_label}" if env_label else ""} {day_lower} that were not seen in the previous week.
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th style="width:80px">{day_label}</th>
          <th style="width:180px">App</th>
          <th>Error Message</th>
          <th style="width:120px">Severity</th>
        </tr></thead>
        <tbody>{rows}
        </tbody>
      </table>
    </div>
  </div>"""


def _recurring_section(env: str, items: list[dict], day_label: str = "Today") -> str:
    table_id = f"recurring-{env}"
    count = len(items)
    spikes = [e for e in items if e["trend"] == "spike"]

    header = f"""
  <div class="section">
    <div class="section-header">
      <h2>Recurring Errors</h2>
      <span class="badge badge-recurring">{count} patterns</span>
    </div>"""

    if count == 0:
        return header + """
    <div class="empty-state">
      <div class="icon">&#9679;</div>
      <p>No recurring error patterns.</p>
    </div>
  </div>"""

    callout = ""
    if spikes:
        s = spikes[0]
        callout = f"""
    <div class="callout warning">
      <strong>Spike detected:</strong> <code>{_e(s["message"][:80])}</code> jumped from <strong>{_f(s["lastweek"])}</strong> last week to <strong>{_f(s["today"])}</strong> {day_label.lower()}.
    </div>"""

    rows = ""
    for e in items:
        if e["trend"] == "spike":
            rows += f"""
          <tr style="background:var(--orange-dim);">
            <td class="count-cell trend-up" style="font-weight:900">{_f(e["today"])}</td>
            <td class="count-cell count-week">{_f(e["lastweek"])}</td>
            <td class="trend-up">&#9650; spike</td>
            <td><span class="app-tag">{_e(e["app_tag"])}</span></td>
            <td class="msg-cell">{_e(e["message"])}{_detail_html(e["detail"], "color:var(--orange);")}</td>
          </tr>"""
        elif e["trend"] == "lower":
            rows += f"""
          <tr>
            <td class="count-cell count-today">{_f(e["today"])}</td>
            <td class="count-cell count-week">{_f(e["lastweek"])}</td>
            <td class="trend-down">&#9660; lower</td>
            <td><span class="app-tag">{_e(e["app_tag"])}</span></td>
            <td class="msg-cell">{_e(e["message"])}{_detail_html(e["detail"])}</td>
          </tr>"""
        else:
            rows += f"""
          <tr>
            <td class="count-cell count-today">{_f(e["today"])}</td>
            <td class="count-cell count-week">{_f(e["lastweek"])}</td>
            <td class="trend-same">&#9679; normal</td>
            <td><span class="app-tag">{_e(e["app_tag"])}</span></td>
            <td class="msg-cell">{_e(e["message"])}{_detail_html(e["detail"])}</td>
          </tr>"""

    return header + callout + f"""
    <div class="filter-bar">
      <input type="text" placeholder="Filter errors..." oninput="filterTable('{table_id}', this.value)">
    </div>
    <div class="table-wrap">
      <table id="{table_id}">
        <thead><tr>
          <th style="width:80px">{day_label}</th>
          <th style="width:90px">Last Week</th>
          <th style="width:60px">Trend</th>
          <th style="width:180px">App</th>
          <th>Error Message</th>
        </tr></thead>
        <tbody>{rows}
        </tbody>
      </table>
    </div>
  </div>"""


def _resolved_section(env: str, items: list[dict], day_label: str = "Today", env_label: str | None = None) -> str:
    table_id = f"resolved-{env}"
    total = len(items)
    shown_items = items[:MAX_RESOLVED_SHOWN]
    shown = len(shown_items)
    if env_label is None:
        env_label = "staging" if env == "staging" else "production"

    header = f"""
  <div class="section">
    <div class="section-header">
      <h2>Errors Stopped {day_label}</h2>
      <span class="badge badge-resolved">{total} patterns</span>
    </div>"""

    if total == 0:
        return header + f"""
    <div class="empty-state">
      <div class="icon">&#9679;</div>
      <p>No resolved error patterns{f" on {env_label}" if env_label else ""}.</p>
    </div>
  </div>"""

    top3 = ", ".join(f'"{e["message"][:50]}"' for e in shown_items[:3])
    callout = f"""
    <div class="callout success">
      <strong>Major improvements:</strong> {total} error patterns from last week are gone, including {top3}.
    </div>"""

    rows = ""
    for e in shown_items:
        rows += f"""
          <tr>
            <td class="count-cell" style="color:var(--green)">{_f(e["lastweek"])}</td>
            <td><span class="app-tag">{_e(e["app_tag"])}</span></td>
            <td class="msg-cell">{_e(e["message"])}{_detail_html(e["detail"])}</td>
          </tr>"""

    truncation = ""
    if total > MAX_RESOLVED_SHOWN:
        truncation = f"""
    <div style="text-align:center;padding:16px;color:var(--text-dim);font-size:12px;">
      Showing top {shown} of {total} resolved patterns.
    </div>"""

    return header + callout + f"""
    <div class="filter-bar">
      <input type="text" placeholder="Filter errors..." oninput="filterTable('{table_id}', this.value)">
    </div>
    <div class="table-wrap">
      <table id="{table_id}">
        <thead><tr>
          <th style="width:100px">Last Week</th>
          <th style="width:200px">App</th>
          <th>Error Message</th>
        </tr></thead>
        <tbody>{rows}
        </tbody>
      </table>
    </div>{truncation}
  </div>"""


def _env_tab(env: str, env_data: dict, active: bool, day_label: str = "Today") -> str:
    """Generate a full environment tab (staging or production)."""
    d = env_data
    active_cls = " active" if active else ""
    color = "var(--blue)" if env == "staging" else "var(--orange)"

    stats = f"""
  <div class="stats-row" style="margin-top:24px;">
    <div class="stat-card">
      <div class="label">{day_label} Errors</div>
      <div class="value" style="color:{color};">{_f(d["today_count"])}</div>
      <div class="sub">{d["today_unique"]} unique patterns</div>
    </div>
    <div class="stat-card">
      <div class="label">Last Week Errors</div>
      <div class="value" style="color:var(--text-dim);">{_f(d["lastweek_count"])}</div>
      <div class="sub">{d["lastweek_unique"]} unique patterns</div>
    </div>
    <div class="stat-card red">
      <div class="label">New {day_label}</div>
      <div class="value">{d["new_count"]}</div>
      <div class="sub">{d["new_count"]} new error types</div>
    </div>
    <div class="stat-card">
      <div class="label">Stopped {day_label}</div>
      <div class="value" style="color:var(--green);">{d["resolved_count"]}</div>
      <div class="sub">Patterns from last week gone</div>
    </div>
  </div>"""

    new_html = _new_section(env, d["new"], day_label)
    recurring_html = _recurring_section(env, d["recurring"], day_label)
    resolved_html = _resolved_section(env, d["resolved"], day_label)

    return f"""
<div id="tab-{env}" class="tab-content{active_cls}">
{stats}
{new_html}
{recurring_html}
{resolved_html}
</div>"""


def generate_html(data: dict) -> str:
    s = data["summary"]
    day_label = data.get("day_label", "Today")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Mezmo Error Dashboard &mdash; {_e(data["date_display"])}</title>
<style>
{CSS}
</style>
</head>
<body>

<div class="header">
  <h1><span>Mezmo</span> Error Dashboard</h1>
  <div class="header-meta">
    <span class="badge badge-staging"><span class="badge-dot"></span> Staging</span>
    <span class="badge badge-prod"><span class="badge-dot"></span> Production</span>
    <span style="color:var(--text-dim);font-size:13px;">{_e(data["date_display"])} &middot; {_e(data["time_range_display"])}</span>
  </div>
</div>

<div class="stats-row">
  <div class="stat-card red">
    <div class="label">New Error Patterns</div>
    <div class="value">{_f(s["new_total"])}</div>
    <div class="sub">{_f(s["new_staging"])} staging &middot; {_f(s["new_production"])} production</div>
  </div>
  <div class="stat-card orange">
    <div class="label">Recurring Patterns</div>
    <div class="value">{_f(s["recurring_total"])}</div>
    <div class="sub">{_f(s["recurring_staging"])} staging &middot; {_f(s["recurring_production"])} production</div>
  </div>
  <div class="stat-card green">
    <div class="label">Resolved / Gone</div>
    <div class="value">{_f(s["resolved_total"])}</div>
    <div class="sub">{_f(s["resolved_staging"])} staging &middot; {_f(s["resolved_production"])} production</div>
  </div>
  <div class="stat-card blue">
    <div class="label">Total Errors {day_label}</div>
    <div class="value">{_f(s["today_total"])}</div>
    <div class="sub">{_f(s["today_staging"])} staging &middot; {_f(s["today_production"])} production</div>
  </div>
  <div class="stat-card accent">
    <div class="label">Total Last Week</div>
    <div class="value">{_f(s["lastweek_total"])}</div>
    <div class="sub">{_f(s["lastweek_staging"])} staging &middot; {_f(s["lastweek_production"])} production</div>
  </div>
</div>

<div class="tabs">
  <div class="tab active" onclick="switchTab('staging')">Staging</div>
  <div class="tab" onclick="switchTab('production')">Production</div>
</div>

{_env_tab("staging", data["staging"], active=True, day_label=day_label)}
{_env_tab("production", data["production"], active=False, day_label=day_label)}

<div style="text-align:center;padding:32px 0 16px;color:var(--text-dim);font-size:12px;border-top:1px solid var(--border);margin-top:24px;">
  Generated from Mezmo logs &middot; Apps: {", ".join(APPS)}
  <br>Time range: {_e(data["date_display"])} {_e(data["time_range_display"])}
</div>

<script>
{JS}
</script>
</body>
</html>"""


def generate_single_html(data: dict) -> str:
    """Generate an HTML dashboard for a single-query result (no staging/production tabs)."""
    s = data["summary"]
    day_label = data.get("day_label", "Today")

    new_html = _new_section("main", data["new"], day_label, env_label="")
    recurring_html = _recurring_section("main", data["recurring"], day_label)
    resolved_html = _resolved_section("main", data["resolved"], day_label, env_label="")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Mezmo Error Report &mdash; {_e(data["date_display"])}</title>
<style>
{CSS}
</style>
</head>
<body>

<div class="header">
  <h1><span>Mezmo</span> Error Report</h1>
  <div class="header-meta">
    <span style="color:var(--text-dim);font-size:13px;">{_e(data["date_display"])} &middot; {_e(data["time_range_display"])}</span>
  </div>
</div>

<div style="color:var(--text-dim);font-size:12px;margin-bottom:24px;">
  Query: <code style="color:var(--accent);font-size:11px;">{_e(data.get("query", ""))}</code>
</div>

<div class="stats-row">
  <div class="stat-card red">
    <div class="label">New Patterns</div>
    <div class="value">{s["new_count"]}</div>
  </div>
  <div class="stat-card orange">
    <div class="label">Recurring</div>
    <div class="value">{s["recurring_count"]}</div>
  </div>
  <div class="stat-card green">
    <div class="label">Resolved</div>
    <div class="value">{s["resolved_count"]}</div>
  </div>
  <div class="stat-card blue">
    <div class="label">{day_label} Total</div>
    <div class="value">{_f(s["today_total"])}</div>
    <div class="sub">{s["today_unique"]} unique patterns</div>
  </div>
  <div class="stat-card accent">
    <div class="label">Last Week Total</div>
    <div class="value">{_f(s["lastweek_total"])}</div>
    <div class="sub">{s["lastweek_unique"]} unique patterns</div>
  </div>
</div>

{new_html}
{recurring_html}
{resolved_html}

<div style="text-align:center;padding:32px 0 16px;color:var(--text-dim);font-size:12px;border-top:1px solid var(--border);margin-top:24px;">
  Generated from Mezmo logs
  <br>Time range: {_e(data["date_display"])} {_e(data["time_range_display"])}
</div>

<script>
{JS}
</script>
</body>
</html>"""


# ── Reusable entry point (for Apify actor) ───────────────────────────────

def run_deploy_duty(
    query: str,
    mcp_token: str,
    mcp_url: str = "https://mcp.mezmo.com/mcp",
    debug: bool = False,
    day: str = "today",
    days_back: int = 7,
    min_count: int = 1,
) -> dict:
    """Run deploy-duty analysis for a single Mezmo query.

    Returns a JSON-serializable dict with today-vs-last-week error comparison.
    """
    # ── Time ranges ─────────────────────────────────────────────────────
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if day == "yesterday":
        day_end = today_start                          # end of yesterday = start of today
        day_start = today_start - timedelta(days=1)    # start of yesterday
    elif day == "last24h":
        day_end = now                                  # up to current time
        day_start = now - timedelta(hours=24)          # 24 hours ago
    else:  # "today" (default)
        day_end = now                                  # up to current time
        day_start = today_start                        # start of today

    baseline_start = day_start - timedelta(days=days_back)

    day_end_iso = day_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    day_start_iso = day_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    baseline_iso = baseline_start.strftime("%Y-%m-%dT%H:%M:%SZ")

    date_display = day_start.strftime("%b %d, %Y")
    if day == "yesterday":
        time_display = (
            f"{day_start.strftime('%b %d')} (full day) vs. "
            f"{baseline_start.strftime('%b %d')}\u2013{(day_start - timedelta(days=1)).strftime('%b %d')}"
        )
    elif day == "last24h":
        time_display = (
            f"Last 24h ({day_start.strftime('%b %d %H:%M')}\u2013{day_end.strftime('%H:%M')} UTC) vs. "
            f"{baseline_start.strftime('%b %d')}\u2013{(day_start - timedelta(days=1)).strftime('%b %d')}"
        )
    else:
        time_display = (
            f"00:00 \u2013 {now.strftime('%H:%M')} UTC vs. "
            f"{baseline_start.strftime('%b %d')}\u2013{(day_start - timedelta(days=1)).strftime('%b %d')}"
        )

    # ── Single MCP query with a fresh session ──────────────────────────
    ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"
    MIN_CHUNK = timedelta(minutes=5)

    def _query_one(label: str, start: datetime, end: datetime):
        """Run one Mezmo dedup query with a fresh MCP client/session.

        Returns (resp, too_large). Raises RuntimeError on any error other
        than the server's "Query Too Large" response, which is signalled
        via too_large=True so the caller can split the range.
        """
        client = MCPClient(mcp_url, mcp_token)
        init = client.initialize()
        if "error" in init:
            raise RuntimeError(f"{label}: MCP init failed: {init['error']}")

        args = {
            "fromTime": start.strftime(ISO_FMT),
            "toTime": end.strftime(ISO_FMT),
            "query": query,
        }
        resp = client.call_tool("deduplicate_logs_time_range", args)

        if "error" in resp:
            raise RuntimeError(f"{label}: MCP error: {resp['error']}")

        text = extract_text(resp)
        if resp.get("result", {}).get("isError"):
            if "query too large" in text.lower():
                return resp, True
            raise RuntimeError(f"{label}: MCP tool error: {text[:500]}")

        entries = parse_entries(text)
        print(f"  {label}: {len(entries)} deduplicated entries")
        if debug:
            debug_path = SCRIPT_DIR / f"debug_{label}.json"
            debug_path.write_text(json.dumps(resp, indent=2, default=str))
            print(f"    debug saved to {debug_path}")
        return resp, False

    def _query_chunked(label: str, start: datetime, end: datetime) -> list:
        """Query a time range; recursively split in half on 'Query Too Large'.

        Each MCP call uses its own client/session. Raises RuntimeError if a
        chunk at or below MIN_CHUNK is still too large — rather than silently
        returning zero results.
        """
        resp, too_large = _query_one(label, start, end)
        if not too_large:
            return [resp]
        duration = end - start
        if duration <= MIN_CHUNK:
            raise RuntimeError(
                f"{label}: chunk {start.isoformat()}..{end.isoformat()} "
                f"({duration}) still exceeds Mezmo's 1M log-line limit after "
                f"splitting — narrow your query with more filters"
            )
        mid = start + duration / 2
        print(f"  {label}: too large, splitting {duration} into halves")
        return (
            _query_chunked(f"{label}.a", start, mid)
            + _query_chunked(f"{label}.b", mid, end)
        )

    # ── Query errors (today + last week in parallel) ───────────────────
    print(f"Querying Mezmo ({day_start_iso[:10]} + last {days_back} days)...")
    with ThreadPoolExecutor(max_workers=2) as pool:
        future_today = pool.submit(_query_chunked, "today", day_start, day_end)
        future_week = pool.submit(_query_chunked, "lastweek", baseline_start, day_start)
        # Failures propagate out — zero results is a real bug, not an OK outcome.
        raw_responses = {
            "today": future_today.result(),
            "lastweek": future_week.result(),
        }

    # ── Process (merge entries from all chunk responses) ────────────────
    print("Processing results...")

    def _process(key):
        all_entries = []
        for resp in raw_responses.get(key, []):
            text = extract_text(resp)
            all_entries.extend(parse_entries(text))
        return process_entries(all_entries)

    today_g, today_total = _process("today")
    week_g, week_total = _process("lastweek")

    new, recurring, resolved = classify(today_g, week_g, days_back=days_back, min_count=min_count)

    # ── Day label for display ────────────────────────────────────────────
    day_label = {"yesterday": "Yesterday", "last24h": "Last 24h"}.get(day, "Today")

    # ── Build JSON output ──────────────────────────────────────────────
    data = {
        "date_display": date_display,
        "time_range_display": time_display,
        "day_label": day_label,
        "query": query,
        "summary": {
            "today_total": today_total,
            "today_unique": len(today_g),
            "lastweek_total": week_total,
            "lastweek_unique": len(week_g),
            "new_count": len(new),
            "recurring_count": len(recurring),
            "resolved_count": len(resolved),
        },
        "new": new,
        "recurring": recurring,
        "resolved": resolved,
    }

    print(f"  New: {len(new)}, Recurring: {len(recurring)}, Resolved: {len(resolved)}")
    return data


# ── Main (standalone CLI) ────────────────────────────────────────────────

def open_file(path: Path):
    """Open a file in the default browser."""
    s = platform.system()
    if s == "Darwin":
        subprocess.run(["open", str(path)])
    elif s == "Linux":
        subprocess.run(["xdg-open", str(path)])
    else:
        subprocess.run(["start", str(path)], shell=True)


def main():
    if not MCP_TOKEN:
        sys.exit(
            "MEZMO_MCP_TOKEN not set.\n"
            "Usage: MEZMO_MCP_TOKEN=sts_xxx python3 deploy_duty.py [--debug]"
        )

    mcp = MCPClient(MCP_URL, MCP_TOKEN)

    # ── Initialize ──────────────────────────────────────────────────────
    print("Initializing MCP session...")
    init = mcp.initialize()
    if "error" in init:
        sys.exit(f"MCP init failed: {init['error']}")
    server = init.get("result", {}).get("serverInfo", {})
    print(f"  Connected to {server.get('name', '?')} v{server.get('version', '?')}")

    # ── Time ranges ─────────────────────────────────────────────────────
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_ago = today_start - timedelta(days=7)

    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    today_iso = today_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    week_iso = week_ago.strftime("%Y-%m-%dT%H:%M:%SZ")

    date_display = now.strftime("%b %d, %Y")
    time_display = (
        f"00:00 \u2013 {now.strftime('%H:%M')} UTC vs. "
        f"{week_ago.strftime('%b %d')}\u2013{(today_start - timedelta(days=1)).strftime('%d')}"
    )

    # ── Helper: query a single time range ─────────────────────────────────
    def _query_range(label, from_iso, to_iso, q):
        args = {"fromTime": from_iso, "toTime": to_iso, "query": q}
        resp = mcp.call_tool("deduplicate_logs_time_range", args)
        text = extract_text(resp)
        if "query too large" in text.lower() or (
            resp.get("result", {}).get("isError") and "query too large" in text.lower()
        ):
            return label, resp, True
        entries = parse_entries(text)
        print(f"  {label}: {len(entries)} deduplicated entries")
        if DEBUG:
            debug_path = SCRIPT_DIR / f"debug_{label}.json"
            debug_path.write_text(json.dumps(resp, indent=2, default=str))
            print(f"    debug saved to {debug_path}")
        return label, resp, False

    def _daily_chunks(start: datetime, end: datetime):
        cursor = start
        while cursor < end:
            chunk_end = min(cursor + timedelta(days=1), end)
            yield cursor, chunk_end
            cursor = chunk_end

    def _query_chunked(label, start: datetime, end: datetime, q):
        start_iso = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = end.strftime("%Y-%m-%dT%H:%M:%SZ")
        _, resp, too_large = _query_range(label, start_iso, end_iso, q)
        if not too_large:
            return [resp]
        chunks = list(_daily_chunks(start, end))
        print(f"  {label}: query too large, splitting into {len(chunks)} daily chunks...")
        chunk_responses = []
        with ThreadPoolExecutor(max_workers=min(len(chunks), 4)) as chunk_pool:
            chunk_futures = {
                chunk_pool.submit(
                    _query_range, f"{label}_d{i}",
                    cs.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    ce.strftime("%Y-%m-%dT%H:%M:%SZ"), q,
                ): i
                for i, (cs, ce) in enumerate(chunks)
            }
            for future in as_completed(chunk_futures):
                i = chunk_futures[future]
                try:
                    _, chunk_resp, chunk_too_large = future.result()
                    if chunk_too_large:
                        print(f"  {label}_d{i}: still too large, skipping", file=sys.stderr)
                    else:
                        chunk_responses.append(chunk_resp)
                except Exception as exc:
                    print(f"  {label}_d{i}: FAILED - {exc}", file=sys.stderr)
        return chunk_responses

    # ── Query errors (4 parallel with auto-chunking) ───────────────────
    print(f"Querying Mezmo ({today_iso[:10]} + last 7 days, staging + prod)...")
    raw_responses = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        task_map = {
            "staging_today":    (today_start, now, STAGING_Q),
            "staging_lastweek": (week_ago, today_start, STAGING_Q),
            "prod_today":       (today_start, now, PROD_Q),
            "prod_lastweek":    (week_ago, today_start, PROD_Q),
        }
        futures = {
            pool.submit(_query_chunked, name, s, e, q): name
            for name, (s, e, q) in task_map.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                raw_responses[name] = future.result()
            except Exception as exc:
                print(f"  {name}: FAILED - {exc}", file=sys.stderr)
                raw_responses[name] = []

    # ── Process (merge entries from all chunk responses) ────────────────
    print("Processing results...")

    def _process(key):
        all_entries = []
        for resp in raw_responses.get(key, []):
            text = extract_text(resp)
            all_entries.extend(parse_entries(text))
        return process_entries(all_entries)

    stg_today_g, stg_today_total = _process("staging_today")
    stg_week_g, stg_week_total = _process("staging_lastweek")
    prod_today_g, prod_today_total = _process("prod_today")
    prod_week_g, prod_week_total = _process("prod_lastweek")

    stg_new, stg_recurring, stg_resolved = classify(stg_today_g, stg_week_g)
    prod_new, prod_recurring, prod_resolved = classify(prod_today_g, prod_week_g)

    # ── Build data structure ────────────────────────────────────────────
    data = {
        "date_display": date_display,
        "time_range_display": time_display,
        "day_label": "Today",
        "summary": {
            "new_total": len(stg_new) + len(prod_new),
            "new_staging": len(stg_new),
            "new_production": len(prod_new),
            "recurring_total": len(stg_recurring) + len(prod_recurring),
            "recurring_staging": len(stg_recurring),
            "recurring_production": len(prod_recurring),
            "resolved_total": len(stg_resolved) + len(prod_resolved),
            "resolved_staging": len(stg_resolved),
            "resolved_production": len(prod_resolved),
            "today_total": stg_today_total + prod_today_total,
            "today_staging": stg_today_total,
            "today_production": prod_today_total,
            "lastweek_total": stg_week_total + prod_week_total,
            "lastweek_staging": stg_week_total,
            "lastweek_production": prod_week_total,
        },
        "staging": {
            "today_count": stg_today_total,
            "today_unique": len(stg_today_g),
            "lastweek_count": stg_week_total,
            "lastweek_unique": len(stg_week_g),
            "new_count": len(stg_new),
            "resolved_count": len(stg_resolved),
            "new": stg_new,
            "recurring": stg_recurring,
            "resolved": stg_resolved,
        },
        "production": {
            "today_count": prod_today_total,
            "today_unique": len(prod_today_g),
            "lastweek_count": prod_week_total,
            "lastweek_unique": len(prod_week_g),
            "new_count": len(prod_new),
            "resolved_count": len(prod_resolved),
            "new": prod_new,
            "recurring": prod_recurring,
            "resolved": prod_resolved,
        },
    }

    if DEBUG:
        data_path = SCRIPT_DIR / "debug_data.json"
        data_path.write_text(json.dumps(data, indent=2, default=str))
        print(f"  Data saved to {data_path}")

    # ── Generate HTML ───────────────────────────────────────────────────
    print("Generating dashboard...")
    html = generate_html(data)
    OUTPUT_FILE.write_text(html)
    print(f"  Written to {OUTPUT_FILE}")

    # ── Open ────────────────────────────────────────────────────────────
    open_file(OUTPUT_FILE)

    # ── Summary ─────────────────────────────────────────────────────────
    sm = data["summary"]
    print("\n── Deploy Duty Summary ──────────────────────────")
    print(f"  Date:       {date_display} ({time_display})")
    print(f"  New:        {sm['new_total']} ({sm['new_staging']} stg / {sm['new_production']} prod)")
    print(f"  Recurring:  {sm['recurring_total']} ({sm['recurring_staging']} stg / {sm['recurring_production']} prod)")
    print(f"  Resolved:   {sm['resolved_total']} ({sm['resolved_staging']} stg / {sm['resolved_production']} prod)")
    print(f"  Today:      {_f(sm['today_total'])} errors ({_f(sm['today_staging'])} stg / {_f(sm['today_production'])} prod)")
    print(f"  Last week:  {_f(sm['lastweek_total'])} errors ({_f(sm['lastweek_staging'])} stg / {_f(sm['lastweek_production'])} prod)")

    stg_spikes = [e for e in stg_recurring if e["trend"] == "spike"]
    prod_spikes = [e for e in prod_recurring if e["trend"] == "spike"]
    if stg_spikes or prod_spikes:
        print("\n  SPIKES:")
        for e in stg_spikes:
            print(f"    [stg] {e['app_tag']}: {e['message'][:60]} ({_f(e['lastweek'])} -> {_f(e['today'])})")
        for e in prod_spikes:
            print(f"    [prod] {e['app_tag']}: {e['message'][:60]} ({_f(e['lastweek'])} -> {_f(e['today'])})")

    if prod_new:
        print("\n  NEW IN PRODUCTION:")
        for e in prod_new[:5]:
            print(f"    {e['app_tag']}: {e['message'][:70]} (x{_f(e['today'])})")

    print("\n  Dashboard opened in browser.")


if __name__ == "__main__":
    main()
