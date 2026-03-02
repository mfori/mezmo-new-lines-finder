#!/usr/bin/env python3
"""Mezmo Deploy Duty — Deterministic Error Analysis.

Calls the Mezmo MCP server directly (no LLM needed) to compare today's
errors against the previous week for a given query, returning structured
JSON results for the Apify actor.
"""

import json
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Missing dependency. Run: pip install requests")

SCRIPT_DIR = Path(__file__).resolve().parent


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
        for raw in resp.iter_lines(decode_unicode=True):
            if raw and raw.startswith("data: "):
                try:
                    parsed = json.loads(raw[6:])
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


def normalize(msg: str) -> str:
    """Normalize a log message by stripping variable parts."""
    msg = re.sub(
        r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-'
        r'[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', '<UUID>', msg)
    msg = re.sub(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d+)?', '<IP>', msg)
    msg = re.sub(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.\dZ+:\-]*', '<TS>', msg)
    msg = re.sub(r'\b\d{6,}\b', '<ID>', msg)
    msg = re.sub(r'-[a-z0-9]{5,10}-[a-z0-9]{5}\b', '-<POD>', msg)
    msg = re.sub(r'\s+', ' ', msg).strip()
    return msg


def short_app(app: str) -> str:
    """Shorten app names for display."""
    for pfx in ("apify-infinite-daemons-", "apify-app-daemons-"):
        if app.startswith(pfx) and len(app) > len(pfx):
            return app[len(pfx):]
    return app


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

        # Extract detail info
        details = []
        for f in ("exception", "errorMessage", "error_message", "collection",
                   "statusCode", "status_code", "errorCode", "error_code"):
            v = ld.get(f)
            if v and str(v) != str(msg):
                if isinstance(v, dict):
                    v = v.get("name") or v.get("message") or json.dumps(v)
                details.append(f"{f}: {v}")

        # Exception name for grouping "Failed to handle request" variants
        exc = ld.get("exception", "")
        if isinstance(exc, dict):
            exc = exc.get("name") or exc.get("type") or ""

        norm = normalize(str(msg))
        if "failed to handle request" in norm.lower() and exc:
            key = f"{app}::{norm}::{normalize(str(exc))}"
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
                "message": str(msg)[:200],
                "detail": "; ".join(details)[:300] if details else None,
                "sample": sample,
            }
        groups[key]["count"] += count

    return groups, total


def classify(today_g: dict, week_g: dict):
    """Classify error patterns into new, recurring, resolved."""
    tk, wk = set(today_g), set(week_g)

    new = sorted(
        [{"today": today_g[k]["count"], "app_tag": short_app(today_g[k]["app"]),
          "message": today_g[k]["message"], "detail": today_g[k]["detail"],
          "sample": today_g[k].get("sample")}
         for k in tk - wk],
        key=lambda x: -x["today"],
    )

    recurring = []
    for k in sorted(tk & wk, key=lambda k: -today_g[k]["count"]):
        tc, wc = today_g[k]["count"], week_g[k]["count"]
        avg = wc / 7
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
         for k in wk - tk],
        key=lambda x: -x["lastweek"],
    )

    return new, recurring, resolved


def run_deploy_duty(
    query: str,
    mcp_token: str,
    mcp_url: str = "https://mcp.mezmo.com/mcp",
    debug: bool = False,
) -> dict:
    """Run deploy-duty analysis for a single Mezmo query.

    Returns a JSON-serializable dict with today-vs-last-week error comparison.
    """
    mcp = MCPClient(mcp_url, mcp_token)

    # Initialize
    print("Initializing MCP session...")
    init = mcp.initialize()
    if "error" in init:
        raise RuntimeError(f"MCP init failed: {init['error']}")
    server = init.get("result", {}).get("serverInfo", {})
    print(f"  Connected to {server.get('name', '?')} v{server.get('version', '?')}")

    # Time ranges
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

    # Query errors (2 parallel: today + last week)
    queries = {
        "today":    {"fromTime": today_iso, "toTime": now_iso,   "query": query},
        "lastweek": {"fromTime": week_iso,  "toTime": today_iso, "query": query},
    }

    results = {}
    print(f"Querying Mezmo ({today_iso[:10]} + last 7 days)...")
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            pool.submit(mcp.call_tool, "deduplicate_logs_time_range", args): name
            for name, args in queries.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                resp = future.result()
                results[name] = resp
                text = extract_text(resp)
                entries = parse_entries(text)
                print(f"  {name}: {len(entries)} deduplicated entries")
                if debug:
                    debug_path = SCRIPT_DIR / f"debug_{name}.json"
                    debug_path.write_text(json.dumps(resp, indent=2, default=str))
                    print(f"    debug saved to {debug_path}")
            except Exception as exc:
                print(f"  {name}: FAILED - {exc}", file=sys.stderr)
                results[name] = {"result": {"content": []}}

    # Process
    print("Processing results...")

    def _process(key):
        text = extract_text(results[key])
        entries = parse_entries(text)
        return process_entries(entries)

    today_g, today_total = _process("today")
    week_g, week_total = _process("lastweek")

    new, recurring, resolved = classify(today_g, week_g)

    # Build JSON output
    data = {
        "date_display": date_display,
        "time_range_display": time_display,
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
