"""Slack notification for Mezmo log analysis results.

Posts a summary message to a Slack channel and follows up with
threaded details for new errors and error spikes.
"""

from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)

SLACK_API = "https://slack.com/api"
MAX_TEXT_LENGTH = 3000
ERRORS_PER_MESSAGE = 5
MAX_ERROR_MESSAGES = 3


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def send_slack_notification(
    result: dict,
    slack_token: str,
    slack_channel: str,
    dashboard_url: str | None = None,
) -> dict | None:
    """Send analysis results to Slack.

    Returns the main message API response, or None on failure.
    """
    try:
        channel_id = _resolve_channel(slack_token, slack_channel)
        if not channel_id:
            logger.error("Could not resolve Slack channel: %s", slack_channel)
            return None

        day_label = result.get("day_label", "today").lower()

        summary_blocks = _build_summary_blocks(result, dashboard_url)
        fallback = _build_fallback_text(result)

        main_resp = _post_message(slack_token, channel_id, summary_blocks, fallback)
        if not main_resp or not main_resp.get("ok"):
            err = main_resp.get("error", "unknown") if main_resp else "no response"
            logger.error("Slack summary message failed: %s", err)
            return main_resp

        thread_ts = main_resp["ts"]

        new_errors = result.get("new", [])
        if new_errors:
            _post_new_errors_thread(slack_token, channel_id, thread_ts, new_errors, day_label)

        spikes = [e for e in result.get("recurring", []) if e.get("trend") == "spike"]
        if spikes:
            _post_spikes_thread(slack_token, channel_id, thread_ts, spikes, day_label)

        return main_resp

    except Exception:
        logger.exception("Failed to send Slack notification")
        return None


# ---------------------------------------------------------------------------
# Channel resolution
# ---------------------------------------------------------------------------

def _resolve_channel(token: str, channel: str) -> str | None:
    """Return a Slack channel ID.

    Accepts a channel ID (e.g. ``C0123456789``) or a channel name
    (with or without leading ``#``).
    """
    channel = channel.strip().lstrip("#")

    # Already looks like an ID
    if channel.startswith("C") and channel.isalnum() and len(channel) >= 9:
        return channel

    # Look up by name
    cursor = None
    while True:
        params: dict = {"types": "public_channel,private_channel", "limit": 200}
        if cursor:
            params["cursor"] = cursor

        resp = _slack_get(token, "conversations.list", params)
        if not resp or not resp.get("ok"):
            logger.error("conversations.list failed: %s", resp.get("error") if resp else "no response")
            return None

        for ch in resp.get("channels", []):
            if ch.get("name") == channel:
                return ch["id"]

        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    logger.error("Slack channel '%s' not found", channel)
    return None


# ---------------------------------------------------------------------------
# Block Kit builders
# ---------------------------------------------------------------------------

def _build_summary_blocks(result: dict, dashboard_url: str | None = None) -> list[dict]:
    s = result["summary"]
    date = result.get("date_display", "")
    time_range = result.get("time_range_display", "")
    query = result.get("query", "")
    day_label = result.get("day_label", "Today")

    blocks: list[dict] = []

    # Header
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": f"\U0001f50d Mezmo Log Report \u2014 {date}", "emoji": True},
    })

    # Context — time range & query
    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": f":clock1: {time_range}"},
            {"type": "mrkdwn", "text": f"Query: `{_trunc(query, 200)}`"},
        ],
    })

    blocks.append({"type": "divider"})

    # Key counts
    blocks.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f":rotating_light: *New Errors*\n{s['new_count']}"},
            {"type": "mrkdwn", "text": f":repeat: *Recurring*\n{s['recurring_count']}"},
            {"type": "mrkdwn", "text": f":white_check_mark: *Resolved*\n{s['resolved_count']}"},
            {"type": "mrkdwn", "text": f":bar_chart: *{day_label}*\n{_fmt(s['today_total'])} ({s['today_unique']} unique)"},
        ],
    })

    blocks.append({"type": "divider"})

    # Week comparison
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f"*Week Comparison*\n"
                f"{day_label}: *{_fmt(s['today_total'])}* errors ({s['today_unique']} unique)\n"
                f"Last 7 days: *{_fmt(s['lastweek_total'])}* errors ({s['lastweek_unique']} unique)"
            ),
        },
    })

    # Spike callout
    spikes = [e for e in result.get("recurring", []) if e.get("trend") == "spike"]
    if spikes:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":warning: *{len(spikes)} error(s) spiking* \u2014 see thread for details"},
        })

    # New errors callout
    if s["new_count"] > 0:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":new: *{s['new_count']} new error(s) detected* \u2014 see thread for details"},
        })

    # Dashboard link
    if dashboard_url:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":chart_with_upwards_trend: <{dashboard_url}|View full dashboard>"},
        })

    return blocks


def _build_new_error_batch_blocks(errors: list[dict], start_idx: int, day_label: str = "today") -> list[dict]:
    blocks: list[dict] = []
    for i, err in enumerate(errors, start=start_idx):
        app_label = _format_apps_slack(err)
        msg = _trunc(err.get("message", ""), 300)
        detail = err.get("detail") or ""
        count = err.get("today", 0)

        parts = [
            f"*#{i}* {app_label} \u2014 *{_fmt(count)}x {day_label}*",
            f"```{msg}```",
        ]
        if detail:
            parts.append(f"_{_trunc(detail, 400)}_")

        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(parts)}})
        blocks.append({"type": "divider"})

    return blocks


# ---------------------------------------------------------------------------
# Thread helpers
# ---------------------------------------------------------------------------

def _post_new_errors_thread(
    token: str, channel: str, thread_ts: str, new_errors: list[dict],
    day_label: str = "today",
) -> None:
    max_shown = ERRORS_PER_MESSAGE * MAX_ERROR_MESSAGES
    shown = new_errors[:max_shown]
    remaining = len(new_errors) - max_shown

    for i in range(0, len(shown), ERRORS_PER_MESSAGE):
        batch = shown[i : i + ERRORS_PER_MESSAGE]
        blocks = _build_new_error_batch_blocks(batch, start_idx=i + 1, day_label=day_label)
        _post_message(token, channel, blocks, f"New errors {i + 1}\u2013{i + len(batch)}", thread_ts)

    if remaining > 0:
        _post_message(
            token, channel,
            [{"type": "section", "text": {"type": "mrkdwn", "text": f"_\u2026and {remaining} more new error(s). See full report in Apify dataset._"}}],
            f"...and {remaining} more new errors",
            thread_ts,
        )


def _post_spikes_thread(
    token: str, channel: str, thread_ts: str, spikes: list[dict],
    day_label: str = "today",
) -> None:
    blocks: list[dict] = [{
        "type": "header",
        "text": {"type": "plain_text", "text": "\U0001f4c8 Spiking Errors", "emoji": True},
    }]

    for spike in spikes[:10]:
        app_label = _format_apps_slack(spike)
        msg = _trunc(spike.get("message", ""), 200)
        detail = spike.get("detail") or ""
        today = spike.get("today", 0)
        lastweek = spike.get("lastweek", 0)
        avg = lastweek / 7 if lastweek else 0
        multiplier = f"{today / avg:.1f}x" if avg > 0 else "N/A"

        parts = [
            f"{app_label} *{_fmt(today)} {day_label}* (avg {_fmt(int(avg))}/day, {multiplier})",
            f"```{msg}```",
        ]
        if detail:
            parts.append(f"_{_trunc(detail, 300)}_")

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(parts),
            },
        })

    if len(spikes) > 10:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"_\u2026and {len(spikes) - 10} more spike(s)_"}],
        })

    _post_message(token, channel, blocks, f"{len(spikes)} spiking errors", thread_ts)


# ---------------------------------------------------------------------------
# Slack HTTP helpers
# ---------------------------------------------------------------------------

def _post_message(
    token: str,
    channel: str,
    blocks: list[dict],
    text: str,
    thread_ts: str | None = None,
) -> dict | None:
    payload: dict = {"channel": channel, "blocks": blocks, "text": text}
    if thread_ts:
        payload["thread_ts"] = thread_ts

    try:
        resp = requests.post(
            f"{SLACK_API}/chat.postMessage",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            logger.error("Slack chat.postMessage error: %s", data.get("error", "unknown"))
        return data
    except requests.RequestException:
        logger.exception("Slack chat.postMessage request failed")
        return None


def _slack_get(token: str, method: str, params: dict) -> dict | None:
    try:
        resp = requests.get(
            f"{SLACK_API}/{method}",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException:
        logger.exception("Slack %s request failed", method)
        return None


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _format_apps_slack(entry: dict) -> str:
    apps = entry.get("apps", [])
    if not apps:
        return f"`{entry.get('app_tag', 'unknown')}`"
    if len(apps) == 1:
        return f"`{apps[0]}`"
    if len(apps) <= 3:
        return " ".join(f"`{a}`" for a in apps)
    shown = " ".join(f"`{a}`" for a in apps[:3])
    return f"{shown} +{len(apps) - 3} more"


def _fmt(n: int) -> str:
    return f"{n:,}"


def _trunc(s: str, max_len: int) -> str:
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "\u2026"


def _build_fallback_text(result: dict) -> str:
    s = result["summary"]
    day_label = result.get("day_label", "Today")
    return (
        f"Mezmo Log Report \u2014 {result.get('date_display', '')}\n"
        f"New: {s['new_count']} | Recurring: {s['recurring_count']} | Resolved: {s['resolved_count']}\n"
        f"{day_label}: {_fmt(s['today_total'])} errors | Last 7 days: {_fmt(s['lastweek_total'])} errors"
    )
