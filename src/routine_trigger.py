"""Trigger Claude Code routine to auto-fix errors found by the analyzer."""

from __future__ import annotations

import json
import logging
import time

import requests

logger = logging.getLogger(__name__)

ROUTINE_URL = "https://api.anthropic.com/v1/claude_code/routines/trig_01WvutmqEUMnf24NMdiQUhoY/fire"
MAX_TRIGGERS = 5
TRIGGER_DELAY = 2.0


def trigger_error_fixes(
    errors: list[dict],
    claude_token: str,
    slack_channel: str,
    slack_thread_ts: str,
) -> int:
    """Fire the Claude Code routine for each error.

    Returns the number of successfully triggered routines.
    """
    triggered = 0

    for err in errors[:MAX_TRIGGERS]:
        prompt = _build_prompt(err, slack_channel, slack_thread_ts)
        payload = {"text": prompt}

        try:
            resp = requests.post(
                ROUTINE_URL,
                headers={
                    "Authorization": f"Bearer {claude_token}",
                    "Content-Type": "application/json",
                    "anthropic-beta": "experimental-cc-routine-2026-04-01",
                    "anthropic-version": "2023-06-01",
                },
                json=payload,
                timeout=30,
            )
            if resp.status_code in (200, 201, 202):
                triggered += 1
                logger.info("Routine triggered for: %s", _short(err))
            else:
                logger.warning(
                    "Routine trigger failed (%d) for: %s — %s",
                    resp.status_code, _short(err), resp.text[:200],
                )
        except requests.RequestException:
            logger.exception("Routine trigger request failed for: %s", _short(err))

        if triggered < len(errors[:MAX_TRIGGERS]):
            time.sleep(TRIGGER_DELAY)

    skipped = len(errors) - MAX_TRIGGERS
    if skipped > 0:
        logger.info("Skipped %d errors (max %d triggers per run)", skipped, MAX_TRIGGERS)

    return triggered


def _build_prompt(err: dict, slack_channel: str, slack_thread_ts: str) -> str:
    parts = [
        f"message: {err.get('message', '')}",
        f"detail: {err.get('detail') or 'N/A'}",
        f"apps: {', '.join(err.get('apps', []))}",
        f"today_count: {err.get('today', 0)}",
        f"lastweek_count: {err.get('lastweek', 0)}",
        f"trend: {err.get('trend', 'new')}",
        f"slack_channel: {slack_channel}",
        f"slack_thread_ts: {slack_thread_ts}",
    ]
    sample = err.get("sample", "")
    if sample:
        parts.append(f"sample (raw JSON log entry):\n{sample}")
    return "\n".join(parts)


def _short(err: dict) -> str:
    msg = err.get("message", "")
    return msg[:80] + "…" if len(msg) > 80 else msg
