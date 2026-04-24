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
        payload = {
            "inputs": {
                "message": err.get("message", ""),
                "detail": err.get("detail") or "",
                "sample": err.get("sample", ""),
                "apps": err.get("apps", []),
                "app_tag": err.get("app_tag", ""),
                "today": err.get("today", 0),
                "lastweek": err.get("lastweek", 0),
                "trend": err.get("trend", ""),
                "slack_channel": slack_channel,
                "slack_thread_ts": slack_thread_ts,
            },
        }

        try:
            resp = requests.post(
                ROUTINE_URL,
                headers={
                    "Authorization": f"Bearer {claude_token}",
                    "Content-Type": "application/json",
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


def _short(err: dict) -> str:
    msg = err.get("message", "")
    return msg[:80] + "…" if len(msg) > 80 else msg
