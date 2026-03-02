# Mezmo New Lines Finder

Apify Actor that analyzes Mezmo (LogDNA) error logs and classifies them by comparing a selected day against a historical baseline. It identifies **new**, **recurring**, and **resolved** error patterns, and optionally sends a summary to Slack.

## How it works

1. Connects to Mezmo using the provided service key.
2. Queries deduplicated logs for two time ranges:
   - **Selected day** — today (start of UTC day to now) or yesterday (full UTC day).
   - **Baseline** — the N days immediately before the selected day (default 7).
3. Normalizes error messages (strips UUIDs, IPs, timestamps, pod names, etc.) and groups them by `app::normalized_message`.
4. Classifies each error group:
   - **New** — appeared on the selected day but not in the baseline period.
   - **Recurring** — appeared in both. Trend is computed from the daily average:
     - *spike* — today's count > 2x daily average
     - *lower* — today's count < 0.5x daily average
     - *normal* — within expected range
   - **Resolved** — appeared in the baseline but not on the selected day.
5. Outputs structured JSON to the default dataset and key-value store.
6. Optionally posts a summary to a Slack channel.

## Input

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `query` | string | Yes | — | Mezmo search query (e.g. `(level:error OR level:fatal) tag:production app:my-app`) |
| `mezmoServiceKey` | string | Yes | — | Mezmo [service key](https://docs.mezmo.com/docs/ingestion-key#service-keys-sts) for API authentication (starts with `sts_`) |
| `day` | string | No | `today` | Which day to analyze: `today`, `yesterday`, or `last24h` |
| `daysBack` | integer | No | `7` | Number of days before the selected day to use as the comparison baseline (1–30) |
| `minCount` | integer | No | `1` | Minimum number of occurrences for an error to appear in results |
| `debug` | boolean | No | `false` | Save raw API responses for debugging |
| `slackToken` | string | No | — | Slack Bot OAuth token for sending notifications |
| `slackChannel` | string | No | — | Slack channel name or ID (requires `slackToken`) |

### Example input

```json
{
    "query": "(level:error OR level:fatal) tag:production app:my-app",
    "mezmoServiceKey": "sts_xxx",
    "day": "yesterday",
    "daysBack": 14,
    "minCount": 5,
    "slackToken": "xoxb-xxx",
    "slackChannel": "#errors"
}
```

## Output

The Actor pushes a JSON object to the default dataset with this structure:

```json
{
    "date_display": "Mar 02, 2026",
    "time_range_display": "00:00 – 14:30 UTC vs. Feb 23–Mar 01",
    "query": "...",
    "summary": {
        "today_total": 1234,
        "today_unique": 45,
        "lastweek_total": 5678,
        "lastweek_unique": 89,
        "new_count": 10,
        "recurring_count": 25,
        "resolved_count": 54
    },
    "new": [
        {
            "today": 42,
            "app_tag": "api",
            "message": "TypeError: Cannot read property ...",
            "detail": "exception: TypeError",
            "sample": "raw log line"
        }
    ],
    "recurring": [
        {
            "today": 100,
            "lastweek": 200,
            "trend": "spike",
            "app_tag": "api",
            "message": "Connection timeout ...",
            "detail": null,
            "sample": "raw log line"
        }
    ],
    "resolved": [
        {
            "lastweek": 50,
            "app_tag": "worker",
            "message": "Redis connection refused ...",
            "detail": null,
            "sample": "raw log line"
        }
    ]
}
```

The same object is also stored as `OUTPUT` in the default key-value store.
