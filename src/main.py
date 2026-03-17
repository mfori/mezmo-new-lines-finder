"""Apify Actor entry point for Mezmo log analysis.

Reads a Mezmo query from actor input, compares today's logs against the
previous 7 days, and outputs structured JSON with new/recurring/resolved
classifications.  Optionally posts results to Slack.
"""

from __future__ import annotations

import asyncio
from functools import partial

from apify import Actor

from .deploy_duty import run_deploy_duty, generate_single_html
from .slack_notifier import send_slack_notification


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}

        query = actor_input.get('query')
        mcp_token = actor_input.get('mezmoServiceKey')
        mcp_url = 'https://mcp.mezmo.com/mcp'
        debug = actor_input.get('debug', False)
        slack_channel = actor_input.get('slackChannel')
        slack_token = actor_input.get('slackToken')
        day = actor_input.get('day', 'today')
        days_back = actor_input.get('daysBack', 7)
        min_count = actor_input.get('minCount', 1)

        if not query:
            raise ValueError("Missing required input 'query'")
        if not mcp_token:
            raise ValueError("Missing required input 'mezmoServiceKey'")

        Actor.log.info(f'Running deploy duty analysis for query: {query[:80]}...')

        # run_deploy_duty is synchronous (uses requests + ThreadPoolExecutor),
        # so run it in an executor to avoid blocking the event loop
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            partial(run_deploy_duty, query=query, mcp_token=mcp_token, mcp_url=mcp_url, debug=debug, day=day, days_back=days_back, min_count=min_count),
        )

        Actor.log.info(
            f"Analysis complete — New: {result['summary']['new_count']}, "
            f"Recurring: {result['summary']['recurring_count']}, "
            f"Resolved: {result['summary']['resolved_count']}"
        )

        # Generate HTML dashboard and store in KVS
        html = generate_single_html(result)
        kvs = await Actor.open_key_value_store()
        await kvs.set_value('dashboard.html', html, content_type='text/html')
        dashboard_url = await kvs.get_public_url('dashboard.html')
        Actor.log.info(f'Dashboard stored: {dashboard_url}')

        # Send Slack notification if configured
        if slack_token and slack_channel:
            Actor.log.info(f'Sending Slack notification to channel {slack_channel}...')
            try:
                slack_resp = await loop.run_in_executor(
                    None,
                    partial(
                        send_slack_notification,
                        result=result, slack_token=slack_token,
                        slack_channel=slack_channel, dashboard_url=dashboard_url,
                    ),
                )
                if slack_resp and slack_resp.get('ok'):
                    Actor.log.info('Slack notification sent successfully.')
                else:
                    err = slack_resp.get('error', 'unknown') if slack_resp else 'no response'
                    Actor.log.warning(f'Slack notification failed: {err}')
            except Exception as exc:
                Actor.log.warning(f'Slack notification failed: {exc}')
        elif slack_channel and not slack_token:
            Actor.log.warning('slackChannel is set but slackToken input is missing — skipping Slack notification.')

        # Store in default dataset (accessible via API / UI)
        await Actor.push_data(result)

        # Also store as OUTPUT in key-value store for direct access
        await kvs.set_value('OUTPUT', result)
