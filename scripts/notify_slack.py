"""Slack notification script for Nova AI Suite CI/CD pipeline.

Posts messages to Slack channels using the Bot Token API.
Can be called from auto_qc.py or as a standalone CLI tool.

Requirements:
    - SLACK_BOT_TOKEN environment variable must be set
    - Bot must be invited to the target channel
"""

import json
import logging
import os
import sys
import urllib.request
import urllib.error
from typing import Optional

logger = logging.getLogger(__name__)

SLACK_API_URL = "https://slack.com/api/chat.postMessage"


def notify_slack(channel: str, message: str, token: Optional[str] = None) -> bool:
    """Post a message to a Slack channel.

    Args:
        channel: Slack channel ID or name (e.g., '#deployments' or 'C01ABCDEF').
        message: The message text to post.
        token: Slack Bot Token. Falls back to SLACK_BOT_TOKEN env var if not provided.

    Returns:
        True if the message was posted successfully, False otherwise.
    """
    bot_token = token or os.environ.get("SLACK_BOT_TOKEN") or ""
    if not bot_token:
        logger.error("SLACK_BOT_TOKEN is not set")
        return False

    payload = json.dumps({"channel": channel, "text": message}).encode("utf-8")

    req = urllib.request.Request(
        SLACK_API_URL,
        data=payload,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {bot_token}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            if body.get("ok"):
                logger.info(f"Slack message sent to {channel}")
                return True
            else:
                error = body.get("error") or "unknown error"
                logger.error(f"Slack API error: {error}")
                return False
    except urllib.error.URLError as exc:
        logger.error(f"Failed to reach Slack API: {exc}", exc_info=True)
        return False
    except json.JSONDecodeError as exc:
        logger.error(f"Invalid JSON in Slack response: {exc}", exc_info=True)
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <channel> <message>")
        print(f"Example: {sys.argv[0]} '#deployments' 'Deploy successful'")
        sys.exit(1)

    ch = sys.argv[1]
    msg = " ".join(sys.argv[2:])
    success = notify_slack(ch, msg)
    sys.exit(0 if success else 1)
