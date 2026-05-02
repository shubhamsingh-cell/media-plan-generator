"""apis.browser -- AI-native browser automation backends.

This sub-package hosts thin Python wrappers for higher-level browser
automation services that sit *above* Joveo's Playwright/MCP stack. Each
backend is feature-flag gated and degrades gracefully when its remote
endpoint or API key is not configured.

Currently exposed:
    * ``stagehand_act``     -- Stagehand v3 ``act`` primitive
      (natural-language click/type/scroll on a live page).
    * ``stagehand_extract`` -- structured extraction with a JSON schema.
    * ``stagehand_observe`` -- enumerate candidate elements / actions.

All three call the Stagehand JSON-RPC remote endpoint pointed to by the
``STAGEHAND_API_URL`` env var (Stagehand has no stable hosted REST API as
of May 2026, so production deployments self-host the Node.js server and
expose ``/v1/act``, ``/v1/extract``, ``/v1/observe`` over JSON). When the
endpoint or key is missing, callers always receive a normalized error
dict rather than an exception.
"""

from __future__ import annotations

from .stagehand_wrapper import (
    stagehand_act,
    stagehand_extract,
    stagehand_observe,
)

__all__ = ["stagehand_act", "stagehand_extract", "stagehand_observe"]
