#!/usr/bin/env python3
"""Liveness check for the router's free OpenRouter routes (needs network).

Fetches the public catalog (GET https://openrouter.ai/api/v1/models, no key)
and checks that every ":free" slug in llm_router.PROVIDER_CONFIG is listed
with prompt and completion price "0". Exits 1 and prints one line per
problem. Meant for a scheduled run (cron / routine); CI does not call the
network. On failure, follow tests/test_model_id_audit.py: pick a listed free
model of comparable class, or disable the route with a boot WARNING; never
substitute a paid route.

Usage: python3 scripts/check_openrouter_free_routes.py
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

CATALOG_URL = "https://openrouter.ai/api/v1/models"


def free_route_slugs() -> List[str]:
    """Every ":free" OpenRouter model id the router can call."""
    import llm_router

    return sorted(
        {
            str(cfg.get("model"))
            for cfg in llm_router.PROVIDER_CONFIG.values()
            if isinstance(cfg, dict)
            and "openrouter.ai" in str(cfg.get("endpoint", ""))
            and str(cfg.get("model", "")).endswith(":free")
        }
    )


def problems(catalog: Dict[str, Any]) -> List[str]:
    """One line per free route that is unlisted or no longer free."""
    listed = {m.get("id"): m for m in catalog.get("data") or [] if isinstance(m, dict)}
    out: List[str] = []
    for slug in free_route_slugs():
        m = listed.get(slug)
        if m is None:
            out.append(f"{slug}: not in the OpenRouter catalog")
            continue
        price = m.get("pricing") or {}
        if str(price.get("prompt")) != "0" or str(price.get("completion")) != "0":
            out.append(f"{slug}: no longer free ({price.get('prompt')}/{price.get('completion')})")
    return out


def main() -> int:
    with urllib.request.urlopen(CATALOG_URL, timeout=30) as resp:  # noqa: S310 -- fixed https URL
        catalog = json.loads(resp.read().decode("utf-8"))
    found = problems(catalog)
    for line in found:
        print(line)
    if not found:
        print(f"ok: {len(free_route_slugs())} free OpenRouter routes listed and free")
    return 1 if found else 0


if __name__ == "__main__":
    raise SystemExit(main())
