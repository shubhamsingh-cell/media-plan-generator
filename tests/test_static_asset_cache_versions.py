"""Regression lock: immutable-cached static assets must bump ?v= when they change.

Static assets are served with `Cache-Control: public, max-age=31536000,
immutable` -- browsers hold them for a YEAR and never revalidate. The only
way a change reaches returning visitors is a new URL, i.e. bumping the
`?v=` cache-buster where the asset is referenced (hub.html for top-level
CSS/JS, hub.js's `modules` array for the dynamically-injected motion
modules).

This has silently bitten twice on 2026-07-20 alone: the initParallax
hero-drag fix shipped in motion-engine.js while hub.js still referenced
`?v=2.0.0`, so every returning visitor kept the buggy file; the hub.css
guard removal shipped the same way (later covered by a concurrent
session's unrelated bump). Nothing in the pipeline catches this class of
miss -- hence this pin.

Mechanism: tests/baselines/static_asset_versions.json maps each
?v=-referenced asset file to {version, sha256-of-content}. The test
recomputes both and fails when:
  * an asset's content hash changed but its ?v= did not -- the change is
    undeliverable to returning visitors; bump ?v= in the referencing file
    (hub.html or hub.js), then refresh the baseline; or
  * the ?v= or file set changed without a baseline refresh -- forces the
    baseline to stay current so the NEXT unbumped change is caught.

Refresh the baseline after any intentional change:
``python3 tests/test_static_asset_cache_versions.py --update``

Runs under pytest, or standalone:
``python3 tests/test_static_asset_cache_versions.py``.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BASELINE_PATH = Path(__file__).resolve().parent / "baselines" / "static_asset_versions.json"

# Files whose /static/...?v=... references define the deliverable asset set.
REFERRERS = [
    PROJECT_ROOT / "templates" / "hub.html",
    PROJECT_ROOT / "static" / "js" / "hub.js",
]

_REF_RE = re.compile(r"/static/[\w./-]+\?v=[\w.-]+")


def collect_versioned_assets() -> dict[str, dict[str, str]]:
    """Return {repo-relative asset path: {version, sha256, referrer}}."""
    assets: dict[str, dict[str, str]] = {}
    for referrer in REFERRERS:
        text = referrer.read_text(encoding="utf-8")
        for ref in _REF_RE.findall(text):
            url_path, version = ref.split("?v=", 1)
            rel_path = url_path.lstrip("/")  # /static/js/x.js -> static/js/x.js
            asset_file = PROJECT_ROOT / rel_path
            if not asset_file.is_file():
                raise AssertionError(
                    f"{referrer.name} references {url_path}?v={version} "
                    f"but {rel_path} does not exist"
                )
            digest = hashlib.sha256(asset_file.read_bytes()).hexdigest()
            existing = assets.get(rel_path)
            if existing and existing["version"] != version:
                raise AssertionError(
                    f"{rel_path} is referenced with conflicting versions: "
                    f"?v={existing['version']} (in {existing['referrer']}) vs "
                    f"?v={version} (in {referrer.name}) -- unify them"
                )
            assets[rel_path] = {
                "version": version,
                "sha256": digest,
                "referrer": referrer.name,
            }
    return assets


def test_versioned_assets_match_baseline() -> None:
    assert BASELINE_PATH.is_file(), (
        f"missing baseline {BASELINE_PATH.relative_to(PROJECT_ROOT)} -- generate it: "
        "python3 tests/test_static_asset_cache_versions.py --update"
    )
    baseline: dict[str, dict[str, str]] = json.loads(
        BASELINE_PATH.read_text(encoding="utf-8")
    )
    current = collect_versioned_assets()

    problems: list[str] = []
    for rel_path, cur in sorted(current.items()):
        base = baseline.get(rel_path)
        if base is None:
            problems.append(
                f"NEW versioned asset {rel_path} (?v={cur['version']}) not in "
                "baseline -- refresh it (--update)"
            )
            continue
        content_changed = cur["sha256"] != base["sha256"]
        version_changed = cur["version"] != base["version"]
        if content_changed and not version_changed:
            problems.append(
                f"{rel_path} CONTENT CHANGED but ?v= is still "
                f"{cur['version']} -- with the 1-year immutable cache, returning "
                f"visitors will NEVER receive this change. Bump ?v= in "
                f"{cur['referrer']}, then refresh the baseline (--update)"
            )
        elif version_changed or content_changed:
            problems.append(
                f"{rel_path} changed (?v= {base['version']} -> {cur['version']}) "
                "-- intentional? refresh the baseline (--update) so the next "
                "unbumped change is still caught"
            )
    for rel_path in sorted(set(baseline) - set(current)):
        problems.append(
            f"{rel_path} disappeared from the referenced asset set -- refresh "
            "the baseline (--update)"
        )

    assert not problems, "\n".join(problems)


def _update_baseline() -> None:
    current = collect_versioned_assets()
    BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    BASELINE_PATH.write_text(
        json.dumps(current, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"baseline written: {BASELINE_PATH} ({len(current)} assets)")


if __name__ == "__main__":
    if "--update" in sys.argv:
        _update_baseline()
    else:
        test_versioned_assets_match_baseline()
        print("ok: all versioned static assets match baseline")
