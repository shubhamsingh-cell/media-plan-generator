"""Template composer: assembles split template partials into complete HTML pages.

Loads partial files from templates/partials/<page>/ at startup and caches
the composed result. Thread-safe via a read-write lock pattern.

Usage:
    from template_composer import get_composed_template

    html_bytes = get_composed_template("index")
"""

from __future__ import annotations

import logging
import os
import re
import threading
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── Paths ──
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
PARTIALS_DIR = TEMPLATES_DIR / "partials"

# ── Cache ──
_composed_cache: dict[str, bytes] = {}
_composed_cache_lock = threading.Lock()

# ── Include pattern: <!-- @include partial_name.html --> ──
_INCLUDE_RE = re.compile(r"<!--\s*@include\s+([\w./\-]+)\s*-->")


def _resolve_includes(template_text: str, partials_dir: Path, depth: int = 0) -> str:
    """Resolve <!-- @include filename.html --> directives in template text.

    Recursively resolves includes up to 5 levels deep to prevent infinite loops.

    Args:
        template_text: The template content with include directives.
        partials_dir: Directory to look for partial files.
        depth: Current recursion depth (max 5).

    Returns:
        Template text with all includes resolved.
    """
    if depth > 5:
        logger.warning("Template include depth exceeded 5, stopping recursion")
        return template_text

    def _replacer(match: re.Match) -> str:
        filename = match.group(1)
        partial_path = partials_dir / filename
        if not partial_path.is_file():
            logger.error("Template partial not found: %s", partial_path)
            return f"<!-- MISSING PARTIAL: {filename} -->"
        try:
            content = partial_path.read_text(encoding="utf-8")
            # Recursively resolve nested includes
            return _resolve_includes(content, partials_dir, depth + 1)
        except OSError as e:
            logger.error(
                "Failed to read template partial %s: %s", partial_path, e, exc_info=True
            )
            return f"<!-- ERROR READING PARTIAL: {filename} -->"

    return _INCLUDE_RE.sub(_replacer, template_text)


def compose_template(page_name: str) -> Optional[bytes]:
    """Compose a full HTML page from its shell template and partials.

    Looks for templates/<page_name>.html as the shell, resolves all
    <!-- @include ... --> directives from templates/partials/<page_name>/,
    and returns the complete HTML as bytes.

    Args:
        page_name: The template page name (e.g. "index").

    Returns:
        Composed HTML as bytes, or None if the shell template doesn't exist.
    """
    shell_path = TEMPLATES_DIR / f"{page_name}.html"
    if not shell_path.is_file():
        logger.error("Shell template not found: %s", shell_path)
        return None

    page_partials_dir = PARTIALS_DIR / page_name
    if not page_partials_dir.is_dir():
        # No partials -- just return the raw file (backward-compatible)
        try:
            return shell_path.read_bytes()
        except OSError as e:
            logger.error("Failed to read template %s: %s", shell_path, e, exc_info=True)
            return None

    try:
        shell_text = shell_path.read_text(encoding="utf-8")
    except OSError as e:
        logger.error(
            "Failed to read shell template %s: %s", shell_path, e, exc_info=True
        )
        return None

    composed = _resolve_includes(shell_text, page_partials_dir)
    return composed.encode("utf-8")


def get_composed_template(page_name: str) -> Optional[bytes]:
    """Get a composed template, using cache for performance.

    Thread-safe. First call composes and caches; subsequent calls return cached.

    Args:
        page_name: The template page name (e.g. "index").

    Returns:
        Composed HTML as bytes, or None on failure.
    """
    with _composed_cache_lock:
        cached = _composed_cache.get(page_name)
    if cached is not None:
        return cached

    result = compose_template(page_name)
    if result is not None:
        with _composed_cache_lock:
            _composed_cache[page_name] = result
        logger.info(
            "Composed template '%s': %d bytes from partials",
            page_name,
            len(result),
        )
    return result


def invalidate_cache(page_name: Optional[str] = None) -> None:
    """Invalidate composed template cache.

    Args:
        page_name: Specific page to invalidate, or None to clear all.
    """
    with _composed_cache_lock:
        if page_name:
            _composed_cache.pop(page_name, None)
        else:
            _composed_cache.clear()


def precompose_all() -> dict[str, int]:
    """Pre-compose all templates that have partials directories.

    Called at startup to warm the cache. Thread-safe.

    Returns:
        Dict mapping page names to composed byte sizes.
    """
    results: dict[str, int] = {}
    if not PARTIALS_DIR.is_dir():
        return results

    for entry in PARTIALS_DIR.iterdir():
        if entry.is_dir():
            page_name = entry.name
            composed = get_composed_template(page_name)
            if composed:
                results[page_name] = len(composed)
    return results
