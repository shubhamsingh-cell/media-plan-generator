"""
Startup CSS/JS minifier for the Nova AI Suite.

Generates .min.css / .min.js versions of design-system assets at server boot.
Uses lightweight regex-based minification (no external dependencies).
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# Files to minify (relative to project root)
_CSS_FILES = [
    "static/css/tokens.css",
    "static/css/nova.css",
    "static/css/platform.css",
]

_JS_FILES = [
    "static/js/nova.js",
    "static/js/platform.js",
]


def _minify_css(src: str) -> str:
    """Strip comments, collapse whitespace, and trim CSS."""
    # Remove multi-line comments
    out = re.sub(r"/\*[\s\S]*?\*/", "", src)
    # Collapse whitespace around selectors / braces / colons / semicolons
    out = re.sub(r"\s*([{}:;,>~+])\s*", r"\1", out)
    # Collapse remaining runs of whitespace
    out = re.sub(r"\s{2,}", " ", out)
    # Remove leading/trailing whitespace per line then collapse newlines
    out = re.sub(r"\n\s*", "", out)
    # Trim trailing semicolons before closing braces
    out = out.replace(";}", "}")
    return out.strip()


def _minify_js(src: str) -> str:
    """Conservative JS minifier: strip comments and collapse leading whitespace.

    Does NOT collapse whitespace around operators (too risky with template
    literals, HTML-in-JS, regex, etc.).  Just removes comments and indentation
    which typically saves 25-35% on well-commented code.
    """
    lines: list[str] = []
    in_block_comment = False

    for line in src.split("\n"):
        # Handle block comments
        if in_block_comment:
            end_idx = line.find("*/")
            if end_idx != -1:
                in_block_comment = False
                line = line[end_idx + 2 :]
            else:
                continue

        # Remove block comment starts on this line
        while "/*" in line:
            start = line.index("/*")
            end = line.find("*/", start + 2)
            if end != -1:
                line = line[:start] + line[end + 2 :]
            else:
                line = line[:start]
                in_block_comment = True
                break

        # Remove single-line comments (but not URLs like http:// or regex like /pattern/)
        # Only strip // comments that are NOT inside a string or regex literal
        stripped = line
        in_str: str | None = None
        in_regex = False
        i = 0
        while i < len(stripped):
            ch = stripped[i]
            if in_regex:
                # Inside regex literal -- skip until unescaped /
                if ch == "\\" and i + 1 < len(stripped):
                    i += 2
                    continue
                if ch == "/":
                    in_regex = False
            elif in_str:
                if ch == "\\" and i + 1 < len(stripped):
                    i += 2
                    continue
                if ch == in_str:
                    in_str = None
            else:
                if ch in ('"', "'", "`"):
                    in_str = ch
                elif ch == "/" and i + 1 < len(stripped) and stripped[i + 1] == "/":
                    # Check if this is a regex end (e.g. /pattern//) or a comment
                    # Heuristic: if preceded by a non-operator char, it's likely
                    # the end of a regex + flags, not a comment
                    before = stripped[:i].rstrip()
                    if before and before[-1] in "=({[,;!&|?:~^%":
                        # After operator: this IS a // comment
                        stripped = stripped[:i]
                        break
                    elif before and before[-1] == "/":
                        # Could be end of regex like /foo/, skip
                        pass
                    elif not before or before.endswith(
                        (
                            "return",
                            "case",
                            "typeof",
                            "void",
                            "delete",
                            "in",
                            "instanceof",
                        )
                    ):
                        # After keyword: this IS a // comment
                        stripped = stripped[:i]
                        break
                    else:
                        # Likely end of regex literal or division, skip it
                        pass
                elif (
                    ch == "/"
                    and i + 1 < len(stripped)
                    and stripped[i + 1] != "/"
                    and stripped[i + 1] != "*"
                ):
                    # Possible regex start -- check if preceded by operator/keyword
                    before = stripped[:i].rstrip()
                    if not before or before[-1] in "=({[,;!&|?:~^%><+-*/":
                        in_regex = True
            i += 1

        # Strip leading/trailing whitespace only (preserve internal spacing)
        stripped = stripped.strip()
        if stripped:
            lines.append(stripped)

    # Join with newlines (not spaces -- preserves ASI safety)
    return "\n".join(lines)


def _content_hash(data: bytes) -> str:
    """Return short SHA-256 hex digest for ETag / cache-busting."""
    return hashlib.sha256(data).hexdigest()[:12]


# Global registry: original path -> (minified bytes, etag)
_MINIFIED_CACHE: dict[str, tuple[bytes, str]] = {}


def get_minified(filepath: str) -> tuple[bytes, str] | None:
    """Return (minified_bytes, etag) for a given filepath, or None."""
    return _MINIFIED_CACHE.get(filepath)


def minify_static_assets() -> int:
    """Minify all registered CSS/JS files. Returns count of files processed."""
    root = Path(__file__).parent
    count = 0

    for rel in _CSS_FILES:
        src_path = root / rel
        if not src_path.is_file():
            logger.warning("[minifier] CSS source not found: %s", rel)
            continue
        try:
            raw = src_path.read_text(encoding="utf-8")
            minified = _minify_css(raw)
            data = minified.encode("utf-8")
            etag = _content_hash(data)
            # Store in cache keyed by the URL path
            url_path = f"/{rel}"
            _MINIFIED_CACHE[url_path] = (data, etag)
            savings = len(raw.encode("utf-8")) - len(data)
            logger.info(
                "[minifier] %s: %d -> %d bytes (-%d, %.0f%% smaller)",
                rel,
                len(raw.encode("utf-8")),
                len(data),
                savings,
                (savings / max(len(raw.encode("utf-8")), 1)) * 100,
            )
            count += 1
        except OSError as e:
            logger.error("[minifier] Failed to read %s: %s", rel, e, exc_info=True)

    for rel in _JS_FILES:
        src_path = root / rel
        if not src_path.is_file():
            logger.warning("[minifier] JS source not found: %s", rel)
            continue
        try:
            raw = src_path.read_text(encoding="utf-8")
            minified = _minify_js(raw)
            data = minified.encode("utf-8")
            etag = _content_hash(data)
            url_path = f"/{rel}"
            _MINIFIED_CACHE[url_path] = (data, etag)
            savings = len(raw.encode("utf-8")) - len(data)
            logger.info(
                "[minifier] %s: %d -> %d bytes (-%d, %.0f%% smaller)",
                rel,
                len(raw.encode("utf-8")),
                len(data),
                savings,
                (savings / max(len(raw.encode("utf-8")), 1)) * 100,
            )
            count += 1
        except OSError as e:
            logger.error("[minifier] Failed to read %s: %s", rel, e, exc_info=True)

    logger.info("[minifier] Minified %d static assets", count)
    return count
