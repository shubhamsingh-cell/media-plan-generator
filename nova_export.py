"""
Nova conversation export module.

Generates exportable conversation transcripts in both HTML and plain
text/markdown formats. HTML output uses the Nova dark theme for screen
display with @media print overrides for clean printing.

All CSS is inline -- no external dependencies. Standalone HTML documents.
"""

from __future__ import annotations

import html
import re
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Theme Colors (match Nova chat widget)
# ---------------------------------------------------------------------------
_BG_DARK = "#000000"
_BG_SURFACE = "#111111"
_BG_MSG_USER = "#1e2a4a"
_BG_MSG_ASSISTANT = "#141826"
_ACCENT = "#6366F1"
_ACCENT_LIGHT = "#818CF8"
_TEXT_PRIMARY = "#E2E8F0"
_TEXT_SECONDARY = "rgba(226, 232, 240, 0.7)"
_TEXT_DIM = "rgba(226, 232, 240, 0.5)"
_BORDER = "rgba(255, 255, 255, 0.06)"

# Print-friendly overrides
_PRINT_BG = "#ffffff"
_PRINT_TEXT = "#1a1a2e"
_PRINT_TEXT_MUTED = "#555566"
_PRINT_USER_BG = "#e8eaf6"
_PRINT_ASSISTANT_BG = "#f5f5f5"
_PRINT_ACCENT = "#5A54BD"

# Brand colors
_PORT_GORE = "#202058"
_BLUE_VIOLET = "#5A54BD"


def _safe(value: Any) -> str:
    """HTML-escape any user-provided value."""
    if value is None:
        return ""
    return html.escape(str(value))


def _strip_html(text: str) -> str:
    """Remove HTML tags from a string."""
    if not text:
        return ""
    # Remove HTML tags
    clean = re.sub(r"<[^>]+>", "", text)
    # Decode common HTML entities
    clean = clean.replace("&amp;", "&")
    clean = clean.replace("&lt;", "<")
    clean = clean.replace("&gt;", ">")
    clean = clean.replace("&quot;", '"')
    clean = clean.replace("&#39;", "'")
    clean = clean.replace("&nbsp;", " ")
    clean = clean.replace("&mdash;", "--")
    clean = clean.replace("&ndash;", "-")
    clean = clean.replace("&middot;", "*")
    return clean.strip()


def export_conversation_html(
    conversation_history: List[Dict[str, Any]],
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """Export a Nova conversation as a standalone HTML document.

    Parameters
    ----------
    conversation_history : list of dict
        Each dict has: role ("user" | "assistant"), content (str),
        timestamp (str, optional), sources (list, optional),
        confidence (float, optional).

    metadata : dict, optional
        Optional metadata: session_id, client_name, export_date.

    Returns
    -------
    str
        Complete standalone HTML document.
    """
    metadata = metadata or {}
    export_date = metadata.get(
        "export_date",
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    )
    session_id = metadata.get("session_id") or ""
    client_name = metadata.get("client_name") or ""

    # Build subtitle line
    subtitle_parts = []
    if client_name:
        subtitle_parts.append(f"Client: {_safe(client_name)}")
    if session_id:
        subtitle_parts.append(f"Session: {_safe(session_id[:16])}")
    subtitle_parts.append(_safe(export_date))
    subtitle_line = " &middot; ".join(subtitle_parts)

    # Build message elements
    messages_html_parts = []
    for msg in conversation_history:
        role = msg.get("role", "user")
        content = msg.get("content") or ""
        timestamp = msg.get("timestamp") or ""
        sources = msg.get("sources") or []
        confidence = msg.get("confidence")

        is_user = role == "user"

        # Alignment and styling
        align = "flex-end" if is_user else "flex-start"
        bg_class = "msg-user" if is_user else "msg-assistant"
        role_label = "You" if is_user else "Nova"

        # Build metadata line below message
        meta_parts = []
        if timestamp:
            meta_parts.append(_safe(str(timestamp)))
        if confidence is not None:
            try:
                conf_pct = float(confidence)
                if conf_pct > 0:
                    meta_parts.append(f"Confidence: {conf_pct:.0f}%")
            except (TypeError, ValueError):
                pass
        meta_html = ""
        if meta_parts:
            meta_html = f'<div class="msg-meta">{" &middot; ".join(meta_parts)}</div>'

        # Sources
        sources_html = ""
        if sources and not is_user:
            src_items = "".join(
                f'<span class="msg-source">{_safe(str(s))}</span>' for s in sources[:5]
            )
            sources_html = f'<div class="msg-sources">{src_items}</div>'

        # Escape content -- preserve basic newlines
        safe_content = _safe(content).replace("\n", "<br>")

        messages_html_parts.append(
            f"""
        <div class="msg-row" style="align-self: {align};">
          <div class="msg-label">{role_label}</div>
          <div class="msg-bubble {bg_class}">
            <div class="msg-content">{safe_content}</div>
            {sources_html}
            {meta_html}
          </div>
        </div>
        """
        )

    messages_html = "\n".join(messages_html_parts)
    msg_count = len(conversation_history)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Nova AI - Conversation Export</title>
<style>
  *, *::before, *::after {{ margin: 0; padding: 0; box-sizing: border-box; }}

  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    font-size: 14px;
    line-height: 1.6;
    background: {_BG_DARK};
    color: {_TEXT_PRIMARY};
    padding: 0;
    min-height: 100vh;
  }}

  /* ── Header ── */
  .export-header {{
    background: linear-gradient(135deg, {_BG_SURFACE} 0%, rgba(30, 40, 70, 0.95) 100%);
    padding: 28px 32px;
    border-bottom: 1px solid {_BORDER};
    text-align: center;
  }}
  .export-title {{
    font-size: 20px;
    font-weight: 700;
    color: {_ACCENT_LIGHT};
    letter-spacing: 2px;
  }}
  .export-subtitle {{
    font-size: 12px;
    color: {_TEXT_DIM};
    margin-top: 6px;
    letter-spacing: 0.5px;
  }}
  .export-count {{
    font-size: 11px;
    color: {_TEXT_DIM};
    margin-top: 4px;
  }}

  /* ── Messages Container ── */
  .messages-container {{
    max-width: 800px;
    margin: 0 auto;
    padding: 24px 32px;
    display: flex;
    flex-direction: column;
    gap: 16px;
  }}

  /* ── Message Row ── */
  .msg-row {{
    display: flex;
    flex-direction: column;
    max-width: 85%;
  }}
  .msg-row[style*="flex-end"] {{
    margin-left: auto;
  }}
  .msg-row[style*="flex-start"] {{
    margin-right: auto;
  }}

  .msg-label {{
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: {_TEXT_DIM};
    margin-bottom: 4px;
    padding: 0 8px;
    font-weight: 600;
  }}

  .msg-bubble {{
    padding: 14px 18px;
    border-radius: 16px;
    word-wrap: break-word;
    overflow-wrap: break-word;
  }}
  .msg-user {{
    background: {_BG_MSG_USER};
    border: 1px solid rgba(99, 102, 241, 0.15);
    border-bottom-right-radius: 4px;
  }}
  .msg-assistant {{
    background: {_BG_MSG_ASSISTANT};
    border: 1px solid {_BORDER};
    border-bottom-left-radius: 4px;
  }}

  .msg-content {{
    font-size: 14px;
    line-height: 1.65;
    color: {_TEXT_PRIMARY};
  }}

  .msg-meta {{
    font-size: 11px;
    color: {_TEXT_DIM};
    margin-top: 8px;
    padding-top: 6px;
    border-top: 1px solid rgba(255, 255, 255, 0.04);
  }}

  .msg-sources {{
    margin-top: 8px;
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }}
  .msg-source {{
    font-size: 10px;
    background: rgba(99, 102, 241, 0.1);
    color: {_ACCENT_LIGHT};
    padding: 2px 8px;
    border-radius: 10px;
    border: 1px solid rgba(99, 102, 241, 0.15);
  }}

  /* ── Footer ── */
  .export-footer {{
    text-align: center;
    padding: 24px 32px;
    border-top: 1px solid {_BORDER};
    margin-top: 16px;
  }}
  .export-footer-text {{
    font-size: 12px;
    color: {_TEXT_DIM};
  }}
  .export-footer-brand {{
    color: {_ACCENT_LIGHT};
    font-weight: 600;
  }}

  /* ── Print Styles ── */
  @media print {{
    @page {{
      size: A4;
      margin: 15mm 12mm;
    }}
    body {{
      background: {_PRINT_BG};
      color: {_PRINT_TEXT};
      font-size: 11px;
      padding: 0;
    }}
    .export-header {{
      background: none;
      border-bottom: 2px solid {_PRINT_ACCENT};
      padding: 16px 0;
    }}
    .export-title {{
      color: {_PORT_GORE};
      font-size: 18px;
    }}
    .export-subtitle, .export-count {{
      color: {_PRINT_TEXT_MUTED};
    }}
    .messages-container {{
      padding: 16px 0;
      max-width: none;
    }}
    .msg-user {{
      background: {_PRINT_USER_BG};
      border-color: #c5cae9;
    }}
    .msg-assistant {{
      background: {_PRINT_ASSISTANT_BG};
      border-color: #e0e0e0;
    }}
    .msg-content {{
      color: {_PRINT_TEXT};
    }}
    .msg-label {{
      color: {_PRINT_TEXT_MUTED};
    }}
    .msg-meta {{
      color: {_PRINT_TEXT_MUTED};
      border-top-color: #e0e0e0;
    }}
    .msg-source {{
      background: #ede7f6;
      color: {_PRINT_ACCENT};
      border-color: #c5cae9;
    }}
    .export-footer {{
      border-top-color: #e0e0e0;
    }}
    .export-footer-text {{
      color: {_PRINT_TEXT_MUTED};
    }}
    .export-footer-brand {{
      color: {_PRINT_ACCENT};
    }}
    .msg-row {{
      page-break-inside: avoid;
    }}
  }}

  /* ── Responsive ── */
  @media screen and (max-width: 640px) {{
    .messages-container {{ padding: 16px; }}
    .msg-row {{ max-width: 95%; }}
  }}
</style>
</head>
<body>

  <div class="export-header">
    <div class="export-title">Nova AI - Conversation Export</div>
    <div class="export-subtitle">{subtitle_line}</div>
    <div class="export-count">{msg_count} message{"s" if msg_count != 1 else ""}</div>
  </div>

  <div class="messages-container">
    {messages_html}
  </div>

  <div class="export-footer">
    <div class="export-footer-text">
      Powered by <span class="export-footer-brand">Nova AI Suite</span>
    </div>
  </div>

</body>
</html>"""


def export_conversation_text(
    conversation_history: List[Dict[str, Any]],
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """Export a Nova conversation as plain text/markdown.

    Parameters
    ----------
    conversation_history : list of dict
        Each dict has: role ("user" | "assistant"), content (str),
        timestamp (str, optional).

    metadata : dict, optional
        Optional metadata: session_id, client_name, export_date.

    Returns
    -------
    str
        Clean plain-text transcript.
    """
    metadata = metadata or {}
    export_date = metadata.get(
        "export_date",
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    )
    session_id = metadata.get("session_id") or ""
    client_name = metadata.get("client_name") or ""

    lines = ["# Nova AI Conversation Export"]
    lines.append(f"Date: {export_date}")
    if client_name:
        lines.append(f"Client: {client_name}")
    if session_id:
        lines.append(f"Session: {session_id[:16]}")
    lines.append("---")
    lines.append("")

    for msg in conversation_history:
        role = msg.get("role", "user")
        content = msg.get("content") or ""
        timestamp = msg.get("timestamp") or ""

        # Strip HTML from content if present
        clean_content = _strip_html(content) if "<" in content else content

        role_label = "User" if role == "user" else "Nova"

        # Timestamp suffix
        ts_suffix = f"  ({timestamp})" if timestamp else ""

        lines.append(f"[{role_label}]{ts_suffix}")
        lines.append(clean_content)
        lines.append("")

    lines.append("---")
    lines.append("Powered by Nova AI Suite")
    lines.append("")

    return "\n".join(lines)
