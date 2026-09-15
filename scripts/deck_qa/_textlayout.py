"""Shared text-layout engine for deck_qa's collide.py and render.py.

Both tools need to agree on where every glyph lands so a collision found by
``collide.py`` shows up at the same pixel in ``render.py``'s PNG. This module
is the single source of truth for that layout: per-run font resolution
(defaults 18pt when a run carries no explicit size), real advances via
``ImageFont.getlength`` (font loaded at 4x point size for sub-pixel
precision), word-wrap against the text frame's usable width, 1.2x line
height, paragraph ``space_before``/``space_after``, vertical anchor
(TOP/MIDDLE/BOTTOM) and horizontal alignment (LEFT/CENTER/RIGHT).

Deliberately independent of ppt_generator's own ``_estimate_lines`` /
``_measure_lines`` -- a checker that reused the generator's own ruler could
never catch a defect in that ruler.
"""

import os
from functools import lru_cache

EMU = 914400.0
DEFAULT_MARGIN_LR_EMU = 91440  # python-pptx default left/right inset
DEFAULT_MARGIN_TB_EMU = 45720  # python-pptx default top/bottom inset
DEFAULT_FONT_PT = 18.0
LINE_HEIGHT_MULT = 1.2
_FONT_PX = 400  # render at 100pt x4 for sub-point precision

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
FONTS_DIR = os.path.join(_REPO_ROOT, "fonts")


@lru_cache(maxsize=8)
def _pil_font(size_pt: float, bold: bool):
    from PIL import ImageFont

    name = "Poppins-Bold.ttf" if bold else "Poppins-Regular.ttf"
    path = os.path.join(FONTS_DIR, name)
    return ImageFont.truetype(path, max(int(size_pt * 4), 4))


def adv_in(text: str, size_pt: float, bold: bool) -> float:
    """Real glyph advance of ``text`` at ``size_pt``, in inches."""
    if not text:
        return 0.0
    return _pil_font(size_pt, bold).getlength(text) / (4.0 * 72.0)


def wrap(text: str, size_pt: float, bold: bool, avail_in: float):
    """Word-wrap ``text`` into lines that fit ``avail_in`` inches."""
    if avail_in <= 0:
        return [text] if text else []
    lines, cur = [], ""
    for word in text.split():
        trial = (cur + " " + word).strip()
        if not cur or adv_in(trial, size_pt, bold) <= avail_in:
            cur = trial
        else:
            lines.append(cur)
            cur = word
    if cur:
        lines.append(cur)
    return lines or [""]


def _run_color(run):
    """Best-effort RGB hex for a run's font color, or None."""
    try:
        color = run.font.color
        if color is not None and color.type is not None and color.rgb is not None:
            return str(color.rgb)
    except (AttributeError, ValueError, KeyError):
        pass
    return None


def layout_textframe(shape):
    """Lay out ``shape``'s text frame and return a list of line records.

    Each record is a dict: ``text``, ``x``/``y`` (baseline-box top-left, in),
    ``w``/``h`` (advance box, in), ``ink`` (glyph bbox as (x0,y0,x1,y1) in,
    absolute), ``size_pt``, ``bold``, ``color`` (hex str or None), ``align``.
    Coordinates are absolute inches on the 13.333x7.5in slide canvas.
    """
    tf = shape.text_frame
    left, top = (shape.left or 0) / EMU, (shape.top or 0) / EMU
    width, height = (shape.width or 0) / EMU, (shape.height or 0) / EMU
    ml = (tf.margin_left if tf.margin_left is not None else DEFAULT_MARGIN_LR_EMU) / EMU
    mr = (
        tf.margin_right if tf.margin_right is not None else DEFAULT_MARGIN_LR_EMU
    ) / EMU
    mt = (tf.margin_top if tf.margin_top is not None else DEFAULT_MARGIN_TB_EMU) / EMU
    avail = max(width - ml - mr, 0.05)
    wrap_on = tf.word_wrap is not False

    plan, total = [], 0.0
    for para in tf.paragraphs:
        runs = para.runs
        text = "".join(r.text for r in runs)
        size = next((r.font.size.pt for r in runs if r.font.size), None)
        if size is None:
            size = para.font.size.pt if para.font.size else DEFAULT_FONT_PT
        bold = any(r.font.bold for r in runs)
        color = next((_run_color(r) for r in runs if _run_color(r)), None)
        spacing = para.line_spacing
        line_h = (
            size
            * (spacing if isinstance(spacing, float) else 1.0)
            * LINE_HEIGHT_MULT
            / 72.0
        )
        before = (para.space_before.pt if para.space_before else 0) / 72.0
        after = (para.space_after.pt if para.space_after else 0) / 72.0
        lines = wrap(text, size, bold, avail) if wrap_on else ([text] if text else [""])
        plan.append((para, lines, size, bold, color, line_h, before, after))
        total += before + after + line_h * len(lines)

    anchor = str(tf.vertical_anchor or "")
    y = top + mt
    if "MIDDLE" in anchor:
        y = top + max((height - total) / 2.0, 0)
    elif "BOTTOM" in anchor:
        y = top + max(height - total, 0)

    records = []
    for para, lines, size, bold, color, line_h, before, after in plan:
        y += before
        align = str(para.alignment or "")
        for line in lines:
            w = adv_in(line, size, bold)
            x = left + ml
            if "CENTER" in align:
                x = left + ml + max((avail - w) / 2.0, 0)
            elif "RIGHT" in align:
                x = left + ml + max(avail - w, 0)
            ink = None
            if line.strip():
                bbox = _pil_font(size, bold).getbbox(line)
                ink = tuple(v / (4.0 * 72.0) for v in bbox)
                ink = (x + ink[0], y + ink[1], x + ink[2], y + ink[3])
            records.append(
                {
                    "text": line,
                    "x": x,
                    "y": y,
                    "w": w,
                    "h": line_h,
                    "ink": ink,
                    "size_pt": size,
                    "bold": bold,
                    "color": color,
                    "align": align,
                }
            )
            y += line_h
        y += after
    return records
