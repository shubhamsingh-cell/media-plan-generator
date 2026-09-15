"""PPTX -> PNG rasterizer for decks generated on a Mac with no Office
renderer (no Keynote/PowerPoint/LibreOffice/brew available here).

Walks each slide's shapes in z-order and draws, best-effort, with PIL:

  * solid-fill autoshapes (rect / rounded-rect / ellipse) -- reads
    ``fill.fore_color.rgb`` when ``fill.type`` is solid, skips otherwise
    (gradients/patterns/no-fill are not attempted);
  * pictures -- ``shape.image.blob`` scaled into the shape's box;
  * tables -- cell borders + text;
  * text frames -- laid out with the SAME wrap/anchor/alignment rules as
    ``collide.py`` (both import ``_textlayout.py``, so the numbers driving
    collision detection and the pixels on screen always agree).

Each shape is wrapped in its own try/except (specific exceptions only) so one
bad shape can never kill the whole render.

Usage:
    python3 scripts/deck_qa/render.py <pptx> <outdir> [--dpi 110]

Writes ``<outdir>/slide-NN.png`` (one per slide) and ``<outdir>/report.json``:
a list of per-text-shape records ``{slide, idx, name, box (in), font_pt_min,
n_lines, text_height_in, overflow_in}``.

KNOWN LIMITATION: the deck uses 9 glyphs (tilde-bullet markers such as the
triangle/circle/hex/arrow set) that Poppins does not contain -- PIL draws
"tofu" boxes for them. This is a renderer limitation, not a defect in the
deck: PowerPoint/Keynote substitute a fallback font for those code points and
would render them correctly.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _textlayout import EMU, FONTS_DIR, layout_textframe  # noqa: E402

from PIL import Image, ImageDraw, ImageFont  # noqa: E402
from pptx import Presentation  # noqa: E402
from pptx.enum.dml import MSO_FILL_TYPE  # noqa: E402
from pptx.enum.shapes import MSO_SHAPE_TYPE  # noqa: E402

CANVAS_W_IN = 13.333
CANVAS_H_IN = 7.5


def _rgb(color_obj):
    try:
        if color_obj is not None and color_obj.type is not None:
            rgb = color_obj.rgb
            if rgb is not None:
                return (rgb[0], rgb[1], rgb[2])
    except (AttributeError, ValueError, KeyError, TypeError):
        pass
    return None


def _draw_autoshape(draw, shape, dpi):
    try:
        fill = shape.fill
        if fill.type != MSO_FILL_TYPE.SOLID:
            return
        color = _rgb(fill.fore_color)
        if color is None:
            return
    except (AttributeError, ValueError, KeyError, TypeError):
        return
    x0 = (shape.left or 0) / EMU * dpi
    y0 = (shape.top or 0) / EMU * dpi
    x1 = x0 + (shape.width or 0) / EMU * dpi
    y1 = y0 + (shape.height or 0) / EMU * dpi
    try:
        auto_type = str(shape.auto_shape_type or "")
    except (AttributeError, ValueError):
        auto_type = ""
    if "OVAL" in auto_type:
        draw.ellipse([x0, y0, x1, y1], fill=color)
    elif "ROUNDED" in auto_type:
        radius = min(x1 - x0, y1 - y0) * 0.15
        draw.rounded_rectangle([x0, y0, x1, y1], radius=max(radius, 1), fill=color)
    else:
        draw.rectangle([x0, y0, x1, y1], fill=color)


def _draw_picture(img, shape, dpi):
    try:
        blob = shape.image.blob
    except (AttributeError, ValueError, OSError) as exc:
        print(f"  [render] picture shape#{shape.shape_id} skipped: {exc}")
        return
    try:
        from io import BytesIO

        pic = Image.open(BytesIO(blob)).convert("RGBA")
        w = max(int((shape.width or 0) / EMU * dpi), 1)
        h = max(int((shape.height or 0) / EMU * dpi), 1)
        pic = pic.resize((w, h))
        x0 = int((shape.left or 0) / EMU * dpi)
        y0 = int((shape.top or 0) / EMU * dpi)
        img.paste(pic, (x0, y0), pic)
    except (OSError, ValueError) as exc:
        print(f"  [render] picture shape#{shape.shape_id} draw failed: {exc}")


def _draw_table(draw, shape, dpi, font_cache):
    try:
        table = shape.table
    except (AttributeError, ValueError) as exc:
        print(f"  [render] table shape#{shape.shape_id} skipped: {exc}")
        return
    left = (shape.left or 0) / EMU * dpi
    top = (shape.top or 0) / EMU * dpi
    y = top
    for row in table.rows:
        x = left
        row_h = (row.height or 0) / EMU * dpi
        for cell in row.cells:
            col_w = (cell.width or 0) / EMU * dpi if hasattr(cell, "width") else 0
            try:
                draw.rectangle([x, y, x + col_w, y + row_h], outline=(180, 180, 180))
                text = cell.text_frame.text if cell.has_text_frame else ""
                if text.strip():
                    font = font_cache(9, False)
                    draw.text((x + 3, y + 3), text[:60], fill=(30, 30, 30), font=font)
            except (AttributeError, ValueError) as exc:
                print(f"  [render] table cell skipped: {exc}")
            x += col_w
        y += row_h


def _draw_textframe(img, draw, shape, dpi, font_cache, report, slide_no, idx):
    try:
        recs = layout_textframe(shape)
    except (AttributeError, ValueError, OSError) as exc:
        print(f"  [render] text shape#{idx} slide {slide_no} skipped: {exc}")
        return
    if not recs:
        return
    sizes = [r["size_pt"] for r in recs if r["text"].strip()]
    box_h_in = (shape.height or 0) / EMU
    text_bottom_in = max((r["y"] + r["h"] for r in recs), default=0)
    box_top_in = (shape.top or 0) / EMU
    overflow_in = max(0.0, text_bottom_in - (box_top_in + box_h_in))
    report.append(
        {
            "slide": slide_no,
            "idx": idx,
            "name": shape.name,
            "box_in": [
                round((shape.left or 0) / EMU, 3),
                round((shape.top or 0) / EMU, 3),
                round((shape.width or 0) / EMU, 3),
                round((shape.height or 0) / EMU, 3),
            ],
            "font_pt_min": round(min(sizes), 1) if sizes else None,
            "n_lines": len([r for r in recs if r["text"].strip()]),
            "text_height_in": round(text_bottom_in - box_top_in, 3),
            "overflow_in": round(overflow_in, 3),
        }
    )
    for r in recs:
        if not r["text"].strip():
            continue
        try:
            color = (32, 32, 88)
            if r["color"]:
                hexs = r["color"]
                color = tuple(int(hexs[i : i + 2], 16) for i in (0, 2, 4))
            font = font_cache(r["size_pt"], r["bold"])
            draw.text((r["x"] * dpi, r["y"] * dpi), r["text"], fill=color, font=font)
        except (OSError, ValueError, IndexError) as exc:
            print(f"  [render] line draw failed on shape#{idx} slide {slide_no}: {exc}")


def render_pptx(path, outdir, dpi=110):
    os.makedirs(outdir, exist_ok=True)
    prs = Presentation(path)
    px_w = int(CANVAS_W_IN * dpi)
    px_h = int(CANVAS_H_IN * dpi)

    _font_cache = {}

    def font_cache(size_pt, bold):
        key = (round(size_pt, 1), bold)
        if key not in _font_cache:
            name = "Poppins-Bold.ttf" if bold else "Poppins-Regular.ttf"
            _font_cache[key] = ImageFont.truetype(
                os.path.join(FONTS_DIR, name), max(int(size_pt * dpi / 72.0), 4)
            )
        return _font_cache[key]

    report = []
    for i, slide in enumerate(prs.slides, 1):
        img = Image.new("RGB", (px_w, px_h), (255, 252, 249))
        draw = ImageDraw.Draw(img)
        for idx, shape in enumerate(slide.shapes):
            try:
                stype = shape.shape_type
            except (AttributeError, ValueError, KeyError) as exc:
                print(f"  [render] shape#{idx} slide {i}: type lookup failed: {exc}")
                continue
            try:
                if stype == MSO_SHAPE_TYPE.PICTURE:
                    _draw_picture(img, shape, dpi)
                    draw = ImageDraw.Draw(img)
                elif shape.has_table:
                    _draw_table(draw, shape, dpi, font_cache)
                elif stype == MSO_SHAPE_TYPE.AUTO_SHAPE:
                    _draw_autoshape(draw, shape, dpi)
            except (AttributeError, ValueError, KeyError, TypeError) as exc:
                print(f"  [render] shape#{idx} slide {i} fill/table draw failed: {exc}")

            try:
                if shape.has_text_frame and shape.text_frame.text.strip():
                    _draw_textframe(img, draw, shape, dpi, font_cache, report, i, idx)
            except (AttributeError, ValueError, OSError) as exc:
                print(f"  [render] shape#{idx} slide {i} text draw failed: {exc}")

        out_path = os.path.join(outdir, f"slide-{i:02d}.png")
        img.save(out_path)

        non_bg = sum(1 for px in img.getdata() if px != (255, 252, 249)) / float(
            px_w * px_h
        )
        if i <= 2:
            print(f"slide {i}: non-background pixel fraction = {non_bg:.4f}")

    with open(os.path.join(outdir, "report.json"), "w") as f:
        json.dump(report, f, indent=2)
    print(f"Rendered {len(prs.slides)} slides to {outdir}")
    return len(prs.slides)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pptx")
    ap.add_argument("outdir")
    ap.add_argument("--dpi", type=int, default=110)
    args = ap.parse_args()
    render_pptx(args.pptx, args.outdir, args.dpi)


if __name__ == "__main__":
    main()
