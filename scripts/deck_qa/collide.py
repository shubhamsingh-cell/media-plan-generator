"""Text-INK collision analyzer for generated .pptx decks.

Renders every text frame's layout independently (see ``_textlayout.py``,
which does NOT reuse ppt_generator's own ``_estimate_lines``/``_measure_lines``
-- a checker built on the generator's own ruler cannot catch a defect in that
ruler) and reports, per slide:

  * pairs of text frames whose painted glyph ink overlaps by more than
    ``INK_TOL_IN`` on BOTH axes (touching hairlines are not a finding);
  * frames whose smallest run/paragraph font is under 8pt;
  * ink that falls outside the 13.333x7.5in canvas (padded slightly to
    13.353x7.52in to allow for sub-hairline rounding).

Usage:
    python3 scripts/deck_qa/collide.py <pptx>... [--slides 5,7]

Exit code is 1 if any finding is reported across the given deck(s), 0
otherwise. Always prints ``TOTAL FINDINGS: n`` as its last line.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _textlayout import layout_textframe  # noqa: E402

from pptx import Presentation  # noqa: E402

INK_TOL_IN = 0.015
MIN_FONT_PT = 8.0
CANVAS_W_IN = 13.353
CANVAS_H_IN = 13.353 * 7.5 / 13.333  # keep the 13.333x7.5 aspect, padded evenly
CANVAS_H_IN = 7.52


def _frame_ink_rects(shape):
    recs = layout_textframe(shape)
    return [(r["ink"], r["text"], r["size_pt"]) for r in recs if r["ink"]]


def _min_font_pt(shape):
    recs = layout_textframe(shape)
    sizes = [r["size_pt"] for r in recs if r["text"].strip()]
    return min(sizes) if sizes else None


def analyze_slide(slide, slide_no):
    findings = []
    frames = []
    for idx, shape in enumerate(slide.shapes):
        if not shape.has_text_frame or not shape.text_frame.text.strip():
            continue
        rects = _frame_ink_rects(shape)
        if rects:
            frames.append((idx, shape, rects))

        min_pt = _min_font_pt(shape)
        if min_pt is not None and min_pt < MIN_FONT_PT:
            findings.append(
                f"slide {slide_no}: shape#{idx} has min font {min_pt:.1f}pt (< {MIN_FONT_PT}pt)"
            )

        for ink, text, size in rects:
            x0, y0, x1, y1 = ink
            if (
                x0 < -INK_TOL_IN
                or y0 < -INK_TOL_IN
                or x1 > CANVAS_W_IN + INK_TOL_IN
                or y1 > CANVAS_H_IN + INK_TOL_IN
            ):
                findings.append(
                    f"slide {slide_no}: shape#{idx} ink outside canvas "
                    f"({x0:.3f},{y0:.3f},{x1:.3f},{y1:.3f}) text={text!r}"
                )

    for i in range(len(frames)):
        for j in range(i + 1, len(frames)):
            for ra, ta, _sa in frames[i][2]:
                for rb, tb, _sb in frames[j][2]:
                    ox = min(ra[2], rb[2]) - max(ra[0], rb[0])
                    oy = min(ra[3], rb[3]) - max(ra[1], rb[1])
                    if ox > INK_TOL_IN and oy > INK_TOL_IN:
                        findings.append(
                            f"slide {slide_no}: shape#{frames[i][0]} {ta!r} "
                            f"overlaps shape#{frames[j][0]} {tb!r} "
                            f"(ox={ox:.3f}in oy={oy:.3f}in)"
                        )
    return findings


def analyze_pptx(path, only_slides=None):
    prs = Presentation(path)
    total = []
    for i, slide in enumerate(prs.slides, 1):
        if only_slides and i not in only_slides:
            continue
        hits = analyze_slide(slide, i)
        if hits:
            print(
                f"--- {os.path.basename(path)} : slide {i} : {len(hits)} finding(s) ---"
            )
            for h in hits:
                print(f"  {h}")
        total.extend(hits)
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pptx", nargs="+")
    ap.add_argument(
        "--slides", default="", help="comma-separated 1-based slide numbers"
    )
    args = ap.parse_args()
    only = None
    if args.slides.strip():
        only = {int(s) for s in args.slides.split(",") if s.strip()}

    grand_total = 0
    for path in args.pptx:
        hits = analyze_pptx(path, only)
        grand_total += len(hits)

    print(f"TOTAL FINDINGS: {grand_total}")
    return 1 if grand_total > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
