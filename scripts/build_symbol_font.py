#!/usr/bin/env python3
"""Build fonts/NovaDeckSymbols-{Regular,Bold}.ttf -- the deck's symbol face.

WHY THIS EXISTS
---------------
``ppt_generator`` embeds Poppins into every generated .pptx so the deck renders
on-brand on machines without Poppins installed. But Poppins is a 504-glyph Latin
text face: it contains no check mark, arrow, triangle, circle or hexagon. Every
such character the deck drew (bullet markers, benchmark up/down arrows, legend
swatches, and arrows arriving from KB data like
``data/joveo_media_plan_deck_2026.json``) therefore fell OUTSIDE the embedding
guarantee and was rendered by whatever the viewer's machine substituted -- or as
a tofu box. This builds a tiny companion face that covers exactly those symbols,
so the "every rendered codepoint is covered by an embedded face" invariant holds
for the whole deck.

Source is the DejaVu Sans shipped inside matplotlib (already a hard dependency of
this project -- see requirements.txt -- and already this deck's chart-font
fallback, ``_CHART_FONT_FAMILY``). DejaVu's Bitstream Vera licence permits
modification and redistribution provided the result is renamed away from
"Bitstream"/"Vera" and the copyright + permission notice travels with it; both
are done here (the notice is written to fonts/LICENSE_NOVA_DECK_SYMBOLS.txt and
retained in the font's own name table).

Regenerate with:  python3 scripts/build_symbol_font.py
The generated .ttf files ARE committed -- Render deploys must not depend on
matplotlib's internal data paths at runtime.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

from fontTools import subset
from fontTools.ttLib import TTFont

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FONTS_DIR = PROJECT_ROOT / "fonts"

# The typeface name the deck sets on symbol runs and embeds under. Must match
# ppt_generator.SYMBOL_FONT_FAMILY exactly, and must not contain "Bitstream"
# or "Vera" (Bitstream Vera licence condition for modified fonts).
FAMILY = "Nova Deck Symbols"

# Codepoints Poppins lacks that a Joveo deck plausibly renders. The nine marked
# CORE are the ones the deck actually drew while un-embedded; the rest are cheap
# insurance so a future KB/data edit that introduces, say, a "←" does not
# silently reintroduce the same class of bug. Anything outside this set is
# caught loudly by tests/test_deck_layout.py::test_every_rendered_character_
# is_covered_by_its_embedded_font rather than shipping tofu.
SYMBOLS = [
    # --- CORE: the nine the deck already drew ---
    0x25B8,  # ▸ bullet marker
    0x2713,  # ✓ check bullet
    0x25CF,  # ● bullet marker
    0x25CB,  # ○ legend swatch
    0x2192,  # → in-prose arrow (also arrives from KB data)
    0x2B22,  # ⬢ legend swatch
    0x25B2,  # ▲ beating benchmark
    0x25BC,  # ▼ trailing benchmark
    0x25B6,  # ▶ section icon
    # --- headroom ---
    0x2190,
    0x2191,
    0x2193,
    0x2194,
    0x21D2,  # other arrows
    0x2714,
    0x2717,
    0x2718,
    0x2726,
    0x2605,
    0x2606,  # checks / crosses / stars
    0x25A0,
    0x25A1,
    0x25AA,
    0x25AB,  # squares
    0x25B4,
    0x25BE,
    0x25C0,
    0x25C6,
    0x25C7,  # more triangles / diamonds
    0x2023,
    0x2043,
    0x2739,
    0x26A0,
    0x2261,  # bullets / warning / misc
    # --- CURRENCY SIGNS plan_currency can emit that Poppins cannot render ---
    # plan_currency._CODE_TO_SYMBOL maps 44 ISO codes to display symbols, and
    # its own comment used to justify the choices against "Inter / Calibri" --
    # font families the deck stopped using when it standardised on Poppins.
    # Eight of those symbols were left renderable by NO embedded face, which
    # would have put a tofu box on EVERY money figure for that market. These
    # seven are restored here; BDT's ৳ (U+09F3) is not in DejaVu either, so
    # plan_currency falls back to the "BDT " ISO-code form for that one.
    0x0E3F,  # ฿ THB
    0x20A6,  # ₦ NGN
    0x20A9,  # ₩ KRW
    0x20AA,  # ₪ ILS
    0x20AB,  # ₫ VND
    0x20B1,  # ₱ PHP
    0x20B4,  # ₴ UAH
    # headroom: the rest of the Currency Symbols block DejaVu covers, so adding
    # a market to plan_currency does not silently reintroduce the same bug.
    0x20A1,
    0x20A2,
    0x20A3,
    0x20A4,
    0x20A5,
    0x20A7,
    0x20A8,
    0x20AC,
    0x20AD,
    0x20AE,
    0x20AF,
    0x20B0,
    0x20B2,
    0x20B3,
    0x20B5,
]

# (source DejaVu file, output filename, OS/2+head style to stamp)
FACES = [
    ("DejaVuSans.ttf", f"NovaDeckSymbols-Regular.ttf", "Regular"),
    ("DejaVuSans-Bold.ttf", f"NovaDeckSymbols-Bold.ttf", "Bold"),
]


def _dejavu_dir() -> Path:
    import matplotlib

    return Path(matplotlib.__file__).parent / "mpl-data" / "fonts" / "ttf"


# head.created / head.modified default to "now", which would make every rebuild
# emit a byte-different .ttf and therefore a spurious git diff -- and would make
# it impossible to check that a committed font really is what this script
# produces. Pin them (seconds since the 1904 Mac epoch; this is 2020-01-01).
_FIXED_TIMESTAMP = 3660508800


def _rename(font: TTFont, style: str) -> None:
    """Rewrite the name table to the Nova family, keeping the licence records.

    nameID 0 (copyright) and 13/14 (licence) are DELIBERATELY preserved -- the
    Bitstream Vera licence requires the notice to travel with every copy.
    """
    full = f"{FAMILY} {style}" if style != "Regular" else FAMILY
    ps = full.replace(" ", "")
    new = {1: FAMILY, 2: style, 3: f"{FAMILY}:{style}", 4: full, 6: ps}
    for rec in font["name"].names:
        if rec.nameID in new:
            font["name"].setName(
                new[rec.nameID], rec.nameID, rec.platformID, rec.platEncID, rec.langID
            )
    # Drop preferred-family/subfamily so the family resolves unambiguously.
    for nid in (16, 17):
        font["name"].removeNames(nameID=nid)


def build() -> int:
    src_dir = _dejavu_dir()
    FONTS_DIR.mkdir(parents=True, exist_ok=True)
    wanted = sorted(set(SYMBOLS))

    for src_name, out_name, style in FACES:
        src = src_dir / src_name
        if not src.exists():
            print(f"FAIL: source font not found: {src}", file=sys.stderr)
            return 1

        opts = subset.Options()
        opts.name_IDs = ["*"]  # keep the name table so licence records survive
        opts.name_legacy = True
        opts.notdef_outline = True  # keep a visible .notdef rather than a blank
        opts.recalc_bounds = True
        opts.drop_tables = []

        font = TTFont(str(src))
        sub = subset.Subsetter(options=opts)
        sub.populate(unicodes=wanted)
        sub.subset(font)
        _rename(font, style)
        font["head"].created = _FIXED_TIMESTAMP
        font["head"].modified = _FIXED_TIMESTAMP

        out = FONTS_DIR / out_name
        font.save(str(out))

        # Verify what we just wrote, rather than trusting the build.
        check = TTFont(str(out), lazy=True)
        cmap: set[int] = set()
        for t in check["cmap"].tables:
            cmap |= set(t.cmap.keys())
        missing = [hex(c) for c in wanted if c not in cmap]
        fam = check["name"].getDebugName(1)
        if missing:
            print(f"FAIL: {out_name} missing {missing}", file=sys.stderr)
            return 1
        if fam != FAMILY:
            print(
                f"FAIL: {out_name} family is {fam!r}, expected {FAMILY!r}",
                file=sys.stderr,
            )
            return 1
        print(
            f"  {out_name:32s} {out.stat().st_size:>6,} bytes  "
            f"{len(cmap)} glyphs  family={fam!r}"
        )

    lic_src = src_dir / "LICENSE_DEJAVU"
    lic_dst = FONTS_DIR / "LICENSE_NOVA_DECK_SYMBOLS.txt"
    if lic_src.exists():
        header = (
            f"{FAMILY} is a renamed subset of DejaVu Sans, generated by\n"
            "scripts/build_symbol_font.py. It contains only the symbol glyphs the\n"
            "Joveo deck renders and that Poppins does not cover.\n\n"
            "The upstream DejaVu / Bitstream Vera licence follows, reproduced in\n"
            "full as that licence requires. The font has been renamed away from\n"
            'the "Bitstream" and "Vera" names as the licence requires for\n'
            "modified fonts.\n\n" + "=" * 70 + "\n\n"
        )
        lic_dst.write_text(
            header + lic_src.read_text(encoding="utf-8"), encoding="utf-8"
        )
        print(f"  {lic_dst.name:32s} {lic_dst.stat().st_size:>6,} bytes")
    else:
        print(f"FAIL: DejaVu licence not found at {lic_src}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    print(f"Building {FAMILY} ({len(set(SYMBOLS))} codepoints) into {FONTS_DIR}")
    sys.exit(build())
