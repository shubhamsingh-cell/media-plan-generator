#!/usr/bin/env python3
"""Generate the branded 1200x630 OG/LinkedIn social card for shared scorecards.

Writes ``static/og-scorecard.png`` (referenced by scorecard_generator.py's
``og:image`` meta tag). Re-run after a brand refresh. Deterministic — committed
output, not a runtime dependency.

Usage:  python3 scripts/generate_og_card.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.colors as mc  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

from joveo_brand_2026 import INDIGO, PURPLE, TEAL, MAGENTA  # noqa: E402

W, H = 1200, 630
OUT = PROJECT_ROOT / "static" / "og-scorecard.png"


def main() -> int:
    fig = plt.figure(figsize=(W / 100, H / 100), dpi=100)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, W)
    ax.set_ylim(0, H)
    ax.axis("off")

    # Indigo -> purple diagonal gradient (brand title-slide style)
    grad = np.zeros((H, W, 3))
    c0 = np.array(mc.to_rgb(INDIGO))
    c1 = np.array(mc.to_rgb(PURPLE))
    for x in range(W):
        t = x / W
        grad[:, x, :] = c0 * (1 - t) + c1 * t
    ax.imshow(grad, extent=[0, W, 0, H], aspect="auto", zorder=0)

    # Teal -> magenta accent ribbon along the bottom
    ribbon = np.zeros((12, W, 3))
    ct = np.array(mc.to_rgb(TEAL))
    cm = np.array(mc.to_rgb(MAGENTA))
    for x in range(W):
        t = x / W
        ribbon[:, x, :] = ct * (1 - t) + cm * t
    ax.imshow(ribbon, extent=[0, W, 0, 14], aspect="auto", zorder=2)

    ax.text(70, H - 80, "joveo", color="white", fontsize=54, fontweight="bold",
            fontfamily="DejaVu Sans", zorder=3)
    ax.text(70, 360, "AI Media Plan", color="white", fontsize=76,
            fontweight="bold", fontfamily="DejaVu Sans", zorder=3)
    ax.text(70, 280, "Recruitment Marketing Intelligence", color="#D7D4F5",
            fontsize=34, fontfamily="DejaVu Sans", zorder=3)
    ax.add_patch(FancyBboxPatch(
        (70, 150), 720, 70, boxstyle="round,pad=8,rounding_size=18",
        facecolor="#FFFFFF", edgecolor="none", alpha=0.12, zorder=3))
    ax.text(95, 175, "Channel mix · Budgets · CPA benchmarks · ROI projections",
            color="white", fontsize=26, fontfamily="DejaVu Sans", zorder=4)

    fig.savefig(str(OUT), dpi=100, facecolor=INDIGO)
    plt.close(fig)
    print(f"Wrote {OUT} ({OUT.stat().st_size} bytes, {W}x{H})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
