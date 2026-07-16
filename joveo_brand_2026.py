"""Joveo Brand System 2026 -- canonical source of truth.

Extracted from the official Joveo client deck ("Invisible Media Planning
Approach", June 2026) and aligned to Joveo brand guidelines. Every
presentation the platform generates (Google Slides, PPTX, PDF) and the Nova
chatbot UI must use these exact values -- do not hardcode brand hexes
elsewhere; import from here.

Palette roles (deck-exact):
  Indigo / Port Gore  #202058  -- titles, headers, table header row, wordmark
  Purple / Blue Violet #5A54BE  -- primary accent, KPI numbers, links, series 1
  Purple hover         #5A4FC4
  Purple light         #8680D6  -- secondary accent, series 4
  Teal / Downy         #6BB5CE  -- highlight / "pull" channels / series 2
  Teal deep            #3E8FAB
  Magenta              #B7669E  -- single pop accent / series 3
  Lavender 50          #F4F4FF  -- card surface
  Lavender 100         #ECEAF7  -- alt card / bot chat bubble
  Canvas               #FFFCF9  -- warm white slide canvas
  Ink                  #1F2937  -- body text
  Muted                #6E6E8C  -- captions / footnotes
  Border               #E3E1F1  -- card & table borders

Fonts:  Poppins (headings)  |  Inter (body)
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Hex constants (deck-exact)
# ---------------------------------------------------------------------------
INDIGO_900 = "#131A38"
INDIGO_800 = "#1A1A4E"
INDIGO = "#202058"  # Port Gore -- primary dark
INDIGO_600 = "#262261"
INDIGO_500 = "#33334F"

PURPLE = "#5A54BE"  # Blue Violet -- primary accent
PURPLE_HOVER = "#5A4FC4"
PURPLE_LIGHT = "#8680D6"
PURPLE_300 = "#A6A3C1"

TEAL = "#6BB5CE"  # Downy -- secondary accent
TEAL_DEEP = "#3E8FAB"
TEAL_500 = "#45B6C8"

MAGENTA = "#B7669E"  # pink/magenta pop accent

# Surfaces
WHITE = "#FFFFFF"
CANVAS = "#FFFCF9"  # warm white slide canvas
LAVENDER_50 = "#F4F4FF"  # card surface
LAVENDER_100 = "#ECEAF7"  # alt card / bot bubble
BLUE_50 = "#EEF6FF"

# Neutrals
INK = "#1F2937"  # body text
MUTED = "#6E6E8C"  # captions
MUTED_2 = "#6B7280"
BORDER = "#E3E1F1"
BORDER_STRONG = "#D8D8D8"
BLACK = "#000000"

# Functional / status (kept for data-viz + status, not brand)
GREEN = "#22C55E"
AMBER = "#F59E0B"
RED = "#EF4444"

# ---------------------------------------------------------------------------
# Legacy alias names (so existing generator code can swap in cleanly)
# ---------------------------------------------------------------------------
PORT_GORE = INDIGO  # was #202058 (unchanged)
BLUE_VIOLET = PURPLE  # was #5A54BD -> #5A54BE
DOWNY_TEAL = TEAL  # was #6BB3CD -> #6BB5CE
PINK_ACCENT = MAGENTA  # was #C8589C / #B5669C -> #B7669E

# ---------------------------------------------------------------------------
# Data-viz palette (categorical order) + sequential purple ramp
# ---------------------------------------------------------------------------
DATAVIZ = [PURPLE, TEAL, MAGENTA, PURPLE_LIGHT, TEAL_DEEP, INDIGO]
SEQUENTIAL_PURPLE = [LAVENDER_100, PURPLE_300, PURPLE_LIGHT, PURPLE, INDIGO]

# ---------------------------------------------------------------------------
# Typography
# ---------------------------------------------------------------------------
FONT_HEADING = "Poppins"
FONT_BODY = "Inter"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def hex_to_rgb_tuple(hex_str: str) -> tuple[int, int, int]:
    """'#5A54BE' -> (90, 84, 190). Use with pptx RGBColor(*hex_to_rgb_tuple(x))."""
    h = hex_str.lstrip("#")
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))


def hex_to_slides_rgb(hex_str: str) -> dict[str, float]:
    """'#5A54BE' -> {'red': .., 'green': .., 'blue': ..} for Google Slides API."""
    r, g, b = hex_to_rgb_tuple(hex_str)
    return {"red": r / 255.0, "green": g / 255.0, "blue": b / 255.0}


# Google Slides API color dicts (deck-exact), ready to use:
SLIDES = {
    "indigo": hex_to_slides_rgb(INDIGO),
    "purple": hex_to_slides_rgb(PURPLE),
    "purple_light": hex_to_slides_rgb(PURPLE_LIGHT),
    "teal": hex_to_slides_rgb(TEAL),
    "teal_deep": hex_to_slides_rgb(TEAL_DEEP),
    "magenta": hex_to_slides_rgb(MAGENTA),
    "lavender_50": hex_to_slides_rgb(LAVENDER_50),
    "lavender_100": hex_to_slides_rgb(LAVENDER_100),
    "canvas": hex_to_slides_rgb(CANVAS),
    "white": hex_to_slides_rgb(WHITE),
    "ink": hex_to_slides_rgb(INK),
    "muted": hex_to_slides_rgb(MUTED),
    "border": hex_to_slides_rgb(BORDER),
}
