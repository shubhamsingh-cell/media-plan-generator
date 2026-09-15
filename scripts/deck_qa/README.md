# deck_qa

Render-verified deck QA for `ppt_generator.py`. This Mac has no Office
renderer (no Keynote/PowerPoint/LibreOffice/brew), so the documented
Keynote-AppleScript verification loop can't run here -- these three scripts
substitute a PIL-based render pipeline built on the deck's own shipped fonts.

## Commands

```
python3 scripts/deck_qa/collide.py <pptx>... [--slides 5,7]   # ink-collision sweep, exit 1 on findings
python3 scripts/deck_qa/matrix.py <outdir>                     # ~40-deck stress envelope
python3 scripts/deck_qa/render.py <pptx> <outdir> [--dpi 110]  # PNG rasterization + report.json
```

## Two rules

1. **A clean `collide.py` sweep is necessary, not sufficient: render and
   look.** The line-layout math can be self-consistent and still miss a
   defect no ink-rectangle model captures (color contrast, a truncated
   clause, a badge that silently didn't render). Always eyeball the PNGs for
   anything the collision sweep is asked to bless.
2. **Envelope decks must use real channel keys
   (`programmatic_dsp`/`global_boards`/`niche_boards`/`regional_boards`/
   `social_media`/`employer_branding`/`apac_regional`/`emea_regional`) plus
   `_synthesized` data, or slides 5 and 7 never get stressed.** Display names
   silently fall through to `ppt_generator`'s 3-channel fallback, and an
   empty `_synthesized` means slide 5's benchmark table and slide 7's
   competitor cards stay at their trivial minimum -- a "clean" run against
   either mistake proves nothing.

## Poppins-dingbat caveat

The deck uses 9 glyphs (`▸ ✓ ● ○ → ⬢ ▲ ▼ ▶`) that the embedded Poppins faces
do not contain. `render.py` draws PIL "tofu" boxes for these -- this is a
**renderer limitation**, not a defect in the deck: PowerPoint/Keynote
substitute a fallback font for those code points on a real machine. Do not
read a tofu box in a rendered PNG as a missing-glyph bug in the generator.
