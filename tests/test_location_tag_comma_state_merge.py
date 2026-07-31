"""Regression pin for the location-tag comma/state-code merge in
templates/partials/index/body_app_js.html's ``addTag``.

Background: the ``locationInput`` field commits a tag on every "," keydown
(``e.preventDefault()`` blocks the comma from ever being typed) so users can
rapid-add "Atlanta, Chicago, Miami". That collides with the "City, ST"
disambiguation format the location resolver supports: typing
"Springfield, MO" character-by-character committed "Springfield" the
instant the comma was pressed, then "MO" landed as a SEPARATE tag on
Enter/blur -- silently splitting one disambiguated city into a bare state
name and a still-ambiguous city, exactly the failure mode this whole
feature exists to prevent. Verified live with real simulated keystrokes
(KeyboardEvent per character, not JS array pushes) in the session that
added this fix: typing "Springfield, MO" now produces one tag
"Springfield, MO" (resolved, Greene County), while "Atlanta, Chicago,
Miami" still produces three separate tags, "CA, NY" (two independent bare
states) stays two tags, and "Remote, TX" stays two tags.

This repo has no JS execution test harness (no Jest/Playwright), so this
is a STRUCTURAL pin, not a behavioral one: it asserts the merge logic's
key literals and branches are still present in the shipped source, so a
future refactor that silently drops this logic fails loudly here instead
of only failing "in the field" the next time someone types a
disambiguated city. Re-verify live in the browser (real KeyboardEvents)
after touching addTag/VALID_US_STATES/`_LOCATION_MERGE_EXCLUDED`.
"""

from __future__ import annotations

from pathlib import Path

_JS_PATH = (
    Path(__file__).resolve().parent.parent
    / "templates"
    / "partials"
    / "index"
    / "body_app_js.html"
)


def _source() -> str:
    return _JS_PATH.read_text(encoding="utf-8")


def test_merge_exclusion_list_covers_location_kind_keywords():
    """"Remote"/"Nationwide"/etc. are location KINDS, not city names --
    "Remote, TX" must never merge into one tag."""
    src = _source()
    assert "_LOCATION_MERGE_EXCLUDED" in src
    for keyword in ("remote", "nationwide", "united states", "usa"):
        assert f'"{keyword}"' in src, f"{keyword!r} missing from the merge-exclusion list"


def test_merge_logic_present_and_guards_against_double_bare_state():
    """The merge must require the PREVIOUS tag to look like a city, not
    itself a bare state ("CA" then "NY" must stay two independent tags)."""
    src = _source()
    assert "prevIsBareState" in src, "bare-state-after-bare-state guard was removed"
    assert "!prevTag.includes(\",\")" in src, "already-qualified-tag guard was removed"
    assert "VALID_US_STATES.has(bare)" in src, "state-code detection was removed"


def test_merge_only_applies_to_locations_container():
    """Roles and competitors never use comma-as-multi-add, so this logic
    must stay scoped to locationsContainer and not leak into those tag
    inputs."""
    src = _source()
    idx = src.index("_LOCATION_MERGE_EXCLUDED")
    merge_block = src[idx : idx + 2000]
    assert 'containerId === "locationsContainer"' in merge_block


def test_valid_us_states_set_is_defined_before_any_use_in_addtag():
    """addTag references VALID_US_STATES; it's declared later in the same
    top-level script (fine at runtime -- addTag only runs from an event
    handler, long after the script has finished initial execution -- but
    a future split into separate script tags would break this silently)."""
    src = _source()
    add_tag_idx = src.index("function addTag(")
    states_idx = src.index("const VALID_US_STATES")
    assert states_idx > add_tag_idx, (
        "VALID_US_STATES moved before addTag's definition point -- re-verify this "
        "is still a single top-level script (no separate <script> tags/modules), "
        "since ES module scoping would change these ordering guarantees"
    )
