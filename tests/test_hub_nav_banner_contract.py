"""Locks the measurement-free nav/banner layout contract on the hub.

The guest "Previewing Nova" banner (#nova-guest-banner, injected by
static/nova-auth-gate.js with inline top:0 / z-index:999998) must sit BELOW
the fixed nav via a CSS override — never the nav below the banner via a
JS-measured offset. This is the permanent fix for the logo-clip defect: the
nav is pinned at top:0 with the constant height --nav-h, so nothing at the
top of the viewport is ever measured at runtime.

If any assertion here fails, someone has reintroduced a runtime-measured
offset (the mechanism that desynced) or broken the stacking contract.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HUB_CSS = (ROOT / "static" / "css" / "hub.css").read_text(encoding="utf-8")
HUB_HTML = (ROOT / "templates" / "hub.html").read_text(encoding="utf-8")
HUB_JS = (ROOT / "static" / "js" / "hub.js").read_text(encoding="utf-8")


def _block(css: str, selector: str) -> str:
    """Return the first rule block for a selector (naive but stable here)."""
    m = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", css)
    assert m, f"selector {selector!r} not found in hub.css"
    return m.group(1)


class TestNavIsConstant:
    def test_nav_pinned_at_top_zero(self):
        nav = _block(HUB_CSS, ".nav")
        assert re.search(r"top:\s*0\s*;", nav), ".nav must be pinned at top: 0"

    def test_nav_height_uses_token(self):
        nav = _block(HUB_CSS, ".nav")
        assert "height: var(--nav-h)" in nav, ".nav height must be var(--nav-h)"

    def test_nav_h_token_defined(self):
        assert re.search(r"--nav-h:\s*64px", HUB_CSS), ":root must define --nav-h: 64px"

    def test_nav_never_reads_banner_height(self):
        nav = _block(HUB_CSS, ".nav")
        assert "--nova-banner-h" not in nav, (
            ".nav must not offset by a measured banner height — that mechanism "
            "desyncs (see the 'measurement-free' commit message)"
        )


class TestBannerSlidesBelowNav:
    def test_banner_override_exists(self):
        banner = _block(HUB_CSS, "#nova-guest-banner")
        assert "top: var(--nav-h) !important" in banner, (
            "#nova-guest-banner must be repositioned below the nav with "
            "!important (it ships inline top:0 from nova-auth-gate.js)"
        )

    def test_banner_stacks_under_nav(self):
        banner = _block(HUB_CSS, "#nova-guest-banner")
        m = re.search(r"z-index:\s*(\d+)\s*!important", banner)
        assert m, "#nova-guest-banner must force a z-index"
        banner_z = int(m.group(1))
        nav = _block(HUB_CSS, ".nav")
        nav_z = int(re.search(r"z-index:\s*(\d+)", nav).group(1))
        assert banner_z < nav_z, (
            f"banner z ({banner_z}) must be below nav z ({nav_z}) so the "
            "mobile dropdown paints over the banner"
        )


class TestNoRuntimeMeasurement:
    def test_sync_script_stays_deleted(self):
        assert "--nova-banner-h" not in HUB_HTML, (
            "hub.html must not reintroduce the banner-measuring sync script"
        )

    def test_scroll_shrink_writes_the_token(self):
        assert 'setProperty("--nav-h"' in HUB_JS, (
            "hub.js scroll shrink must drive --nav-h so nav height, banner "
            "offset, and the dropdown anchor stay in lockstep"
        )
        assert re.search(r"nav\.style\.height\s*=", HUB_JS) is None, (
            "hub.js must not write an inline nav height — the token is the "
            "single source of truth"
        )
