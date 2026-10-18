"""/help/media-plans: the in-app guide, its links, and its claims.

The guide must only describe things the product does. Each claim test below
pins a phrase the guide uses to the place in the code that makes it true, so a
renamed button or sheet fails here instead of leaving the guide wrong.
"""

import io
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from routes import pages  # noqa: E402

TEMPLATES = PROJECT_ROOT / "templates"
HELP_PATH = "/help/media-plans"


class _Handler:
    """The slice of MediaPlanHandler that handle_page_routes touches."""

    def __init__(self):
        self.status = None
        self.headers_sent = {}
        self.headers = {}
        self.wfile = io.BytesIO()

    def send_response(self, code):
        self.status = code

    def send_header(self, key, value):
        self.headers_sent[key] = value

    def end_headers(self):
        pass

    def send_error(self, code, message=""):
        self.status = code

    def _serve_file(self, path, content_type):
        self.status = 200
        self.wfile.write(Path(path).read_bytes())


def _get(path):
    handler = _Handler()
    handled = pages.handle_page_routes(handler, path, None)
    return handled, handler.status, handler.wfile.getvalue().decode("utf-8")


def _help_html():
    return (TEMPLATES / "help-media-plans.html").read_text(encoding="utf-8")


class _Collect(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs, self.ids, self.h2, self._in_h2 = [], set(), [], False

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "a" and a.get("href"):
            self.hrefs.append(a["href"])
        if a.get("id"):
            self.ids.add(a["id"])
        if tag == "h2":
            self._in_h2 = True
            self.h2.append("")

    def handle_endtag(self, tag):
        if tag == "h2":
            self._in_h2 = False

    def handle_data(self, data):
        if self._in_h2:
            self.h2[-1] += data


def _parsed():
    parser = _Collect()
    parser.feed(_help_html())
    return parser


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


def test_route_returns_200_html_with_key_headings():
    handled, status, body = _get(HELP_PATH)
    assert handled is True and status == 200
    assert "<h1>How to use the Media Plan Generator and Nova</h1>" in body
    headings = [h.strip() for h in _parsed().h2 if h.strip()]
    for needed in (
        "What it is for",
        "Enter a brief",
        "What you get",
        "Read the numbers",
        "Before you send",
        "When to involve supply",
        "Start over",
        "FAQ",
    ):
        assert needed in headings, (needed, headings)


def test_trailing_slash_alias_serves_the_same_page():
    _, status_a, body_a = _get(HELP_PATH)
    _, status_b, body_b = _get(HELP_PATH + "/")
    assert status_a == status_b == 200
    assert body_a == body_b


# ---------------------------------------------------------------------------
# Links
# ---------------------------------------------------------------------------


def test_wizard_header_links_to_the_guide():
    from template_composer import get_composed_template

    wizard = get_composed_template("index").decode("utf-8")
    tag = re.search(r"<a\b[^>]*\bstb-help\b[^>]*>", wizard, re.S)
    assert tag, "wizard header lost its How-to-use link"
    assert 'href="/help/media-plans"' in tag.group(0)
    assert "How to use" in wizard


def test_nova_page_and_chat_empty_state_link_to_the_guide():
    nova_html = (TEMPLATES / "nova.html").read_text(encoding="utf-8")
    assert nova_html.count('href="/help/media-plans"') >= 2  # hero + chip fallback
    assert "np-help-link" in nova_html
    nova_js = (PROJECT_ROOT / "static" / "js" / "nova.js").read_text(encoding="utf-8")
    assert 'href="/help/media-plans"' in nova_js
    # nova.js is immutable-cached: its ?v= must move with it
    assert "/static/js/nova.js?v=s61" in nova_html


def test_every_internal_link_on_the_guide_resolves():
    parsed = _parsed()
    assert parsed.hrefs
    nova_special = {"/nova", "/nova/", "/", "", "/hub", "/hub/"}
    for href in parsed.hrefs:
        if href.startswith("#"):
            assert href[1:] in parsed.ids, f"dangling anchor {href}"
        elif href.startswith("/static/"):
            assert (PROJECT_ROOT / href.lstrip("/").split("?")[0]).exists(), href
        elif href.startswith("/"):
            assert href in pages._PAGE_LOOKUP or href in nova_special, (
                f"{href} has no route"
            )
        else:
            raise AssertionError(f"unexpected external/relative link {href}")


def test_stylesheet_and_icon_assets_exist():
    for ref in re.findall(r'(?:href|src)="(/static/[^"?]+)', _help_html()):
        assert (PROJECT_ROOT / ref.lstrip("/")).exists(), ref


# ---------------------------------------------------------------------------
# Theme, responsiveness, accessibility
# ---------------------------------------------------------------------------


def test_uses_shared_tokens_and_no_hardcoded_hex_in_css():
    html = _help_html()
    assert "/static/css/tokens.css" in html
    css = re.search(r"<style>(.*?)</style>", html, re.S).group(1)
    assert not re.findall(r"#[0-9a-fA-F]{3,8}\b", css), "use tokens, not hex"
    assert "var(--accent" in css and "var(--bg-primary)" in css


def test_responsive_down_to_375():
    html = _help_html()
    assert 'name="viewport" content="width=device-width, initial-scale=1.0"' in html
    assert "@media (max-width: 600px)" in html
    assert "@media (max-width: 900px)" in html
    # nothing may force a width wider than a 375px screen
    assert not re.search(r"min-width:\s*(?:[4-9]\d\d|\d{4,})px", html)


def test_keyboard_and_motion_accessibility():
    html = _help_html()
    assert 'class="skip-link" href="#main-content"' in html
    assert 'id="main-content"' in html
    assert ":focus-visible" in html
    assert html.count("<details>") >= 5  # native, keyboard-operable disclosure
    assert "<summary>" in html
    # smooth scrolling only when the user has not asked for reduced motion
    assert re.search(
        r"@media \(prefers-reduced-motion: no-preference\)\s*\{\s*html\s*\{\s*scroll-behavior: smooth",
        html,
    )
    assert "animation" not in html and "transition" not in html


def test_every_section_has_an_accessible_name():
    parsed = _parsed()
    for section_id in re.findall(r'<section id="([^"]+)" aria-labelledby="([^"]+)"', _help_html()):
        assert section_id[1] in parsed.ids


# ---------------------------------------------------------------------------
# The guide only describes what exists
# ---------------------------------------------------------------------------


def _wizard_source():
    parts = (PROJECT_ROOT / "templates" / "partials" / "index").glob("*.html")
    return "\n".join(p.read_text(encoding="utf-8") for p in parts)


def test_wizard_claims_match_the_wizard():
    wizard = _wizard_source()
    for phrase in (
        "Client Info",
        "Industry & Targeting",
        "Review & Export",
        "Exclude job boards",
        "Clear Form",
        "Click again to clear all inputs",
        "Quality Check Found Issues",
        "Ask Nova",
        "5 lakh",
        "10k-15k",
        "Per campaign (total)",
        "Per month",
        "Per quarter",
        "Per year",
        "7 MB",
        "Any spelling works",
        "removed from every sheet and slide",
    ):
        assert phrase in wizard, f"guide relies on wizard text: {phrase!r}"


def test_budget_claims_match_the_budget_parser():
    import wizard_inputs

    assert "Weekly, daily and hourly budgets aren't supported" in (
        PROJECT_ROOT / "wizard_inputs.py"
    ).read_text(encoding="utf-8")
    # the formats the guide lists are the ones the parser reads
    for raw, amount in (
        ("150,000", 150000),
        ("1.5M", 1500000),
        ("1.5 million", 1500000),
        ("500K", 500000),
        ("5 lakh", 500000),
    ):
        parsed = wizard_inputs.parse_budget_input(raw)
        assert parsed.ok and parsed.amount == amount, (raw, parsed)
    ranged = wizard_inputs.parse_budget_input("10k-15k")
    assert ranged.ok and ranged.amount == 12500  # planned at its midpoint


def test_nova_claims_match_nova():
    nova = (TEMPLATES / "nova.html").read_text(encoding="utf-8")
    assert "New Chat" in nova
    for prompt in ("average CPC", "Indeed vs LinkedIn", "compliance", "methodology"):
        assert prompt.lower() in nova.lower(), prompt


def test_output_claims_match_the_generators():
    import plan_trust

    source = (PROJECT_ROOT / "excel_v2.py").read_text(encoding="utf-8")
    for sheet in (
        "Executive Summary",
        "Channels & Strategy",
        "Market Intelligence",
        "Sources & Confidence",
        "Review before sending",
    ):
        assert f'"{sheet}"' in source, sheet
    assert plan_trust.NOT_RECORDED in _help_html()
    ppt = (PROJECT_ROOT / "ppt_generator.py").read_text(encoding="utf-8")
    assert "REVIEW BEFORE SENDING" not in ppt  # text lives in plan_trust
    assert "REVIEW BEFORE SENDING" in (PROJECT_ROOT / "plan_trust.py").read_text(
        encoding="utf-8"
    )
    # the dagger legend the guide quotes
    import publisher_filter

    assert "relationship unverified" in publisher_filter.RELATIONSHIP_NOTE
    assert "confirm before quoting" in publisher_filter.RELATIONSHIP_NOTE


def test_guide_contains_no_unbacked_numbers_or_promises():
    text = re.sub(r"<style>.*?</style>", " ", _help_html(), flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    body = text.lower()
    for forbidden in (
        "guarantee",
        "always accurate",
        "100%",
        "in seconds",
        "instantly",
        "coming soon",
        "will soon",
    ):
        assert forbidden not in body, forbidden


# --- the "estimates" claims name their real basis (design-judge veto F5) -----

import html as _html  # noqa: E402

ESTIMATES_SENTENCE = (
    "Projected hires and costs are estimates, not promises: they divide the "
    "media budget by an assumed cost per hire — Nova's own industry table "
    "(an all-in figure with no external source) or, where one exists, a cited "
    "local range — and the plan names which."
)


def _help_text(raw=None):
    raw = _help_html() if raw is None else raw
    raw = re.sub(r"<style>.*?</style>|<!--.*?-->", " ", raw, flags=re.S)
    return " ".join(_html.unescape(re.sub(r"<[^>]+>", " ", raw)).split())


def test_help_estimates_sentence_names_the_real_basis():
    assert ESTIMATES_SENTENCE in _help_text()


def test_help_never_calls_the_estimates_industry_benchmarks():
    status, body = _get(HELP_PATH)[1:]
    assert status == 200
    assert "industry benchmarks" not in _help_text(body).lower()
    assert "industry benchmarks" not in _help_html().lower()


def test_help_example_is_the_checklist_wording_verbatim():
    import plan_trust

    # No plan data -> the Nova-table basis, the case the example describes.
    checklist_item = plan_trust.review_items({})[0]
    assert "cost-per-hire table" in checklist_item
    assert checklist_item in _help_text()  # the whole item, word for word


def test_new_help_sentences_hold_no_us_only_marker():
    import plan_trust
    from us_only_markers import _US_ONLY_MARKERS

    for s in (ESTIMATES_SENTENCE, plan_trust.review_items({})[0]):
        assert s in _help_text()
        assert not [m for m in _US_ONLY_MARKERS if m.lower() in s.lower()], s
