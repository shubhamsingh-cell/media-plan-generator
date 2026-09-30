"""D-12: a "City, Country" site must stay ONE site for every real country,
and a bare country name must never resolve to a US town.

Audit finding (wizard Step 1 "Target Locations"): typing or pasting
``Warsaw, Poland`` produced TWO tags ("Warsaw", "Poland"). The wizard's
VALID_COUNTRIES (and the server's mirror, plan_location._SPLIT_COUNTRY_TOKENS)
only knew ~43 markets, so 15 of 36 tested "City, Country" pairs split. The
echo-back resolver then read each half on its own: "Warsaw matches 9 US
cities" and "Poland matches 2 US cities - which one?" -- because
plan_location.resolve_location ran the bare-US-city rule (5) and the fuzzy
rule (9) BEFORE the non-US rule (8), so a bare country name such as "Poland"
was matched to Poland, OH / Poland, ME.

These tests run the REAL shipped code: the wizard handlers in node (harness in
tests/test_location_multi_site_entry.py), the server normalizer and the
resolver in-process.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

import plan_geo
import plan_location
from tests.test_location_multi_site_entry import _normalize, _run_wizard

_JS_PATH = Path(__file__).resolve().parent.parent / "templates" / "partials" / "index" / "body_app_js.html"

# (pair as a user types it, the bare country / the country half of the pair)
# First block: the 15 that split before the fix. Second block: the 21 that
# already worked and must keep working.
FAILED_BEFORE = [
    ("Warsaw, Poland", "Poland"),
    ("Istanbul, Turkey", "Turkey"),
    ("Jakarta, Indonesia", "Indonesia"),
    ("Lagos, Nigeria", "Nigeria"),
    ("Nairobi, Kenya", "Kenya"),
    ("Tel Aviv, Israel", "Israel"),
    ("Riyadh, Saudi Arabia", "Saudi Arabia"),
    ("Cairo, Egypt", "Egypt"),
    ("Prague, Czech Republic", "Czech Republic"),
    ("Bucharest, Romania", "Romania"),
    ("Athens, Greece", "Greece"),
    ("Dhaka, Bangladesh", "Bangladesh"),
    ("Karachi, Pakistan", "Pakistan"),
    ("Casablanca, Morocco", "Morocco"),
    ("Warsaw, PL", "Poland"),
]
PASSED_BEFORE = [
    ("Paris, France", "France"),
    ("London, UK", "UK"),
    ("Berlin, Germany", "Germany"),
    ("Toronto, Canada", "Canada"),
    ("Sydney, Australia", "Australia"),
    ("Mumbai, India", "India"),
    ("Tokyo, Japan", "Japan"),
    ("Singapore, Singapore", "Singapore"),
    ("Dublin, Ireland", "Ireland"),
    ("Amsterdam, Netherlands", "Netherlands"),
    ("Madrid, Spain", "Spain"),
    ("Milan, Italy", "Italy"),
    ("Stockholm, Sweden", "Sweden"),
    ("Zurich, Switzerland", "Switzerland"),
    ("Sao Paulo, Brazil", "Brazil"),
    ("Mexico City, Mexico", "Mexico"),
    ("Manila, Philippines", "Philippines"),
    ("Bangkok, Thailand", "Thailand"),
    ("Johannesburg, South Africa", "South Africa"),
    ("Dubai, UAE", "UAE"),
    ("Seoul, South Korea", "South Korea"),
]
PAIRS = FAILED_BEFORE + PASSED_BEFORE
PAIR_IDS = [p for p, _c in PAIRS]

# Aliases and spellings people actually type for the same countries.
ALIAS_PAIRS = [
    ("Istanbul, Türkiye", "Türkiye"),
    ("Istanbul, Turkiye", "Turkiye"),
    ("Prague, Czechia", "Czechia"),
    ("Abidjan, Ivory Coast", "Ivory Coast"),
    ("Abidjan, Côte d'Ivoire", "Côte d'Ivoire"),
    ("Riyadh, KSA", "KSA"),
    ("Seoul, Korea", "Korea"),
    ("Kyiv, Ukraine", "Ukraine"),
    ("Kuala Lumpur, Malaysia", "Malaysia"),
    ("Ho Chi Minh City, Viet Nam", "Viet Nam"),
    ("Kinshasa, DR Congo", "DR Congo"),
    ("Lagos, NG", "Nigeria"),
    ("Nairobi, KE", "Kenya"),
    ("Seoul, Republic of Korea", "Republic of Korea"),
]

# Bare country names that used to be read as US towns (Poland OH/ME, Lebanon
# x16, Peru x6, Mexico x5, Jordan MN/UT, Turkey TX, Denmark SC/WI ...).
BARE_COUNTRIES = [
    "Poland", "Turkey", "Lebanon", "Peru", "Mexico", "Jordan", "Denmark",
    "Norway", "Sweden", "Egypt", "Nigeria", "Kenya", "Israel", "Greece",
    "Romania", "Indonesia", "Bangladesh", "Pakistan", "Morocco", "Cuba",
    "Panama", "China", "Chile", "Malta", "Czech Republic", "Saudi Arabia",
    "Scotland", "Wales", "England", "UK", "UAE", "Türkiye",
]


def _is_non_us_site(res: plan_location.LocationResolution) -> bool:
    return (
        res.status == "unresolved"
        and res.kind == "unknown"
        and res.matched_via == "non_us_signal"
        and not res.state_usps
        and not res.alternatives
    )


# ── Wizard: pasted ──────────────────────────────────────────────────────


@pytest.mark.parametrize("pair", PAIR_IDS + [p for p, _c in ALIAS_PAIRS])
def test_wizard_paste_city_country_is_one_tag(pair):
    out = _run_wizard([{"type": "paste", "text": pair}, {"type": "key", "key": "Enter"}])
    assert out["locations"] == [pair], out


@pytest.mark.parametrize("pair", PAIR_IDS)
def test_wizard_paste_committed_by_blur_is_one_tag(pair):
    out = _run_wizard([{"type": "paste", "text": pair}, {"type": "blur"}])
    assert out["locations"] == [pair], out


# ── Wizard: typed (the comma key commits each token) ────────────────────


@pytest.mark.parametrize("pair", PAIR_IDS + [p for p, _c in ALIAS_PAIRS])
def test_wizard_typed_city_country_is_one_tag(pair):
    out = _run_wizard([{"type": "type", "text": pair}, {"type": "key", "key": "Enter"}])
    assert out["locations"] == [pair], out
    assert out["input"] == ""


def test_wizard_city_country_list_is_one_tag_per_site():
    sites = ["Warsaw, Poland", "Lagos, Nigeria", "Dallas, TX", "Riyadh, Saudi Arabia", "Paris, France"]
    for sep in ("\n", "; ", ", "):
        pasted = _run_wizard([{"type": "paste", "text": sep.join(sites)}, {"type": "key", "key": "Enter"}])
        assert pasted["locations"] == sites, ("paste", sep, pasted)
        typed = _run_wizard([{"type": "type", "text": sep.join(sites)}, {"type": "key", "key": "Enter"}])
        assert typed["locations"] == sites, ("typed", sep, typed)


# ── Server: request-boundary normalizer ─────────────────────────────────


@pytest.mark.parametrize("pair", PAIR_IDS + [p for p, _c in ALIAS_PAIRS])
def test_server_string_payload_keeps_city_country_together(pair):
    # Pre-fix: "Warsaw, Poland" as a STRING payload split on every comma.
    assert _normalize(pair) == [pair]
    assert plan_location.split_location_entries(pair) == [pair]


@pytest.mark.parametrize("pair", PAIR_IDS + [p for p, _c in ALIAS_PAIRS])
def test_server_list_payload_keeps_city_country_together(pair):
    assert _normalize([pair]) == [pair]
    assert plan_location.split_location_entries([pair]) == [pair]


def test_server_string_payload_city_country_list():
    assert _normalize("Warsaw, Poland; Lagos, Nigeria\nDallas, TX") == [
        "Warsaw, Poland",
        "Lagos, Nigeria",
        "Dallas, TX",
    ]
    assert _normalize("Warsaw, Poland, Lagos, Nigeria, Dallas, TX") == [
        "Warsaw, Poland",
        "Lagos, Nigeria",
        "Dallas, TX",
    ]


# ── Resolver: a non-US site is never matched to a US town ───────────────


@pytest.mark.parametrize("pair,country", PAIRS + ALIAS_PAIRS)
def test_resolver_city_country_pair_is_not_a_us_match(pair, country):
    res = plan_location.resolve_location(pair)
    assert _is_non_us_site(res), (pair, res)
    assert "outside the US" in res.note
    assert "exactly as entered" in res.note


@pytest.mark.parametrize("pair,country", PAIRS + ALIAS_PAIRS)
def test_resolver_bare_country_is_not_a_us_match(pair, country):
    # The orphan half of the old split: "Poland matches 2 US cities".
    res = plan_location.resolve_location(country)
    assert _is_non_us_site(res), (country, res)


@pytest.mark.parametrize("country", BARE_COUNTRIES)
def test_resolver_bare_country_names_never_become_us_towns(country):
    res = plan_location.resolve_location(country)
    assert _is_non_us_site(res), (country, res)


def test_generate_boundary_leaves_foreign_locations_verbatim():
    """/api/generate rewrites every "resolved"/"corrected" location into the
    plan text. Before D-12 the fuzzy rule ran ahead of the non-US rule, so
    "Milan, Italy" became "Midland City, AL", "France" became "Frannie, WY",
    "Canada" became "Canadian, OK" and "Germany" became "Germania, NJ" in the
    generated plan. Foreign entries must reach the plan exactly as entered."""
    import app

    sites = [
        "Milan, Italy", "France", "Canada", "Germany", "Warsaw, Poland",
        "Casablanca, Morocco", "Poland", "Turkey", "Dubai, UAE",
    ]
    data = {"locations": list(sites)}
    app._resolve_and_rewrite_locations(data)
    assert data["locations"] == sites
    # US sites in the same list are still resolved/canonicalised as before.
    data = {"locations": ["Lebanon, TN", "Dallas, TX", "Milan, Italy"]}
    app._resolve_and_rewrite_locations(data)
    assert data["locations"][0].startswith("Lebanon, TN")
    assert data["locations"][2] == "Milan, Italy"


# ── Unchanged behaviour ─────────────────────────────────────────────────


def test_us_state_sharing_a_name_with_a_country_is_still_the_us_state():
    res = plan_location.resolve_location("Georgia")
    assert (res.status, res.kind, res.state_usps, res.matched_via) == (
        "resolved",
        "state",
        "GA",
        "state_bare",
    )
    # Both "Atlanta, GA" and "Atlanta, Georgia" stay Georgia-the-state.
    assert plan_location.resolve_location("Atlanta, Georgia").state_usps == "GA"
    assert plan_location.resolve_location("Atlanta, GA").state_usps == "GA"


@pytest.mark.parametrize(
    "site,usps",
    [
        ("Springfield, IL", "IL"),
        ("Indianapolis, IN", "IN"),
        ("Boise, ID", "ID"),
        ("Boston, MA", "MA"),
        ("Denver, CO", "CO"),
        ("Philadelphia, PA", "PA"),
        ("Los Angeles, CA", "CA"),
        ("Atlanta, GA", "GA"),
        ("Wilmington, DE", "DE"),
    ],
)
def test_us_city_state_code_that_is_also_an_iso_code_stays_us(site, usps):
    # IL/IN/ID/MA/CO/PA/CA/GA/DE are ISO country codes too (Israel, India,
    # Indonesia, Morocco, Colombia, Panama, Canada, Gabon, Germany). They
    # are US states here and must never be read as countries.
    res = plan_location.resolve_location(site)
    assert res.state_usps == usps and res.matched_via != "non_us_signal", res
    assert _normalize(site) == [site]
    out = _run_wizard([{"type": "paste", "text": site}, {"type": "key", "key": "Enter"}])
    assert out["locations"] == [site]


# Country names that are also real US towns (Lebanon x16, Peru x6, Cuba x6 ...).
# Widening the country list must not tear "Lebanon, TN" into a country plus a
# state. (state code, full name) per pair; every one exists in the US place data.
COUNTRY_NAMED_US_TOWNS = [
    ("Lebanon", "TN", "Tennessee"),
    ("Lebanon", "PA", "Pennsylvania"),
    ("Jordan", "MN", "Minnesota"),
    ("Turkey", "TX", "Texas"),
    ("Peru", "IL", "Illinois"),
    ("Mexico", "MO", "Missouri"),
    ("Poland", "OH", "Ohio"),
    ("Cuba", "NY", "New York"),
    ("Panama", "IA", "Iowa"),
    ("Egypt", "AR", "Arkansas"),
    ("Malta", "MT", "Montana"),
    ("Norway", "ME", "Maine"),
    ("Denmark", "WI", "Wisconsin"),
    ("Greece", "NY", "New York"),
    ("China", "TX", "Texas"),
]
COUNTRY_TOWN_SITES = [f"{t}, {code}" for t, code, _n in COUNTRY_NAMED_US_TOWNS] + [
    f"{t}, {name}" for t, _c, name in COUNTRY_NAMED_US_TOWNS
]


@pytest.mark.parametrize("site", COUNTRY_TOWN_SITES)
def test_country_named_us_town_with_its_state_is_one_site(site):
    for action in ({"type": "paste", "text": site}, {"type": "type", "text": site}):
        out = _run_wizard([action, {"type": "key", "key": "Enter"}])
        assert out["locations"] == [site], (action["type"], out)
    assert _normalize(site) == [site]
    assert _normalize([site]) == [site]


@pytest.mark.parametrize("town,code,name", COUNTRY_NAMED_US_TOWNS)
def test_country_named_us_town_resolves_to_the_us_town(town, code, name):
    for text in (f"{town}, {code}", f"{town}, {name}"):
        res = plan_location.resolve_location(text)
        assert res.state_usps == code, (text, res)
        assert res.status == "resolved" and res.matched_via == "place_city_state", (text, res)


def test_country_named_us_town_list_is_one_site_each():
    sites = ["Lebanon, TN", "Warsaw, Poland", "Peru, Illinois", "Lagos, Nigeria", "Jordan, MN"]
    for sep in ("\n", "; ", ", "):
        pasted = _run_wizard([{"type": "paste", "text": sep.join(sites)}, {"type": "key", "key": "Enter"}])
        assert pasted["locations"] == sites, ("paste", sep, pasted)
    assert _normalize(", ".join(sites)) == sites
    assert _normalize(["Lebanon, TN, Peru, IL"]) == ["Lebanon, TN", "Peru, IL"]


def test_country_followed_by_a_state_with_no_such_town_keeps_the_country_reading():
    """The state only qualifies a country-named town when that town exists in
    that state. There is no Mexico in Texas, so "Mexico City, Mexico, Texas"
    stays a city in its country plus the state of Texas (two targets), and a
    country that is not a US town at all ("Canada, Texas") stays two."""
    cases = {
        "Mexico City, Mexico, Texas": ["Mexico City, Mexico", "Texas"],
        "Mexico City, Mexico, TX": ["Mexico City, Mexico", "TX"],
        "Canada, Texas": ["Canada", "Texas"],
    }
    for text, expected in cases.items():
        pasted = _run_wizard([{"type": "paste", "text": text}, {"type": "key", "key": "Enter"}])
        assert pasted["locations"] == expected, ("paste", text, pasted)
        assert _normalize(text) == expected, ("server", text)
    # Server string payload for a bare "Mexico, TX" is unchanged from before
    # D-12 (two entries). The wizard's addTag has always merged any tag plus a
    # state code, so its "Mexico, TX" is one tag -- also unchanged.
    assert _normalize("Mexico, TX") == ["Mexico", "TX"]


def test_bare_city_before_a_country_named_town_does_not_swallow_it():
    """"Atlanta, Lebanon, TN": Lebanon is a US town here, not Atlanta's country.
    Pasted (look-ahead) and typed comma by comma (split back out when the state
    arrives) must agree with the server."""
    expected = ["Atlanta", "Lebanon, TN"]
    text = "Atlanta, Lebanon, TN"
    pasted = _run_wizard([{"type": "paste", "text": text}, {"type": "key", "key": "Enter"}])
    typed = _run_wizard([{"type": "type", "text": text}, {"type": "key", "key": "Enter"}])
    assert pasted["locations"] == expected, pasted
    assert typed["locations"] == expected, typed
    assert _normalize(text) == expected
    # After a state-qualified city: the Hershey, PA / Lebanon, PA neighbours.
    text = "Hershey, PA, Lebanon, PA, Hazleton, PA"
    expected = ["Hershey, PA", "Lebanon, PA", "Hazleton, PA"]
    pasted = _run_wizard([{"type": "paste", "text": text}, {"type": "key", "key": "Enter"}])
    typed = _run_wizard([{"type": "type", "text": text}, {"type": "key", "key": "Enter"}])
    assert pasted["locations"] == expected, pasted
    assert typed["locations"] == expected, typed
    assert _normalize(text) == expected
    # Full state names take the same path.
    text = "Atlanta, Lebanon, Tennessee"
    typed = _run_wizard([{"type": "type", "text": text}, {"type": "key", "key": "Enter"}])
    assert typed["locations"] == ["Atlanta", "Lebanon, Tennessee"], typed


@pytest.mark.parametrize(
    "text,expected",
    [
        # A city-state spelled twice after a state-qualified city.
        (
            "Dallas, TX, Singapore, Singapore, London, UK",
            ["Dallas, TX", "Singapore, Singapore", "London, UK"],
        ),
        # Adjacent "X, Country" and "CountryNamedTown, ST".
        ("Beirut, Lebanon, Lebanon, KY", ["Beirut, Lebanon", "Lebanon, KY"]),
        ("Lima, Peru, Peru, IN", ["Lima, Peru", "Peru, IN"]),
        ("Amman, Jordan, Jordan, MN, Warsaw, Poland", ["Amman, Jordan", "Jordan, MN", "Warsaw, Poland"]),
        ("Hershey, PA, Lebanon, PA, Hazleton, PA", ["Hershey, PA", "Lebanon, PA", "Hazleton, PA"]),
        ("Toronto, ON, Canada, Turkey, TX", ["Toronto, ON, Canada", "Turkey, TX"]),
    ],
)
def test_mixed_us_and_foreign_sequences_split_one_site_each(text, expected):
    assert _normalize(text) == expected
    pasted = _run_wizard([{"type": "paste", "text": text}, {"type": "key", "key": "Enter"}])
    assert pasted["locations"] == expected, pasted


def test_us_country_tails_are_still_stripped():
    for tail in ("US", "USA", "United States"):
        res = plan_location.resolve_location(f"Hershey, PA, {tail}")
        assert res.matched_via != "non_us_signal", (tail, res)
        res = plan_location.resolve_location(f"Seattle, {tail}")
        assert res.matched_via != "non_us_signal", (tail, res)
    assert plan_location.resolve_location("USA").kind == "country"


def test_bare_us_city_without_a_state_is_still_ambiguous():
    # A US town that is NOT a country name keeps the "which one?" flow.
    res = plan_location.resolve_location("Warsaw")
    assert res.status == "ambiguous" and res.matched_via == "place_city_ambiguous", res
    assert plan_location.resolve_location("Springfield").status == "ambiguous"


def test_pinned_splitter_behaviour_is_unchanged():
    cases = {
        "CA, NY": ["CA", "NY"],
        "Remote, TX": ["Remote", "TX"],
        "Atlanta, Chicago, Miami": ["Atlanta", "Chicago", "Miami"],
        "London, Manchester, UK": ["London", "Manchester, UK"],
        "Paris, France": ["Paris, France"],
        "Springfield, IL": ["Springfield, IL"],
        "Hershey, PA": ["Hershey, PA"],
    }
    for text, expected in cases.items():
        pasted = _run_wizard([{"type": "paste", "text": text}, {"type": "key", "key": "Enter"}])
        typed = _run_wizard([{"type": "type", "text": text}, {"type": "key", "key": "Enter"}])
        assert pasted["locations"] == expected, ("paste", text, pasted)
        assert typed["locations"] == expected, ("typed", text, typed)
        assert _normalize(text) == expected, ("server", text)


# ── City, Region, Country: what is and is not implemented ───────────────


def test_three_part_with_a_known_region_is_one_site():
    # The first-level region lists from b05f589 (CA/AU/IN/UK) still work.
    for site in ("Toronto, Ontario, Canada", "Bengaluru, Karnataka, India", "Sydney, NSW, Australia"):
        out = _run_wizard([{"type": "paste", "text": site}, {"type": "key", "key": "Enter"}])
        assert out["locations"] == [site], out
        assert _normalize(site) == [site]


def test_three_part_with_an_unlisted_region_is_a_documented_limitation():
    """KNOWN LIMITATION, pinned on purpose -- not a desired outcome.

    Only the first-level regions in INTL_REGIONS (Canada, Australia, India,
    the UK nations) qualify a city. There is no Poland voivodeship list, and
    "any middle token" cannot be a region: "London, Manchester, UK" must stay
    two sites. So "Warsaw, Masovia, Poland" reads as "Warsaw" plus
    "Masovia, Poland" -- typed and pasted alike. What IS guaranteed, and
    asserted here, is that the country stays attached to a place (never its
    own orphan site) and that no token is dropped.
    """
    text = "Warsaw, Masovia, Poland"
    pasted = _run_wizard([{"type": "paste", "text": text}, {"type": "key", "key": "Enter"}])["locations"]
    typed = _run_wizard([{"type": "type", "text": text}, {"type": "key", "key": "Enter"}])["locations"]
    server = _normalize(text)
    assert pasted == typed == server == ["Warsaw", "Masovia, Poland"]
    for sites in (pasted, typed, server):
        assert "Poland" not in sites  # the country is never an orphan site
        assert ", ".join(sites) == text  # nothing dropped or reordered


# ── Lists stay identical between the wizard and the server ──────────────


def _js_set(name: str) -> set:
    src = _JS_PATH.read_text(encoding="utf-8")
    body = re.search(rf"const {name} = new Set\(\[([\s\S]*?)\]\);", src).group(1)
    return {t.lower() for t in re.findall(r'"([^"]+)"', body)}


def test_country_list_is_a_full_world_list():
    js = _js_set("VALID_COUNTRIES")
    # 195 sovereign states + aliases + ISO codes: far beyond the old 43.
    assert len(js) > 400, len(js)
    for _pair, country in PAIRS + ALIAS_PAIRS:
        assert plan_location._norm_key(country) in js, country
    for must in ("czechia", "turkiye", "uae", "ksa", "south korea", "ivory coast", "pl", "ng"):
        assert must in js, must


def test_country_list_parity_with_server():
    assert _js_set("VALID_COUNTRIES") == set(plan_location._SPLIT_COUNTRY_TOKENS) - {
        "united states of america",
        "america",
    }


def _js_country_towns() -> dict:
    src = _JS_PATH.read_text(encoding="utf-8")
    body = re.search(r"const COUNTRY_NAMED_US_TOWNS = \{([\s\S]*?)\n  \};", src).group(1)
    return {
        town.lower(): set(re.findall(r'"([^"]+)"', states))
        for town, states in re.findall(r'"([^"]+)":\s*\[([^\]]*)\]', body)
    }


def test_country_named_us_towns_match_the_us_place_data():
    """The wizard's COUNTRY_NAMED_US_TOWNS is derived from data/geo/us_places.tsv:
    every (country name, state) pair that is a real US place, codes and names.
    A data refresh that adds or drops one fails here instead of silently
    tearing "Town, ST" in two (or merging a state that has no such town)."""
    plan_location._ensure_loaded()
    expected: dict = {}
    for tok in plan_location._SPLIT_COUNTRY_TOKENS:
        for usps, _key in plan_location._places_by_city.get(tok, []):
            name = plan_location._states_by_usps.get(usps, "").upper().replace(".", "")
            expected.setdefault(tok, set()).update({usps, name} - {""})
    assert _js_country_towns() == expected


@pytest.mark.parametrize("town,code,name", COUNTRY_NAMED_US_TOWNS)
def test_server_helper_agrees_with_the_js_table(town, code, name):
    key = plan_location._norm_key(town)
    assert plan_location._is_country_named_us_town(key, code)
    assert plan_location._is_country_named_us_town(key, name)
    assert {code, name.upper()} <= _js_country_towns()[key]


def test_iso_codes_never_shadow_a_us_state_or_a_region_code():
    """A two/three-letter country token must not be a US state/territory code
    or an INTL_REGIONS code: SA (South Australia), IN (Indiana), IL
    (Illinois), ID (Idaho), MA, DE, GA, CA, PA, CO ... stay what they are."""
    short = {t for t in _js_set("VALID_COUNTRIES") if len(t) <= 3}
    us_codes = {c.lower() for c in _js_set("VALID_US_STATES")} | {c.lower() for c in plan_geo.US_STATE_ABBR}
    regions = _js_set("INTL_REGIONS") | set(plan_location._INTL_REGION_TOKENS)
    assert not (short & us_codes), sorted(short & us_codes)
    assert not (short & regions), sorted(short & regions)
    assert "georgia" not in _js_set("VALID_COUNTRIES")  # the US state wins


def test_js_country_key_matches_server_norm_key():
    node = shutil.which("node")
    if not node:
        pytest.skip("node not available")
    src = _JS_PATH.read_text(encoding="utf-8")
    fn = re.search(r"function _countryKey\(text\) \{[\s\S]*?\n  \}", src)
    assert fn, "_countryKey missing from the wizard script"
    samples = [
        "Türkiye",
        "Côte d'Ivoire",
        "U.A.E.",
        "Guinea-Bissau",
        "São Tomé and Príncipe",
        "  Czech   Republic ",
        "Bosnia & Herzegovina",
        "Timor-Leste",
        "Saudi Arabia",
        "PL",
        "",
    ]
    script = fn.group(0) + "\nconsole.log(JSON.stringify(JSON.parse(process.argv[1]).map(_countryKey)));"
    proc = subprocess.run(
        [node, "-e", script, json.dumps(samples)], capture_output=True, text=True, timeout=30
    )
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout) == [plan_location._norm_key(s).upper() for s in samples]
