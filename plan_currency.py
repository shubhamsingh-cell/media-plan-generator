"""Currency localization for media plan generation.

Plan-gen historically rendered every monetary figure with ``$`` regardless of
the plan's country (backlog Q4/Q5 = FAIL). This module provides a single,
dependency-free source of truth for:

  - ISO currency code -> display symbol (incl. Cyrillic ₽/₴ for RUB/UAH)
  - country name/slug -> ISO currency code
  - ``format_money(value, code)`` for consistent rendering

Planning math in Joveo decks (CPA, CPC, budget) is intentionally USD-coded, so
this module is used for **local-context** figures that carry their own currency
in the source data (e.g. salary ranges from intl_role_benchmarks_v1.json where
each entry has both ``value`` in local currency and ``value_usd``). The symbol
and the value therefore always come from the same source -- never relabel a USD
number with a £ sign.

Usage:
    from plan_currency import currency_for_country, format_money, symbol_for_code
    code = currency_for_country("United Kingdom")   # -> "GBP"
    format_money(28407, code)                        # -> "£28,407"
    symbol_for_code("RUB")                            # -> "₽"
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ISO currency code -> display symbol
# ---------------------------------------------------------------------------
# Every symbol here MUST be renderable by a font the deck actually embeds --
# Poppins (fonts/Poppins-*.ttf) or the symbol face (fonts/NovaDeckSymbols-*.ttf).
# Otherwise EVERY money figure for that market carries a currency sign drawn by
# whatever face the viewer's OS falls back to -- platform-dependent and off-brand.
# This is enforced by tests/test_currency_formatting.py::
# test_every_currency_symbol_is_renderable, so the table and the fonts cannot
# drift apart -- add a market and the test tells you if the glyph needs adding
# to scripts/build_symbol_font.py.
#
# (The superseded rule was "renders in Inter / Calibri", chosen when the
# generators used those families. The deck standardised on Poppins, a 504-glyph
# Latin face, which silently invalidated eight of these entries.)
#
# Where NO embeddable font covers a currency's sign, use the ISO code plus a
# space instead -- unambiguous, on-brand, and the same convention this table
# already uses for AED/SAR/QAR/CHF. BDT is the current case: ৳ (U+09F3) is not
# in DejaVu, the source of the symbol face.
# JPY/CNY share ¥; INR uses the official ₹ (U+20B9).
_CODE_TO_SYMBOL: dict[str, str] = {
    "USD": "$",
    "GBP": "£",
    "EUR": "€",
    "INR": "₹",
    "JPY": "¥",
    "CNY": "¥",
    "CAD": "C$",
    "AUD": "A$",
    "NZD": "NZ$",
    "SGD": "S$",
    "HKD": "HK$",
    "BRL": "R$",
    "MXN": "MX$",
    "AED": "AED ",
    "SAR": "SAR ",
    "QAR": "QAR ",
    "ZAR": "R",
    "CHF": "CHF ",
    "SEK": "kr ",
    "NOK": "kr ",
    "DKK": "kr ",
    "PLN": "zł ",
    "CZK": "Kč ",
    "HUF": "Ft ",
    "RON": "lei ",
    "TRY": "₺",
    "RUB": "₽",
    "UAH": "₴",
    "ILS": "₪",
    "PHP": "₱",
    "THB": "฿",
    "IDR": "Rp ",
    "MYR": "RM ",
    "VND": "₫",
    "KRW": "₩",
    "PKR": "₨ ",
    # ৳ (U+09F3) is in no font the deck can embed -- ISO code, not a fallback
    # glyph from an arbitrary system face.
    "BDT": "BDT ",
    "LKR": "₨ ",
    "NGN": "₦",
    "KES": "KSh ",
    "EGP": "E£",
    "COP": "COL$",
    "ARS": "AR$",
    "CLP": "CLP$",
}

# ---------------------------------------------------------------------------
# Country name / slug / ISO code -> ISO currency code
# ---------------------------------------------------------------------------
# Keys are lowercase. Covers the 15 dataset countries plus the broader set the
# plan generator may receive, including RUB/UAH markets (backlog Q4).
_COUNTRY_TO_CODE: dict[str, str] = {
    # United States
    "us": "USD",
    "usa": "USD",
    "u.s.": "USD",
    "u.s.a.": "USD",
    "united states": "USD",
    "united states of america": "USD",
    "america": "USD",
    # United Kingdom
    "uk": "GBP",
    "u.k.": "GBP",
    "united kingdom": "GBP",
    "great britain": "GBP",
    "britain": "GBP",
    "england": "GBP",
    "scotland": "GBP",
    "wales": "GBP",
    "gb": "GBP",
    "gbr": "GBP",
    # Eurozone
    "germany": "EUR",
    "deutschland": "EUR",
    "de": "EUR",
    "deu": "EUR",
    "france": "EUR",
    "fr": "EUR",
    "fra": "EUR",
    "spain": "EUR",
    "espana": "EUR",
    "es": "EUR",
    "esp": "EUR",
    "netherlands": "EUR",
    "holland": "EUR",
    "the netherlands": "EUR",
    "nl": "EUR",
    "nld": "EUR",
    "ireland": "EUR",
    "republic of ireland": "EUR",
    "ie": "EUR",
    "irl": "EUR",
    "italy": "EUR",
    "italia": "EUR",
    "it": "EUR",
    "ita": "EUR",
    "portugal": "EUR",
    "pt": "EUR",
    "prt": "EUR",
    "belgium": "EUR",
    "be": "EUR",
    "bel": "EUR",
    "austria": "EUR",
    "at": "EUR",
    "aut": "EUR",
    "greece": "EUR",
    "gr": "EUR",
    "grc": "EUR",
    "finland": "EUR",
    "fi": "EUR",
    "fin": "EUR",
    # India
    "india": "INR",
    "bharat": "INR",
    "in": "INR",
    "ind": "INR",
    # Canada
    "canada": "CAD",
    "ca": "CAD",
    "can": "CAD",
    # Australia / NZ
    "australia": "AUD",
    "au": "AUD",
    "aus": "AUD",
    "new zealand": "NZD",
    "nz": "NZD",
    "nzl": "NZD",
    # APAC
    "singapore": "SGD",
    "sg": "SGD",
    "sgp": "SGD",
    "hong kong": "HKD",
    "hk": "HKD",
    "hkg": "HKD",
    "japan": "JPY",
    "nippon": "JPY",
    "jp": "JPY",
    "jpn": "JPY",
    "china": "CNY",
    "prc": "CNY",
    "cn": "CNY",
    "chn": "CNY",
    "south korea": "KRW",
    "korea": "KRW",
    "kr": "KRW",
    "kor": "KRW",
    "philippines": "PHP",
    "ph": "PHP",
    "phl": "PHP",
    "thailand": "THB",
    "th": "THB",
    "tha": "THB",
    "indonesia": "IDR",
    "id": "IDR",
    "idn": "IDR",
    "malaysia": "MYR",
    "my": "MYR",
    "mys": "MYR",
    "vietnam": "VND",
    "vn": "VND",
    "vnm": "VND",
    "pakistan": "PKR",
    "pk": "PKR",
    "pak": "PKR",
    "bangladesh": "BDT",
    "bd": "BDT",
    "bgd": "BDT",
    "sri lanka": "LKR",
    "lk": "LKR",
    "lka": "LKR",
    # Middle East
    "uae": "AED",
    "united arab emirates": "AED",
    "emirates": "AED",
    "dubai": "AED",
    "abu dhabi": "AED",
    "ae": "AED",
    "are": "AED",
    "saudi arabia": "SAR",
    "saudi": "SAR",
    "sa": "SAR",
    "sau": "SAR",
    "qatar": "QAR",
    "qa": "QAR",
    "qat": "QAR",
    "israel": "ILS",
    "il": "ILS",
    "isr": "ILS",
    "turkey": "TRY",
    "turkiye": "TRY",
    "tr": "TRY",
    "tur": "TRY",
    # Latin America
    "brazil": "BRL",
    "brasil": "BRL",
    "br": "BRL",
    "bra": "BRL",
    "mexico": "MXN",
    "mx": "MXN",
    "mex": "MXN",
    "colombia": "COP",
    "co": "COP",
    "col": "COP",
    "argentina": "ARS",
    "ar": "ARS",
    "arg": "ARS",
    "chile": "CLP",
    "cl": "CLP",
    "chl": "CLP",
    # Africa
    "south africa": "ZAR",
    "za": "ZAR",
    "zaf": "ZAR",
    "nigeria": "NGN",
    "ng": "NGN",
    "nga": "NGN",
    "kenya": "KES",
    "ke": "KES",
    "ken": "KES",
    "egypt": "EGP",
    "eg": "EGP",
    "egy": "EGP",
    # Eastern Europe (backlog Q4: RUB/UAH)
    "russia": "RUB",
    "russian federation": "RUB",
    "ru": "RUB",
    "rus": "RUB",
    "ukraine": "UAH",
    "ua": "UAH",
    "ukr": "UAH",
    "poland": "PLN",
    "pl": "PLN",
    "pol": "PLN",
    "czech republic": "CZK",
    "czechia": "CZK",
    "cz": "CZK",
    "cze": "CZK",
    "hungary": "HUF",
    "hu": "HUF",
    "hun": "HUF",
    "romania": "RON",
    "ro": "RON",
    "rou": "RON",
    "switzerland": "CHF",
    "ch": "CHF",
    "che": "CHF",
    "sweden": "SEK",
    "se": "SEK",
    "swe": "SEK",
    "norway": "NOK",
    "no": "NOK",
    "nor": "NOK",
    "denmark": "DKK",
    "dk": "DKK",
    "dnk": "DKK",
}

# Codes whose symbol should be placed AFTER the number (e.g. "100 zł").
_SUFFIX_CODES: frozenset[str] = frozenset()  # all current symbols prefix-style


def symbol_for_code(code: str | None) -> str:
    """Return the display symbol for an ISO currency code. Defaults to '$'."""
    if not code or not isinstance(code, str):
        return "$"
    return _CODE_TO_SYMBOL.get(code.strip().upper(), code.strip().upper() + " ")


# US state / territory abbreviations. These collide with ISO country codes
# (IL=Illinois vs Israel, CA=California vs Canada, IN=Indiana vs India,
# DE=Delaware vs Germany, AL=Alabama vs Albania, ...). A trailing 2-letter US
# state token in "City, ST" means a US location, NOT a foreign country.
_US_STATE_ABBR = frozenset(
    {
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
        "DC",
        "PR",
    }
)


def currency_for_country(country: str | None) -> str | None:
    """Map a free-form country/slug/ISO label to its ISO currency code.

    Returns ``None`` when the country is unknown so callers can decide whether
    to fall back to USD. Handles ``"London, UK"`` by trying the trailing token.
    """
    if not country or not isinstance(country, str):
        return None
    key = country.strip().lower()
    if key in _COUNTRY_TO_CODE:
        return _COUNTRY_TO_CODE[key]
    # "City, Country" -> try the last comma-separated token
    if "," in key:
        last = key.rsplit(",", 1)[-1].strip()
        # "City, ST" US locations: a 2-letter US state code is NOT a country
        # code (IL=Illinois not Israel, CA=California not Canada).
        if last.upper() in _US_STATE_ABBR:
            return "USD"
        if last in _COUNTRY_TO_CODE:
            return _COUNTRY_TO_CODE[last]
    # Substring fallback only for aliases >= 5 chars (avoid "ca"/"in" inside
    # unrelated words like "antarctica" / "india" collisions handled by exact
    # match above).
    for alias in sorted(_COUNTRY_TO_CODE, key=len, reverse=True):
        if len(alias) >= 5 and alias in key:
            return _COUNTRY_TO_CODE[alias]
    return None


def format_money(
    value: float | int | None,
    code: str | None = "USD",
    decimals: int | None = None,
) -> str:
    """Format a numeric value with the correct currency symbol.

    Args:
        value: The amount. ``None`` / non-numeric -> "N/A".
        code: ISO currency code (e.g. "GBP"). ``None`` -> USD.
        decimals: Force decimal places. If ``None``, uses 0 for whole numbers
            and amounts >= 1000, else 2.

    Returns:
        e.g. ``format_money(28407, "GBP")`` -> ``"£28,407"``;
             ``format_money(7.5, "EUR")``   -> ``"€7.50"``.
        Never raises.
    """
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        return "N/A"
    sym = symbol_for_code(code)
    try:
        if decimals is None:
            if abs(value) >= 1000 or float(value).is_integer():
                decimals = 0
            else:
                decimals = 2
        body = f"{value:,.{decimals}f}"
    except (ValueError, TypeError):
        return "N/A"
    if (code or "").strip().upper() in _SUFFIX_CODES:
        return f"{body} {sym}".strip()
    return f"{sym}{body}"


def is_non_usd(country: str | None) -> bool:
    """True if the country maps to a non-USD currency."""
    code = currency_for_country(country)
    return bool(code and code != "USD")


# ---------------------------------------------------------------------------
# Reading a currency the CLIENT declared (convert-vs-declare, 2026-09).
#
# The wizard has no currency field, so plan currency was resolved purely by
# guessing from the location string -- which meant a client who typed
# "$2,000,000" against a London office had it rendered back to them as
# "£2M", their own declaration silently overwritten by a guess, with no
# conversion applied. Same plan, same number, different sign: a ~27%
# misstatement of the budget on the front page of a client deck. Worse, the
# guess flipped on the ORDER of the locations list ("Dallas, London" -> $;
# "London, Dallas" -> £).
#
# So: read the symbol the client actually typed and treat it as a
# declaration. Location is demoted to a tie-breaker that may only fill a gap
# or disambiguate WITHIN the declared symbol -- it can never contradict one.
#
# Multi-code symbols stay ambiguous on purpose. A bare "$" is USD by
# convention unless the plan's own market says CAD/AUD/etc; "¥" is JPY unless
# the market says CNY. The symbol constrains the answer, the market only
# picks among the codes that share that symbol.
# ---------------------------------------------------------------------------
_SYMBOL_TO_CODES: "list[tuple[str, tuple[str, ...]]]" = [
    # Longest-first, and any symbol that CONTAINS another must be tested
    # first: "US$" contains "S$" (Singapore), so a US$ budget would otherwise
    # resolve to SGD. Likewise every "X$" form must precede bare "$".
    ("US$", ("USD",)),
    ("NZ$", ("NZD",)),
    ("HK$", ("HKD",)),
    ("MX$", ("MXN",)),
    ("C$", ("CAD",)),
    ("A$", ("AUD",)),
    ("S$", ("SGD",)),
    ("R$", ("BRL",)),
    ("AED", ("AED",)),
    ("SAR", ("SAR",)),
    ("QAR", ("QAR",)),
    ("CHF", ("CHF",)),
    ("£", ("GBP",)),
    ("€", ("EUR",)),
    ("₹", ("INR",)),
    ("₱", ("PHP",)),
    ("฿", ("THB",)),
    ("₽", ("RUB",)),
    ("₴", ("UAH",)),
    ("¥", ("JPY", "CNY")),
    ("zł", ("PLN",)),
    ("Kč", ("CZK",)),
    ("RM", ("MYR",)),
    ("Rp", ("IDR",)),
    ("kr", ("SEK", "NOK", "DKK")),
    ("$", ("USD", "CAD", "AUD", "NZD", "SGD", "HKD", "MXN", "BRL")),
]


def currency_codes_from_symbol(text: str | None) -> "tuple[str, ...]":
    """ISO codes consistent with the currency symbol written in ``text``.

    Returns ``()`` when no symbol is present (i.e. the client declared
    nothing). A single-element tuple is an unambiguous declaration; a longer
    one lists the codes that share that symbol, most common first, for a
    caller to disambiguate against the plan's market.

        currency_codes_from_symbol("£2,000,000")  -> ("GBP",)
        currency_codes_from_symbol("$2,000,000")  -> ("USD", "CAD", ...)
        currency_codes_from_symbol("2,000,000")   -> ()
    """
    if not text or not isinstance(text, str):
        return ()
    for symbol, codes in _SYMBOL_TO_CODES:
        if symbol in text:
            return codes
    return ()


def resolve_declared_currency(
    budget_text: str | None,
    explicit_code: str | None = None,
    market_codes: "list[str] | tuple[str, ...] | None" = None,
) -> "tuple[str | None, str]":
    """Resolve a plan's currency, preferring what the client actually declared.

    Args:
        budget_text: the budget exactly as the client entered it.
        explicit_code: an explicit ``currency`` / ``currency_code`` field.
        market_codes: codes implied by the plan's markets, in plan order.

    Returns:
        ``(code, basis)`` where basis is one of ``"explicit"`` (a currency
        field), ``"declared"`` (a symbol the client typed), ``"market"``
        (inferred from a single unambiguous market) or ``"default"`` (nothing
        to go on, or markets disagreed -- caller should use USD and say so).
        ``code`` is ``None`` only for ``"default"``.
    """
    if isinstance(explicit_code, str) and explicit_code.strip():
        return explicit_code.strip().upper(), "explicit"

    markets = [c for c in (market_codes or []) if isinstance(c, str) and c]
    declared = currency_codes_from_symbol(budget_text)

    if declared:
        if len(declared) == 1:
            return declared[0], "declared"
        # Ambiguous symbol ("$" is shared by USD/CAD/AUD/SGD/...). A market may
        # pick among the codes sharing it, but ONLY when the markets agree on
        # one currency -- the same bar the market path below has to clear.
        # Scanning the list for any member of the symbol's code set would let a
        # London+Singapore plan resolve a "$" budget to SGD purely because
        # Singapore happens to share the glyph and appears in the list, which
        # is the list-order guessing this function exists to stop. Unqualified
        # "$" means USD by convention, so the symbol's primary code is the
        # safe answer whenever the markets do not speak with one voice.
        unique_markets = set(markets)
        if len(unique_markets) == 1:
            only = next(iter(unique_markets))
            if only in declared:
                return only, "declared"
        return declared[0], "declared"

    # Nothing declared. A market may fill the gap only when every market the
    # plan targets agrees -- otherwise the "right" answer would depend on
    # list order, which is exactly the guess this function exists to stop.
    unique = set(markets)
    if len(unique) == 1:
        return markets[0], "market"
    return None, "default"


def currency_for_plan_with_basis(data: dict | None) -> "tuple[str, str]":
    """Resolve a plan's currency and say WHY -- the single shared resolver.

    The deck, the workbook, the scorecard and the delivery gate must agree on a
    plan's currency: when they used separate resolvers the scorecard published
    every non-USD plan in dollars on a public share link while the deck beside
    it read correctly. So there is exactly one implementation, here, and it is
    the declare-not-convert rule above -- the symbol the client typed outranks
    any guess from the location list.

    Returns ``(code, basis)`` with basis as in :func:`resolve_declared_currency`;
    code is ``"USD"`` when basis is ``"default"``. Never raises.
    """
    if not isinstance(data, dict):
        return "USD", "default"
    explicit = data.get("currency_code") or data.get("currency")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip().upper(), "explicit"

    candidates: list[str] = []
    for key in ("country", "primary_location"):
        val = data.get(key)
        if isinstance(val, str) and val.strip():
            candidates.append(val)
    locs = data.get("locations") or []
    if isinstance(locs, (list, tuple)):
        for loc in locs:
            if isinstance(loc, str) and loc.strip():
                candidates.append(loc)
            elif isinstance(loc, dict):
                country = loc.get("country") or loc.get("location") or ""
                if isinstance(country, str) and country.strip():
                    candidates.append(country)
    market_codes: list[str] = []
    for cand in candidates:
        try:
            code = currency_for_country(cand)
        except Exception:  # noqa: BLE001 - resolution is best-effort
            code = None
        if code:
            market_codes.append(code)

    try:
        code, basis = resolve_declared_currency(
            budget_text=data.get("budget") or data.get("budget_range") or "",
            explicit_code=None,
            market_codes=market_codes,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        logger.debug("Declared-currency resolution failed (%s) -- USD", exc)
        code, basis = None, "default"
    return (code or "USD"), basis


def currency_for_plan(data: dict | None) -> str:
    """ISO currency code for a whole plan dict (see currency_for_plan_with_basis)."""
    return currency_for_plan_with_basis(data)[0]
