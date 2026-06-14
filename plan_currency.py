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
# Symbols chosen to render in the Inter / Calibri font families used by the
# Slides and Excel generators (both cover Latin-1 + common currency glyphs +
# Cyrillic). JPY/CNY share ¥; INR uses the official ₹ (U+20B9).
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
    "BDT": "৳ ",
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
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
        "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
        "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
        "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
        "WI", "WY", "DC", "PR",
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
