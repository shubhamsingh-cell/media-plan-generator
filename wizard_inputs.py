"""How the media-plan wizard's free-text inputs are read -- ONE definition.

The wizard's live preview (templates/partials/index/body_inputs_js.html),
``/api/estimate`` and ``/api/generate`` must read the same budget text as the
same number. They used to carry three different parsers: typing
"1.5 million" previewed $1.5M while the plan was generated for $1.50
(``shared_utils.parse_budget`` read only the "1.5"), "10000-15000" previewed
$10,000 but planned $12,500, and a hub template's "$50,000 - $250,000"
previewed $50K and planned $150K (wizard audit D-02/D-03, 2026-10-01).

So this module is the single source of truth for:

* budget text -> amount (:func:`parse_budget_input`), including the range
  rule and the error for anything that is not an amount;
* campaign duration -> months (:func:`campaign_months`), the months -> weeks
  conversion (:func:`months_to_weeks`) and the per-period budget multiplier;
* the per-field input limits the server enforces (:data:`INPUT_LIMITS`).

The browser never re-types these tables: :func:`page_config` is embedded
into the wizard page at compose time (template_composer), and the JS port of
the parser compiles the SAME regex sources and reads the SAME tables. The
algorithm itself is ported by hand, so tests/fixtures/budget_input_golden.json
is run through both implementations (tests/test_wizard_budget_parser.py).

Portability rules for the regex sources below (they are compiled by Python
``re`` AND JavaScript ``RegExp``): input is lower-cased first; only
``[...]`` classes with ASCII ranges, ``(?:...)``, one capture group used as a
left boundary ``(^|[^a-z])`` (no look-behind, which older Safari rejects), and
``(?!...)`` look-ahead; no ``\\b``/``\\d``/``\\s`` (their Unicode meaning
differs between the two engines). Stdlib only.
"""

from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

# ---------------------------------------------------------------------------
# Input limits -- enforced by /api/generate and /api/estimate, embedded into
# the wizard page so client-side checks can never drift from them.
# ---------------------------------------------------------------------------
# Budget bounds apply to the CAMPAIGN TOTAL (after per-period scaling), in the
# plan's own currency units (plans declare their currency, never convert).
#   * BUDGET_MIN 100: the parser has accepted "small employer budgets" down to
#     100 since the old 1,000 noise floor was lowered (scripts/generate_qc_
#     audit_docx.py "parse_budget() Threshold Lowered to $100"); anything
#     smaller is a mis-typed amount ("1.5" meant 1.5M) that used to plan 0
#     hires silently.
#   * BUDGET_MAX 1,000,000,000: the /api/generate cap since it was raised from
#     $100M for enterprise budgets; now also applied by /api/estimate (it used
#     to preview any amount and only fail at Generate).
BUDGET_MIN: float = 100.0
BUDGET_MAX: float = 1_000_000_000.0

INPUT_LIMITS: dict[str, int] = {
    "budget_min": int(BUDGET_MIN),
    "budget_max": int(BUDGET_MAX),
    "budget_max_chars": 120,
    "client_name_max_chars": 200,
    "use_case_max_chars": 10_000,
    "call_transcript_max_chars": 50_000,
    "roles_max": 30,
    "competitors_max": 20,
    "locations_max": 100,
    # The whole /api/generate JSON body (app.py's global body cap).
    "request_max_bytes": 10 * 1024 * 1024,
    # Uploaded files travel base64-encoded inside that body (x4/3), so the
    # raw total must stay well under 10 MB * 3/4 = 7.5 MB; 7 MB leaves room
    # for the rest of the brief. (The UI used to promise "max 10MB each" and
    # an 8 MB file then failed at Generate with a 413.)
    "upload_total_max_bytes": 7 * 1024 * 1024,
}


def _fmt_int(value: float) -> str:
    return f"{int(value):,}"


# ---------------------------------------------------------------------------
# Campaign duration -> months / weeks
# ---------------------------------------------------------------------------
# Campaign months per wizard #campaignDuration option. A range option uses its
# MIDPOINT ("6-12 months" -> 9), the same rule the budget uses for a range.
# Mirrored (not re-typed) by DURATION_MONTHS in templates/partials/index/
# body_preview_js.html -- tests/test_budget_period_preview_parity.py.
CAMPAIGN_DURATION_MONTHS: dict[str, float] = {
    "2 weeks": 0.5,
    "1 month": 1,
    "2 months": 2,
    "3 months": 3,
    "4 months": 4,
    "6 months": 6,
    "9 months": 9,
    "12 months": 12,
    "18 months": 18,
    "24 months": 24,
    "1-3 months": 2,
    "3-6 months": 4.5,
    "6-12 months": 9,
    "1-2 years": 18,
    "2-5 years (Long-term)": 42,
    "Ongoing": 12,
}
BUDGET_PERIOD_MONTHS: dict[str, int] = {"monthly": 1, "quarterly": 3, "annual": 12}
# Months assumed when no duration is given (the preview's long-standing default).
DEFAULT_CAMPAIGN_MONTHS: float = 6.0

# "N weeks" / "N months" / "N years" / "N days", or a range "N-M <unit>"
# (midpoint). Python-only (the JS mirror is freeTextDurationMonths).
_EXPLICIT_DURATION_RE = re.compile(
    r"(\d+(?:\.\d+)?)(?:\s*(?:-|–|to)\s*(\d+(?:\.\d+)?))?\s*"
    r"(days?|weeks?|wks?|months?|mos?|years?|yrs?)\b",
    re.IGNORECASE,
)
_EXPLICIT_DURATION_UNIT_MONTHS: dict[str, float] = {
    "d": 12 / 365,
    "w": 12 / 52,
    "m": 1.0,
    "y": 12.0,
}


def explicit_duration_months(duration: Any) -> float:
    """Months for an explicit free-text duration ("24 months" -> 24,
    "16 weeks" -> 16*12/52, "1 year" -> 12, "6-12 months" -> 9), or 0.0 when
    the string states no number+unit."""
    m = _EXPLICIT_DURATION_RE.search(str(duration or ""))
    if not m:
        return 0.0
    lo = float(m.group(1))
    hi = float(m.group(2)) if m.group(2) else lo
    unit = _EXPLICIT_DURATION_UNIT_MONTHS[m.group(3)[0].lower()]
    return (lo + hi) / 2 * unit


def known_campaign_months(duration: Any) -> float:
    """Months for a wizard option (case-insensitive) or an explicit
    number+unit duration; 0.0 when the string is neither."""
    dur = str(duration or "").strip()
    if not dur:
        return 0.0
    low = dur.lower()
    for option, months in CAMPAIGN_DURATION_MONTHS.items():
        if low == option.lower():
            return float(months)
    return explicit_duration_months(dur)


def campaign_months(
    duration: Any, legacy_weeks: Optional[Callable[[str], int]] = None
) -> float:
    """Months a campaign runs -- THE value the budget multiplier, the week
    count and the duration label are all derived from.

    Wizard options and explicit durations resolve via
    :func:`known_campaign_months`. Other free text with a digit (API callers)
    may go through ``legacy_weeks`` (display_format's phrase ladder); empty
    or unparseable input is :data:`DEFAULT_CAMPAIGN_MONTHS`.
    """
    months = known_campaign_months(duration)
    if months > 0:
        return months
    dur = str(duration or "").strip()
    if dur and legacy_weeks is not None and re.search(r"[0-9]", dur):
        weeks = legacy_weeks(dur)
        if weeks > 0:
            return weeks * 12 / 52
    return DEFAULT_CAMPAIGN_MONTHS


def months_to_weeks(months: float) -> int:
    """Weeks in ``months`` at 52/12 weeks per month, rounded half-up (the
    same rounding the wizard's JS ``Math.round`` applies)."""
    return int(math.floor(months * 52 / 12 + 0.5))


def budget_period_multiplier(budget_period: Any, months: float) -> float:
    """How many budget periods a campaign of ``months`` covers (>= 1; 1 for
    a campaign total). The floor of 1 keeps a per-period amount from ever
    being shrunk below what the user typed."""
    period_months = BUDGET_PERIOD_MONTHS.get(str(budget_period or "").strip().lower())
    if not period_months:
        return 1.0
    return max(months / period_months, 1.0)


# ---------------------------------------------------------------------------
# Budget text -> amount
# ---------------------------------------------------------------------------
# Every regex source below is shared verbatim with the JS port (see module
# docstring for the portability rules).
_B = "(^|[^a-z])"  # left boundary: start or a non-letter (kept in the output)
_E = "(?![a-z])"  # right boundary: not followed by a letter

BUDGET_REGEX: dict[str, str] = {
    # Unicode spaces (NBSP, thin, narrow NBSP, figure, ideographic...) -> " "
    "spaces": r"[\t\n\r\f\v\x1c-\x1f\x85  ᠎ -     　﻿]+",
    # hyphen/dash/minus variants -> "-"
    "dashes": r"[‐‑‒–—―−﹘﹣－]",
    # apostrophe variants (Swiss grouping 1'000'000) -> "'"
    "quotes": r"[‘’ʼ´`]",
    "exponent": r"[0-9] ?e ?[-+]?[0-9]",
    "currency_symbol": r"(?:cad|aud|nzd|hkd|sgd|usd|us|ca|au|nz|hk|mx|sg|nt|c|a|s|r)?\$|[€£₹¥₩₱฿₽₴₦₪₺]|zł|kč",
    "currency_word": _B
    + r"(?:usd|eur|gbp|inr|cad|aud|nzd|sgd|hkd|jpy|cny|rmb|chf|sek|nok|dkk|pln|czk|huf|mxn|brl|zar|aed|sar|qar|kwd|myr|idr|php|thb|krw|try|ils|egp|ngn|kes|pkr|bdt|lkr|vnd|ron|rm|rp|kr|rs|dollars?|euros?|pounds?|rupees?)"
    + _E
    + r"\.?",
    "period_monthly": r"/ ?(?:months?|mos?|mths?|mons?|m)"
    + _E
    + r"\.?|"
    + _B
    + r"(?:(?:per|a|an|each|every) (?:months?|mos?|mths?|mons?)"
    + _E
    + r"\.?|monthly"
    + _E
    + r"|pcm"
    + _E
    + r")",
    "period_quarterly": r"/ ?(?:quarters?|qtrs?|q)"
    + _E
    + r"\.?|"
    + _B
    + r"(?:(?:per|a|an|each|every) (?:quarters?|qtrs?)"
    + _E
    + r"\.?|quarterly"
    + _E
    + r")",
    "period_annual": r"/ ?(?:years?|yrs?|y|annum)"
    + _E
    + r"\.?|"
    + _B
    + r"(?:(?:per|a|an|each|every) (?:years?|yrs?|annum)"
    + _E
    + r"\.?|annual(?:ly)?"
    + _E
    + r"|yearly"
    + _E
    + r"|p\.? ?a\.?"
    + _E
    + r")",
    "period_unsupported": r"/ ?(?:weeks?|wks?|w|days?|d|hours?|hrs?|h)"
    + _E
    + r"|"
    + _B
    + r"(?:(?:per|a|an|each|every) (?:weeks?|wks?|days?|hours?|hrs?)"
    + _E
    + r"|weekly"
    + _E
    + r"|daily"
    + _E
    + r"|hourly"
    + _E
    + r")",
    "between": _B + r"between" + _E,
    "filler_words": _B
    + r"(?:up to|upto|less than|more than|no more than|not more than|at least|at most|or more|or less|in total|approximately|approx|about|around|roughly|circa|ca|under|below|over|above|maximum|minimum|max|min|budget|total|overall|between|of|is|only|just)"
    + _E
    + r"\.?",
    "filler_symbols": r"[<>~=:+≤≥≈]",
    "token": r"(?:[0-9]+|\.[0-9]+)(?:[.,' ][0-9]+)*|[a-z]+\.?|-|[^ ]",
    "number": r"^(?:[0-9]+|\.[0-9]+)(?:[.,' ][0-9]+)*$",
    # Arabic decimal / thousands separators (U+066B / U+066C)
    "arabic_decimal": r"\u066b",
    "arabic_group": r"\u066c",
    "word": r"^[a-z]+\.?$",
    "sep": r"[.,' ]",
}

# Magnitude words/suffixes -> power of ten (1.5M = 15 * 10**5 exactly).
BUDGET_MULTIPLIER_EXP: dict[str, int] = {
    "k": 3,
    "thousand": 3,
    "thousands": 3,
    "m": 6,
    "mm": 6,
    "mn": 6,
    "mio": 6,
    "million": 6,
    "millions": 6,
    "b": 9,
    "bn": 9,
    "billion": 9,
    "billions": 9,
    "lakh": 5,
    "lakhs": 5,
    "lac": 5,
    "lacs": 5,
    "crore": 7,
    "crores": 7,
    "cr": 7,
}
# Words that separate the two ends of a range ("and" only after "between").
BUDGET_RANGE_WORDS: tuple[str, ...] = ("to",)
# A range whose high end is more than this multiple of its low end is almost
# certainly a typo ("10-15000" -> planning $7,505 silently) -- error instead.
BUDGET_MAX_RANGE_RATIO: float = 100.0
# Integer digits (+ magnitude exponent) above which an amount is too_large
# outright (>= 10**13, 10,000x the cap): keeps every amount in cents below
# 2**53 so both parsers convert it to the identical double.
BUDGET_MAX_DIGITS: int = 13

BUDGET_ERROR_MESSAGES: dict[str, str] = {
    "empty": "Enter a budget amount, e.g. 150,000 or 1.5M.",
    "not_a_number": "Couldn't read this as an amount. Type a number like 150,000, $1.5M or 500K.",
    "negative": "Budget can't be negative.",
    "zero": "Budget must be greater than 0.",
    "percent": "A percentage isn't a budget. Enter the amount, e.g. 150,000.",
    "exponent": "Scientific notation isn't supported. Type the amount in full, e.g. 1,000,000 or 1M.",
    "malformed_number": "Check the number: use 1,500,000 or 1.500.000 (or 1.5M).",
    "multiple_amounts": "Enter one amount or one range (e.g. 10k-15k); pick the duration and period separately.",
    "conflicting_period": "The amount mentions two different periods. Enter the amount and choose the period below.",
    "unsupported_period": "Weekly, daily and hourly budgets aren't supported. Enter a monthly amount and choose 'Per month', or the campaign total.",
    "implausible_range": "Check this range: its high end is more than 100x its low end.",
    "too_long": f"That's too long for a budget (max {INPUT_LIMITS['budget_max_chars']} characters).",
    "too_small": f"Budget must be at least {_fmt_int(BUDGET_MIN)} for a media plan.",
    "too_large": f"Budget can't exceed {_fmt_int(BUDGET_MAX)}.",
}

_RX: dict[str, "re.Pattern[str]"] = {k: re.compile(v) for k, v in BUDGET_REGEX.items()}


@dataclass(frozen=True)
class BudgetParse:
    """Outcome of reading one budget input.

    ``amount`` is the figure the plan uses: the number typed, or the MIDPOINT
    of a range (``low``/``high`` keep both ends). ``period`` is a per-period
    marker found in the text ("50k/mo" -> "monthly") -- informational only:
    the wizard's budget-period select alone decides scaling, as before.
    ``error`` is a stable code (see :data:`BUDGET_ERROR_MESSAGES`).
    """

    ok: bool
    amount: float = 0.0
    low: float = 0.0
    high: float = 0.0
    is_range: bool = False
    currency: str = ""
    period: str = ""
    error: str = ""
    message: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "amount": self.amount,
            "low": self.low,
            "high": self.high,
            "is_range": self.is_range,
            "currency": self.currency,
            "period": self.period,
            "error": self.error,
            "message": self.message,
        }


def _fail(code: str) -> BudgetParse:
    return BudgetParse(ok=False, error=code, message=BUDGET_ERROR_MESSAGES[code])


def _sub_keep_boundary(rx: "re.Pattern[str]", text: str) -> str:
    """Replace every match with its left-boundary group (if any) + a space."""
    return rx.sub(lambda m: (m.group(1) or "") + " " if m.re.groups else " ", text)


def _number_parts(token: str, has_mult: bool) -> Optional[tuple[str, str]]:
    """Split a digit run into (integer digits, fraction digits), or None.

    Separators: "," and "." may group thousands or mark decimals; " " and "'"
    only group. A single "."/"," followed by exactly 3 digits after a 1-3
    digit lead is a thousands separator ("150.000", "1,500") -- except a "."
    before a magnitude word ("1.500M" is 1.5M); any other single "." is a
    decimal point, and a single "," is a decimal comma only with 1-3 digits
    after it ("1,5M"). With 2+ separators, a final "."/"," that occurs once is
    the decimal mark; the rest must be one grouping character in groups of 3
    (or Indian lakh grouping with "," -- "2,50,00,000").
    """
    seps = _RX["sep"].findall(token)
    groups = _RX["sep"].split(token)
    # A zero-led integer part is only meaningful as exactly "0" ("0,750M" =
    # 0.750M); "01,500k", "010,000k", "07,620 lacs", "0050" are typos whose
    # magnitude cannot be known -- an explicit error, never a guess.
    if len(groups[0]) > 1 and groups[0][:1] == "0":
        return None
    if not seps:
        return token, ""
    last = seps[-1]
    if len(seps) == 1:
        lead, tail = groups[0], groups[1]
        # A leading-zero group is never a thousands group: "0,750" is 0.750
        # ("0,750M" used to plan $750,000,000).
        thousands = len(tail) == 3 and 1 <= len(lead) <= 3 and lead[:1] != "0"
        if last in " '":
            return (lead + tail, "") if thousands else None
        if thousands and not (last == "." and has_mult):
            return lead + tail, ""
        if last == "." or len(tail) <= 3:
            return lead, tail
        return None
    if last in ".," and seps.count(last) == 1:
        int_groups, frac = groups[:-1], groups[-1]
        int_seps = seps[:-1]
    else:
        int_groups, frac, int_seps = groups, "", seps
    if len(set(int_seps)) != 1:
        return None
    if int_groups[0][:1] == "0":
        return None  # "0,750,000": a grouped number never leads with 0
    western = 1 <= len(int_groups[0]) <= 3 and all(len(g) == 3 for g in int_groups[1:])
    indian = (
        int_seps[0] == ","
        and len(int_groups) >= 3
        and 1 <= len(int_groups[0]) <= 2
        and all(len(g) == 2 for g in int_groups[1:-1])
        and len(int_groups[-1]) == 3
    )
    if not (western or indian):
        return None
    return "".join(int_groups), frac


def _cents(int_digits: str, frac_digits: str, exp: int) -> Optional[float]:
    """int_digits.frac_digits x 10**exp rounded HALF UP to cents, or None when
    it has more than BUDGET_MAX_DIGITS integer digits.

    Exact integer arithmetic (JS: BigInt), then one division of an integer
    below 2**53 by 100, so Python and the JS port return the identical
    double -- and float noise from API clients ("110000.00000000001",
    50000/3) rounds to cents instead of failing.
    """
    if len(int_digits.lstrip("0")) + exp > BUDGET_MAX_DIGITS:
        return None
    numerator = int((int_digits + frac_digits) or "0")
    scale = 10 ** len(frac_digits)
    cents = (numerator * 10 ** (exp + 2) * 2 + scale) // (2 * scale)
    return cents / 100


def _amount_value(token: str, exp: int) -> "tuple[Optional[float], str]":
    """(amount in cents precision, "") or (None, error code) for a digit run
    times 10**exp."""
    parts = _number_parts(token, exp > 0)
    if parts is None:
        return None, "malformed_number"
    value = _cents(parts[0], parts[1], exp)
    if value is None:
        return None, "too_large"
    return value, ""


_DIGIT_ZEROS: "tuple[int, ...]" = tuple(
    cp
    for cp in range(0x80, 0x20000)
    if unicodedata.decimal(chr(cp), -1) == 0
    and all(unicodedata.decimal(chr(cp + d), -1) == d for d in range(10))
)


def _ascii_digits(text: str) -> str:
    """Every Unicode decimal digit (Arabic-Indic, Devanagari, Bengali...) as
    its ASCII digit -- via the _DIGIT_ZEROS table the page also gets."""
    out = []
    for ch in text:
        cp = ord(ch)
        if cp > 0x7F:
            for zero in _DIGIT_ZEROS:
                if zero <= cp <= zero + 9:
                    ch = chr(0x30 + cp - zero)
                    break
        out.append(ch)
    return "".join(out)


def _clean(raw: str) -> str:
    text = _ascii_digits(unicodedata.normalize("NFKC", raw))
    text = _RX["arabic_decimal"].sub(".", text)
    text = _RX["arabic_group"].sub(",", text)
    text = _RX["spaces"].sub(" ", text)
    text = _RX["dashes"].sub("-", text)
    text = _RX["quotes"].sub("'", text)
    # strip(" ") not strip(): every whitespace is " " by now, and a bare
    # strip() would also eat characters JS String.trim() keeps.
    return text.lower().strip(" ")


def parse_budget_input(raw: Any) -> BudgetParse:
    """Read a budget the way every surface must: preview, estimate, generate.

    Accepts a number, or text with: currency symbols/ISO codes/words
    ($, EUR, Rs, rupees...) anywhere; thousands separators "," "." " " "'"
    (incl. Indian lakh grouping) and a decimal "." or ","; magnitude suffixes
    and words (k, m, mm, mn, b, bn, thousand, million, billion, lakh, crore);
    per-period markers (/mo, per month, monthly, quarterly, p.a., annual...),
    reported in ``period`` but never applied; bound words and symbols
    ("<", "up to", "+") -- the bound itself is the amount; and one range
    ("10000-15000", "10k to 15k", "between 1M and 2M"), planned at its
    MIDPOINT, a bare low end borrowing the high end's magnitude ("10-15k").

    Everything else is an explicit error -- never a silent default: "", "$",
    "abc", "50%", "1e9", "1.5.2M", two amounts, negatives and zero.
    """
    if raw is None:
        return _fail("empty")
    if isinstance(raw, bool):
        return _fail("not_a_number")
    if isinstance(raw, (int, float)):
        value = float(raw)
        if not math.isfinite(value):
            return _fail("not_a_number")
        if value < 0:
            return _fail("negative")
        if value == 0:
            return _fail("zero")
        if 0.01 <= value < 10**BUDGET_MAX_DIGITS:
            # Round JSON-number noise to cents exactly as the same number
            # arrives as text (/api/generate's sanitizer str()s numbers):
            # repr() and JS String() print the same plain decimal here.
            int_digits, _, frac_digits = repr(value).partition(".")
            value = _cents(int_digits, frac_digits, 0) or value
        return BudgetParse(ok=True, amount=value, low=value, high=value)
    if not isinstance(raw, str):
        return _fail("not_a_number")
    if len(raw) > INPUT_LIMITS["budget_max_chars"]:
        return _fail("too_long")

    text = _clean(raw)
    if not text:
        return _fail("empty")
    if "%" in text:
        return _fail("percent")
    if _RX["exponent"].search(text):
        return _fail("exponent")

    currency = ""
    m_sym = _RX["currency_symbol"].search(text)
    m_word = _RX["currency_word"].search(text)
    if m_sym and (not m_word or m_sym.start() <= m_word.start()):
        currency = m_sym.group(0).upper()
    elif m_word:
        currency = m_word.group(0)[len(m_word.group(1) or "") :].rstrip(".").upper()
    text = _RX["currency_symbol"].sub(" ", text)
    text = _sub_keep_boundary(_RX["currency_word"], text)

    if _RX["period_unsupported"].search(text):
        return _fail("unsupported_period")
    periods = []
    for name in ("monthly", "quarterly", "annual"):
        rx = _RX[f"period_{name}"]
        if rx.search(text):
            periods.append(name)
            text = _sub_keep_boundary(rx, text)
    if len(periods) > 1:
        return _fail("conflicting_period")
    period = periods[0] if periods else ""

    between = bool(_RX["between"].search(text))
    text = _sub_keep_boundary(_RX["filler_words"], text)
    text = _RX["filler_symbols"].sub(" ", text)
    text = re.sub(" +", " ", text).strip(" ")
    while text.endswith(".") or text.endswith(","):
        text = text[:-1].rstrip(" ")
    if not text:
        return _fail("not_a_number")

    tokens = _RX["token"].findall(text)
    if tokens and tokens[0] == "-":
        return _fail("negative")

    # Shape: AMOUNT [SEP AMOUNT], AMOUNT = NUMBER [MULTIPLIER].
    shape: list[str] = []
    amounts: list[list[Any]] = []  # [number_token, exponent or None]
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if _RX["number"].match(tok):
            exp: Optional[int] = None
            if i + 1 < len(tokens) and _RX["word"].match(tokens[i + 1]):
                word = tokens[i + 1].rstrip(".")
                if word in BUDGET_MULTIPLIER_EXP:
                    exp = BUDGET_MULTIPLIER_EXP[word]
                    i += 1
            amounts.append([tok, exp])
            shape.append("A")
        elif tok == "-" or tok.rstrip(".") in BUDGET_RANGE_WORDS or (
            between and tok.rstrip(".") == "and"
        ):
            shape.append("S")
        else:
            return _fail("not_a_number")
        i += 1

    if not amounts:
        return _fail("not_a_number")
    if len(amounts) > 2 or (len(amounts) == 2 and shape != ["A", "S", "A"]):
        return _fail("multiple_amounts")
    if shape not in (["A"], ["A", "S", "A"]):
        return _fail("not_a_number")

    if len(amounts) == 2 and amounts[0][1] is None and amounts[1][1] is not None:
        amounts[0][1] = amounts[1][1]  # "10-15k" -> 10k to 15k
    values: list[float] = []
    for tok, exp in amounts:
        value, error = _amount_value(tok, exp or 0)
        if value is None:
            return _fail(error)
        values.append(value)
    if any(v == 0 for v in values):
        return _fail("zero")
    low, high = min(values), max(values)
    if high > low * BUDGET_MAX_RANGE_RATIO:
        return _fail("implausible_range")
    amount = (low + high) / 2 if len(values) == 2 else low
    return BudgetParse(
        ok=True,
        amount=amount,
        low=low,
        high=high,
        is_range=len(values) == 2,
        currency=currency,
        period=period,
    )


def budget_bounds_error(total: float) -> str:
    """Error code when a campaign total is outside the supported range."""
    if total < BUDGET_MIN:
        return "too_small"
    if total > BUDGET_MAX:
        return "too_large"
    return ""


@dataclass(frozen=True)
class PlanBudget:
    """The campaign budget a request plans with (parse + period scaling +
    bounds), identical for /api/estimate, /api/generate and the preview."""

    ok: bool
    parse: BudgetParse
    period: str = "campaign"
    months: float = DEFAULT_CAMPAIGN_MONTHS
    multiplier: float = 1.0
    total: float = 0.0
    error: str = ""
    message: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "amount": self.parse.amount,
            "low": self.parse.low,
            "high": self.parse.high,
            "is_range": self.parse.is_range,
            "currency": self.parse.currency,
            "text_period": self.parse.period,
            "period": self.period,
            "months": self.months,
            "multiplier": self.multiplier,
            "total": self.total,
            "error": self.error,
            "message": self.message,
        }


def resolve_plan_budget(
    raw: Any,
    budget_period: Any,
    campaign_duration: Any,
    legacy_weeks: Optional[Callable[[str], int]] = None,
) -> PlanBudget:
    """Parse ``raw``, scale it by the budget period over the campaign's
    months, and check the bounds on the resulting campaign total."""
    parsed = parse_budget_input(raw)
    period = str(budget_period or "campaign").strip().lower() or "campaign"
    months = campaign_months(campaign_duration, legacy_weeks)
    if not parsed.ok:
        return PlanBudget(
            ok=False,
            parse=parsed,
            period=period,
            months=months,
            error=parsed.error,
            message=parsed.message,
        )
    multiplier = budget_period_multiplier(period, months)
    # Cents, rounded half up with the identical float ops the JS port runs.
    total = math.floor(parsed.amount * multiplier * 100 + 0.5) / 100
    bound = budget_bounds_error(total)
    return PlanBudget(
        ok=not bound,
        parse=parsed,
        period=period,
        months=months,
        multiplier=multiplier,
        total=total,
        error=bound,
        message=BUDGET_ERROR_MESSAGES[bound] if bound else "",
    )


def format_budget_amount(amount: float) -> str:
    """Grouped figure for a canonical budget string: whole units without
    decimals, otherwise 2 decimals ("1,500,000" / "1,500,000.50")."""
    if float(amount).is_integer():
        return f"{amount:,.0f}"
    return f"{amount:,.2f}"


def page_config() -> dict[str, Any]:
    """Everything the wizard page needs to read inputs exactly as the server
    does -- embedded into templates/partials/index/body_inputs_js.html at
    compose time (template_composer), never re-typed in JS."""
    return {
        "limits": dict(INPUT_LIMITS),
        "budget": {
            "regex": dict(BUDGET_REGEX),
            "multiplier_exp": dict(BUDGET_MULTIPLIER_EXP),
            "range_words": list(BUDGET_RANGE_WORDS),
            "max_range_ratio": BUDGET_MAX_RANGE_RATIO,
            "max_digits": BUDGET_MAX_DIGITS,
            "digit_zeros": list(_DIGIT_ZEROS),
            "min": BUDGET_MIN,
            "max": BUDGET_MAX,
            "messages": dict(BUDGET_ERROR_MESSAGES),
        },
        "duration_months": dict(CAMPAIGN_DURATION_MONTHS),
        "period_months": dict(BUDGET_PERIOD_MONTHS),
        "default_months": DEFAULT_CAMPAIGN_MONTHS,
    }
