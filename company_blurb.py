"""Company-description ("blurb") entity validation + boundary-safe truncation.

Single owner of the rule every client-facing company description passes
through -- the deck cover tagline, the Competitive Landscape "Description:"
row, the workbook's Market Intelligence company profile, and the
Wikipedia/Clearbit lookups in api_enrichment that feed them.

Origin defect (prod, AWP Safety, 2026-09-25): the Wikipedia name-guess
lookup accepted the article for the AWP *sniper rifle* because its extract
contained the word "company" ("...manufactured by the British company
Accuracy International"), and slide 1 printed it under the client's name.
The synthesizer's own entity check DID flag it, but only swapped a
"<Client> is a company in the <raw_industry_key> industry." fallback into
two of the three surfaces -- the cover read the raw lookup and shipped the
rifle text anyway.

Policy (conservative -- omit rather than assert):
    A description is accepted only when ALL of these hold, else the blurb is
    omitted entirely (no generic fallback sentence):
      1. it is a real article summary (not empty, not a disambiguation page);
      2. its first sentence's SUBJECT (or the article title) names the client
         -- a distinctive client-name token, or the client's acronym;
      3. its first sentence's PREDICATE head positively signals an
         organisation ("... is an American multinational company", "... is a
         major airline", "... is a fast food chain") or the subject carries a
         corporate suffix (Inc., plc, LLC ...), and does NOT describe a
         person;
      4. (when the plan industry is known) it carries none of the
         industry-mismatch keywords for that industry.

Everything here is stdlib-only and never raises on odd input.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Sentence splitting (abbreviation-aware)
# ---------------------------------------------------------------------------
# "Automatic Data Processing, Inc. (ADP) is ..." must not end at "Inc." --
# the old split(".")[0] check cut it there and then rejected the CORRECT ADP
# article because "(ADP)" fell outside the truncated "first sentence".
_ABBREVIATIONS = frozenset(
    {
        "inc",
        "corp",
        "co",
        "ltd",
        "llc",
        "plc",
        "bros",
        "jr",
        "sr",
        "st",
        "mt",
        "ft",
        "mr",
        "mrs",
        "ms",
        "dr",
        "prof",
        "no",
        "vs",
        "etc",
        "approx",
        "dept",
        "est",
        "u.s",
        "u.k",
        "e.g",
        "i.e",
    }
)
_SENTENCE_END_RE = re.compile(r"[.!?](?=\s+[\"'“(]?[A-Z0-9])")


def split_sentences(text: Any) -> List[str]:
    """Split prose into sentences without breaking on corporate/honorific
    abbreviations ("Inc.", "Co.", "U.S.") or single-letter initials."""
    if not isinstance(text, str):
        return []
    text = text.strip()
    if not text:
        return []
    out: List[str] = []
    start = 0
    for m in _SENTENCE_END_RE.finditer(text):
        before = text[start : m.start()]
        tok_m = re.search(r"(\S+)$", before)
        tok = (tok_m.group(1) if tok_m else "").strip("\"'“”()[]")
        low = tok.lower().rstrip(".")
        if (
            low in _ABBREVIATIONS
            or re.fullmatch(r"[A-Z]", tok)
            or re.fullmatch(r"(?:[A-Za-z]\.)+[A-Za-z]", tok)
        ):
            continue
        out.append(text[start : m.end()].strip())
        start = m.end()
    tail = text[start:].strip()
    if tail:
        out.append(tail)
    return out


def first_sentence(text: Any) -> str:
    parts = split_sentences(text)
    return parts[0] if parts else ""


# ---------------------------------------------------------------------------
# Name tokens / agreement
# ---------------------------------------------------------------------------
_NAME_STOP = frozenset(
    {
        "inc",
        "llc",
        "ltd",
        "limited",
        "corp",
        "corporation",
        "company",
        "companies",
        "co",
        "plc",
        "gmbh",
        "ag",
        "sa",
        "nv",
        "bv",
        "pty",
        "lp",
        "llp",
        "group",
        "holdings",
        "international",
        "global",
        "the",
        "a",
        "an",
        "of",
        "and",
    }
)
_ACRONYM_SKIP = frozenset({"of", "and", "the", "&", "for", "de"})


def _fold(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.replace("’", "'").replace("‘", "'")


def _tokens(text: Any) -> List[str]:
    """Lower-cased word tokens; possessive/apostrophes folded away so
    "McDonald's" == "McDonalds" and "Hershey's" also yields "hershey"."""
    if not isinstance(text, str):
        return []
    out: List[str] = []
    for raw in re.split(r"[^A-Za-z0-9']+", _fold(text).lower()):
        raw = raw.strip("'")
        if not raw:
            continue
        joined = raw.replace("'", "")
        out.append(joined)
        if raw.endswith("'s"):
            out.append(raw[:-2].replace("'", ""))
    return out


def name_tokens(name: Any) -> List[str]:
    """Distinctive tokens of a company name, in order, suffixes removed."""
    seen: List[str] = []
    for t in _tokens(name):
        if t and t not in _NAME_STOP and t not in seen:
            seen.append(t)
    return seen


def _acronyms(text: str) -> set:
    """Initials of every run of 2-6 consecutive Capitalised words
    ("Automatic Data Processing" -> "adp")."""
    words = re.findall(r"[A-Za-z][A-Za-z'&.-]*", _fold(text or ""))
    out: set = set()
    caps: List[str] = []
    for w in words + [""]:
        if w and w[0].isupper():
            caps.append(w)
            continue
        if w.lower() in _ACRONYM_SKIP and caps:
            continue  # "Bank of America" -> "boa" keeps the run going
        for i in range(len(caps)):
            for j in range(i + 2, min(len(caps), i + 6) + 1):
                out.add("".join(c[0] for c in caps[i:j]).lower())
        caps = []
    return out


def name_agrees(client_name: Any, *candidates: Any) -> bool:
    """True when ``candidates`` (a sentence subject, an article title, a
    Clearbit suggestion name ...) name the client -- the FULL distinctive
    name, not a fragment of it.

    Every distinctive client token must appear (adjacent candidate tokens
    may join: "Delta Airlines" ~ "Delta Air Lines"); a 2-6 letter
    single-token name may instead match an acronym of the candidate
    ("ADP" ~ "Automatic Data Processing"), and a multi-token name may match
    via its own initials appearing as a token. Sharing some words is NOT
    enough: "Brookdale Senior Living" must not match "Emeritus Senior
    Living" (verifier, 2026-10-01 -- the old two-shared-words rule did).
    """
    ctoks = name_tokens(client_name)
    if not ctoks:
        return False
    hay: set = set()
    acr: set = set()
    for c in candidates:
        if isinstance(c, str) and c:
            toks = _tokens(c)
            hay.update(toks)
            hay.update(a + b for a, b in zip(toks, toks[1:]))
            acr |= _acronyms(c)
    if not hay:
        return False
    if all(t in hay for t in ctoks):
        return True
    if len(ctoks) == 1:
        t = ctoks[0]
        return 2 <= len(t) <= 6 and t in acr
    initials = "".join(t[0] for t in ctoks)
    return len(initials) >= 2 and initials in hay


def title_matches_name(client_name: Any, title: Any) -> bool:
    """True when an article TITLE is the client's own name (suffixes such as
    Inc./Company/Corporation and a trailing "(company)" qualifier ignored).
    Required for any article reached only through the free-text search
    fallback, where the top hit for "<name> company" is very often a
    different company that merely shares a word."""
    if not isinstance(title, str) or not title.strip():
        return False
    bare = re.sub(r"\s*\([^()]*\)\s*$", "", title)
    return bool(name_tokens(client_name)) and name_tokens(client_name) == name_tokens(bare)


# ---------------------------------------------------------------------------
# Organisation / person cues
# ---------------------------------------------------------------------------
_ORG_NOUN_RE = re.compile(
    r"\b(?:compan(?:y|ies)|corporations?|conglomerates?|firms?|manufacturers?|"
    r"makers?|automakers?|carmakers?|retailers?|wholesalers?|distributors?|"
    r"suppliers?|airlines?|carriers?|banks?|lenders?|insurers?|breweries|"
    r"brewery|winery|distillery|chains?|franchise[sd]?|franchisors?|"
    r"co-?operatives?|enterprises?|business(?:es)?|subsidiar(?:y|ies)|"
    r"divisions?|providers?|operators?|contractors?|developers?|publishers?|"
    r"broadcasters?|agenc(?:y|ies)|organi[sz]ations?|non-?profits?|"
    r"charit(?:y|ies)|foundations?|associations?|institutions?|hospitals?|"
    r"clinics?|health ?(?:care )?systems?|universit(?:y|ies)|colleges?|"
    r"school (?:district|system)s?|utilit(?:y|ies)|start-?ups?|"
    r"consultanc(?:y|ies)|partnerships?|record labels?|brands?|employers?|consortium|consortia|"
    r"restaurants?|hotels?|casinos?|stores?|railroads?|marketplaces?)\b",
    re.IGNORECASE,
)
_PERSON_NOUN_RE = re.compile(
    r"\b(?:actor|actress|singer|songwriter|musician|rapper|politician|"
    r"businessman|businesswoman|businessperson|magnate|industrialist|"
    r"entrepreneur|investor|philanthropist|author|writer|novelist|poet|"
    r"journalist|footballer|player|athlete|boxer|wrestler|coach|painter|"
    r"artist|filmmaker|director|comedian|presenter|host|model|lawyer|judge|"
    r"scientist|physicist|chemist|engineer|inventor|architect|economist|"
    r"activist|bishop|priest|king|queen|prince|princess|emperor|officer|"
    r"soldier|astronaut|explorer|founder|tycoon|billionaire|"
    r"record producer|film producer|television producer|chef)s?\b",
    re.IGNORECASE,
)
_NON_ORG_HEAD_RE = re.compile(
    r"\b(?:aircraft carriers?|rifles?|firearms?|guns?|pistols?|weapons?|"
    r"missiles?|cartridges?|films?|movies?|songs?|albums?|singles?|novels?|"
    r"television series|tv series|sitcoms?|video games?|bands?|musical groups?|"
    r"rock groups?|characters?|episodes?|planets?|elements?|species|genus|"
    r"fruits?|rivers?|mountains?|lakes?|islands?|cit(?:y|ies)|towns?|"
    r"villages?|count(?:y|ies)|countr(?:y|ies)|pandemics?|wars?|battles?|"
    r"elections?|surnames?|given names?|landforms?|diseases?)\b",
    re.IGNORECASE,
)
_CORP_SUFFIX_RE = re.compile(
    r"(?:,?\s)(?:Inc\.?|Incorporated|Corp\.?|Corporation|Company|Co\.|LLC|"
    r"L\.L\.C\.|Ltd\.?|Limited|plc|PLC|GmbH|AG|S\.A\.|N\.V\.|SE|AB|ASA|Oyj|"
    r"K\.K\.|S\.p\.A\.|Pty|LLP|LP|Holdings|Group)(?=$|[\s,.)(])"
)
_COPULA_RE = re.compile(r"\b(?:is|was|are|were)\b")
# The definition's head runs to the first clause-ending marker -- NOT to the
# first comma: "Johnson & Johnson is an American multinational,
# pharmaceutical, and medical technologies corporation" was cut at
# "multinational," and wrongly omitted (verifier, 2026-10-01).
_HEAD_MAX_WORDS = 20
_HEAD_CUT_RE = re.compile(
    r";|:|—|\s(?:which|that|who|whose|where|designed|manufactured|"
    r"made|produced|developed|owned|operated|founded|based|headquartered|"
    r"created|written|directed|released|known|located|by)\s",
    re.IGNORECASE,
)
_DISAMBIG_RE = re.compile(r"\b(?:may|can|might) (?:also )?refer to\b", re.IGNORECASE)

# Industry mismatch keywords -- a description carrying one of these while
# the plan's industry label contains a listed word is a different entity
# (moved here from data_synthesizer so the cover, the profile and the
# lookup all apply the same rule).
INDUSTRY_MISMATCH_MAP: Dict[str, Tuple[str, ...]] = {
    "video game": (
        "localization",
        "translation",
        "staffing",
        "healthcare",
        "financial",
        "insurance",
        "logistics",
        "manufacturing",
    ),
    "anime": (
        "localization",
        "translation",
        "staffing",
        "healthcare",
        "financial",
        "insurance",
        "logistics",
        "manufacturing",
    ),
    "manga": (
        "localization",
        "translation",
        "staffing",
        "healthcare",
        "financial",
        "insurance",
        "logistics",
        "manufacturing",
    ),
    "record label": (
        "technology",
        "software",
        "healthcare",
        "staffing",
        "localization",
        "financial",
        "insurance",
    ),
    "professional wrestler": (
        "technology",
        "software",
        "healthcare",
        "localization",
        "financial",
        "staffing",
    ),
    "television series": (
        "technology",
        "software",
        "healthcare",
        "localization",
        "financial",
        "staffing",
    ),
    "film": (
        "technology",
        "software",
        "healthcare",
        "localization",
        "financial",
        "staffing",
        "logistics",
    ),
    "musical group": (
        "technology",
        "software",
        "healthcare",
        "localization",
        "financial",
        "staffing",
    ),
    "fictional": (
        "technology",
        "software",
        "healthcare",
        "localization",
        "financial",
        "staffing",
        "logistics",
    ),
}


# Positive industry agreement (verifier, 2026-10-01): a real organisation
# that merely shares the client's name ("Pinnacle" -> Pinnacle Foods for a
# healthcare plan, "Atlas" -> Atlas Copco for a trucking plan, "AWP" -> a
# Swiss news agency) must not be presented as the client. When the plan's
# industry is known, the article must use at least one of that industry's
# words (or a distinctive word from the plan's own role titles); otherwise
# the blurb is omitted -- omission is the safe failure.
INDUSTRY_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "healthcare_medical": ("health", "healthcare", "health care", "hospital", "hospitals", "medical", "clinic", "clinics", "nursing", "senior living", "assisted living", "physician", "physicians", "patient", "patients", "care"),
    "mental_health": ("mental health", "behavioral", "behavioural", "psychiatric", "counseling", "therapy", "addiction", "hospital", "hospitals", "health", "healthcare"),
    "pharma_biotech": ("pharmaceutical", "pharmaceuticals", "biotechnology", "biotech", "drug", "drugs", "medicine", "medicines", "vaccine", "vaccines", "life sciences", "medical"),
    "tech_engineering": ("technology", "technologies", "software", "computer", "computing", "internet", "semiconductor", "semiconductors", "electronics", "engineering", "cloud computing", "information technology", "it services", "consulting", "payroll", "human resources"),
    "finance_banking": ("bank", "banks", "banking", "financial", "finance", "investment", "investments", "payment", "payments", "credit", "lending", "mortgage", "brokerage", "asset management", "fintech", "insurance"),
    "insurance": ("insurance", "insurer", "insurers", "reinsurance", "assurance", "underwriting", "annuities", "annuity"),
    "retail_consumer": ("retail", "retailer", "retailers", "store", "stores", "supermarket", "supermarkets", "grocery", "department store", "apparel", "clothing", "consumer", "e-commerce", "home improvement", "chain", "fashion"),
    "hospitality_travel": ("hotel", "hotels", "hospitality", "restaurant", "restaurants", "fast food", "airline", "airlines", "travel", "resort", "resorts", "cruise", "casino", "casinos", "tourism", "lodging", "dining", "food service", "foodservice"),
    "food_beverage": ("food", "foods", "beverage", "beverages", "chocolate", "confectionery", "snack", "snacks", "dairy", "drink", "drinks", "brewing", "brewery", "coffee", "bakery", "meat", "candy", "restaurant", "restaurants", "fast food", "coffeehouse", "coffeehouses", "roastery"),
    "logistics_supply_chain": ("logistics", "freight", "shipping", "transport", "transportation", "trucking", "delivery", "courier", "package", "parcel", "supply chain", "warehouse", "warehousing", "railroad", "e-commerce", "distribution", "moving"),
    "automotive": ("automobile", "automobiles", "automotive", "automaker", "vehicle", "vehicles", "car", "cars", "trucks", "motor", "motors", "auto parts", "manufacturing", "manufacturer"),
    "construction_real_estate": ("construction", "contractor", "contractors", "building", "buildings", "engineering", "real estate", "property", "properties", "infrastructure", "homebuilder", "civil engineering", "traffic control", "road", "roads", "highway"),
    "energy_utilities": ("energy", "oil", "gas", "petroleum", "electric", "electricity", "power", "utility", "utilities", "natural gas", "renewable", "solar", "nuclear", "pipeline"),
    "telecommunications": ("telecommunications", "telecom", "wireless", "mobile", "cable", "broadband", "network", "networks", "telephone", "internet service", "fiber"),
    "media_entertainment": ("media", "entertainment", "television", "film", "films", "studio", "studios", "broadcasting", "broadcaster", "publishing", "publisher", "music", "streaming", "news", "newspaper", "video games"),
    "legal_services": ("law firm", "legal", "attorneys", "lawyers", "litigation", "law"),
    "education": ("university", "college", "school", "schools", "education", "educational", "academy", "learning", "research university"),
    "aerospace_defense": ("aerospace", "defense", "defence", "aircraft", "aviation", "space", "spacecraft", "military", "missile", "missiles", "security"),
    "military_recruitment": ("armed forces", "army", "navy", "air force", "military", "marine corps", "defense", "defence"),
    "maritime_marine": ("shipping", "maritime", "marine", "ship", "ships", "vessel", "vessels", "shipbuilding", "port", "ports", "container", "offshore", "cruise"),
    "blue_collar_trades": ("manufacturing", "manufacturer", "construction", "industrial", "maintenance", "contractor", "contractors", "facilities", "trades", "staffing", "services"),
    "general_entry_level": ("retail", "retailer", "store", "stores", "restaurant", "restaurants", "chain", "warehouse", "logistics", "staffing", "call center", "customer service", "hospitality", "nonprofit", "non-profit", "charity", "food bank"),
    "rideshare": ("ridesharing", "ride-hailing", "ride sharing", "rideshare", "delivery", "gig", "transportation", "taxi", "mobility"),
}
_ROLE_STOPWORDS = frozenset(
    {
        "senior", "junior", "lead", "manager", "associate", "specialist",
        "assistant", "coordinator", "director", "officer", "analyst",
        "engineer", "technician", "operator", "worker", "representative",
        "supervisor", "staff", "team", "member", "general", "entry", "level",
        "part", "time", "full", "shift", "hourly", "sales", "service",
        "services", "customer", "head", "chief", "principal", "intern",
    }
)


def _industry_key(industry: Any) -> str:
    """Industry key or label -> an INDUSTRY_KEYWORDS key ("" if unknown)."""
    if not isinstance(industry, str) or not industry.strip():
        return ""
    raw = industry.strip()
    if raw.lower() in INDUSTRY_KEYWORDS:
        return raw.lower()
    try:
        from shared_utils import INDUSTRY_LABEL_MAP
    except ImportError:  # pragma: no cover
        INDUSTRY_LABEL_MAP = {}
    for key, label in INDUSTRY_LABEL_MAP.items():
        if label.lower() == raw.lower():
            return key
    return ""


def industry_agrees(description: Any, industry: Any, roles: Any = ()) -> bool:
    """True when ``description`` uses a word of the plan's industry (or a
    distinctive word of one of its role titles). An industry we cannot map
    to a keyword set falls back to its own label words."""
    if not isinstance(description, str) or not description.strip():
        return False
    low = description.lower()
    key = _industry_key(industry)
    words: List[str] = list(INDUSTRY_KEYWORDS.get(key, ()))
    if not words and isinstance(industry, str):
        words = [
            w
            for w in re.split(r"[^a-z]+", industry.lower().replace("_", " "))
            if len(w) >= 4
        ]
    role_list = roles if isinstance(roles, (list, tuple)) else [roles]
    for r in role_list:
        title = (r.get("title") or r.get("role") or "") if isinstance(r, dict) else r
        for w in re.split(r"[^a-z]+", str(title or "").lower()):
            if len(w) >= 5 and w not in _ROLE_STOPWORDS:
                words.append(w)
    return any(
        re.search(rf"(?<![a-z]){re.escape(w)}(?![a-z])", low) for w in words if w
    )


def _industry_text(industry: Any) -> str:
    """Industry key or label -> lower-case words for the mismatch check
    ("healthcare_medical" -> "healthcare & medical")."""
    if not isinstance(industry, str) or not industry.strip():
        return ""
    try:
        import display_format

        return display_format.humanize_key(industry.strip()).lower()
    except ImportError:  # pragma: no cover
        return industry.replace("_", " ").lower()


def validate_company_description(
    client_name: Any,
    description: Any,
    *,
    title: Any = "",
    industry: Any = "",
    roles: Any = (),
    page_type: Any = "standard",
) -> Tuple[bool, str]:
    """Decide whether ``description`` may be shown as ``client_name``'s own
    company blurb. Returns ``(ok, reason)``; ``reason`` is "" when ok and a
    short diagnostic otherwise (for logs only -- never client text).

    See the module docstring for the policy. ``title`` is the source
    article title when known (Wikipedia), ``page_type`` the Wikipedia REST
    ``type`` ("standard" / "disambiguation" / ...). When ``industry`` is
    given (every deliverable surface passes the plan's), the article must
    also positively agree with it (``industry_agrees``) -- a same-name
    company in another sector is omitted. The industry-less form is only the
    lookup's pre-filter (name + organisation).
    """
    name = client_name if isinstance(client_name, str) else ""
    desc = description if isinstance(description, str) else ""
    ttl = title if isinstance(title, str) else ""
    if not desc.strip():
        return False, "empty description"
    if not name.strip():
        return False, "no client name to validate against"
    if isinstance(page_type, str) and page_type and page_type != "standard":
        return False, f"not a standard article (type={page_type})"
    if _DISAMBIG_RE.search(desc):
        return False, "disambiguation page"

    sent = first_sentence(desc)
    no_paren = re.sub(r"\([^()]*\)", " ", sent)
    cop = _COPULA_RE.search(no_paren)
    if not cop:
        # Verb-led definitions ("Brookdale Senior Living Solutions owns and
        # operates retirement homes ... The company was established in
        # 1978"): accept only when the sentence OPENS with the client's full
        # name, an organisation noun follows within two sentences and the
        # opening sentence carries no person cue.
        lead = " ".join(no_paren.split()[: len(name.split()) + 2])
        two = " ".join(split_sentences(desc)[:2])
        if not (
            name_agrees(name, lead)
            and _ORG_NOUN_RE.search(two)
            and not _PERSON_NOUN_RE.search(sent)
        ):
            return False, f"no 'X is a ...' definition sentence: {sent[:120]!r}"
        return _industry_verdict(desc, industry, roles)
    subject = no_paren[: cop.start()]
    # Parenthetical aliases ("(ADP)", "(commonly known as Ford)") belong to
    # the subject for name matching.
    raw_cop = _COPULA_RE.search(sent)
    subject_full = sent[: raw_cop.start()] if raw_cop else sent
    predicate = no_paren[cop.end() :]
    head = " ".join(
        _HEAD_CUT_RE.split(predicate, maxsplit=1)[0].split()[:_HEAD_MAX_WORDS]
    )

    if not name_agrees(name, subject, subject_full, ttl):
        return False, (
            f"entity mismatch: the article subject {subject.strip()[:80]!r} "
            f"does not name {name!r}"
        )
    org_m = _ORG_NOUN_RE.search(head)
    if re.search(r"\baircraft carriers?\b", head, re.IGNORECASE):
        org_m = None
    person_m = _PERSON_NOUN_RE.search(head)
    # A person noun ahead of any organisation noun is a person article even
    # when an org word follows ("an American industrialist and business
    # magnate" -- Henry Ford, not Ford Motor Company).
    if (person_m and (org_m is None or person_m.start() < org_m.start())) or re.search(
        r"\(born\b|;\s*born\b", sent
    ):
        return False, f"describes a person: {head.strip()[:80]!r}"
    has_org = org_m is not None
    if not has_org and _CORP_SUFFIX_RE.search(subject) is None:
        why = "non-organisation" if _NON_ORG_HEAD_RE.search(head) else "no organisation"
        return False, f"{why} definition: {head.strip()[:80]!r}"

    return _industry_verdict(desc, industry, roles)


def _industry_verdict(desc: str, industry: Any, roles: Any) -> Tuple[bool, str]:
    """Industry-mismatch keywords + positive industry agreement (skipped
    when no industry is given -- the lookup's name/organisation pre-filter)."""
    ind = _industry_text(industry)
    if ind:
        low = desc.lower()
        for kw, blocked in INDUSTRY_MISMATCH_MAP.items():
            if kw in low and any(b in ind for b in blocked):
                return False, (
                    f"industry mismatch: description mentions {kw!r} but the "
                    f"plan industry is {industry!r}"
                )
        if not industry_agrees(desc, industry, roles):
            return False, (
                f"no industry agreement: the article never mentions a "
                f"{industry!r} keyword or a role-title word -- a same-name "
                f"organisation in another sector"
            )
    return True, ""


# ---------------------------------------------------------------------------
# Boundary-safe truncation
# ---------------------------------------------------------------------------
_TRAILING_STOPWORDS = frozenset(
    {"a", "an", "the", "and", "or", "of", "in", "on", "for", "to", "by", "with", "as"}
)
ELLIPSIS = "…"


def truncate_at_boundary(text: Any, max_chars: int) -> str:
    """Shorten ``text`` to at most ``max_chars``: whole sentences when at
    least one fits (no ellipsis -- nothing is visibly cut), otherwise the
    last whole word followed by an ellipsis. Never cuts inside a word and
    never leaves a dangling article/preposition before the ellipsis."""
    if not isinstance(text, str):
        return ""
    text = " ".join(text.split())
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    kept = ""
    for s in split_sentences(text):
        cand = f"{kept} {s}".strip()
        if len(cand) > max_chars:
            break
        kept = cand
    if kept:
        return kept
    cut = text[: max_chars - 1]
    if " " in cut and not text[max_chars - 1].isspace():
        cut = cut.rsplit(" ", 1)[0]
    # Prefer ending at a clause boundary when one sits in the last half of
    # the budget: "...is an American multinational company…" reads better
    # than "...company and one of the largest chocolate…".
    floor = int(max_chars * 0.5)
    best = -1
    for sep in (", ", "; ", " — ", " – ", " and ", " which ", " who "):
        idx = cut.rfind(sep)
        if idx >= floor and idx > best:
            best = idx
    if best > 0:
        cut = cut[:best]
    words = cut.rstrip(" ,;:-—–").split(" ")
    while len(words) > 1 and words[-1].lower() in _TRAILING_STOPWORDS:
        words.pop()
    return " ".join(words).rstrip(" ,;:-—–") + ELLIPSIS


def company_tagline(description: Any, max_chars: int = 120) -> str:
    """Cover-slide tagline: the description's first sentence, shortened on a
    word boundary (with an ellipsis) only if it alone exceeds ``max_chars``."""
    return truncate_at_boundary(first_sentence(description), max_chars)


def client_company_description(data: Any) -> str:
    """The ONE validated company description a deliverable may show for the
    plan's client, or "" (omit) when none passes validation.

    Reads the raw enrichment lookup (``data["_enriched"]["company_info"]``)
    and re-validates it here rather than trusting it -- cached lookups from
    before this validator existed (L1-L4 caches, incl. Supabase) can still
    carry a wrong-entity article.
    """
    if not isinstance(data, dict):
        return ""
    enriched = data.get("_enriched") or {}
    info = enriched.get("company_info") if isinstance(enriched, dict) else None
    if not isinstance(info, dict):
        return ""
    desc = info.get("description") or ""
    if not isinstance(desc, str) or not desc.strip():
        return ""
    industry = data.get("industry") or data.get("industry_label") or ""
    if not industry:
        return ""  # cannot check sector agreement -> omit (safe failure)
    ok, _reason = validate_company_description(
        data.get("client_name") or "",
        desc,
        title=info.get("wiki_title") or "",
        industry=industry,
        roles=data.get("target_roles") or data.get("roles") or [],
    )
    return desc.strip() if ok else ""


__all__ = [
    "ELLIPSIS",
    "INDUSTRY_MISMATCH_MAP",
    "client_company_description",
    "company_tagline",
    "first_sentence",
    "name_agrees",
    "name_tokens",
    "split_sentences",
    "truncate_at_boundary",
    "validate_company_description",
]
