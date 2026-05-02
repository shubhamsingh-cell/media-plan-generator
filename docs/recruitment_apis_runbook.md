# Free Recruitment APIs Runbook

**Module:** `recruitment_apis.py` (798 lines, stdlib only).
**Tool wiring:** `chatbot_tools_recruitment.py` exports `RECRUITMENT_TOOLS_SCHEMA` (Anthropic tool-use schemas) and `RECRUITMENT_TOOL_DISPATCH` (name -> callable).
**Contract:** every public function accepts `timeout: int` (default 10) and returns a dict with at least `"source"`. Errors return `{"error": str, "source": str}` — never raise.

| Tool | Auth | Free tier ceiling |
|---|---|---|
| `lookup_skill_esco` | None | Unlimited |
| `lookup_occupation_esco` | None | Unlimited |
| `lookup_healthcare_npi` | None | Unlimited (CMS public) |
| `lookup_trucking_carrier` | None (empty webKey) | Unlimited (FMCSA public mode) |
| `lookup_country_labour_ilostat` | None | Unlimited (auto WB fallback) |
| `lookup_country_indicator_worldbank` | None | Unlimited |
| `lookup_layoffs_warntracker` | None | URL only — no JSON API |
| `lookup_tech_jobs_hnhiring` | None | Unlimited |
| `lookup_compensation_levels` | None | Embed URL only — no JSON API |
| `lookup_company_crunchbase` | `CRUNCHBASE_API_KEY` (paid) | Stub if key absent |
| `enrich_person_pdl` | `PDL_API_KEY` (freemium) | 100 lookups/mo when set; stub if absent |

---

## 1. `lookup_skill_esco`

**Signature** (`recruitment_apis.py:166`):
```python
def lookup_skill_esco(skill: str, lang: str = "en", timeout: int = 10) -> dict
```

**Returns** (success):
```python
{
    "skills": [
        {
            "uri": "http://data.europa.eu/esco/skill/...",
            "title": "use Python (computer programming)",
            "description": "Apply technical principles and procedures..."
        },
        # up to 5
    ],
    "source": "ESCO",
    "count": 1
}
```

**Free tier limits:** unlimited; no auth. EU Commission's open ESCO taxonomy.
**When to use:** translating free-text user skill terms into the canonical ESCO URI used by Joveo's role taxonomy (`role_taxonomy.py`); cross-language skill lookups.
**Sample chatbot question:** *"What does the ESCO taxonomy say about 'welding'?"* / *"Find the ESCO skill ID for Python programming."*
**Example call:**
```python
from recruitment_apis import lookup_skill_esco
result = lookup_skill_esco("python", lang="en", timeout=10)
print(result["count"], result["skills"][0]["uri"])
```
**Error modes:**
```python
{"error": "skill must be non-empty", "source": "ESCO"}
{"error": "<HTTPError/URLError/timeout>", "source": "ESCO"}
```

---

## 2. `lookup_occupation_esco`

**Signature** (`recruitment_apis.py:194`):
```python
def lookup_occupation_esco(occupation: str, lang: str = "en", timeout: int = 10) -> dict
```

**Returns** (success):
```python
{
    "occupations": [
        {"uri": "...", "title": "software developer", "description": "..."},
        # up to 5
    ],
    "source": "ESCO",
    "count": 1
}
```

**Free tier limits:** unlimited; no auth.
**When to use:** mapping free-text role names ("dev lead", "RN") to ESCO occupation URIs for cross-EU job-supply analysis.
**Sample chatbot question:** *"Map 'registered nurse' to ESCO."*
**Example call:**
```python
from recruitment_apis import lookup_occupation_esco
result = lookup_occupation_esco("software developer")
```
**Error modes:** same shape as `lookup_skill_esco` with `"source": "ESCO"`.

---

## 3. `lookup_healthcare_npi`

**Signature** (`recruitment_apis.py:224`):
```python
def lookup_healthcare_npi(
    name_or_npi: str, state: str = "", limit: int = 10, timeout: int = 10
) -> dict
```

**Input rules:** A 10-digit numeric input is treated as an NPI number. Otherwise the first whitespace-separated token is `first_name` and the rest is `last_name`.

**Returns** (success):
```python
{
    "providers": [
        {
            "npi": "1234567890",
            "name": "Jane Smith, MD",
            "taxonomy": "Internal Medicine",
            "addresses": [
                {
                    "address_1": "...", "city": "...", "state": "CA",
                    "postal_code": "...", "country_code": "US",
                    "telephone_number": "...", "address_purpose": "LOCATION"
                }
            ]
        }
    ],
    "source": "NPI Registry",
    "count": 1
}
```

**Free tier limits:** unlimited; no auth required. CMS caps `limit` at 200 server-side; the client clamps to `[1, 200]`.
**When to use:** sizing US healthcare TAM by specialty + state, validating NPI numbers in employer data feeds, building healthcare-vertical talent intel.
**Sample chatbot question:** *"How many internal medicine doctors are in California?"* (combined with `state="CA"` filter) / *"Look up NPI 1234567890."*
**Example call:**
```python
from recruitment_apis import lookup_healthcare_npi
result = lookup_healthcare_npi("Smith", state="CA", limit=20)
```
**Error modes:**
```python
{"error": "name_or_npi must be non-empty", "source": "NPI Registry"}
{"error": "<network exc>", "source": "NPI Registry"}
```

---

## 4. `lookup_trucking_carrier`

**Signature** (`recruitment_apis.py:318`):
```python
def lookup_trucking_carrier(dot_or_name: str, timeout: int = 10) -> dict
```

**Input rules:** digits-only -> USDOT number; otherwise carrier name. Empty `webKey` is intentional (FMCSA public mode).

**Returns** (success):
```python
{
    "carriers": [
        {
            "dot_number": "12345",
            "legal_name": "ACME TRUCKING LLC",
            "dba_name": "ACME EXPRESS",
            "phy_state": "TX",
            "phy_city": "DALLAS",
            "total_drivers": 47,
            "total_power_units": 50,
            "allowed_to_operate": "Y"
        }
    ],
    "source": "FMCSA",
    "count": 1
}
```

**Free tier limits:** unlimited public mode. FMCSA QC Mobile API.
**When to use:** sizing US trucking TAM by carrier headcount, validating commercial fleet operators in vertical campaigns.
**Sample chatbot question:** *"Show me trucking carriers in Texas with 50+ drivers."* / *"What does USDOT 12345 do?"*
**Example call:**
```python
from recruitment_apis import lookup_trucking_carrier
result = lookup_trucking_carrier("Acme Trucking")
```
**Error modes:**
```python
{"error": "dot_or_name must be non-empty", "source": "FMCSA"}
{"error": "<network exc>", "source": "FMCSA"}
```

---

## 5. `lookup_country_labour_ilostat`

**Signature** (`recruitment_apis.py:458`):
```python
def lookup_country_labour_ilostat(
    country_iso3: str,
    indicator: str = "UNE_DEAP_SEX_AGE_RT_A",
    timeout: int = 15,
) -> dict
```

**Default indicator:** `UNE_DEAP_SEX_AGE_RT_A` (annual unemployment rate).

**Returns** (success):
```python
{
    "observations": [
        {"year": "2024", "value": 3.7},
        {"year": "2023", "value": 3.6},
        # up to lastNObservations=5
    ],
    "indicator": "UNE_DEAP_SEX_AGE_RT_A",
    "country": "USA",
    "source": "ILOSTAT"  # or "WorldBank fallback" if SDMX failed
}
```

**Free tier limits:** unlimited; no auth.
**Fallback:** on any SDMX failure or empty observations, falls back to World Bank `SL.UEM.TOTL.ZS` and tags `"source": "WorldBank fallback"`.
**When to use:** macro labor signals for international media planning (e.g., "is unemployment rising in Germany right now?").
**Sample chatbot question:** *"What's the latest unemployment rate in France?"*
**Example call:**
```python
from recruitment_apis import lookup_country_labour_ilostat
result = lookup_country_labour_ilostat("USA")
```
**Error modes:**
```python
{"error": "country_iso3 must be a 3-letter code", "source": "ILOSTAT"}
{"error": "<network exc>", "source": "ILOSTAT"}  # only if WB fallback also fails
```

---

## 6. `lookup_country_indicator_worldbank`

**Signature** (`recruitment_apis.py:512`):
```python
def lookup_country_indicator_worldbank(
    country_iso3: str, indicator: str, timeout: int = 10
) -> dict
```

**Returns** (success):
```python
{
    "country": "USA",
    "indicator": "SL.UEM.TOTL.ZS",
    "observations": [
        {"year": "2024", "value": 3.7},
        # up to 20 observations between 2020:2024
    ],
    "source": "World Bank"
}
```

**Free tier limits:** unlimited; no auth. 1500+ indicators available.
**Common indicator codes:**

| Code | Series |
|---|---|
| `SL.UEM.TOTL.ZS` | Unemployment, % of total labor force |
| `NY.GDP.MKTP.CD` | GDP (current US$) |
| `SL.TLF.TOTL.IN` | Total labor force |
| `SE.TER.ENRR` | Tertiary education enrollment |
| `SL.EMP.TOTL.SP.NE.ZS` | Employment-to-population ratio |

**When to use:** any country macro indicator beyond the ILOSTAT default (GDP, education, demographics).
**Sample chatbot question:** *"What's GDP per capita in Brazil?"* / *"Show labor force size in Indonesia 2020-2024."*
**Example call:**
```python
from recruitment_apis import lookup_country_indicator_worldbank
result = lookup_country_indicator_worldbank("DEU", "SL.UEM.TOTL.ZS")
```
**Error modes:**
```python
{"error": "country_iso3 must be a 3-letter code", "source": "World Bank"}
{"error": "indicator must be non-empty", "source": "World Bank"}
{"error": "<network exc>", "source": "World Bank"}
```

---

## 7. `lookup_layoffs_warntracker`

**Signature** (`recruitment_apis.py:547`):
```python
def lookup_layoffs_warntracker(
    state: str = "", since_year: int = 2026, timeout: int = 15
) -> dict
```

**Returns** (always; never errors):
```python
{
    "layoffs": [],
    "source": "WARNTracker",
    "note": "Live scraping not implemented. Use https://www.warntracker.com/?year=2026&state=CA",
    "url": "https://www.warntracker.com/?year=2026&state=CA"
}
```

**Free tier limits:** N/A — URL stub. WARNTracker.com has no documented JSON API.
**When to use:** when the chatbot needs to cite a layoff source for the user but cannot fetch live data. Returned URL is stable and clickable.
**Sample chatbot question:** *"Where can I see California layoffs in 2026?"* — chatbot returns the WARNTracker URL.
**Example call:**
```python
from recruitment_apis import lookup_layoffs_warntracker
result = lookup_layoffs_warntracker(state="CA", since_year=2026)
```
**Error modes:** none — function never raises and never returns an error key. Always returns the URL stub.

---

## 8. `lookup_tech_jobs_hnhiring`

**Signature** (`recruitment_apis.py:578`):
```python
def lookup_tech_jobs_hnhiring(query: str, limit: int = 10, timeout: int = 10) -> dict
```

**Filter rule:** only returns hits where `story_text` or `comment_text` contains "hiring" (case-insensitive).

**Returns** (success):
```python
{
    "jobs": [
        {
            "title": "Ask HN: Who is hiring? (May 2026)",
            "url": "https://news.ycombinator.com/item?id=...",
            "comments_url": "https://news.ycombinator.com/item?id=...",
            "created_at": "2026-05-01T16:00:00.000Z"
        },
        # up to limit (capped at 50)
    ],
    "source": "HN Algolia",
    "count": 1
}
```

**Free tier limits:** unlimited; Algolia public API. `limit` is clamped to `[1, 50]`.
**When to use:** real-time pulse on tech hiring sentiment, surfacing companies actively recruiting in a niche (e.g., "rust embedded remote").
**Sample chatbot question:** *"What companies are hiring Python remote on HN this month?"*
**Example call:**
```python
from recruitment_apis import lookup_tech_jobs_hnhiring
result = lookup_tech_jobs_hnhiring("python remote senior", limit=20)
```
**Error modes:**
```python
{"error": "query must be non-empty", "source": "HN Algolia"}
{"error": "<network exc>", "source": "HN Algolia"}
```

---

## 9. `lookup_compensation_levels`

**Signature** (`recruitment_apis.py:635`):
```python
def lookup_compensation_levels(role: str, location: str = "", timeout: int = 10) -> dict
```

**Returns** (always; never errors):
```python
{
    "role": "Software Engineer",
    "location": "San Francisco",
    "embed_url": "https://www.levels.fyi/comp.html?title=Software+Engineer&location=San+Francisco",
    "source": "Levels.fyi",
    "note": "Public embed available; programmatic API requires application at levels.fyi/api-access"
}
```

**Free tier limits:** N/A — embed URL stub. Levels.fyi has no public JSON API.
**When to use:** when the chatbot needs to cite a compensation source but cannot fetch live data. URL is parameterized for the requested role + location.
**Sample chatbot question:** *"What's a software engineer paid in Seattle?"* — chatbot returns the Levels.fyi embed URL.
**Example call:**
```python
from recruitment_apis import lookup_compensation_levels
result = lookup_compensation_levels("Software Engineer", location="San Francisco")
```
**Error modes:** none — function never raises and never returns an error key. Always returns the URL stub.

---

## 10. `lookup_company_crunchbase`

**Signature** (`recruitment_apis.py:667`):
```python
def lookup_company_crunchbase(name: str, timeout: int = 10) -> dict
```

**Returns** (when `CRUNCHBASE_API_KEY` is unset — graceful stub):
```python
{
    "company": "Acme Corp",
    "source": "Crunchbase",
    "note": "CRUNCHBASE_API_KEY not set; sign up at crunchbase.com/api"
}
```

**Returns** (when key set, success):
```python
{
    "results": [
        {
            "uuid": "abc-123-...",
            "name": "Acme Corp",
            "permalink": "acme-corp",
            "short_description": "...",
            "website": "https://acme.com",
            "funding_total": {"value": 50_000_000, "currency": "USD"},
            "layoffs_count": 0
        },
        # up to 5
    ],
    "source": "Crunchbase",
    "count": 1
}
```

**Free tier limits:** Crunchbase v4 is paid only. Set `CRUNCHBASE_API_KEY` after signing up at crunchbase.com/api.
**When to use:** company profiling for ABM-style recruitment campaigns, layoff signal detection.
**Sample chatbot question:** *"What's Stripe's funding total?"* / *"Has Klarna had layoffs?"*
**Example call:**
```python
from recruitment_apis import lookup_company_crunchbase
result = lookup_company_crunchbase("Stripe")
```
**Error modes:**
```python
{"error": "name must be non-empty", "source": "Crunchbase"}
{"error": "<network exc>", "source": "Crunchbase"}
# When key absent, returns stub instead of error -- check for "note" key.
```

---

## 11. `enrich_person_pdl`

**Signature** (`recruitment_apis.py:744`):
```python
def enrich_person_pdl(linkedin_or_email: str, timeout: int = 10) -> dict
```

**Input rules:** input containing `@` is treated as email; otherwise as a LinkedIn URL or handle.

**Returns** (when `PDL_API_KEY` is unset — graceful stub):
```python
{
    "person": "jane@example.com",
    "source": "PeopleDataLabs",
    "note": "PDL_API_KEY not set; sign up free at peopledatalabs.com (100 lookups/mo)"
}
```

**Returns** (when key set, success):
```python
{
    "person": {
        "full_name": "Jane Smith",
        "job_title": "Senior Software Engineer",
        "job_company_name": "Acme Corp",
        "linkedin_url": "linkedin.com/in/janesmith",
        "skills": ["python", "kubernetes", "..."]
    },
    "source": "PeopleDataLabs"
}
```

**Free tier limits:** 100 lookups/month with `PDL_API_KEY` set. Beyond that the API returns 429 / payment-required (surfaced as `{"error": ..., "source": "PeopleDataLabs"}`).
**When to use:** enriching inbound candidate emails with LinkedIn / job-title / skill data; ABM signal building.
**Sample chatbot question:** *"What does this person do? jane@example.com"*
**Example call:**
```python
from recruitment_apis import enrich_person_pdl
result = enrich_person_pdl("https://linkedin.com/in/janesmith")
```
**Error modes:**
```python
{"error": "linkedin_or_email must be non-empty", "source": "PeopleDataLabs"}
{"error": "<network exc>", "source": "PeopleDataLabs"}
# When key absent, returns stub instead of error -- check for "note" key.
```

---

## Wiring into Nova chatbot

`chatbot_tools_recruitment.py` exports two symbols. The parent agent wires them as follows:

```python
from chatbot_tools_recruitment import (
    RECRUITMENT_TOOLS_SCHEMA,   # list[dict] -- Anthropic tool-use schemas
    RECRUITMENT_TOOL_DISPATCH,  # dict[name, callable]
)

# 1. Append the schemas to the chatbot tool list passed to Anthropic.
all_tools = existing_tools + RECRUITMENT_TOOLS_SCHEMA

# 2. At tool-use time, look up the function and invoke it with the
#    Claude-supplied arguments dict.
def execute_tool(name: str, args: dict) -> dict:
    fn = RECRUITMENT_TOOL_DISPATCH[name]
    return fn(**args)
```

A sanity-check `assert` at the bottom of `chatbot_tools_recruitment.py:344` enforces that the schema names and dispatch keys match exactly. Any future addition must update both.

---

## Common SSL fallback

`recruitment_apis._http_get_json()` retries once with an unverified SSL context on `CERTIFICATE_VERIFY_FAILED` errors. Some `.gov` endpoints (notably FMCSA) ship incomplete certificate chains. The retry is logged at `WARNING` and gated to SSL-specific errors only — non-SSL `URLError` instances re-raise normally.
