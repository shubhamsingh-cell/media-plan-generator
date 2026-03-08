"""
Research Data Module - Provides real market data for media plan generation.
Contains curated knowledge base of US labor market data, universities,
radio stations, competitors, and career events by location and industry.
"""

import re
import json
import os

# ── Canonical taxonomy standardizer ──
# Used to normalize country/location lookups via a single source of truth.
try:
    from standardizer import (
        normalize_location as _std_normalize_location,
        COUNTRY_MAP as _STD_COUNTRY_MAP,
        US_STATE_MAP as _STD_US_STATE_MAP,
    )
    _HAS_STANDARDIZER = True
except ImportError:
    _HAS_STANDARDIZER = False

# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL COUNTRY DATA - 40+ Countries
# ═══════════════════════════════════════════════════════════════════════════════

COUNTRY_DATA = {
    "United States": {"coli": 100, "median_salary": 65000, "population": "333M", "unemployment": "3.7%", "currency": "USD", "top_industries": "Technology, Healthcare, Finance, Manufacturing", "top_boards": "Indeed, LinkedIn, ZipRecruiter, Glassdoor, CareerBuilder", "region": "North America"},
    "United Kingdom": {"coli": 105, "median_salary": 42000, "population": "67M", "unemployment": "4.2%", "currency": "GBP", "top_industries": "Financial Services, Healthcare (NHS), Technology, Manufacturing", "top_boards": "Reed.co.uk, Indeed UK, Totaljobs, CV Library, Guardian Jobs", "region": "Europe"},
    "Germany": {"coli": 95, "median_salary": 50000, "population": "84M", "unemployment": "3.2%", "currency": "EUR", "top_industries": "Automotive, Engineering, Pharma, Technology", "top_boards": "StepStone, Xing, Indeed DE, Arbeitsagentur, Kimeta", "region": "Europe"},
    "France": {"coli": 96, "median_salary": 40000, "population": "68M", "unemployment": "7.2%", "currency": "EUR", "top_industries": "Aerospace, Luxury, Technology, Tourism", "top_boards": "Pôle Emploi, Indeed FR, Monster FR, APEC, Welcome to the Jungle", "region": "Europe"},
    "Netherlands": {"coli": 102, "median_salary": 48000, "population": "18M", "unemployment": "3.6%", "currency": "EUR", "top_industries": "Technology, Logistics, Agriculture, Finance", "top_boards": "Nationale Vacaturebank, Indeed NL, Werkzoeken, werk.nl", "region": "Europe"},
    "Japan": {"coli": 97, "median_salary": 38000, "population": "125M", "unemployment": "2.6%", "currency": "JPY", "top_industries": "Automotive, Electronics, Manufacturing, Technology", "top_boards": "Mynavi, DODA, Baitoru, CareerCross, Wantedly", "region": "APAC"},
    "Italy": {"coli": 88, "median_salary": 35000, "population": "59M", "unemployment": "7.8%", "currency": "EUR", "top_industries": "Fashion, Automotive, Food, Tourism, Manufacturing", "top_boards": "InfoJobs, Indeed IT, LinkedIn, Trovolavoro, Bakeca.it", "region": "Europe"},
    "India": {"coli": 25, "median_salary": 8500, "population": "1.4B", "unemployment": "7.5%", "currency": "INR", "top_industries": "IT/Technology, Manufacturing, Services, Pharma", "top_boards": "Naukri.com, Indeed India, LinkedIn, Shine, TimesJobs", "region": "APAC"},
    "Australia": {"coli": 107, "median_salary": 55000, "population": "26M", "unemployment": "3.7%", "currency": "AUD", "top_industries": "Mining, Healthcare, Technology, Finance", "top_boards": "SEEK, Indeed AU, Jora, CareerOne, Adzuna", "region": "APAC"},
    "Canada": {"coli": 98, "median_salary": 52000, "population": "40M", "unemployment": "5.4%", "currency": "CAD", "top_industries": "Technology, Finance, Energy, Healthcare", "top_boards": "Indeed CA, LinkedIn, Job Bank, Workopolis, CareerBeacon", "region": "North America"},
    "Ireland": {"coli": 108, "median_salary": 48000, "population": "5.1M", "unemployment": "4.3%", "currency": "EUR", "top_industries": "Technology, Pharma, Financial Services, MedTech", "top_boards": "IrishJobs.ie, Jobs.ie, Indeed IE, RecruitIreland", "region": "Europe"},
    "South Africa": {"coli": 38, "median_salary": 12000, "population": "60M", "unemployment": "32.1%", "currency": "ZAR", "top_industries": "Mining, Financial Services, Manufacturing, Tourism", "top_boards": "CareerJunction, Pnet, Indeed ZA, Careers24", "region": "MEA"},
    "Kenya": {"coli": 32, "median_salary": 6000, "population": "54M", "unemployment": "5.7%", "currency": "KES", "top_industries": "Agriculture, Technology, Tourism, Finance", "top_boards": "BrighterMonday, MyJobMag, Fuzu, KenyaJobs.com", "region": "MEA"},
    "Argentina": {"coli": 35, "median_salary": 8000, "population": "46M", "unemployment": "6.3%", "currency": "ARS", "top_industries": "Agriculture, Manufacturing, Technology, Mining", "top_boards": "Bumeran, ZonaJobs, CompuTrabajo, Indeed AR", "region": "LATAM"},
    "Brazil": {"coli": 42, "median_salary": 10000, "population": "214M", "unemployment": "7.9%", "currency": "BRL", "top_industries": "Agriculture, Manufacturing, Mining, Technology", "top_boards": "Vagas.com.br, Catho, Indeed BR, InfoJobs, Trabalha Brasil", "region": "LATAM"},
    "Singapore": {"coli": 115, "median_salary": 52000, "population": "5.9M", "unemployment": "2.1%", "currency": "SGD", "top_industries": "Finance, Technology, Logistics, Biotech", "top_boards": "JobStreet SG, Indeed SG, Jobsdb, MyCareersFuture", "region": "APAC"},
    "Spain": {"coli": 82, "median_salary": 33000, "population": "47M", "unemployment": "11.7%", "currency": "EUR", "top_industries": "Tourism, Automotive, Renewable Energy, Technology", "top_boards": "InfoJobs, Indeed ES, Talent.com, CornerJob", "region": "Europe"},
    "Mexico": {"coli": 38, "median_salary": 9500, "population": "130M", "unemployment": "2.8%", "currency": "MXN", "top_industries": "Manufacturing, Automotive, Oil & Gas, Tourism", "top_boards": "OCC Mundial, CompuTrabajo, Indeed MX, Talenteca", "region": "LATAM"},
    "Poland": {"coli": 55, "median_salary": 22000, "population": "38M", "unemployment": "2.9%", "currency": "PLN", "top_industries": "Manufacturing, Technology, Automotive, BPO", "top_boards": "Pracuj.pl, Indeed PL, OLX.pl, No Fluff Jobs, Just Join IT", "region": "Europe"},
    "Switzerland": {"coli": 155, "median_salary": 82000, "population": "8.8M", "unemployment": "2.0%", "currency": "CHF", "top_industries": "Finance, Pharma, Manufacturing, Technology", "top_boards": "Jobs.ch, Indeed CH, JobScout24, Topjobs.ch", "region": "Europe"},
    "UAE": {"coli": 80, "median_salary": 35000, "population": "10M", "unemployment": "2.9%", "currency": "AED", "top_industries": "Oil & Gas, Finance, Tourism, Real Estate, Technology", "top_boards": "Bayt.com, GulfTalent, Indeed AE, dubizzle", "region": "MEA"},
    "Hong Kong": {"coli": 115, "median_salary": 45000, "population": "7.5M", "unemployment": "2.9%", "currency": "HKD", "top_industries": "Finance, Trade, Technology, Professional Services", "top_boards": "JobsDB HK, Indeed HK, LinkedIn, CTgoodjobs", "region": "APAC"},
    "Belgium": {"coli": 102, "median_salary": 48000, "population": "11.6M", "unemployment": "5.5%", "currency": "EUR", "top_industries": "Pharma, Chemical, Logistics, Technology", "top_boards": "StepStone BE, Indeed BE, Le Forem, Actiris", "region": "Europe"},
    "Sweden": {"coli": 107, "median_salary": 45000, "population": "10.5M", "unemployment": "7.5%", "currency": "SEK", "top_industries": "Technology, Automotive, Engineering, Clean Energy", "top_boards": "Platsbanken, Indeed SE, Blocket Jobb, Jobbsafari", "region": "Europe"},
    "Denmark": {"coli": 120, "median_salary": 55000, "population": "5.9M", "unemployment": "2.7%", "currency": "DKK", "top_industries": "Pharma, Clean Energy, Shipping, Technology", "top_boards": "JobIndex, Indeed DK, Jobnet, StepStone DK", "region": "Europe"},
    "Czech Republic": {"coli": 58, "median_salary": 22000, "population": "10.8M", "unemployment": "2.5%", "currency": "CZK", "top_industries": "Automotive, Manufacturing, Technology, Tourism", "top_boards": "JOBS.CZ, PRACE.CZ, Profesia.cz, No Fluff Jobs", "region": "Europe"},
    "Philippines": {"coli": 30, "median_salary": 5500, "population": "115M", "unemployment": "4.3%", "currency": "PHP", "top_industries": "BPO, Electronics, Agriculture, Tourism", "top_boards": "JobStreet PH, Indeed PH, Kalibrr, Bossjob", "region": "APAC"},
    "Malaysia": {"coli": 38, "median_salary": 12000, "population": "33M", "unemployment": "3.4%", "currency": "MYR", "top_industries": "Electronics, Oil & Gas, Manufacturing, Technology", "top_boards": "JobStreet MY, Indeed MY, Hiredly, A Job Thing", "region": "APAC"},
    "New Zealand": {"coli": 105, "median_salary": 48000, "population": "5.1M", "unemployment": "3.4%", "currency": "NZD", "top_industries": "Agriculture, Tourism, Technology, Film", "top_boards": "SEEK NZ, Trade Me Jobs, Indeed NZ", "region": "APAC"},
    "Austria": {"coli": 105, "median_salary": 48000, "population": "9.1M", "unemployment": "5.0%", "currency": "EUR", "top_industries": "Manufacturing, Technology, Tourism, Energy", "top_boards": "StepStone AT, Karriere.at, Hokify, MeinJob.at", "region": "Europe"},
    "Portugal": {"coli": 72, "median_salary": 22000, "population": "10.4M", "unemployment": "6.5%", "currency": "EUR", "top_industries": "Tourism, Technology, Manufacturing, Agriculture", "top_boards": "Indeed PT, Sapo Emprego, Net Empregos, ITJobs.pt", "region": "Europe"},
    "Colombia": {"coli": 30, "median_salary": 7000, "population": "52M", "unemployment": "10.0%", "currency": "COP", "top_industries": "Oil & Gas, Mining, Agriculture, Technology", "top_boards": "Elempleo, CompuTrabajo, Indeed CO", "region": "LATAM"},
    "Chile": {"coli": 45, "median_salary": 12000, "population": "19M", "unemployment": "8.5%", "currency": "CLP", "top_industries": "Mining, Agriculture, Forestry, Technology", "top_boards": "Trabajando, Laborum, CompuTrabajo, Chiletrabajos", "region": "LATAM"},
    "Norway": {"coli": 130, "median_salary": 62000, "population": "5.5M", "unemployment": "1.8%", "currency": "NOK", "top_industries": "Oil & Gas, Maritime, Fisheries, Technology", "top_boards": "Finn.no, NAV.no, Arbeidsplassen, Indeed NO", "region": "Europe"},
    "Finland": {"coli": 108, "median_salary": 42000, "population": "5.5M", "unemployment": "7.2%", "currency": "EUR", "top_industries": "Technology, Forestry, Manufacturing, Clean Energy", "top_boards": "Duunitori, Indeed FI, Oikotie, Job Market Finland", "region": "Europe"},
    "South Korea": {"coli": 82, "median_salary": 32000, "population": "52M", "unemployment": "2.6%", "currency": "KRW", "top_industries": "Electronics, Automotive, Shipbuilding, Technology", "top_boards": "Saramin, JobKorea, LinkedIn, Indeed KR", "region": "APAC"},
    "Israel": {"coli": 110, "median_salary": 42000, "population": "9.8M", "unemployment": "3.5%", "currency": "ILS", "top_industries": "Technology, Defense, Pharma, Agriculture", "top_boards": "AllJobs.co.il, Drushim, LinkedIn, Indeed IL", "region": "MEA"},
    "China": {"coli": 55, "median_salary": 14000, "population": "1.4B", "unemployment": "5.2%", "currency": "CNY", "top_industries": "Technology, Manufacturing, E-commerce, Finance", "top_boards": "Zhaopin, 51job, Boss Zhipin, Lagou, Liepin", "region": "APAC"},
    "Indonesia": {"coli": 28, "median_salary": 4500, "population": "275M", "unemployment": "5.5%", "currency": "IDR", "top_industries": "Manufacturing, Mining, Agriculture, Technology", "top_boards": "JobStreet ID, Glints, Kalibrr, Indeed ID", "region": "APAC"},
    "Thailand": {"coli": 35, "median_salary": 7500, "population": "72M", "unemployment": "1.0%", "currency": "THB", "top_industries": "Tourism, Automotive, Electronics, Agriculture", "top_boards": "JobThai, JobsDB TH, Indeed TH, LinkedIn", "region": "APAC"},
    "Vietnam": {"coli": 28, "median_salary": 5000, "population": "99M", "unemployment": "2.0%", "currency": "VND", "top_industries": "Manufacturing, Technology, Agriculture, Tourism", "top_boards": "VietnamWorks, TopCV, CareerBuilder VN, Indeed VN", "region": "APAC"},
}

# Country name matching helpers
COUNTRY_ALIASES = {
    "uk": "United Kingdom", "england": "United Kingdom", "britain": "United Kingdom", "gb": "United Kingdom",
    "us": "United States", "usa": "United States", "america": "United States",
    "de": "Germany", "deutschland": "Germany",
    "fr": "France", "jp": "Japan", "it": "Italy", "in": "India",
    "au": "Australia", "ca": "Canada", "ie": "Ireland",
    "za": "South Africa", "ke": "Kenya", "ar": "Argentina", "br": "Brazil",
    "sg": "Singapore", "es": "Spain", "mx": "Mexico", "pl": "Poland",
    "ch": "Switzerland", "ae": "UAE", "uae": "UAE", "hk": "Hong Kong",
    "be": "Belgium", "se": "Sweden", "dk": "Denmark", "cz": "Czech Republic",
    "ph": "Philippines", "my": "Malaysia", "nz": "New Zealand", "at": "Austria",
    "pt": "Portugal", "co": "Colombia", "cl": "Chile", "no": "Norway",
    "fi": "Finland", "kr": "South Korea", "il": "Israel", "cn": "China",
    "id": "Indonesia", "th": "Thailand", "vn": "Vietnam",
}

def _detect_country(location_str):
    """Detect if a location string refers to a country (non-US)."""
    loc_lower = location_str.strip().lower()
    # US state abbreviations that conflict with country aliases
    _US_STATE_ABBREVS = {
        "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in",
        "ia","ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv",
        "nh","nj","nm","ny","nc","nd","oh","ok","or","pa","ri","sc","sd","tn",
        "tx","ut","vt","va","wa","wv","wi","wy","dc"
    }
    # Check if location looks like "City, STATE" (US format) before country detection
    parts = [p.strip() for p in loc_lower.split(",")]
    if len(parts) >= 2 and parts[-1] in _US_STATE_ABBREVS:
        return None  # This is a US location
    # Also check "City STATE" format (space-separated, no comma) — e.g. "Indianapolis IN"
    space_parts = loc_lower.split()
    if len(space_parts) >= 2 and space_parts[-1] in _US_STATE_ABBREVS:
        return None  # This is a US location like "Indianapolis IN"
    # Check if location matches a known US metro area — prevents "Indianapolis" matching "India"
    # Extract the city part (before comma or state abbreviation) for matching
    _city_part = loc_lower.split(",")[0].strip()
    # If location ends with a state abbreviation, strip it for city matching
    _city_words = _city_part.split()
    if len(_city_words) >= 2 and _city_words[-1] in _US_STATE_ABBREVS:
        _city_part = " ".join(_city_words[:-1])
    _city_norm = re.sub(r'[^a-z0-9]', '', _city_part)
    for _metro_key in METRO_DATA:
        _metro_norm = re.sub(r'[^a-z0-9]', '', _metro_key.lower())
        # Require exact normalized match (not substring) to avoid "india" matching "indianapolis"
        if _metro_norm == _city_norm:
            return None  # Known US city, not an international location
    # Direct country name match — require word-boundary match, not mere substring
    # This prevents "Indianapolis" from matching "India"
    for country in COUNTRY_DATA:
        country_lower = country.lower()
        if country_lower == loc_lower:
            return country
        # Use word boundary regex to prevent partial city name matches
        if re.search(r'\b' + re.escape(country_lower) + r'\b', loc_lower):
            return country
    # Alias match — also require word boundary or exact positional match
    for alias, country in COUNTRY_ALIASES.items():
        if alias == loc_lower or f", {alias}" in loc_lower or f"({alias})" in loc_lower:
            # Extra guard: if alias is a US state abbreviation, skip it
            if alias in _US_STATE_ABBREVS:
                continue
            return country
        # Check for alias as a trailing word (e.g. "SomeCity IN" should not match India via "in" alias)
        if loc_lower.endswith(f" {alias}") and alias not in _US_STATE_ABBREVS:
            return country
    # Check if location contains a known country city
    city_to_country = {
        "london": "United Kingdom", "manchester": "United Kingdom", "edinburgh": "United Kingdom", "birmingham uk": "United Kingdom",
        "berlin": "Germany", "munich": "Germany", "hamburg": "Germany", "frankfurt": "Germany",
        "paris": "France", "lyon": "France", "marseille": "France",
        "amsterdam": "Netherlands", "rotterdam": "Netherlands", "the hague": "Netherlands",
        "tokyo": "Japan", "osaka": "Japan", "nagoya": "Japan",
        "milan": "Italy", "rome": "Italy", "turin": "Italy",
        "mumbai": "India", "bangalore": "India", "delhi": "India", "hyderabad": "India",
        "sydney": "Australia", "melbourne": "Australia", "brisbane": "Australia",
        "toronto": "Canada", "vancouver": "Canada", "montreal": "Canada",
        "dublin": "Ireland", "cork": "Ireland", "galway": "Ireland",
        "johannesburg": "South Africa", "cape town": "South Africa",
        "nairobi": "Kenya", "buenos aires": "Argentina",
        "são paulo": "Brazil", "rio de janeiro": "Brazil", "sao paulo": "Brazil",
        "zurich": "Switzerland", "geneva": "Switzerland", "basel": "Switzerland",
        "dubai": "UAE", "abu dhabi": "UAE",
        "madrid": "Spain", "barcelona": "Spain",
        "mexico city": "Mexico", "monterrey": "Mexico",
        "warsaw": "Poland", "krakow": "Poland",
        "copenhagen": "Denmark", "prague": "Czech Republic",
        "stockholm": "Sweden", "oslo": "Norway", "helsinki": "Finland",
        "lisbon": "Portugal", "brussels": "Belgium", "vienna": "Austria",
        "bogotá": "Colombia", "bogota": "Colombia", "santiago": "Chile",
        "manila": "Philippines", "kuala lumpur": "Malaysia",
        "auckland": "New Zealand", "wellington": "New Zealand",
        "seoul": "South Korea", "tel aviv": "Israel",
        "shanghai": "China", "beijing": "China", "shenzhen": "China",
        "jakarta": "Indonesia", "bangkok": "Thailand", "ho chi minh": "Vietnam",
    }
    for city, country in city_to_country.items():
        if city in loc_lower:
            return country
    # --- Standardizer fallback: try canonical location normalization ---
    # The standardizer has a richer alias set (50+ countries) than the local
    # COUNTRY_ALIASES dict.  If the above didn't match, ask the standardizer.
    if _HAS_STANDARDIZER:
        try:
            std_result = _std_normalize_location(location_str)
            # normalize_location returns a dict with 'country' key
            std_country = std_result.get("country", "") if isinstance(std_result, dict) else ""
            if std_country and std_country in COUNTRY_DATA:
                return std_country
        except Exception:
            pass  # Non-fatal; fall through to None
    return None

_global_supply_cache = None


def _load_global_supply():
    """Load global supply data if available (cached after first successful load)."""
    global _global_supply_cache
    if _global_supply_cache is not None:
        return _global_supply_cache
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "global_supply.json")
    try:
        if os.path.exists(path):
            with open(path, "r") as f:
                _global_supply_cache = json.load(f)
                return _global_supply_cache
    except json.JSONDecodeError:
        pass  # Corrupt JSON -- fall through to empty dict
    except OSError:
        pass  # File read error -- fall through to empty dict
    return {}

# ═══════════════════════════════════════════════════════════════════════════════
# LOCATION DATA - All 50 US States
# ═══════════════════════════════════════════════════════════════════════════════

STATE_DATA = {
    "AL": {"name": "Alabama", "coli": 89, "median_household_income": 56200, "population": "5.1M", "unemployment": "2.8%", "labor_force": "2.3M", "top_industries": "Aerospace, Automotive, Healthcare"},
    "AK": {"name": "Alaska", "coli": 127, "median_household_income": 77800, "population": "733K", "unemployment": "4.4%", "labor_force": "350K", "top_industries": "Oil & Gas, Fishing, Military"},
    "AZ": {"name": "Arizona", "coli": 103, "median_household_income": 65900, "population": "7.4M", "unemployment": "3.5%", "labor_force": "3.5M", "top_industries": "Technology, Healthcare, Manufacturing"},
    "AR": {"name": "Arkansas", "coli": 86, "median_household_income": 52100, "population": "3.0M", "unemployment": "3.3%", "labor_force": "1.4M", "top_industries": "Agriculture, Retail, Healthcare"},
    "CA": {"name": "California", "coli": 142, "median_household_income": 84900, "population": "39.0M", "unemployment": "4.8%", "labor_force": "19.2M", "top_industries": "Technology, Entertainment, Agriculture"},
    "CO": {"name": "Colorado", "coli": 105, "median_household_income": 82250, "population": "5.8M", "unemployment": "3.4%", "labor_force": "3.2M", "top_industries": "Technology, Aerospace, Energy"},
    "CT": {"name": "Connecticut", "coli": 111, "median_household_income": 83800, "population": "3.6M", "unemployment": "3.8%", "labor_force": "1.9M", "top_industries": "Financial Services, Insurance, Healthcare"},
    "DE": {"name": "Delaware", "coli": 102, "median_household_income": 72700, "population": "1.0M", "unemployment": "3.7%", "labor_force": "490K", "top_industries": "Financial Services, Chemical, Healthcare"},
    "FL": {"name": "Florida", "coli": 103, "median_household_income": 63100, "population": "22.6M", "unemployment": "3.2%", "labor_force": "10.8M", "top_industries": "Tourism, Healthcare, Aerospace"},
    "GA": {"name": "Georgia", "coli": 93, "median_household_income": 65000, "population": "10.9M", "unemployment": "3.3%", "labor_force": "5.2M", "top_industries": "Logistics, Film, Technology"},
    "HI": {"name": "Hawaii", "coli": 192, "median_household_income": 84600, "population": "1.4M", "unemployment": "3.0%", "labor_force": "680K", "top_industries": "Tourism, Military, Agriculture"},
    "ID": {"name": "Idaho", "coli": 97, "median_household_income": 64900, "population": "1.9M", "unemployment": "2.9%", "labor_force": "950K", "top_industries": "Agriculture, Technology, Manufacturing"},
    "IL": {"name": "Illinois", "coli": 95, "median_household_income": 72200, "population": "12.5M", "unemployment": "4.4%", "labor_force": "6.3M", "top_industries": "Financial Services, Manufacturing, Healthcare"},
    "IN": {"name": "Indiana", "coli": 90, "median_household_income": 61900, "population": "6.8M", "unemployment": "3.2%", "labor_force": "3.3M", "top_industries": "Manufacturing, Agriculture, Healthcare"},
    "IA": {"name": "Iowa", "coli": 90, "median_household_income": 65600, "population": "3.2M", "unemployment": "2.7%", "labor_force": "1.7M", "top_industries": "Agriculture, Manufacturing, Financial Services"},
    "KS": {"name": "Kansas", "coli": 90, "median_household_income": 64500, "population": "2.9M", "unemployment": "2.9%", "labor_force": "1.5M", "top_industries": "Agriculture, Aviation, Energy"},
    "KY": {"name": "Kentucky", "coli": 90, "median_household_income": 55600, "population": "4.5M", "unemployment": "3.9%", "labor_force": "2.1M", "top_industries": "Manufacturing, Healthcare, Logistics"},
    "LA": {"name": "Louisiana", "coli": 92, "median_household_income": 52800, "population": "4.6M", "unemployment": "3.7%", "labor_force": "2.1M", "top_industries": "Oil & Gas, Maritime, Healthcare"},
    "ME": {"name": "Maine", "coli": 112, "median_household_income": 64700, "population": "1.4M", "unemployment": "3.0%", "labor_force": "700K", "top_industries": "Healthcare, Tourism, Manufacturing"},
    "MD": {"name": "Maryland", "coli": 115, "median_household_income": 90200, "population": "6.2M", "unemployment": "2.8%", "labor_force": "3.2M", "top_industries": "Government, Healthcare, Cybersecurity"},
    "MA": {"name": "Massachusetts", "coli": 135, "median_household_income": 89700, "population": "7.0M", "unemployment": "3.4%", "labor_force": "3.8M", "top_industries": "Biotech, Technology, Education"},
    "MI": {"name": "Michigan", "coli": 91, "median_household_income": 63400, "population": "10.0M", "unemployment": "4.1%", "labor_force": "4.9M", "top_industries": "Automotive, Manufacturing, Healthcare"},
    "MN": {"name": "Minnesota", "coli": 98, "median_household_income": 77700, "population": "5.7M", "unemployment": "2.8%", "labor_force": "3.1M", "top_industries": "Healthcare, Financial Services, Manufacturing"},
    "MS": {"name": "Mississippi", "coli": 84, "median_household_income": 46500, "population": "2.9M", "unemployment": "3.7%", "labor_force": "1.3M", "top_industries": "Manufacturing, Agriculture, Military"},
    "MO": {"name": "Missouri", "coli": 89, "median_household_income": 61000, "population": "6.2M", "unemployment": "3.2%", "labor_force": "3.1M", "top_industries": "Healthcare, Manufacturing, Agriculture"},
    "MT": {"name": "Montana", "coli": 97, "median_household_income": 60600, "population": "1.1M", "unemployment": "2.7%", "labor_force": "550K", "top_industries": "Agriculture, Mining, Tourism"},
    "NE": {"name": "Nebraska", "coli": 92, "median_household_income": 66600, "population": "2.0M", "unemployment": "2.2%", "labor_force": "1.1M", "top_industries": "Agriculture, Financial Services, Manufacturing"},
    "NV": {"name": "Nevada", "coli": 104, "median_household_income": 63300, "population": "3.2M", "unemployment": "5.2%", "labor_force": "1.6M", "top_industries": "Tourism, Mining, Logistics"},
    "NH": {"name": "New Hampshire", "coli": 113, "median_household_income": 88200, "population": "1.4M", "unemployment": "2.4%", "labor_force": "780K", "top_industries": "Manufacturing, Technology, Tourism"},
    "NJ": {"name": "New Jersey", "coli": 118, "median_household_income": 87700, "population": "9.3M", "unemployment": "4.1%", "labor_force": "4.6M", "top_industries": "Pharma, Financial Services, Technology"},
    "NM": {"name": "New Mexico", "coli": 93, "median_household_income": 53600, "population": "2.1M", "unemployment": "3.8%", "labor_force": "950K", "top_industries": "Government, Energy, Military"},
    "NY": {"name": "New York", "coli": 126, "median_household_income": 74300, "population": "19.5M", "unemployment": "4.3%", "labor_force": "9.5M", "top_industries": "Financial Services, Technology, Healthcare"},
    "NC": {"name": "North Carolina", "coli": 96, "median_household_income": 62900, "population": "10.7M", "unemployment": "3.5%", "labor_force": "5.2M", "top_industries": "Technology, Banking, Manufacturing"},
    "ND": {"name": "North Dakota", "coli": 93, "median_household_income": 68000, "population": "780K", "unemployment": "2.0%", "labor_force": "420K", "top_industries": "Energy, Agriculture, Military"},
    "OH": {"name": "Ohio", "coli": 91, "median_household_income": 60000, "population": "11.8M", "unemployment": "3.8%", "labor_force": "5.7M", "top_industries": "Manufacturing, Healthcare, Logistics"},
    "OK": {"name": "Oklahoma", "coli": 87, "median_household_income": 55800, "population": "4.0M", "unemployment": "3.0%", "labor_force": "1.9M", "top_industries": "Energy, Aerospace, Agriculture"},
    "OR": {"name": "Oregon", "coli": 113, "median_household_income": 71300, "population": "4.2M", "unemployment": "3.7%", "labor_force": "2.1M", "top_industries": "Technology, Forestry, Manufacturing"},
    "PA": {"name": "Pennsylvania", "coli": 97, "median_household_income": 67600, "population": "13.0M", "unemployment": "3.5%", "labor_force": "6.4M", "top_industries": "Healthcare, Education, Manufacturing"},
    "RI": {"name": "Rhode Island", "coli": 107, "median_household_income": 71200, "population": "1.1M", "unemployment": "3.2%", "labor_force": "560K", "top_industries": "Healthcare, Education, Manufacturing"},
    "SC": {"name": "South Carolina", "coli": 94, "median_household_income": 58200, "population": "5.3M", "unemployment": "3.3%", "labor_force": "2.4M", "top_industries": "Manufacturing, Military, Tourism"},
    "SD": {"name": "South Dakota", "coli": 93, "median_household_income": 63800, "population": "900K", "unemployment": "2.0%", "labor_force": "480K", "top_industries": "Agriculture, Financial Services, Healthcare"},
    "TN": {"name": "Tennessee", "coli": 91, "median_household_income": 59700, "population": "7.1M", "unemployment": "3.2%", "labor_force": "3.3M", "top_industries": "Healthcare, Automotive, Music/Entertainment"},
    "TX": {"name": "Texas", "coli": 93, "median_household_income": 67300, "population": "30.5M", "unemployment": "4.0%", "labor_force": "14.6M", "top_industries": "Energy, Technology, Healthcare"},
    "UT": {"name": "Utah", "coli": 103, "median_household_income": 77600, "population": "3.4M", "unemployment": "2.5%", "labor_force": "1.7M", "top_industries": "Technology, Healthcare, Outdoor Recreation"},
    "VT": {"name": "Vermont", "coli": 112, "median_household_income": 65200, "population": "647K", "unemployment": "2.3%", "labor_force": "340K", "top_industries": "Tourism, Agriculture, Manufacturing"},
    "VA": {"name": "Virginia", "coli": 104, "median_household_income": 80600, "population": "8.6M", "unemployment": "2.8%", "labor_force": "4.4M", "top_industries": "Government, Technology, Military"},
    "WA": {"name": "Washington", "coli": 115, "median_household_income": 84200, "population": "7.8M", "unemployment": "3.7%", "labor_force": "3.9M", "top_industries": "Technology, Aerospace, Agriculture"},
    "WV": {"name": "West Virginia", "coli": 84, "median_household_income": 48000, "population": "1.8M", "unemployment": "4.0%", "labor_force": "770K", "top_industries": "Mining, Healthcare, Government"},
    "WI": {"name": "Wisconsin", "coli": 93, "median_household_income": 67100, "population": "5.9M", "unemployment": "2.9%", "labor_force": "3.1M", "top_industries": "Manufacturing, Agriculture, Healthcare"},
    "WY": {"name": "Wyoming", "coli": 96, "median_household_income": 65000, "population": "577K", "unemployment": "3.1%", "labor_force": "290K", "top_industries": "Mining, Tourism, Agriculture"},
    "DC": {"name": "District of Columbia", "coli": 152, "median_household_income": 101700, "population": "690K", "unemployment": "4.7%", "labor_force": "410K", "top_industries": "Government, Technology, Healthcare"},
}

# ═══════════════════════════════════════════════════════════════════════════════
# METRO AREA DATA - Top 60+ Metros with detailed info
# ═══════════════════════════════════════════════════════════════════════════════

METRO_DATA = {
    # Oregon
    "portland": {"state": "OR", "coli": 130, "population": "2.5M metro", "median_salary": 72000, "unemployment": "3.5%", "metro_name": "Portland-Vancouver-Hillsboro MSA", "major_employers": "Nike, Intel, OHSU, Providence Health"},
    "eugene": {"state": "OR", "coli": 110, "population": "382K metro", "median_salary": 55000, "unemployment": "4.2%", "metro_name": "Eugene-Springfield MSA", "major_employers": "University of Oregon, PeaceHealth, Hynix"},
    "salem": {"state": "OR", "coli": 105, "population": "433K metro", "median_salary": 52000, "unemployment": "4.0%", "metro_name": "Salem MSA", "major_employers": "State of Oregon, Salem Health, NORPAC Foods"},
    "bend": {"state": "OR", "coli": 122, "population": "200K metro", "median_salary": 60000, "unemployment": "3.8%", "metro_name": "Bend-Redmond MSA", "major_employers": "St. Charles Health, Les Schwab, IBEX"},
    "medford": {"state": "OR", "coli": 103, "population": "223K metro", "median_salary": 48000, "unemployment": "5.0%", "metro_name": "Medford MSA", "major_employers": "Asante Health, Harry & David, Lithia Motors"},
    "roseburg": {"state": "OR", "coli": 98, "population": "110K metro", "median_salary": 44000, "unemployment": "5.5%", "metro_name": "Roseburg MSA", "major_employers": "Mercy Medical, Roseburg Forest Products, VA Roseburg"},
    # Minnesota
    "minneapolis": {"state": "MN", "coli": 107, "population": "3.7M metro", "median_salary": 78000, "unemployment": "2.6%", "metro_name": "Minneapolis-St. Paul-Bloomington MSA", "major_employers": "UnitedHealth Group, Target, 3M, Mayo Clinic, Medtronic"},
    "duluth": {"state": "MN", "coli": 95, "population": "293K metro", "median_salary": 55000, "unemployment": "3.5%", "metro_name": "Duluth MSA", "major_employers": "Essentia Health, St. Luke's, University of MN Duluth"},
    "rochester_mn": {"state": "MN", "coli": 100, "population": "226K metro", "median_salary": 72000, "unemployment": "2.0%", "metro_name": "Rochester MSA", "major_employers": "Mayo Clinic, IBM, Olmsted Medical Center"},
    "st_cloud": {"state": "MN", "coli": 93, "population": "200K metro", "median_salary": 52000, "unemployment": "3.0%", "metro_name": "St. Cloud MSA", "major_employers": "CentraCare Health, St. Cloud State University, Stearns County"},
    # California
    "san_francisco": {"state": "CA", "coli": 180, "population": "4.7M metro", "median_salary": 95000, "unemployment": "3.9%", "metro_name": "San Francisco-Oakland-Berkeley MSA", "major_employers": "Salesforce, Google, Meta, UCSF, Kaiser Permanente"},
    "los_angeles": {"state": "CA", "coli": 166, "population": "13.2M metro", "median_salary": 70000, "unemployment": "5.1%", "metro_name": "Los Angeles-Long Beach-Anaheim MSA", "major_employers": "Cedars-Sinai, Disney, SpaceX, Boeing, UCLA"},
    "san_diego": {"state": "CA", "coli": 155, "population": "3.3M metro", "median_salary": 72000, "unemployment": "3.7%", "metro_name": "San Diego-Chula Vista-Carlsbad MSA", "major_employers": "US Navy, Qualcomm, UC San Diego, Sharp HealthCare"},
    "sacramento": {"state": "CA", "coli": 120, "population": "2.4M metro", "median_salary": 65000, "unemployment": "4.2%", "metro_name": "Sacramento-Roseville-Folsom MSA", "major_employers": "State of California, UC Davis, Sutter Health, Intel"},
    # Texas
    "houston": {"state": "TX", "coli": 96, "population": "7.3M metro", "median_salary": 65000, "unemployment": "4.5%", "metro_name": "Houston-The Woodlands-Sugar Land MSA", "major_employers": "Houston Methodist, ExxonMobil, NASA, MD Anderson"},
    "dallas": {"state": "TX", "coli": 103, "population": "7.9M metro", "median_salary": 67000, "unemployment": "3.6%", "metro_name": "Dallas-Fort Worth-Arlington MSA", "major_employers": "AT&T, Southwest Airlines, Texas Instruments, Baylor Scott & White"},
    "austin": {"state": "TX", "coli": 107, "population": "2.4M metro", "median_salary": 72000, "unemployment": "3.1%", "metro_name": "Austin-Round Rock-Georgetown MSA", "major_employers": "Tesla, Dell, Apple, Samsung, UT Austin"},
    "san_antonio": {"state": "TX", "coli": 92, "population": "2.6M metro", "median_salary": 56000, "unemployment": "3.8%", "metro_name": "San Antonio-New Braunfels MSA", "major_employers": "USAA, Joint Base San Antonio, HEB, Methodist Healthcare"},
    # New York
    "new_york": {"state": "NY", "coli": 187, "population": "20.1M metro", "median_salary": 76000, "unemployment": "4.4%", "metro_name": "New York-Newark-Jersey City MSA", "major_employers": "JPMorgan Chase, Goldman Sachs, NYU Langone, NYC Health + Hospitals"},
    "buffalo": {"state": "NY", "coli": 92, "population": "1.2M metro", "median_salary": 58000, "unemployment": "4.0%", "metro_name": "Buffalo-Cheektowaga MSA", "major_employers": "Kaleida Health, M&T Bank, University at Buffalo"},
    "albany": {"state": "NY", "coli": 96, "population": "880K metro", "median_salary": 62000, "unemployment": "3.3%", "metro_name": "Albany-Schenectady-Troy MSA", "major_employers": "State of New York, Albany Medical Center, GE Research"},
    # Others
    "seattle": {"state": "WA", "coli": 157, "population": "4.0M metro", "median_salary": 88000, "unemployment": "3.5%", "metro_name": "Seattle-Tacoma-Bellevue MSA", "major_employers": "Amazon, Microsoft, Boeing, UW Medicine, Providence"},
    "boston": {"state": "MA", "coli": 148, "population": "4.9M metro", "median_salary": 85000, "unemployment": "3.2%", "metro_name": "Boston-Cambridge-Newton MSA", "major_employers": "Mass General Brigham, Harvard, MIT, Raytheon, Fidelity"},
    "chicago": {"state": "IL", "coli": 107, "population": "9.4M metro", "median_salary": 68000, "unemployment": "4.5%", "metro_name": "Chicago-Naperville-Elgin MSA", "major_employers": "Abbott, Boeing, Walgreens, Northwestern Medicine, United Airlines"},
    "denver": {"state": "CO", "coli": 128, "population": "2.9M metro", "median_salary": 76000, "unemployment": "3.2%", "metro_name": "Denver-Aurora-Lakewood MSA", "major_employers": "Lockheed Martin, UCHealth, DaVita, Arrow Electronics"},
    "phoenix": {"state": "AZ", "coli": 103, "population": "4.9M metro", "median_salary": 62000, "unemployment": "3.4%", "metro_name": "Phoenix-Mesa-Chandler MSA", "major_employers": "Banner Health, Intel, Raytheon, Arizona State University"},
    "atlanta": {"state": "GA", "coli": 107, "population": "6.2M metro", "median_salary": 66000, "unemployment": "3.1%", "metro_name": "Atlanta-Sandy Springs-Alpharetta MSA", "major_employers": "Delta Air Lines, UPS, Home Depot, Emory Healthcare, Coca-Cola"},
    "miami": {"state": "FL", "coli": 123, "population": "6.2M metro", "median_salary": 58000, "unemployment": "3.0%", "metro_name": "Miami-Fort Lauderdale-Pompano Beach MSA", "major_employers": "Baptist Health, Jackson Health, Carnival Cruise, FPL Group"},
    "tampa": {"state": "FL", "coli": 100, "population": "3.2M metro", "median_salary": 56000, "unemployment": "3.0%", "metro_name": "Tampa-St. Petersburg-Clearwater MSA", "major_employers": "BayCare, AdventHealth, USAA, Raymond James, Publix"},
    "detroit": {"state": "MI", "coli": 89, "population": "4.3M metro", "median_salary": 62000, "unemployment": "4.5%", "metro_name": "Detroit-Warren-Dearborn MSA", "major_employers": "Ford, GM, Stellantis, Henry Ford Health, Beaumont"},
    "pittsburgh": {"state": "PA", "coli": 93, "population": "2.3M metro", "median_salary": 60000, "unemployment": "4.0%", "metro_name": "Pittsburgh MSA", "major_employers": "UPMC, Highmark, PNC Financial, Carnegie Mellon University"},
    "philadelphia": {"state": "PA", "coli": 110, "population": "6.2M metro", "median_salary": 68000, "unemployment": "4.0%", "metro_name": "Philadelphia-Camden-Wilmington MSA", "major_employers": "Penn Medicine, Comcast, Jefferson Health, University of Pennsylvania"},
    "nashville": {"state": "TN", "coli": 103, "population": "2.0M metro", "median_salary": 62000, "unemployment": "2.8%", "metro_name": "Nashville-Davidson-Murfreesboro MSA", "major_employers": "HCA Healthcare, Vanderbilt, Nissan, Amazon, Bridgestone"},
    "charlotte": {"state": "NC", "coli": 98, "population": "2.7M metro", "median_salary": 64000, "unemployment": "3.4%", "metro_name": "Charlotte-Concord-Gastonia MSA", "major_employers": "Bank of America, Lowe's, Atrium Health, Duke Energy, Honeywell"},
    "raleigh": {"state": "NC", "coli": 101, "population": "1.5M metro", "median_salary": 70000, "unemployment": "2.9%", "metro_name": "Raleigh-Cary MSA", "major_employers": "WakeMed, Duke Health, Cisco, Red Hat, NC State University"},
    "columbus": {"state": "OH", "coli": 93, "population": "2.1M metro", "median_salary": 60000, "unemployment": "3.4%", "metro_name": "Columbus MSA", "major_employers": "Ohio State University, Nationwide, JPMorgan Chase, Honda"},
    "indianapolis": {"state": "IN", "coli": 92, "population": "2.1M metro", "median_salary": 58000, "unemployment": "3.0%", "metro_name": "Indianapolis-Carmel-Anderson MSA", "major_employers": "IU Health, Eli Lilly, Salesforce, Anthem, Rolls-Royce"},
    "kansas_city": {"state": "MO", "coli": 96, "population": "2.2M metro", "median_salary": 62000, "unemployment": "3.0%", "metro_name": "Kansas City MSA", "major_employers": "Cerner, Sprint/T-Mobile, Hallmark, Burns & McDonnell"},
    "st_louis": {"state": "MO", "coli": 91, "population": "2.8M metro", "median_salary": 62000, "unemployment": "3.2%", "metro_name": "St. Louis MSA", "major_employers": "BJC HealthCare, Emerson Electric, Edward Jones, Boeing"},
    "salt_lake_city": {"state": "UT", "coli": 107, "population": "1.3M metro", "median_salary": 68000, "unemployment": "2.3%", "metro_name": "Salt Lake City MSA", "major_employers": "Intermountain Health, Goldman Sachs, University of Utah, Overstock"},
    "las_vegas": {"state": "NV", "coli": 104, "population": "2.3M metro", "median_salary": 56000, "unemployment": "5.5%", "metro_name": "Las Vegas-Henderson-Paradise MSA", "major_employers": "MGM Resorts, Wynn, Caesars, Station Casinos, Clark County"},
    "richmond": {"state": "VA", "coli": 101, "population": "1.3M metro", "median_salary": 65000, "unemployment": "2.6%", "metro_name": "Richmond MSA", "major_employers": "VCU Health, Capital One, Altria, Dominion Energy, CarMax"},
    "norfolk": {"state": "VA", "coli": 98, "population": "1.8M metro", "median_salary": 60000, "unemployment": "3.2%", "metro_name": "Virginia Beach-Norfolk-Newport News MSA", "major_employers": "US Navy, Sentara Healthcare, Huntington Ingalls Industries, GEICO"},
    "baltimore": {"state": "MD", "coli": 109, "population": "2.8M metro", "median_salary": 70000, "unemployment": "3.5%", "metro_name": "Baltimore-Columbia-Towson MSA", "major_employers": "Johns Hopkins, Under Armour, T. Rowe Price, CareFirst, Leidos"},
    "washington_dc": {"state": "DC", "coli": 152, "population": "6.3M metro", "median_salary": 85000, "unemployment": "3.1%", "metro_name": "Washington-Arlington-Alexandria MSA", "major_employers": "Federal Government, Booz Allen, Lockheed Martin, Amazon HQ2"},
    "san_jose": {"state": "CA", "coli": 185, "population": "2.0M metro", "median_salary": 105000, "unemployment": "3.5%", "metro_name": "San Jose-Sunnyvale-Santa Clara MSA", "major_employers": "Apple, Alphabet, Cisco, Adobe, Intel"},
    "milwaukee": {"state": "WI", "coli": 95, "population": "1.6M metro", "median_salary": 58000, "unemployment": "3.2%", "metro_name": "Milwaukee-Waukesha MSA", "major_employers": "Advocate Aurora Health, Froedtert, Rockwell Automation, Northwestern Mutual"},
    "jacksonville": {"state": "FL", "coli": 98, "population": "1.6M metro", "median_salary": 58000, "unemployment": "3.0%", "metro_name": "Jacksonville MSA", "major_employers": "Mayo Clinic FL, Baptist Health, Naval Station Mayport, CSX"},
    "oklahoma_city": {"state": "OK", "coli": 90, "population": "1.4M metro", "median_salary": 55000, "unemployment": "2.8%", "metro_name": "Oklahoma City MSA", "major_employers": "Tinker AFB, OU Medical Center, Paycom, Devon Energy"},
    "new_orleans": {"state": "LA", "coli": 96, "population": "1.3M metro", "median_salary": 52000, "unemployment": "4.2%", "metro_name": "New Orleans-Metairie MSA", "major_employers": "Ochsner Health, Tulane, Entergy, Lockheed Martin"},
    "memphis": {"state": "TN", "coli": 87, "population": "1.3M metro", "median_salary": 52000, "unemployment": "3.8%", "metro_name": "Memphis MSA", "major_employers": "FedEx, St. Jude, Methodist Le Bonheur, AutoZone, Nike Logistics"},
    "louisville": {"state": "KY", "coli": 92, "population": "1.3M metro", "median_salary": 56000, "unemployment": "3.5%", "metro_name": "Louisville/Jefferson County MSA", "major_employers": "UPS Worldport, Norton Healthcare, Humana, GE Appliances, Ford"},
    "hartford": {"state": "CT", "coli": 108, "population": "1.2M metro", "median_salary": 68000, "unemployment": "3.6%", "metro_name": "Hartford-East Hartford-Middletown MSA", "major_employers": "Hartford HealthCare, United Technologies (RTX), The Hartford, Aetna/CVS"},
    "tucson": {"state": "AZ", "coli": 97, "population": "1.0M metro", "median_salary": 52000, "unemployment": "3.7%", "metro_name": "Tucson MSA", "major_employers": "University of Arizona, Raytheon, Davis-Monthan AFB, Banner Health"},
    "omaha": {"state": "NE", "coli": 95, "population": "960K metro", "median_salary": 60000, "unemployment": "2.3%", "metro_name": "Omaha-Council Bluffs MSA", "major_employers": "Berkshire Hathaway, Mutual of Omaha, Union Pacific, Offutt AFB"},
    "des_moines": {"state": "IA", "coli": 93, "population": "700K metro", "median_salary": 62000, "unemployment": "2.5%", "metro_name": "Des Moines-West Des Moines MSA", "major_employers": "Principal Financial, Wells Fargo, Wellmark, UnityPoint Health"},
    "birmingham": {"state": "AL", "coli": 90, "population": "1.1M metro", "median_salary": 55000, "unemployment": "2.7%", "metro_name": "Birmingham-Hoover MSA", "major_employers": "UAB Health, St. Vincent's, Regions Financial, Southern Company"},
    "honolulu": {"state": "HI", "coli": 192, "population": "1.0M metro", "median_salary": 72000, "unemployment": "3.0%", "metro_name": "Urban Honolulu MSA", "major_employers": "US Military (PACOM), Queen's Health, Hawaiian Airlines, Bank of Hawaii"},
    "anchorage": {"state": "AK", "coli": 127, "population": "400K metro", "median_salary": 65000, "unemployment": "4.5%", "metro_name": "Anchorage MSA", "major_employers": "Providence Alaska, JBER Military Base, ConocoPhillips, State of Alaska"},
    "boise": {"state": "ID", "coli": 103, "population": "780K metro", "median_salary": 60000, "unemployment": "2.7%", "metro_name": "Boise City MSA", "major_employers": "Micron Technology, St. Luke's, HP, Albertsons, Boise State"},
}

# ═══════════════════════════════════════════════════════════════════════════════
# UNIVERSITY DATA BY STATE
# ═══════════════════════════════════════════════════════════════════════════════

STATE_UNIVERSITIES = {
    "AL": [
        {"name": "University of Alabama (Tuscaloosa)", "programs": "Engineering, Healthcare, Business", "enrollment": "38,600"},
        {"name": "Auburn University", "programs": "Engineering, Veterinary, Agriculture", "enrollment": "31,500"},
        {"name": "University of Alabama at Birmingham (UAB)", "programs": "Medicine, Nursing, Health Sciences", "enrollment": "22,500"},
        {"name": "University of South Alabama", "programs": "Healthcare, Engineering, Education", "enrollment": "14,000"},
    ],
    "AK": [
        {"name": "University of Alaska Anchorage", "programs": "Engineering, Nursing, Business", "enrollment": "15,000"},
        {"name": "University of Alaska Fairbanks", "programs": "Arctic Sciences, Engineering, Mining", "enrollment": "8,000"},
    ],
    "AZ": [
        {"name": "Arizona State University (Tempe)", "programs": "Engineering, Business, Healthcare, Technology", "enrollment": "77,000"},
        {"name": "University of Arizona (Tucson)", "programs": "Medicine, Optical Sciences, Mining Engineering", "enrollment": "49,000"},
        {"name": "Northern Arizona University (Flagstaff)", "programs": "Education, Nursing, Forestry", "enrollment": "28,000"},
        {"name": "Grand Canyon University (Phoenix)", "programs": "Nursing, Education, Business", "enrollment": "115,000"},
    ],
    "AR": [
        {"name": "University of Arkansas (Fayetteville)", "programs": "Engineering, Agriculture, Business", "enrollment": "30,000"},
        {"name": "University of Arkansas for Medical Sciences (UAMS)", "programs": "Medicine, Nursing, Pharmacy", "enrollment": "3,200"},
    ],
    "CA": [
        {"name": "University of California, Los Angeles (UCLA)", "programs": "Medicine, Engineering, Business, Film", "enrollment": "46,000"},
        {"name": "Stanford University", "programs": "Engineering, Computer Science, Medicine, Business", "enrollment": "17,500"},
        {"name": "University of California, Berkeley", "programs": "Engineering, Computer Science, Business", "enrollment": "45,500"},
        {"name": "University of California, San Francisco (UCSF)", "programs": "Medicine, Nursing, Pharmacy, Dentistry", "enrollment": "3,100"},
        {"name": "University of Southern California (USC)", "programs": "Engineering, Film, Business, Medicine", "enrollment": "49,500"},
        {"name": "San Diego State University", "programs": "Business, Engineering, Public Health", "enrollment": "36,000"},
        {"name": "California State University System (23 campuses)", "programs": "Broad programs across all disciplines", "enrollment": "460,000"},
    ],
    "CO": [
        {"name": "University of Colorado Boulder", "programs": "Engineering, Aerospace, Business, Sciences", "enrollment": "36,000"},
        {"name": "Colorado State University (Fort Collins)", "programs": "Veterinary, Agriculture, Engineering", "enrollment": "34,000"},
        {"name": "University of Colorado Denver/Anschutz Medical Campus", "programs": "Medicine, Nursing, Public Health, Dental", "enrollment": "18,000"},
        {"name": "Colorado School of Mines (Golden)", "programs": "Mining Engineering, Petroleum, Materials Science", "enrollment": "6,800"},
    ],
    "CT": [
        {"name": "Yale University (New Haven)", "programs": "Medicine, Law, Engineering, Sciences", "enrollment": "14,500"},
        {"name": "University of Connecticut (Storrs)", "programs": "Engineering, Business, Nursing, Pharmacy", "enrollment": "32,500"},
        {"name": "University of Hartford", "programs": "Engineering, Business, Health Sciences", "enrollment": "4,500"},
    ],
    "DE": [
        {"name": "University of Delaware (Newark)", "programs": "Chemical Engineering, Business, Healthcare", "enrollment": "24,000"},
        {"name": "Delaware State University (Dover)", "programs": "Agriculture, Business, Education, Nursing", "enrollment": "5,500"},
    ],
    "FL": [
        {"name": "University of Florida (Gainesville)", "programs": "Medicine, Engineering, Business, Agriculture", "enrollment": "60,000"},
        {"name": "University of South Florida (Tampa)", "programs": "Medicine, Engineering, Public Health", "enrollment": "50,000"},
        {"name": "Florida State University (Tallahassee)", "programs": "Medicine, Business, Engineering, Education", "enrollment": "45,000"},
        {"name": "University of Central Florida (Orlando)", "programs": "Engineering, Hospitality, Optics", "enrollment": "72,000"},
        {"name": "University of Miami", "programs": "Medicine, Marine Science, Business, Engineering", "enrollment": "19,000"},
    ],
    "GA": [
        {"name": "Georgia Institute of Technology (Atlanta)", "programs": "Engineering, Computer Science, Aerospace", "enrollment": "45,000"},
        {"name": "Emory University (Atlanta)", "programs": "Medicine, Public Health, Business, Law", "enrollment": "15,500"},
        {"name": "University of Georgia (Athens)", "programs": "Business, Agriculture, Education, Pharmacy", "enrollment": "40,000"},
        {"name": "Morehouse School of Medicine (Atlanta)", "programs": "Medicine, Public Health, Biomedical Sciences", "enrollment": "570"},
    ],
    "HI": [
        {"name": "University of Hawaii at Manoa", "programs": "Marine Science, Tropical Agriculture, Medicine, Nursing", "enrollment": "19,000"},
        {"name": "Hawaii Pacific University", "programs": "Nursing, Business, Marine Biology", "enrollment": "3,800"},
    ],
    "ID": [
        {"name": "Boise State University", "programs": "Engineering, Business, Health Sciences", "enrollment": "26,000"},
        {"name": "University of Idaho (Moscow)", "programs": "Engineering, Agriculture, Natural Resources", "enrollment": "12,000"},
        {"name": "Idaho State University (Pocatello)", "programs": "Healthcare, Pharmacy, Engineering", "enrollment": "12,400"},
    ],
    "IL": [
        {"name": "University of Illinois Urbana-Champaign", "programs": "Engineering, Computer Science, Business, Agriculture", "enrollment": "56,000"},
        {"name": "Northwestern University (Evanston/Chicago)", "programs": "Medicine, Engineering, Business, Journalism", "enrollment": "22,000"},
        {"name": "University of Chicago", "programs": "Medicine, Business, Economics, Law", "enrollment": "17,800"},
        {"name": "University of Illinois Chicago (UIC)", "programs": "Medicine, Nursing, Pharmacy, Engineering", "enrollment": "34,000"},
    ],
    "IN": [
        {"name": "Purdue University (West Lafayette)", "programs": "Engineering, Agriculture, Computer Science", "enrollment": "50,000"},
        {"name": "Indiana University Bloomington", "programs": "Business, Medicine, Education, Music", "enrollment": "47,000"},
        {"name": "University of Notre Dame", "programs": "Business, Engineering, Law, Sciences", "enrollment": "12,600"},
        {"name": "Rose-Hulman Institute of Technology (Terre Haute)", "programs": "Engineering, Computer Science, Mathematics", "enrollment": "2,100"},
    ],
    "IA": [
        {"name": "University of Iowa (Iowa City)", "programs": "Medicine, Engineering, Business, Nursing", "enrollment": "31,000"},
        {"name": "Iowa State University (Ames)", "programs": "Engineering, Agriculture, Computer Science, Veterinary", "enrollment": "31,000"},
    ],
    "KS": [
        {"name": "University of Kansas (Lawrence)", "programs": "Medicine, Engineering, Pharmacy, Business", "enrollment": "28,000"},
        {"name": "Kansas State University (Manhattan)", "programs": "Agriculture, Engineering, Veterinary", "enrollment": "21,000"},
        {"name": "Wichita State University", "programs": "Engineering, Aviation, Health Professions", "enrollment": "16,000"},
    ],
    "KY": [
        {"name": "University of Kentucky (Lexington)", "programs": "Medicine, Engineering, Pharmacy, Agriculture", "enrollment": "32,000"},
        {"name": "University of Louisville", "programs": "Medicine, Engineering, Business, Nursing", "enrollment": "22,000"},
    ],
    "LA": [
        {"name": "Louisiana State University (Baton Rouge)", "programs": "Engineering, Business, Agriculture, Veterinary", "enrollment": "36,000"},
        {"name": "Tulane University (New Orleans)", "programs": "Medicine, Public Health, Business, Law", "enrollment": "14,000"},
    ],
    "ME": [
        {"name": "University of Maine (Orono)", "programs": "Engineering, Marine Sciences, Forestry", "enrollment": "12,000"},
        {"name": "University of New England (Biddeford)", "programs": "Medicine, Pharmacy, Health Sciences", "enrollment": "7,000"},
    ],
    "MD": [
        {"name": "Johns Hopkins University (Baltimore)", "programs": "Medicine, Public Health, Engineering, Nursing", "enrollment": "28,000"},
        {"name": "University of Maryland, College Park", "programs": "Engineering, Computer Science, Business", "enrollment": "41,000"},
        {"name": "University of Maryland, Baltimore (Medical)", "programs": "Medicine, Nursing, Pharmacy, Dentistry, Law", "enrollment": "7,000"},
    ],
    "MA": [
        {"name": "Harvard University (Cambridge)", "programs": "Medicine, Business, Law, Engineering, Sciences", "enrollment": "23,000"},
        {"name": "MIT (Cambridge)", "programs": "Engineering, Computer Science, AI, Sciences", "enrollment": "11,800"},
        {"name": "Boston University", "programs": "Medicine, Engineering, Business, Communication", "enrollment": "36,000"},
        {"name": "University of Massachusetts Amherst", "programs": "Engineering, Computer Science, Business, Nursing", "enrollment": "32,000"},
        {"name": "Tufts University (Medford)", "programs": "Medicine, Veterinary, Engineering, International Relations", "enrollment": "13,000"},
    ],
    "MI": [
        {"name": "University of Michigan (Ann Arbor)", "programs": "Medicine, Engineering, Business, Nursing", "enrollment": "47,000"},
        {"name": "Michigan State University (East Lansing)", "programs": "Medicine, Engineering, Agriculture, Education", "enrollment": "50,000"},
        {"name": "Wayne State University (Detroit)", "programs": "Medicine, Engineering, Social Work, Nursing", "enrollment": "24,000"},
    ],
    "MN": [
        {"name": "University of Minnesota Twin Cities", "programs": "Medicine, Engineering, Business, Agriculture, Nursing", "enrollment": "52,000"},
        {"name": "Mayo Clinic College of Medicine (Rochester)", "programs": "Medicine, Healthcare Research, Biomedical Sciences", "enrollment": "5,200"},
        {"name": "University of St. Thomas (St. Paul)", "programs": "Engineering, Business, Education, Social Work", "enrollment": "10,000"},
        {"name": "Minnesota State University, Mankato", "programs": "Nursing, Engineering Technology, Business", "enrollment": "14,000"},
        {"name": "St. Cloud State University", "programs": "Education, Business, Health Sciences, Engineering", "enrollment": "11,000"},
        {"name": "Hennepin Technical College (Brooklyn Park)", "programs": "Diesel Technology, Welding, Manufacturing, HVAC", "enrollment": "6,500"},
    ],
    "MS": [
        {"name": "University of Mississippi (Ole Miss)", "programs": "Pharmacy, Business, Engineering, Law", "enrollment": "23,000"},
        {"name": "Mississippi State University", "programs": "Engineering, Agriculture, Veterinary, Business", "enrollment": "23,000"},
    ],
    "MO": [
        {"name": "Washington University in St. Louis", "programs": "Medicine, Engineering, Business, Law", "enrollment": "16,000"},
        {"name": "University of Missouri (Columbia)", "programs": "Medicine, Engineering, Journalism, Agriculture", "enrollment": "31,000"},
    ],
    "MT": [
        {"name": "Montana State University (Bozeman)", "programs": "Engineering, Agriculture, Nursing", "enrollment": "17,000"},
        {"name": "University of Montana (Missoula)", "programs": "Forestry, Business, Education, Pharmacy", "enrollment": "10,000"},
    ],
    "NE": [
        {"name": "University of Nebraska-Lincoln", "programs": "Engineering, Agriculture, Business, Education", "enrollment": "25,000"},
        {"name": "University of Nebraska Medical Center (Omaha)", "programs": "Medicine, Nursing, Pharmacy, Dentistry", "enrollment": "4,300"},
        {"name": "Creighton University (Omaha)", "programs": "Medicine, Nursing, Business, Pharmacy, Dentistry", "enrollment": "8,700"},
    ],
    "NV": [
        {"name": "University of Nevada, Las Vegas (UNLV)", "programs": "Hospitality, Engineering, Business, Nursing", "enrollment": "31,000"},
        {"name": "University of Nevada, Reno", "programs": "Engineering, Mining, Medicine, Business", "enrollment": "21,000"},
    ],
    "NH": [
        {"name": "Dartmouth College (Hanover)", "programs": "Medicine, Engineering, Business", "enrollment": "6,700"},
        {"name": "University of New Hampshire (Durham)", "programs": "Engineering, Business, Health Sciences", "enrollment": "15,000"},
    ],
    "NJ": [
        {"name": "Rutgers University (New Brunswick)", "programs": "Medicine, Engineering, Pharmacy, Business", "enrollment": "68,000"},
        {"name": "Princeton University", "programs": "Engineering, Sciences, Public Policy", "enrollment": "8,600"},
        {"name": "Stevens Institute of Technology (Hoboken)", "programs": "Engineering, Computer Science, AI, Cybersecurity", "enrollment": "7,500"},
    ],
    "NM": [
        {"name": "University of New Mexico (Albuquerque)", "programs": "Medicine, Engineering, Law, Nursing", "enrollment": "25,000"},
        {"name": "New Mexico State University (Las Cruces)", "programs": "Agriculture, Engineering, Business", "enrollment": "13,000"},
    ],
    "NY": [
        {"name": "Columbia University (NYC)", "programs": "Medicine, Engineering, Business, Law, Journalism", "enrollment": "33,000"},
        {"name": "Cornell University (Ithaca)", "programs": "Engineering, Agriculture, Medicine, Veterinary, Hotel Admin", "enrollment": "25,000"},
        {"name": "New York University (NYC)", "programs": "Medicine, Business, Engineering, Arts, Law", "enrollment": "60,000"},
        {"name": "University at Buffalo (SUNY)", "programs": "Medicine, Engineering, Pharmacy, Dental", "enrollment": "32,000"},
        {"name": "Stony Brook University (SUNY)", "programs": "Medicine, Engineering, Computer Science", "enrollment": "27,000"},
    ],
    "NC": [
        {"name": "Duke University (Durham)", "programs": "Medicine, Engineering, Business, Nursing", "enrollment": "17,000"},
        {"name": "University of North Carolina at Chapel Hill", "programs": "Medicine, Pharmacy, Public Health, Business", "enrollment": "31,000"},
        {"name": "NC State University (Raleigh)", "programs": "Engineering, Agriculture, Computer Science, Textiles", "enrollment": "37,000"},
        {"name": "Wake Forest University (Winston-Salem)", "programs": "Medicine, Business, Law, Engineering", "enrollment": "9,000"},
    ],
    "ND": [
        {"name": "University of North Dakota (Grand Forks)", "programs": "Aviation, Medicine, Engineering, Nursing", "enrollment": "14,000"},
        {"name": "North Dakota State University (Fargo)", "programs": "Engineering, Agriculture, Pharmacy", "enrollment": "13,000"},
    ],
    "OH": [
        {"name": "Ohio State University (Columbus)", "programs": "Medicine, Engineering, Business, Nursing, Veterinary", "enrollment": "61,000"},
        {"name": "Case Western Reserve University (Cleveland)", "programs": "Medicine, Engineering, Nursing, Dental", "enrollment": "12,000"},
        {"name": "University of Cincinnati", "programs": "Medicine, Engineering, Music, Design", "enrollment": "47,000"},
    ],
    "OK": [
        {"name": "University of Oklahoma (Norman/OKC)", "programs": "Medicine, Engineering, Business, Meteorology", "enrollment": "32,000"},
        {"name": "Oklahoma State University (Stillwater)", "programs": "Engineering, Agriculture, Veterinary, Business", "enrollment": "25,000"},
    ],
    "OR": [
        {"name": "Oregon Health & Science University (OHSU, Portland)", "programs": "Medicine, Nursing, Dentistry, Public Health", "enrollment": "3,200"},
        {"name": "Oregon State University (Corvallis)", "programs": "Engineering, Forestry, Agriculture, Marine Science", "enrollment": "34,000"},
        {"name": "University of Oregon (Eugene)", "programs": "Business, Education, Journalism, Sciences", "enrollment": "23,000"},
        {"name": "Portland State University", "programs": "Engineering, Social Work, Business, Urban Planning", "enrollment": "22,000"},
        {"name": "Pacific University (Forest Grove)", "programs": "Healthcare, Optometry, Pharmacy, Education", "enrollment": "3,800"},
        {"name": "George Fox University (Newberg)", "programs": "Nursing, Engineering, Business, Education", "enrollment": "4,100"},
        {"name": "Oregon Institute of Technology (Klamath Falls)", "programs": "Engineering Technology, Healthcare Technology, Renewable Energy", "enrollment": "5,500"},
    ],
    "PA": [
        {"name": "University of Pennsylvania (Philadelphia)", "programs": "Medicine, Business (Wharton), Engineering, Nursing", "enrollment": "22,000"},
        {"name": "Penn State University (State College)", "programs": "Engineering, Business, Agriculture, Medicine", "enrollment": "88,000"},
        {"name": "University of Pittsburgh", "programs": "Medicine, Engineering, Nursing, Pharmacy, Business", "enrollment": "34,000"},
        {"name": "Carnegie Mellon University (Pittsburgh)", "programs": "Computer Science, Engineering, Robotics, AI, Business", "enrollment": "16,000"},
        {"name": "Drexel University (Philadelphia)", "programs": "Engineering, Medicine, Business, Co-op Programs", "enrollment": "24,000"},
    ],
    "RI": [
        {"name": "Brown University (Providence)", "programs": "Medicine, Engineering, Sciences", "enrollment": "10,000"},
        {"name": "University of Rhode Island (Kingston)", "programs": "Pharmacy, Engineering, Nursing, Marine Science", "enrollment": "18,000"},
    ],
    "SC": [
        {"name": "Clemson University", "programs": "Engineering, Agriculture, Business, Nursing", "enrollment": "28,000"},
        {"name": "Medical University of South Carolina (MUSC, Charleston)", "programs": "Medicine, Nursing, Pharmacy, Dental", "enrollment": "3,200"},
        {"name": "University of South Carolina (Columbia)", "programs": "Business, Engineering, Nursing, Law", "enrollment": "35,000"},
    ],
    "SD": [
        {"name": "South Dakota State University (Brookings)", "programs": "Engineering, Agriculture, Pharmacy, Nursing", "enrollment": "11,000"},
        {"name": "University of South Dakota (Vermillion)", "programs": "Medicine, Business, Law, Education", "enrollment": "10,000"},
    ],
    "TN": [
        {"name": "Vanderbilt University (Nashville)", "programs": "Medicine, Engineering, Business, Nursing, Education", "enrollment": "13,500"},
        {"name": "University of Tennessee, Knoxville", "programs": "Engineering, Business, Agriculture, Nursing", "enrollment": "34,000"},
        {"name": "University of Tennessee Health Science Center (Memphis)", "programs": "Medicine, Nursing, Pharmacy, Dentistry", "enrollment": "3,300"},
    ],
    "TX": [
        {"name": "University of Texas at Austin", "programs": "Engineering, Business, Computer Science, Law", "enrollment": "52,000"},
        {"name": "Texas A&M University (College Station)", "programs": "Engineering, Agriculture, Veterinary, Business, Military (Corps)", "enrollment": "72,000"},
        {"name": "UT Southwestern Medical Center (Dallas)", "programs": "Medicine, Biomedical Sciences, Healthcare Research", "enrollment": "2,800"},
        {"name": "Rice University (Houston)", "programs": "Engineering, Computer Science, Business, Sciences", "enrollment": "8,000"},
        {"name": "Baylor College of Medicine (Houston)", "programs": "Medicine, Healthcare Research, Nursing", "enrollment": "1,600"},
        {"name": "University of Houston", "programs": "Engineering, Business, Optometry, Hotel Management", "enrollment": "47,000"},
    ],
    "UT": [
        {"name": "University of Utah (Salt Lake City)", "programs": "Medicine, Engineering, Business, Gaming/Entertainment Arts", "enrollment": "34,000"},
        {"name": "Brigham Young University (Provo)", "programs": "Business, Engineering, Education, Nursing", "enrollment": "34,000"},
        {"name": "Utah State University (Logan)", "programs": "Engineering, Agriculture, Education", "enrollment": "28,000"},
    ],
    "VT": [
        {"name": "University of Vermont (Burlington)", "programs": "Medicine, Engineering, Agriculture, Nursing", "enrollment": "13,000"},
    ],
    "VA": [
        {"name": "University of Virginia (Charlottesville)", "programs": "Medicine, Engineering, Business (Darden), Nursing", "enrollment": "25,000"},
        {"name": "Virginia Tech (Blacksburg)", "programs": "Engineering, Agriculture, Computer Science, Architecture", "enrollment": "37,000"},
        {"name": "Virginia Commonwealth University (Richmond)", "programs": "Medicine, Nursing, Pharmacy, Engineering, Arts", "enrollment": "29,000"},
        {"name": "George Mason University (Fairfax)", "programs": "Cybersecurity, Engineering, Computing, Health Sciences", "enrollment": "39,000"},
    ],
    "WA": [
        {"name": "University of Washington (Seattle)", "programs": "Medicine, Engineering, Computer Science, Nursing, Business", "enrollment": "48,000"},
        {"name": "Washington State University (Pullman)", "programs": "Engineering, Agriculture, Veterinary, Pharmacy, Nursing", "enrollment": "32,000"},
        {"name": "Seattle University", "programs": "Nursing, Engineering, Business, Law", "enrollment": "7,200"},
    ],
    "WV": [
        {"name": "West Virginia University (Morgantown)", "programs": "Medicine, Engineering, Nursing, Pharmacy, Mining", "enrollment": "27,000"},
        {"name": "Marshall University (Huntington)", "programs": "Medicine, Engineering, Nursing, Business", "enrollment": "12,000"},
    ],
    "WI": [
        {"name": "University of Wisconsin-Madison", "programs": "Medicine, Engineering, Business, Agriculture, Nursing", "enrollment": "49,000"},
        {"name": "Marquette University (Milwaukee)", "programs": "Engineering, Nursing, Business, Dentistry, Law", "enrollment": "11,500"},
        {"name": "Milwaukee School of Engineering", "programs": "Engineering, Computer Science, Nursing, Business", "enrollment": "2,800"},
    ],
    "WY": [
        {"name": "University of Wyoming (Laramie)", "programs": "Engineering, Energy, Agriculture, Education", "enrollment": "12,000"},
    ],
    "DC": [
        {"name": "Georgetown University", "programs": "Medicine, Law, Business, Foreign Service", "enrollment": "20,000"},
        {"name": "George Washington University", "programs": "Medicine, Public Health, Engineering, International Affairs", "enrollment": "26,000"},
        {"name": "Howard University", "programs": "Medicine, Engineering, Business, Law, Pharmacy", "enrollment": "10,000"},
    ],
}

# ═══════════════════════════════════════════════════════════════════════════════
# RADIO STATIONS BY STATE (Major markets)
# ═══════════════════════════════════════════════════════════════════════════════

STATE_RADIO = {
    "OR": [
        {"name": "KXL-FM 101.1 (Portland)", "listeners": "350K+/week", "genre": "News/Talk", "audience": "Professionals 25-54"},
        {"name": "KEX-AM 1190 (Portland)", "listeners": "280K+/week", "genre": "News/Talk", "audience": "Adults 35-64"},
        {"name": "KOPB-FM 91.5 - OPB (Portland)", "listeners": "400K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals 25-64"},
        {"name": "KINK-FM 101.9 (Portland)", "listeners": "320K+/week", "genre": "Adult Album Alternative", "audience": "Adults 25-54"},
        {"name": "KUPL-FM 98.7 (Portland)", "listeners": "300K+/week", "genre": "Country", "audience": "Blue Collar, Trades 25-54"},
        {"name": "KPOJ-AM 620 (Portland)", "listeners": "120K+/week", "genre": "News/Talk", "audience": "Adults 35+"},
        {"name": "KUGN-AM 590 (Eugene)", "listeners": "85K+/week", "genre": "News/Talk", "audience": "Adults 35-64"},
        {"name": "KLCC-FM 89.7 (Eugene)", "listeners": "95K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "KMED-AM 1440 (Medford)", "listeners": "60K+/week", "genre": "News/Talk", "audience": "Adults 35+"},
    ],
    "MN": [
        {"name": "MPR News 91.1 KNOW (Minneapolis)", "listeners": "800K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals 25-64"},
        {"name": "WCCO-AM 830 (Minneapolis)", "listeners": "650K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "KFAN-FM 100.3 (Minneapolis)", "listeners": "550K+/week", "genre": "Sports Talk", "audience": "Males 18-54"},
        {"name": "KDWB-FM 101.3 (Minneapolis)", "listeners": "500K+/week", "genre": "Top 40/Pop", "audience": "Adults 18-34"},
        {"name": "KS95-FM 94.5 (Minneapolis)", "listeners": "480K+/week", "genre": "Adult Contemporary", "audience": "Women 25-54"},
        {"name": "KQQL-FM 107.9 (Minneapolis)", "listeners": "350K+/week", "genre": "Classic Hits", "audience": "Adults 35-54"},
        {"name": "KDAL-AM 610 (Duluth)", "listeners": "80K+/week", "genre": "News/Talk", "audience": "Adults 35+"},
        {"name": "KROC-AM 1340 (Rochester)", "listeners": "55K+/week", "genre": "News/Talk", "audience": "Professionals 25-64"},
    ],
    "CA": [
        {"name": "KFI-AM 640 (Los Angeles)", "listeners": "1.5M+/week", "genre": "News/Talk", "audience": "Adults 25-54"},
        {"name": "KQED-FM 88.5 (San Francisco)", "listeners": "1.2M+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "KNX-AM 1070 (Los Angeles)", "listeners": "1.1M+/week", "genre": "News", "audience": "Adults 25-64"},
        {"name": "KCBS-AM 740 (San Francisco)", "listeners": "800K+/week", "genre": "News", "audience": "Professionals 25-54"},
        {"name": "KROQ-FM 106.7 (Los Angeles)", "listeners": "900K+/week", "genre": "Alternative Rock", "audience": "Adults 18-44"},
        {"name": "KFBK-AM 1530 (Sacramento)", "listeners": "350K+/week", "genre": "News/Talk", "audience": "Adults 35-64"},
    ],
    "TX": [
        {"name": "WBAP-AM 820 (Dallas-Fort Worth)", "listeners": "1.2M+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "KHOU-FM / KTRH-AM 740 (Houston)", "listeners": "900K+/week", "genre": "News/Talk", "audience": "Adults 25-54"},
        {"name": "KUT-FM 90.5 (Austin)", "listeners": "400K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals 25-54"},
        {"name": "WOAI-AM 1200 (San Antonio)", "listeners": "350K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "KSCS-FM 96.3 (Dallas-Fort Worth)", "listeners": "800K+/week", "genre": "Country", "audience": "Blue Collar/Trades 25-54"},
    ],
    "NY": [
        {"name": "WNYC-FM 93.9 (New York City)", "listeners": "2.0M+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "WOR-AM 710 (New York City)", "listeners": "1.5M+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "WCBS-AM 880 (New York City)", "listeners": "1.2M+/week", "genre": "News", "audience": "Adults 25-54"},
        {"name": "WBEN-AM 930 (Buffalo)", "listeners": "300K+/week", "genre": "News/Talk", "audience": "Adults 35-64"},
        {"name": "WGY-AM 810 (Albany)", "listeners": "250K+/week", "genre": "News/Talk", "audience": "Adults 35-64"},
    ],
    "IL": [
        {"name": "WGN-AM 720 (Chicago)", "listeners": "1.0M+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "WBEZ-FM 91.5 (Chicago)", "listeners": "850K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "WLS-AM 890 (Chicago)", "listeners": "700K+/week", "genre": "News/Talk", "audience": "Adults 35-64"},
        {"name": "WUSN-FM 99.5 (Chicago)", "listeners": "600K+/week", "genre": "Country", "audience": "Blue Collar/Trades 25-54"},
    ],
    "WA": [
        {"name": "KUOW-FM 94.9 (Seattle)", "listeners": "600K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "KOMO-AM 1000 (Seattle)", "listeners": "500K+/week", "genre": "News", "audience": "Adults 25-54"},
        {"name": "KJR-AM 950 (Seattle)", "listeners": "350K+/week", "genre": "Sports Talk", "audience": "Males 25-54"},
        {"name": "KEXP-FM 90.3 (Seattle)", "listeners": "250K+/week", "genre": "Independent/Alternative", "audience": "Adults 18-44"},
    ],
    "GA": [
        {"name": "WSB-AM 750 (Atlanta)", "listeners": "900K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "WABE-FM 90.1 (Atlanta)", "listeners": "600K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "WSTR-FM 94.1 (Atlanta)", "listeners": "500K+/week", "genre": "Adult Contemporary", "audience": "Adults 25-54"},
    ],
    "FL": [
        {"name": "WLRN-FM 91.3 (Miami)", "listeners": "400K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "WIOD-AM 610 (Miami)", "listeners": "350K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "WFLA-AM 970 (Tampa)", "listeners": "300K+/week", "genre": "News/Talk", "audience": "Adults 25-54"},
        {"name": "WJCT-FM 89.9 (Jacksonville)", "listeners": "200K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
    ],
    "PA": [
        {"name": "KYW-AM 1060 (Philadelphia)", "listeners": "800K+/week", "genre": "News", "audience": "Adults 25-54"},
        {"name": "WHYY-FM 90.9 (Philadelphia)", "listeners": "700K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "KDKA-AM 1020 (Pittsburgh)", "listeners": "500K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "WESA-FM 90.5 (Pittsburgh)", "listeners": "300K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
    ],
    "OH": [
        {"name": "WOSU-FM 89.7 (Columbus)", "listeners": "350K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "WTAM-AM 1100 (Cleveland)", "listeners": "400K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "WLW-AM 700 (Cincinnati)", "listeners": "500K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
    ],
    "MI": [
        {"name": "WJR-AM 760 (Detroit)", "listeners": "600K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "WDET-FM 101.9 (Detroit)", "listeners": "350K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "WWJ-AM 950 (Detroit)", "listeners": "500K+/week", "genre": "News", "audience": "Adults 25-54"},
    ],
    "NC": [
        {"name": "WUNC-FM 91.5 (Raleigh-Durham)", "listeners": "400K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "WBT-AM 1110 (Charlotte)", "listeners": "350K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "WFAE-FM 90.7 (Charlotte)", "listeners": "300K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
    ],
    "CO": [
        {"name": "CPR News 90.1 (Denver)", "listeners": "500K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "KOA-AM 850 (Denver)", "listeners": "450K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "KBCO-FM 97.3 (Denver)", "listeners": "400K+/week", "genre": "Adult Album Alternative", "audience": "Adults 25-54"},
    ],
    "TN": [
        {"name": "WPLN-FM 90.3 (Nashville)", "listeners": "350K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "WSM-AM 650 (Nashville)", "listeners": "300K+/week", "genre": "Country/Classic Country", "audience": "Adults 25-64"},
        {"name": "WREC-AM 600 (Memphis)", "listeners": "200K+/week", "genre": "News/Talk", "audience": "Adults 25-54"},
    ],
    "VA": [
        {"name": "WAMU-FM 88.5 (DC Metro/Virginia)", "listeners": "700K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "WRVA-AM 1140 (Richmond)", "listeners": "250K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "WHRV-FM 89.5 (Norfolk)", "listeners": "200K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
    ],
    "IN": [
        {"name": "WIBC-FM 93.1 (Indianapolis)", "listeners": "400K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "WFYI-FM 90.1 (Indianapolis)", "listeners": "250K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
    ],
    "MO": [
        {"name": "KMOX-AM 1120 (St. Louis)", "listeners": "500K+/week", "genre": "News/Talk", "audience": "Adults 25-64"},
        {"name": "KCUR-FM 89.3 (Kansas City)", "listeners": "300K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
    ],
    "MA": [
        {"name": "WBUR-FM 90.9 (Boston)", "listeners": "800K+/week", "genre": "Public Radio/NPR", "audience": "Educated Professionals"},
        {"name": "WBZ-AM 1030 (Boston)", "listeners": "600K+/week", "genre": "News", "audience": "Adults 25-54"},
        {"name": "WEEI-FM 93.7 (Boston)", "listeners": "500K+/week", "genre": "Sports Talk", "audience": "Males 18-54"},
    ],
}

# ═══════════════════════════════════════════════════════════════════════════════
# INDUSTRY-SPECIFIC PODCASTS
# ═══════════════════════════════════════════════════════════════════════════════

INDUSTRY_PODCASTS = {
    "healthcare_medical": [
        {"name": "The Peter Attia Drive", "listeners": "2M+ downloads/mo", "genre": "Medical/Health Science", "audience": "Medical Professionals, Health-conscious Adults"},
        {"name": "Freakonomics, M.D.", "listeners": "500K+ downloads/mo", "genre": "Healthcare Economics", "audience": "Healthcare Professionals, Policy Makers"},
        {"name": "The Curbsiders", "listeners": "350K+ downloads/mo", "genre": "Internal Medicine CME", "audience": "Physicians, Medical Students, Residents"},
        {"name": "EM Over Easy (Emergency Medicine)", "listeners": "200K+ downloads/mo", "genre": "Emergency Medicine", "audience": "EM Physicians, Residents, PAs"},
        {"name": "Nurse Blake Podcast", "listeners": "300K+ downloads/mo", "genre": "Nursing Lifestyle/Career", "audience": "Nurses (RN, NP), Nursing Students"},
        {"name": "Healthcare NOW Radio", "listeners": "150K+ downloads/mo", "genre": "Healthcare Industry", "audience": "Healthcare Administrators, Tech Professionals"},
    ],
    "blue_collar_trades": [
        {"name": "The Dirt (Heavy Equipment)", "listeners": "200K+ downloads/mo", "genre": "Construction/Heavy Equipment", "audience": "Equipment Operators, Construction Workers"},
        {"name": "Skilled Trades Podcast", "listeners": "80K+ downloads/mo", "genre": "Trade Careers", "audience": "Welders, Electricians, Plumbers, HVAC Techs"},
        {"name": "Mike Rowe's The Way I Heard It", "listeners": "1M+ downloads/mo", "genre": "Working Class Stories", "audience": "Blue Collar Workers, Trades Professionals"},
        {"name": "HVAC School Podcast", "listeners": "100K+ downloads/mo", "genre": "HVAC Training", "audience": "HVAC Technicians, Apprentices"},
        {"name": "Shop Talk (Welding)", "listeners": "50K+ downloads/mo", "genre": "Welding/Fabrication", "audience": "Welders, Fabricators, Metal Workers"},
    ],
    "maritime_marine": [
        {"name": "The Maritime Podcast", "listeners": "40K+ downloads/mo", "genre": "Maritime Industry", "audience": "Marine Professionals, Ship Engineers"},
        {"name": "Marine Log Podcast", "listeners": "30K+ downloads/mo", "genre": "Maritime News", "audience": "Maritime Industry Professionals"},
        {"name": "Ocean Navigator Podcast", "listeners": "25K+ downloads/mo", "genre": "Marine Navigation/Lifestyle", "audience": "Mariners, Boat Engineers"},
        {"name": "gCaptain Podcast", "listeners": "35K+ downloads/mo", "genre": "Maritime News & Analysis", "audience": "Maritime Professionals, Naval Architects"},
    ],
    "military_recruitment": [
        {"name": "Jocko Podcast", "listeners": "3M+ downloads/mo", "genre": "Leadership/Military", "audience": "Veterans, Active Duty, Military-curious Youth"},
        {"name": "The Frontlines Podcast", "listeners": "200K+ downloads/mo", "genre": "Military Stories", "audience": "Veterans, Active Duty, Military Families"},
        {"name": "Zero Blog Thirty (Barstool)", "listeners": "500K+ downloads/mo", "genre": "Military Culture/Comedy", "audience": "Veterans, Active Military 18-35"},
        {"name": "Military Matters (Stars & Stripes)", "listeners": "100K+ downloads/mo", "genre": "Military Policy/Issues", "audience": "Active Duty, Veterans, Military Families"},
        {"name": "Cleared Hot Podcast", "listeners": "400K+ downloads/mo", "genre": "Military/Outdoor Lifestyle", "audience": "Veterans, Active Duty, First Responders"},
    ],
    "tech_engineering": [
        {"name": "Lex Fridman Podcast", "listeners": "5M+ downloads/mo", "genre": "Technology/AI/Science", "audience": "Engineers, Scientists, Tech Professionals"},
        {"name": "Software Engineering Daily", "listeners": "300K+ downloads/mo", "genre": "Software Engineering", "audience": "Software Engineers, DevOps, Data Scientists"},
        {"name": "Changelog Podcast", "listeners": "200K+ downloads/mo", "genre": "Open Source/Dev", "audience": "Software Developers, Engineers"},
        {"name": "The Engineering Commons", "listeners": "80K+ downloads/mo", "genre": "Engineering Careers", "audience": "Engineers across disciplines"},
        {"name": "Darknet Diaries", "listeners": "1M+ downloads/mo", "genre": "Cybersecurity Stories", "audience": "InfoSec, IT Professionals, Tech Enthusiasts"},
    ],
    "general_entry_level": [
        {"name": "How I Built This (NPR)", "listeners": "3M+ downloads/mo", "genre": "Business/Entrepreneurship", "audience": "Job Seekers, Young Professionals 18-35"},
        {"name": "The Ken Coleman Show", "listeners": "500K+ downloads/mo", "genre": "Career Development", "audience": "Job Seekers, Career Changers"},
        {"name": "Find Your Dream Job", "listeners": "100K+ downloads/mo", "genre": "Job Search Strategy", "audience": "Active Job Seekers"},
        {"name": "Happen To Your Career", "listeners": "200K+ downloads/mo", "genre": "Career Change", "audience": "Career Changers, Entry-Level Seekers"},
    ],
    "legal_services": [
        {"name": "Strict Scrutiny", "listeners": "500K+ downloads/mo", "genre": "Supreme Court/Legal Analysis", "audience": "Attorneys, Law Students, Legal Professionals"},
        {"name": "Lawyer 2 Lawyer (Legal Talk Network)", "listeners": "100K+ downloads/mo", "genre": "Legal News & Analysis", "audience": "Practicing Attorneys, Paralegals"},
        {"name": "The Lawyerist Podcast", "listeners": "80K+ downloads/mo", "genre": "Law Practice Management", "audience": "Solo/Small Firm Attorneys, Legal Entrepreneurs"},
        {"name": "Above the Law Podcast", "listeners": "150K+ downloads/mo", "genre": "Legal Industry News", "audience": "Associates, Law Students, Legal Recruiters"},
    ],
    "finance_banking": [
        {"name": "Bloomberg Surveillance", "listeners": "1M+ downloads/mo", "genre": "Financial Markets", "audience": "Traders, Analysts, Portfolio Managers"},
        {"name": "The Indicator (NPR)", "listeners": "2M+ downloads/mo", "genre": "Economics/Finance", "audience": "Finance Professionals, Business Leaders"},
        {"name": "Wall Street Oasis Podcast", "listeners": "200K+ downloads/mo", "genre": "Investment Banking Careers", "audience": "IB Analysts, Associates, Finance Students"},
        {"name": "Odd Lots (Bloomberg)", "listeners": "500K+ downloads/mo", "genre": "Markets & Economics", "audience": "Financial Analysts, Economists, Traders"},
    ],
    "mental_health": [
        {"name": "The Therapist Uncensored", "listeners": "300K+ downloads/mo", "genre": "Psychology/Therapy", "audience": "Therapists, Psychologists, Counselors"},
        {"name": "The Social Work Podcast", "listeners": "80K+ downloads/mo", "genre": "Social Work Practice", "audience": "Social Workers, MSW Students, Counselors"},
        {"name": "Terrible, Thanks for Asking", "listeners": "500K+ downloads/mo", "genre": "Mental Health Stories", "audience": "Counselors, Therapists, General Audience"},
        {"name": "Shrink Rap Radio", "listeners": "60K+ downloads/mo", "genre": "Psychology Interviews", "audience": "Psychologists, Psychiatrists, Researchers"},
    ],
    "retail_consumer": [
        {"name": "Retail Brew Daily", "listeners": "200K+ downloads/mo", "genre": "Retail Industry News", "audience": "Retail Managers, Buyers, Executives"},
        {"name": "The Jason & Scot Show", "listeners": "100K+ downloads/mo", "genre": "Retail/E-commerce", "audience": "Retail Leaders, E-commerce Professionals"},
        {"name": "Retail Gets Real (NRF)", "listeners": "80K+ downloads/mo", "genre": "Retail Innovation", "audience": "Store Managers, District Managers, Retail Executives"},
    ],
    "aerospace_defense": [
        {"name": "Aviation Week Check 6", "listeners": "150K+ downloads/mo", "genre": "Aerospace Industry", "audience": "Aerospace Engineers, Program Managers, Defense Professionals"},
        {"name": "The Aerospace Engineering Podcast", "listeners": "60K+ downloads/mo", "genre": "Aerospace Engineering", "audience": "Aerospace Engineers, Students, Researchers"},
        {"name": "Defense One Radio", "listeners": "100K+ downloads/mo", "genre": "Defense Policy & Technology", "audience": "Defense Professionals, Policy Makers, Engineers"},
        {"name": "SpacePod (Houston We Have a Podcast)", "listeners": "200K+ downloads/mo", "genre": "Space/NASA", "audience": "Aerospace Engineers, Space Enthusiasts, Scientists"},
    ],
    "pharma_biotech": [
        {"name": "The Drug Hunter", "listeners": "80K+ downloads/mo", "genre": "Drug Discovery", "audience": "Medicinal Chemists, Researchers, Pharma Scientists"},
        {"name": "BioBoss Podcast", "listeners": "50K+ downloads/mo", "genre": "Biotech Careers", "audience": "Biotech Professionals, Scientists, Entrepreneurs"},
        {"name": "Pharma Intelligence Podcast", "listeners": "60K+ downloads/mo", "genre": "Pharmaceutical Industry", "audience": "Regulatory Affairs, Clinical Research, MSLs"},
        {"name": "STAT Signal Podcast", "listeners": "150K+ downloads/mo", "genre": "Health/Pharma News", "audience": "Pharma Professionals, Biotech Scientists, Clinicians"},
    ],
}

# ═══════════════════════════════════════════════════════════════════════════════
# COMPETITOR DATA BY INDUSTRY
# ═══════════════════════════════════════════════════════════════════════════════

INDUSTRY_COMPETITORS = {
    "healthcare_medical": {
        "Primary Direct Competitor": {"competitors": "HCA Healthcare (182 hospitals, 2,300+ sites), Kaiser Permanente (39 hospitals, 12.6M members), CommonSpirit Health (140 hospitals), Ascension (139 hospitals)", "threat": "HIGH - These systems aggressively recruit physicians, nurses, and allied health staff with sign-on bonuses ($10K-$50K for RNs, $100K+ for specialists), relocation packages, and loan repayment programs. They dominate job board spend."},
        "Federal/National Service Rival": {"competitors": "Veterans Health Administration (VHA - 1,298 facilities, 371K+ employees), Indian Health Service (IHS), US Public Health Service Commissioned Corps, Military Health System (TRICARE)", "threat": "MEDIUM-HIGH - Federal benefits (pension, student loan forgiveness via PSLF, job stability) are very attractive. VHA is the largest integrated health system in the US and actively competes for the same clinical talent."},
        "Education & Training Programs": {"competitors": "Academic medical centers (Mayo Clinic, Cleveland Clinic, Johns Hopkins, UCSF), university hospital residency programs, nursing school pipelines, PA/NP training programs", "threat": "MEDIUM - Top academic centers attract talent with research opportunities, teaching positions, and prestige. Residency and fellowship programs create strong institutional loyalty."},
        "Private Sector / Specialized Companies": {"competitors": "Envision Healthcare (physician staffing), TeamHealth (emergency/hospital medicine), AMN Healthcare (travel nursing), Aya Healthcare (travel nursing), Cross Country Healthcare, Sound Physicians", "threat": "HIGH - Travel nursing/locum tenens agencies offer premium pay (often 2-3x permanent rates), flexibility, and diverse experience. They directly compete for the same nursing and physician talent."},
        "Public Sector / Government": {"competitors": "State health departments, county hospitals, Federally Qualified Health Centers (FQHCs, 1,400+ nationwide), public health agencies, CDC, NIH", "threat": "MEDIUM - Public sector offers loan forgiveness (NHSC, PSLF), work-life balance, and mission-driven work. FQHCs are particularly competitive in underserved areas."},
        "Gig Economy & Flexible Work": {"competitors": "NurseDash, CareRev, ShiftKey (per-diem nursing apps), Nomad Health (travel nursing), Trusted Health, Clipboard Health, IntelyCare", "threat": "MEDIUM-HIGH - Gig nursing platforms offer ultimate flexibility, instant pay, and the ability to pick shifts. Growing rapidly and attracting nurses who prefer flexibility over traditional employment."},
    },
    "blue_collar_trades": {
        "Primary Direct Competitor": {"competitors": "Waste Management (48K+ employees), Republic Services (35K+), Caterpillar (global dealer network), John Deere (regional dealers), Turner Construction, Bechtel, Fluor", "threat": "HIGH - Large construction and industrial firms offer comprehensive benefits, competitive wages ($25-$45/hr for skilled trades), union partnerships, and apprenticeship programs that pipeline talent."},
        "Federal/National Service Rival": {"competitors": "US Army Corps of Engineers, Naval Facilities Engineering Systems Command (NAVFAC), Air Force Civil Engineer Center, Department of Energy facilities", "threat": "MEDIUM - Federal agencies offer stability, pensions, and veterans' preference hiring. They compete for electricians, welders, mechanics, and heavy equipment operators."},
        "Education & Training Programs": {"competitors": "Local union apprenticeship programs (IBEW, UA, Iron Workers), community college trade programs, trade schools (Lincoln Tech, UTI), Job Corps, YouthBuild", "threat": "MEDIUM-HIGH - Union apprenticeship programs offer earn-while-you-learn models ($15-$25/hr during training) with guaranteed employment. They create strong talent pipelines."},
        "Private Sector / Specialized Companies": {"competitors": "Tradesmen International (skilled trades staffing), PeopleReady (general labor), TrueBlue, Kelly Services Industrial, Aerotek (manufacturing/trades)", "threat": "HIGH - Staffing agencies offer immediate placement, competitive rates, and variety. They are often the first stop for workers re-entering the trades market."},
        "Public Sector / Government": {"competitors": "State DOT departments, municipal utilities, public works departments, water/sewer authorities, public school districts (maintenance)", "threat": "MEDIUM - Government trades positions offer pension, benefits, and stability. Often pay less but compensate with work-life balance and job security."},
        "Gig Economy & Flexible Work": {"competitors": "Instawork (hourly/gig), Wonolo (warehouse/logistics gig), TaskRabbit (handyman), Thumbtack (skilled trades marketplace), Handy, Angi (home services)", "threat": "MEDIUM - Gig platforms are gaining traction for handyman and light trade work. Less competitive for heavy industrial/construction but growing in maintenance and repair sectors."},
    },
    "maritime_marine": {
        "Primary Direct Competitor": {"competitors": "Huntington Ingalls Industries (44K employees, Newport News & Ingalls shipyards), General Dynamics NASSCO & Bath Iron Works, BAE Systems Ship Repair, Vigor Industrial (Pacific NW)", "threat": "HIGH - Major shipbuilders and repair facilities aggressively recruit marine mechanics, welders, electricians, and engineers. They offer competitive wages ($28-$55/hr) and benefits."},
        "Federal/National Service Rival": {"competitors": "US Navy (civilian shipyard workers at Norfolk, Puget Sound, Pearl Harbor, Portsmouth), NOAA Corps, US Coast Guard (civilian positions), MARAD (Maritime Administration)", "threat": "HIGH - Navy shipyards employ 36,000+ civilians with federal benefits, job stability, and security clearance opportunities. Strong competition for the exact same skill sets."},
        "Education & Training Programs": {"competitors": "US Merchant Marine Academy (Kings Point), state maritime academies (California, Maine, Massachusetts, Texas A&M, SUNY), SIU Piney Point training center", "threat": "MEDIUM - Maritime academies produce highly trained graduates but in limited numbers. Their pipelines often flow to specific employers through cadet shipping programs."},
        "Private Sector / Specialized Companies": {"competitors": "Rolls-Royce Solutions (marine power), Wärtsilä, MAN Energy Solutions, Caterpillar Marine, Cummins Marine, Maersk, Crowley Maritime, TOTE Maritime", "threat": "HIGH - Marine engine and power companies compete directly for diesel mechanics and marine engineers. Offer manufacturer training, global opportunities, and competitive pay."},
        "Public Sector / Government": {"competitors": "Army Corps of Engineers (waterway maintenance), state port authorities, US Army watercraft operations, Coast Guard Auxiliary", "threat": "MEDIUM - Government maritime positions offer stability and benefits. Army watercraft units compete for the same diesel mechanic and marine operator talent pool."},
        "Gig Economy & Flexible Work": {"competitors": "Marine staffing agencies (Brunel, NES Fircroft, Atlas Professionals, Airswift), contract/project-based maritime positions, offshore energy staffing", "threat": "MEDIUM-HIGH - Maritime staffing agencies offer higher day rates for contract work, rotation schedules, and international assignments. Popular among experienced mariners seeking flexibility."},
    },
    "military_recruitment": {
        "Primary Direct Competitor": {"competitors": "US Army (active, reserve, guard components in all 50 states), US Marines, US Navy, US Air Force, US Space Force, US Coast Guard", "threat": "HIGH - All military branches compete for the same 17-35 age demographic. Each offers unique branding, benefits (GI Bill, housing, healthcare), and career opportunities. Recruiting budgets exceed $500M annually across branches."},
        "Federal/National Service Rival": {"competitors": "AmeriCorps (75K members/yr), Peace Corps, FEMA Corps, Teach For America, Job Corps, National Guard Bureau (if recruiting for a specific state)", "threat": "MEDIUM - National service programs appeal to the same patriotic/service-minded demographic. AmeriCorps offers education awards ($6,895/yr). They compete for the idealistic, mission-driven segment."},
        "Education & Training Programs": {"competitors": "ROTC programs (all branches at 1,700+ colleges), Service Academies (West Point, Annapolis, Air Force Academy, Coast Guard Academy), state-funded vocational programs", "threat": "MEDIUM-HIGH - ROTC and service academies create strong pipelines. Full-ride scholarships worth $100K+ make them very competitive. They may capture candidates before enlisted recruiting can reach them."},
        "Private Sector / Specialized Companies": {"competitors": "Defense contractors (Lockheed Martin, Raytheon, Northrop Grumman, Boeing Defense), private military contractors (Academi/Blackwater successor), cybersecurity firms, tech companies", "threat": "HIGH - Private sector offers higher base pay (often 30-50% more), no deployments, and similar technical work. Defense contractors actively recruit from the same STEM and technically skilled pool."},
        "Public Sector / Government": {"competitors": "Federal law enforcement (FBI, CBP, DEA, Secret Service, ATF, US Marshals), state police, fire departments, EMT/paramedic services, TSA", "threat": "MEDIUM-HIGH - Law enforcement and first responder careers offer similar benefits (pension, healthcare, camaraderie, mission) without deployment risk. They compete for the same physically fit, service-oriented candidates."},
        "Gig Economy & Flexible Work": {"competitors": "Amazon warehouse/delivery ($19-$25/hr), UPS/FedEx ($20-$28/hr), ride-share (Uber/Lyft), DoorDash/food delivery, construction day labor, remote freelancing", "threat": "MEDIUM - Gig economy offers immediate income without commitment. For 17-24 year olds weighing military vs. civilian options, the appeal of instant flexible income vs. 4+ year commitment is a real factor."},
    },
    "tech_engineering": {
        "Primary Direct Competitor": {"competitors": "Google/Alphabet (182K employees), Amazon/AWS (1.5M+ employees), Microsoft (221K employees), Meta (67K employees), Apple (164K employees)", "threat": "HIGH - FAANG/Big Tech companies offer top-of-market compensation ($150K-$400K+ TC for engineers), equity grants, and strong employer brands. They set the market for engineering talent."},
        "Federal/National Service Rival": {"competitors": "NSA, CIA, FBI (cyber division), DARPA, DoD/DISA, NASA, national labs (Sandia, LLNL, LANL, Oak Ridge)", "threat": "MEDIUM - Government tech roles offer security clearance (valuable for career), mission-driven work, and stability. Pay is lower but PSLF and pension benefits partially compensate."},
        "Education & Training Programs": {"competitors": "Coding bootcamps (App Academy, Hack Reactor, Flatiron), online platforms (Coursera, Udemy, Lambda School), university CS programs (Stanford, MIT, CMU, Georgia Tech)", "threat": "MEDIUM - Bootcamps produce 30K+ graduates/year. They pipeline talent directly to employers through hiring partnerships. University programs create strong alumni networks."},
        "Private Sector / Specialized Companies": {"competitors": "Startups (funded by VC, offering equity), consulting firms (McKinsey Digital, Deloitte Digital, Accenture), fintech (Stripe, Square, Plaid), SaaS companies (Salesforce, ServiceNow)", "threat": "HIGH - Startups offer equity upside and rapid career growth. Mid-size tech companies offer better work-life balance than FAANG. Consulting firms offer variety and rapid skill development."},
        "Public Sector / Government": {"competitors": "US Digital Service, 18F (GSA), state digital services offices, city tech departments, public utility tech teams", "threat": "LOW-MEDIUM - Government digital services are growing but still struggle to compete on compensation. Appeal is mission-driven work and work-life balance."},
        "Gig Economy & Flexible Work": {"competitors": "Toptal, Upwork, Fiverr (freelance platforms), consulting via LLC, Turing (remote global engineers), Gun.io, Arc.dev", "threat": "MEDIUM - Freelance engineering can pay $100-$250/hr for senior talent. Remote-first companies and freelance platforms offer ultimate flexibility. Growing trend among experienced engineers."},
    },
    "general_entry_level": {
        "Primary Direct Competitor": {"competitors": "Amazon ($19-$25/hr + benefits, 750K+ warehouse workers), Walmart ($14-$19/hr, 1.6M US employees), Target ($15-$24/hr), Costco ($17-$29/hr), Home Depot ($15-$22/hr)", "threat": "HIGH - Major retailers and warehouse operations offer immediate employment, no experience required, competitive starting wages, and benefits. Amazon's signing bonuses ($1K-$3K) attract entry-level workers."},
        "Federal/National Service Rival": {"competitors": "US Postal Service (USPS, 640K employees), TSA, Census Bureau (seasonal), National Park Service, federal administrative positions (GS-1 through GS-5)", "threat": "MEDIUM - Federal entry-level positions offer stability, benefits, and pension but have slower hiring processes. USPS is a major competitor for delivery/logistics roles."},
        "Education & Training Programs": {"competitors": "Community colleges (offering associates + certificates), workforce development boards, state employment services, vocational schools, online certifications (Google Certificates, CompTIA)", "threat": "LOW-MEDIUM - These programs often pipeline graduates to specific employers. Google Career Certificates partner with 150+ employers for direct placement."},
        "Private Sector / Specialized Companies": {"competitors": "Fast food chains (McDonald's $13-$18/hr, Chick-fil-A $15-$19/hr), retail (Starbucks $15-$24/hr + benefits), customer service centers (remote), hotel chains (Marriott, Hilton)", "threat": "HIGH - Quick-service restaurants and retail offer flexible scheduling, immediate start, and increasingly competitive wages. Starbucks' tuition reimbursement (ASU online) is a strong differentiator."},
        "Public Sector / Government": {"competitors": "City/county government entry-level (clerks, maintenance, parks & rec), public school support staff, state administrative assistants, public transit", "threat": "MEDIUM - Government entry-level roles offer pension, stability, and benefits. Often slower to hire but competitive long-term for workers valuing job security."},
        "Gig Economy & Flexible Work": {"competitors": "Uber/Lyft (drivers), DoorDash/UberEats/Instacart (delivery), Amazon Flex, TaskRabbit, Rover (pet care), Shipt, care.com (caregiving)", "threat": "HIGH - Gig platforms offer instant income, complete flexibility, and low barriers to entry. Young workers (18-25) increasingly choose gig work over traditional entry-level employment."},
    },
    "legal_services": {
        "Primary Direct Competitor": {"competitors": "AmLaw 100 firms (Kirkland & Ellis, Latham & Watkins, DLA Piper, Baker McKenzie), Big Four legal (Deloitte Legal, EY Law, PwC Legal, KPMG Legal)", "threat": "HIGH - Top law firms offer $215K+ starting salary for associates, prestigious brand, and clear partnership track. Big Four expanding legal services with global reach."},
        "Private Sector / Specialized Companies": {"competitors": "Legal staffing (Robert Half Legal, Special Counsel, Axiom), legal tech companies (LegalZoom, Rocket Lawyer, Clio), corporate legal departments (in-house)", "threat": "HIGH - In-house legal departments offer better work-life balance than firms. Legal staffing provides flexibility. Legal tech companies attract tech-savvy attorneys."},
        "Public Sector / Government": {"competitors": "DOJ, SEC, FTC, state attorney general offices, public defender offices, Legal Aid Society, federal courts (clerks)", "threat": "MEDIUM - Government legal roles offer PSLF loan forgiveness, stability, and mission-driven work. Federal clerkships are prestigious career accelerators."},
        "Education & Training Programs": {"competitors": "T14 law schools (Yale, Stanford, Harvard, Columbia, Chicago), LLM programs, bar review companies (Barbri, Themis), legal incubators", "threat": "MEDIUM - Top law schools pipeline directly to BigLaw. LLM programs attract international lawyers. Career services offices are key gatekeepers."},
        "Gig Economy & Flexible Work": {"competitors": "Freelance legal platforms (UpCounsel, Priori, Lawyer Exchange), contract attorney positions, virtual law practices, AI-assisted legal services", "threat": "MEDIUM - Contract lawyering and virtual practices offer flexibility. AI tools changing legal landscape but not yet displacing attorney roles."},
    },
    "finance_banking": {
        "Primary Direct Competitor": {"competitors": "JPMorgan Chase (293K employees), Goldman Sachs ($400K+ avg comp), Morgan Stanley, Bank of America, Citigroup, Wells Fargo", "threat": "HIGH - Bulge bracket banks dominate finance recruiting with top-tier compensation (analyst: $110K-$200K, VP: $250K-$500K), prestige, and global mobility."},
        "Private Sector / Specialized Companies": {"competitors": "Hedge funds (Citadel, Bridgewater, Two Sigma), PE firms (Blackstone, KKR, Apollo), fintech (Stripe, Plaid, Robinhood, Square), consulting (McKinsey, BCG, Bain)", "threat": "HIGH - Hedge funds offer $300K-$1M+ comp for quants. Fintech offers equity upside. PE firms offer carry. Consulting offers variety and skill development."},
        "Public Sector / Government": {"competitors": "Federal Reserve, SEC, FDIC, OCC, Treasury Department, state banking regulators, World Bank, IMF", "threat": "MEDIUM - Regulatory roles offer stability, pension, and transition value (revolving door). Central banking offers prestige and policy influence."},
        "Education & Training Programs": {"competitors": "Top MBA programs (Wharton, HBS, Columbia, Chicago Booth, Stern), CFA Institute, Bloomberg Terminal training, financial modeling bootcamps", "threat": "MEDIUM-HIGH - Top MBA programs are primary pipeline for IB/PE/HF. CFA designation creates career moats. Finance bootcamps growing for career changers."},
        "Gig Economy & Flexible Work": {"competitors": "Freelance CFO/controller services, fractional finance roles, crypto/DeFi platforms, retail trading communities, algorithmic trading platforms", "threat": "MEDIUM - Fractional CFO trend growing. Crypto/DeFi attracting younger finance talent with decentralization philosophy and token-based compensation."},
    },
    "mental_health": {
        "Primary Direct Competitor": {"competitors": "Telehealth platforms (BetterHelp 30K+ therapists, Talkspace, Cerebral, Lyra Health, Spring Health), large health systems (Kaiser BH, Providence BH, Intermountain BH)", "threat": "HIGH - Telehealth platforms offer fully remote work, flexible schedules, and competitive pay ($70-$100/hr). Health systems offer benefits and referral pipelines."},
        "Private Sector / Specialized Companies": {"competitors": "Group therapy practices (Thriveworks, LifeStance Health 5K+ clinicians, Refresh Mental Health), EAP providers (ComPsych, Beacon Health), substance abuse centers (Hazelden Betty Ford)", "threat": "HIGH - LifeStance and similar roll-ups aggressively recruit with equity, admin support, and marketing. EAP providers offer steady caseloads."},
        "Public Sector / Government": {"competitors": "Community Mental Health Centers (CMHCs), VA mental health (largest MH employer), state psychiatric hospitals, school districts (counselors), SAMHSA-funded programs", "threat": "MEDIUM-HIGH - VA offers PSLF, benefits, and no-show protection. CMHCs serve underserved populations with loan repayment programs (NHSC). School counselors offer summers off."},
        "Education & Training Programs": {"competitors": "PhD/PsyD programs (producing 5K+ clinical psychologists/year), MSW programs (producing 20K+ social workers/year), postdoctoral fellowship sites, APA-accredited internships", "threat": "MEDIUM - Training programs pipeline graduates to specific sites. Internship/postdoc training creates institutional loyalty. Limited supervised hours requirements create bottlenecks."},
        "Gig Economy & Flexible Work": {"competitors": "Private practice (solo), therapy marketplace apps (Alma, Headway, Grow Therapy - handle insurance billing), life coaching (ICF certified), mental wellness apps (Calm, Headspace)", "threat": "HIGH - Platforms like Headway/Alma remove billing burden of private practice, enabling therapists to earn $100-$150/session. Growing trend away from organizational employment."},
    },
    "retail_consumer": {
        "Primary Direct Competitor": {"competitors": "Walmart (1.6M US employees), Amazon (fulfillment + retail), Target (440K employees), Costco ($17-$29/hr), Home Depot, Lowe's, TJX Companies (Marshalls/TJ Maxx)", "threat": "HIGH - Major retailers compete on wages ($15-$24/hr), tuition benefits, flexible scheduling, and career advancement. Walmart and Target have expanded benefits significantly."},
        "Private Sector / Specialized Companies": {"competitors": "Luxury retail (LVMH, Nordstrom, Neiman Marcus), specialty (Ulta Beauty, Bath & Body Works, Sephora), e-commerce (Shopify merchants, DTC brands), restaurant groups", "threat": "HIGH - Specialty and luxury retailers offer higher commission, employee discounts, and brand prestige. E-commerce growth creating new roles in digital/omnichannel."},
        "Public Sector / Government": {"competitors": "State employment agencies, workforce development boards, vocational rehab programs, community action agencies", "threat": "LOW - Government doesn't directly compete for retail talent but workforce programs can redirect job seekers to other sectors."},
        "Education & Training Programs": {"competitors": "Retail management certificate programs, community college business programs, company-sponsored management training (Walmart Academy, Target leadership), NRF Foundation", "threat": "MEDIUM - Internal training programs at major retailers pipeline floor staff to management. Walmart Academy has trained 1M+ associates."},
        "Gig Economy & Flexible Work": {"competitors": "DoorDash, Instacart (grocery delivery), Shipt, Uber/Lyft, Amazon Flex, Mercari/Poshmark (reselling), social media influencer marketing", "threat": "HIGH - Gig delivery and reselling platforms offer more flexibility than retail shifts. Younger workers increasingly prefer gig work over traditional retail schedules."},
    },
    "aerospace_defense": {
        "Primary Direct Competitor": {"competitors": "Lockheed Martin (116K employees), RTX/Raytheon (185K), Northrop Grumman (90K), Boeing Defense (66K), General Dynamics (106K), L3Harris (50K)", "threat": "HIGH - Defense primes offer competitive salaries ($90K-$180K for engineers), security clearance sponsorship, and long-term program stability. They dominate the cleared talent market."},
        "Private Sector / Specialized Companies": {"competitors": "SpaceX (13K employees, high intensity), Blue Origin, Virgin Galactic, Anduril Industries, Shield AI, Palantir, defense tech startups", "threat": "HIGH - New space and defense tech companies offer equity, innovation, and startup culture. SpaceX's mission appeal and Anduril's tech-forward approach attract top engineering talent."},
        "Federal/National Service Rival": {"competitors": "NASA (18K civil servants), DoD civilian workforce (750K+), DARPA, national labs (Sandia, LLNL, JPL, APL), NRO, NGA, intelligence agencies", "threat": "MEDIUM-HIGH - Federal aerospace roles offer stability, pension, and clearance. NASA and JPL offer prestige and mission. National labs offer R&D freedom and work-life balance."},
        "Education & Training Programs": {"competitors": "Top aerospace programs (MIT, Caltech, Georgia Tech, Purdue, Michigan, Stanford), military service academies, Air Force Institute of Technology, DAU (Defense Acquisition University)", "threat": "MEDIUM - Top programs pipeline directly to primes and NASA. ROTC programs create military-to-defense-contractor pipeline. DAU credentials valued in acquisition roles."},
        "Gig Economy & Flexible Work": {"competitors": "Defense consulting firms (Booz Allen, SAIC, Leidos, CACI, ManTech), independent consultants with clearances, defense staffing (KGS Group, Jacobs)", "threat": "MEDIUM - Defense contractors and IT staffing offer project variety and sometimes higher hourly rates. Independent consulting grows among senior cleared professionals."},
    },
    "pharma_biotech": {
        "Primary Direct Competitor": {"competitors": "Pfizer (83K employees), Johnson & Johnson (130K), Roche (100K), Novartis (105K), Merck (69K), AbbVie (50K), Eli Lilly, AstraZeneca, Sanofi, GSK", "threat": "HIGH - Big Pharma offers top-tier compensation ($120K-$250K for scientists, $200K+ for medical directors), R&D resources, publication opportunities, and global mobility."},
        "Private Sector / Specialized Companies": {"competitors": "CROs (IQVIA 82K employees, Syneos Health, PPD/Thermo Fisher, Parexel, Covance), biotech startups (Moderna, BioNTech), CDMO/CMOs (Catalent, Lonza, Samsung Biologics)", "threat": "HIGH - CROs offer diverse therapeutic exposure and faster career growth. Biotech startups offer equity upside. CDMOs expanding and competing for manufacturing talent."},
        "Public Sector / Government": {"competitors": "FDA (18K employees), NIH (20K scientists + 50K trainees), CDC, BARDA, national labs, state public health labs, academic medical centers", "threat": "MEDIUM-HIGH - NIH postdocs are primary pipeline for research talent. FDA experience highly valued. Academic positions offer research freedom and tenure track."},
        "Education & Training Programs": {"competitors": "PhD programs (producing 12K+ life science PhDs/year), PharmD programs, postdoctoral fellowships, medical residency programs, clinical research certification (ACRP, SoCRA)", "threat": "MEDIUM - PhD overproduction creates buyer's market for research talent. PharmD programs pipeline to pharma/clinical roles. Certifications create specialist supply."},
        "Gig Economy & Flexible Work": {"competitors": "Medical writing freelancers, contract CRAs, consulting pharmacists, regulatory consulting firms (Parexel Consulting, ICON), scientific staffing (Kelly Science, Yoh)", "threat": "MEDIUM - Contract CRA and medical writing roles growing. Scientific staffing agencies offer variety and higher hourly rates. Remote monitoring expanding contract opportunities."},
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# SALARY DATA BY ROLE CATEGORY
# ═══════════════════════════════════════════════════════════════════════════════

ROLE_SALARY_RANGES = {
    # ── Healthcare & Medical ──
    "Physicians": "$220,000 - $400,000+",
    "Nurses (RN)": "$65,000 - $95,000",
    "Registered Nurse": "$65,000 - $95,000",
    "Nurse Practitioners": "$95,000 - $140,000",
    "Physician Assistants": "$90,000 - $130,000",
    "Behavioral Health Specialists": "$55,000 - $85,000",
    "Medical Students": "Stipend-based ($35,000 - $65,000 during residency)",
    "Residents": "$58,000 - $78,000",
    "Medical Assistant": "$32,000 - $42,000",
    "Travel Nurse": "$90,000 - $180,000",
    "CNA": "$28,000 - $38,000",
    "Certified Nursing Assistant": "$28,000 - $38,000",
    "Licensed Practical Nurse": "$42,000 - $58,000",
    "LPN": "$42,000 - $58,000",
    "Phlebotomist": "$30,000 - $40,000",
    "Pharmacy Technician": "$32,000 - $42,000",
    "Dental Hygienist": "$65,000 - $85,000",
    "Physical Therapist": "$75,000 - $100,000",
    "Occupational Therapist": "$72,000 - $95,000",
    "Radiologic Technologist": "$55,000 - $78,000",
    "Respiratory Therapist": "$55,000 - $75,000",
    "EMT": "$30,000 - $42,000",
    "Paramedic": "$38,000 - $58,000",
    "Home Health Aide": "$25,000 - $33,000",

    # ── Blue Collar / Skilled Trades ──
    "Diesel Mechanics": "$48,000 - $78,000",
    "Welders": "$40,000 - $72,000",
    "Electricians": "$52,000 - $85,000",
    "HVAC Technicians": "$45,000 - $75,000",
    "Heavy Equipment Operators": "$45,000 - $72,000",
    "Construction Workers": "$35,000 - $65,000",
    "Manufacturing Technicians": "$38,000 - $62,000",
    "Plumber": "$48,000 - $80,000",
    "Carpenter": "$40,000 - $68,000",
    "Solar Panel Installer": "$40,000 - $60,000",
    "Aircraft Mechanic": "$50,000 - $90,000",
    "Production Associate": "$35,000 - $52,000",
    "Quality Inspector": "$42,000 - $62,000",
    "CNC Machinist": "$40,000 - $65,000",
    "Maintenance Technician": "$42,000 - $65,000",
    "Forklift Operator": "$32,000 - $44,000",

    # ── Maritime & Marine ──
    "Marine Diesel Mechanics": "$52,000 - $85,000",
    "Ship Repair Technicians": "$48,000 - $78,000",
    "Marine Engineers": "$75,000 - $120,000",
    "Marine Electricians": "$55,000 - $88,000",
    "Field Service Engineers": "$65,000 - $105,000",

    # ── Technology & Engineering ──
    "Software Engineers": "$120,000 - $200,000",
    "Software Engineer": "$120,000 - $200,000",
    "SDE": "$120,000 - $200,000",
    "Software Development Engineer": "$120,000 - $200,000",
    "Data Scientists": "$130,000 - $220,000",
    "Data Scientist": "$130,000 - $220,000",
    "ML Engineer": "$130,000 - $220,000",
    "Machine Learning Engineer": "$130,000 - $220,000",
    "DevOps Engineers": "$100,000 - $165,000",
    "Product Managers": "$120,000 - $180,000",
    "Product Manager": "$120,000 - $180,000",
    "UX Designers": "$80,000 - $140,000",
    "IT Support Technician": "$42,000 - $65,000",
    "Help Desk Technician": "$38,000 - $55,000",
    "Systems Administrator": "$60,000 - $95,000",
    "Network Engineer": "$70,000 - $115,000",
    "Cloud Engineer": "$110,000 - $170,000",
    "Frontend Developer": "$90,000 - $160,000",
    "Backend Developer": "$100,000 - $175,000",
    "Full Stack Developer": "$95,000 - $170,000",
    "QA Engineer": "$70,000 - $110,000",
    "Security Engineer": "$110,000 - $175,000",
    "Data Engineer": "$110,000 - $180,000",
    "Technical Program Manager": "$120,000 - $190,000",
    "Scrum Master": "$85,000 - $130,000",
    "Business Analyst": "$65,000 - $100,000",

    # ── General / Entry-Level / Service ──
    "Customer Service": "$30,000 - $45,000",
    "Customer Service Rep": "$30,000 - $45,000",
    "Customer Service Representative": "$30,000 - $45,000",
    "Retail": "$28,000 - $42,000",
    "Administrative": "$35,000 - $55,000",
    "Administrative Assistant": "$35,000 - $55,000",
    "Office Assistant": "$30,000 - $42,000",
    "Receptionist": "$28,000 - $38,000",
    "Sales": "$40,000 - $75,000 + commission",
    "Barista": "$28,000 - $38,000",
    "Cashier": "$28,000 - $38,000",
    "Housekeeper": "$28,000 - $37,000",
    "Housekeeping": "$28,000 - $37,000",
    "Room Attendant": "$28,000 - $37,000",
    "Front Desk Agent": "$30,000 - $42,000",
    "Front Desk Clerk": "$30,000 - $42,000",
    "Hotel Front Desk": "$30,000 - $42,000",
    "Line Cook": "$32,000 - $46,000",
    "Prep Cook": "$28,000 - $38,000",
    "Dishwasher": "$25,000 - $33,000",
    "Server": "$22,000 - $35,000 + tips",
    "Bartender": "$25,000 - $40,000 + tips",
    "Host/Hostess": "$24,000 - $32,000",
    "Food Runner": "$24,000 - $32,000",
    "Janitor": "$28,000 - $38,000",
    "Custodian": "$28,000 - $38,000",
    "Security Guard": "$30,000 - $42,000",
    "Landscaper": "$28,000 - $40,000",

    # ── Logistics, Warehouse & Delivery ──
    "Warehouse": "$33,000 - $44,000",
    "Warehouse Associate": "$33,000 - $44,000",
    "Warehouse Worker": "$33,000 - $44,000",
    "Package Handler": "$31,000 - $42,000",
    "Delivery Driver": "$37,000 - $52,000",
    "CDL Driver": "$48,000 - $72,000",
    "Truck Driver": "$45,000 - $68,000",
    "Courier": "$32,000 - $45,000",
    "Supply Chain Manager": "$80,000 - $120,000",
    "Supply Chain Analyst": "$65,000 - $95,000",
    "Logistics Coordinator": "$38,000 - $55,000",
    "Inventory Specialist": "$32,000 - $45,000",
    "Operations Manager": "$70,000 - $110,000",
    "Dispatch Coordinator": "$35,000 - $48,000",
    "Picker/Packer": "$30,000 - $40,000",
    "Shipping and Receiving Clerk": "$32,000 - $42,000",
    "Dock Worker": "$33,000 - $45,000",

    # ── Military ──
    "Infantry": "$24,000 - $42,000 + housing/benefits",
    "Artillery": "$24,000 - $42,000 + housing/benefits",
    "Combat Engineers": "$24,000 - $45,000 + housing/benefits",
    "Signal Corps": "$26,000 - $48,000 + housing/benefits",
    "Medical Corps": "$45,000 - $120,000 + housing/benefits",
    "Military Police": "$24,000 - $42,000 + housing/benefits",
    "Aviation": "$35,000 - $65,000 + housing/benefits (enlisted) / $55K-$120K (officers)",

    # ── Legal Services ──
    "Attorneys": "$90,000 - $215,000+",
    "Paralegals": "$45,000 - $75,000",
    "Legal Assistants": "$35,000 - $55,000",
    "Compliance Officers": "$80,000 - $150,000",
    "Compliance Officer": "$80,000 - $150,000",
    "Corporate Counsel": "$150,000 - $300,000",
    "Litigation Support": "$50,000 - $85,000",

    # ── Finance & Banking ──
    "Financial Analysts": "$65,000 - $120,000",
    "Investment Bankers": "$110,000 - $250,000+ (base + bonus)",
    "Risk Managers": "$85,000 - $160,000",
    "Portfolio Managers": "$100,000 - $250,000+",
    "Actuaries": "$75,000 - $150,000",
    "Bank Teller": "$32,000 - $45,000",
    "Teller": "$32,000 - $45,000",
    "Branch Banker": "$45,000 - $70,000",
    "Personal Banker": "$45,000 - $70,000",
    "Quantitative Analyst": "$150,000 - $300,000",
    "Management Consultant": "$90,000 - $180,000",
    "Senior Auditor": "$70,000 - $120,000",
    "Tax Associate": "$55,000 - $80,000",
    "Accountant": "$50,000 - $80,000",
    "Bookkeeper": "$38,000 - $55,000",
    "Revenue Manager": "$70,000 - $110,000",
    "Credit Analyst": "$55,000 - $85,000",
    "Loan Officer": "$45,000 - $80,000",
    "Financial Advisor": "$60,000 - $120,000 + commission",
    "Underwriter": "$55,000 - $90,000",

    # ── Mental Health ──
    "Psychologists": "$80,000 - $130,000",
    "Licensed Therapists (LCSW)": "$55,000 - $90,000",
    "Psychiatric Nurses": "$75,000 - $110,000",
    "Counselors": "$45,000 - $70,000",
    "Social Workers": "$45,000 - $72,000",
    "Behavioral Analysts": "$55,000 - $85,000",

    # ── Retail & Consumer ──
    "Store Managers": "$50,000 - $80,000",
    "Store Manager": "$50,000 - $80,000",
    "Sales Associates": "$28,000 - $42,000 + commission",
    "Merchandisers": "$35,000 - $55,000",
    "Inventory Managers": "$45,000 - $70,000",
    "District Managers": "$75,000 - $120,000",
    "Buyers": "$50,000 - $90,000",
    "Shift Supervisor": "$32,000 - $45,000",
    "Shift Lead": "$32,000 - $45,000",
    "Assistant Manager": "$38,000 - $55,000",
    "Loss Prevention": "$32,000 - $50,000",
    "Visual Merchandiser": "$32,000 - $48,000",

    # ── Aerospace & Defense ──
    "Aerospace Engineers": "$85,000 - $150,000",
    "Systems Engineers": "$90,000 - $160,000",
    "Program Managers": "$100,000 - $180,000",
    "Test Engineers": "$75,000 - $120,000",
    "Quality Engineers": "$70,000 - $110,000",
    "Avionics Technicians": "$55,000 - $90,000",

    # ── Pharma & Biotech ──
    "Clinical Research Associates": "$65,000 - $100,000",
    "Biostatisticians": "$90,000 - $150,000",
    "Medical Science Liaisons": "$120,000 - $200,000",
    "Regulatory Affairs": "$80,000 - $140,000",
    "Lab Technicians": "$40,000 - $65,000",
    "Drug Safety Officers": "$75,000 - $120,000",

    # ── Education ──
    "High School Teacher": "$50,000 - $75,000",
    "Teacher": "$48,000 - $72,000",
    "Elementary Teacher": "$45,000 - $68,000",
    "School Principal": "$85,000 - $130,000",
    "Assistant Principal": "$70,000 - $100,000",
    "Substitute Teacher": "$80 - $150/day (daily rate)",
    "Special Education Aide": "$28,000 - $38,000",
    "Special Education Teacher": "$48,000 - $75,000",
    "Paraprofessional": "$25,000 - $35,000",
    "School Counselor": "$50,000 - $75,000",
    "Professor": "$70,000 - $140,000",
    "Tutor": "$28,000 - $45,000",
    "Instructional Designer": "$60,000 - $90,000",

    # ── Marketing & Creative ──
    "Marketing Manager": "$75,000 - $140,000",
    "Marketing Coordinator": "$42,000 - $60,000",
    "Content Writer": "$45,000 - $72,000",
    "Graphic Designer": "$45,000 - $75,000",
    "Social Media Manager": "$48,000 - $78,000",
    "SEO Specialist": "$50,000 - $80,000",
    "Copywriter": "$45,000 - $72,000",
    "Brand Manager": "$70,000 - $120,000",
    "Public Relations Specialist": "$48,000 - $78,000",
    "Event Coordinator": "$38,000 - $58,000",

    # ── Human Resources ──
    "HR Manager": "$70,000 - $110,000",
    "HR Generalist": "$50,000 - $75,000",
    "Recruiter": "$50,000 - $80,000",
    "Talent Acquisition Specialist": "$55,000 - $90,000",
    "HR Coordinator": "$38,000 - $55,000",
    "Compensation Analyst": "$60,000 - $90,000",
    "Training Specialist": "$48,000 - $72,000",

    # ── Gig / App-based ──
    "Uber Driver": "$37,000 - $52,000",
    "Rideshare Driver": "$37,000 - $52,000",
    "DoorDash Driver": "$37,000 - $52,000",
    "Instacart Shopper": "$28,000 - $42,000",
    "Amazon Flex Driver": "$37,000 - $52,000",

    # ── Energy & Utilities ──
    "Wind Turbine Technician": "$48,000 - $72,000",
    "Power Plant Operator": "$60,000 - $90,000",
    "Utility Lineworker": "$55,000 - $85,000",
    "Environmental Technician": "$38,000 - $58,000",
    "Petroleum Engineer": "$90,000 - $170,000",

    # ── Construction & Real Estate ──
    "Project Manager": "$75,000 - $130,000",
    "Construction Manager": "$80,000 - $130,000",
    "Estimator": "$55,000 - $85,000",
    "Real Estate Agent": "$40,000 - $90,000 + commission",
    "Property Manager": "$45,000 - $72,000",
    "Superintendent": "$70,000 - $110,000",
    "Site Supervisor": "$55,000 - $80,000",
}

# ═══════════════════════════════════════════════════════════════════════════════
# FUZZY ROLE MATCHING - Maps common role title variations to canonical entries
# ═══════════════════════════════════════════════════════════════════════════════

# Normalized keyword patterns mapped to canonical role names in ROLE_SALARY_RANGES.
# Each tuple: (list_of_keywords_all_must_match, canonical_role_name)
# Keywords are checked against the normalized (lowercase, stripped) role title.
_ROLE_FUZZY_PATTERNS = [
    # Technology & Engineering
    (["software", "engineer"], "Software Engineer"),
    (["software", "develop"], "Software Engineer"),
    (["sde"], "SDE"),
    (["full", "stack"], "Full Stack Developer"),
    (["frontend", "dev"], "Frontend Developer"),
    (["front", "end", "dev"], "Frontend Developer"),
    (["backend", "dev"], "Backend Developer"),
    (["back", "end", "dev"], "Backend Developer"),
    (["data", "scientist"], "Data Scientist"),
    (["machine", "learning"], "ML Engineer"),
    (["ml", "engineer"], "ML Engineer"),
    (["ai", "engineer"], "ML Engineer"),
    (["devops"], "DevOps Engineers"),
    (["site", "reliability"], "DevOps Engineers"),
    (["sre"], "DevOps Engineers"),
    (["product", "manager"], "Product Manager"),
    (["ux", "design"], "UX Designers"),
    (["ui", "design"], "UX Designers"),
    (["it", "support"], "IT Support Technician"),
    (["help", "desk"], "Help Desk Technician"),
    (["sys", "admin"], "Systems Administrator"),
    (["system", "admin"], "Systems Administrator"),
    (["cloud", "engineer"], "Cloud Engineer"),
    (["qa", "engineer"], "QA Engineer"),
    (["quality", "assurance", "engineer"], "QA Engineer"),
    (["security", "engineer"], "Security Engineer"),
    (["cybersecurity"], "Security Engineer"),
    (["data", "engineer"], "Data Engineer"),
    (["network", "engineer"], "Network Engineer"),
    (["business", "analyst"], "Business Analyst"),
    (["scrum", "master"], "Scrum Master"),
    (["technical", "program"], "Technical Program Manager"),

    # Delivery / Driving / Gig
    (["delivery", "driver"], "Delivery Driver"),
    (["uber", "driver"], "Delivery Driver"),
    (["ubereats"], "Delivery Driver"),
    (["doordash"], "Delivery Driver"),
    (["instacart"], "Instacart Shopper"),
    (["rideshare"], "Delivery Driver"),
    (["courier"], "Courier"),
    (["cdl", "driver"], "CDL Driver"),
    (["truck", "driver"], "Truck Driver"),
    (["driver", "partner"], "Delivery Driver"),
    (["delivery", "partner"], "Delivery Driver"),
    (["last", "mile"], "Delivery Driver"),

    # Warehouse & Logistics
    (["warehouse", "associate"], "Warehouse Associate"),
    (["warehouse", "worker"], "Warehouse Associate"),
    (["warehouse", "team"], "Warehouse Associate"),
    (["fulfillment", "associate"], "Warehouse Associate"),
    (["fulfillment", "center"], "Warehouse Associate"),
    (["package", "handler"], "Package Handler"),
    (["sortation"], "Package Handler"),
    (["picker", "packer"], "Picker/Packer"),
    (["pick", "pack"], "Picker/Packer"),
    (["dock", "worker"], "Dock Worker"),
    (["forklift"], "Forklift Operator"),
    (["supply", "chain", "manager"], "Supply Chain Manager"),
    (["supply", "chain", "analyst"], "Supply Chain Analyst"),
    (["logistics", "coordinator"], "Logistics Coordinator"),
    (["operations", "manager"], "Operations Manager"),
    (["dispatch"], "Dispatch Coordinator"),
    (["shipping", "receiving"], "Shipping and Receiving Clerk"),

    # Healthcare
    (["registered", "nurse"], "Registered Nurse"),
    (["travel", "nurse"], "Travel Nurse"),
    (["travel", "rn"], "Travel Nurse"),
    (["medical", "assistant"], "Medical Assistant"),
    (["cna"], "CNA"),
    (["certified", "nursing", "assistant"], "CNA"),
    (["nurse", "practitioner"], "Nurse Practitioners"),
    (["physician", "assistant"], "Physician Assistants"),
    (["lpn"], "LPN"),
    (["licensed", "practical"], "LPN"),
    (["phlebotom"], "Phlebotomist"),
    (["pharmacy", "tech"], "Pharmacy Technician"),
    (["dental", "hygien"], "Dental Hygienist"),
    (["physical", "therap"], "Physical Therapist"),
    (["occupational", "therap"], "Occupational Therapist"),
    (["respiratory", "therap"], "Respiratory Therapist"),
    (["emt"], "EMT"),
    (["paramedic"], "Paramedic"),
    (["home", "health"], "Home Health Aide"),
    (["radiolog"], "Radiologic Technologist"),

    # Hospitality / Food Service
    (["barista"], "Barista"),
    (["cashier"], "Cashier"),
    (["housekeeper"], "Housekeeper"),
    (["housekeeping"], "Housekeeper"),
    (["room", "attendant"], "Room Attendant"),
    (["front", "desk"], "Front Desk Agent"),
    (["line", "cook"], "Line Cook"),
    (["prep", "cook"], "Prep Cook"),
    (["dishwasher"], "Dishwasher"),
    (["server"], "Server"),
    (["waiter"], "Server"),
    (["waitress"], "Server"),
    (["bartender"], "Bartender"),
    (["host"], "Host/Hostess"),
    (["hostess"], "Host/Hostess"),
    (["food", "runner"], "Food Runner"),

    # Education
    (["high", "school", "teacher"], "High School Teacher"),
    (["school", "principal"], "School Principal"),
    (["assistant", "principal"], "Assistant Principal"),
    (["substitute", "teacher"], "Substitute Teacher"),
    (["special", "education", "aide"], "Special Education Aide"),
    (["special", "ed", "aide"], "Special Education Aide"),
    (["special", "education", "teacher"], "Special Education Teacher"),
    (["paraprofessional"], "Paraprofessional"),
    (["school", "counselor"], "School Counselor"),
    (["instructional", "design"], "Instructional Designer"),
    (["professor"], "Professor"),
    (["teacher"], "Teacher"),

    # Finance & Banking
    (["bank", "teller"], "Bank Teller"),
    (["teller"], "Teller"),
    (["branch", "banker"], "Branch Banker"),
    (["personal", "banker"], "Personal Banker"),
    (["quantitative", "analyst"], "Quantitative Analyst"),
    (["quant", "analyst"], "Quantitative Analyst"),
    (["management", "consultant"], "Management Consultant"),
    (["senior", "auditor"], "Senior Auditor"),
    (["tax", "associate"], "Tax Associate"),
    (["revenue", "manager"], "Revenue Manager"),
    (["accountant"], "Accountant"),
    (["bookkeeper"], "Bookkeeper"),
    (["credit", "analyst"], "Credit Analyst"),
    (["loan", "officer"], "Loan Officer"),
    (["financial", "advisor"], "Financial Advisor"),
    (["underwriter"], "Underwriter"),
    (["compliance", "officer"], "Compliance Officer"),

    # Retail
    (["store", "manager"], "Store Manager"),
    (["shift", "supervisor"], "Shift Supervisor"),
    (["shift", "lead"], "Shift Lead"),
    (["assistant", "manager"], "Assistant Manager"),
    (["loss", "prevention"], "Loss Prevention"),
    (["visual", "merchandis"], "Visual Merchandiser"),

    # General / Admin
    (["admin", "assistant"], "Administrative Assistant"),
    (["office", "assistant"], "Office Assistant"),
    (["receptionist"], "Receptionist"),
    (["customer", "service"], "Customer Service Rep"),
    (["call", "center"], "Customer Service Rep"),
    (["security", "guard"], "Security Guard"),
    (["janitor"], "Janitor"),
    (["custodian"], "Custodian"),
    (["landscap"], "Landscaper"),

    # Trades
    (["solar", "panel", "install"], "Solar Panel Installer"),
    (["solar", "install"], "Solar Panel Installer"),
    (["aircraft", "mechanic"], "Aircraft Mechanic"),
    (["production", "associate"], "Production Associate"),
    (["production", "worker"], "Production Associate"),
    (["quality", "inspector"], "Quality Inspector"),
    (["cnc", "machinist"], "CNC Machinist"),
    (["maintenance", "tech"], "Maintenance Technician"),
    (["electrician"], "Electricians"),
    (["plumb"], "Plumber"),
    (["carpenter"], "Carpenter"),
    (["welder"], "Welders"),
    (["hvac"], "HVAC Technicians"),

    # Marketing
    (["marketing", "manager"], "Marketing Manager"),
    (["marketing", "coordinator"], "Marketing Coordinator"),
    (["content", "writer"], "Content Writer"),
    (["graphic", "design"], "Graphic Designer"),
    (["social", "media", "manager"], "Social Media Manager"),
    (["seo"], "SEO Specialist"),
    (["copywriter"], "Copywriter"),
    (["brand", "manager"], "Brand Manager"),
    (["public", "relations"], "Public Relations Specialist"),
    (["event", "coordinator"], "Event Coordinator"),

    # HR
    (["hr", "manager"], "HR Manager"),
    (["hr", "generalist"], "HR Generalist"),
    (["recruiter"], "Recruiter"),
    (["talent", "acquisition"], "Talent Acquisition Specialist"),
    (["hr", "coordinator"], "HR Coordinator"),
    (["training", "specialist"], "Training Specialist"),

    # Energy
    (["wind", "turbine"], "Wind Turbine Technician"),
    (["power", "plant"], "Power Plant Operator"),
    (["lineworker"], "Utility Lineworker"),
    (["petroleum", "engineer"], "Petroleum Engineer"),

    # Construction
    (["project", "manager"], "Project Manager"),
    (["construction", "manager"], "Construction Manager"),
    (["estimator"], "Estimator"),
    (["real", "estate", "agent"], "Real Estate Agent"),
    (["property", "manager"], "Property Manager"),
    (["superintendent"], "Superintendent"),
    (["site", "supervisor"], "Site Supervisor"),
]


def _fuzzy_match_role(role_title):
    """
    Attempt to match a role title to a canonical ROLE_SALARY_RANGES entry
    using keyword-based fuzzy matching. Returns the canonical role name or None.
    """
    title_lower = role_title.strip().lower()

    # Try each pattern - all keywords in the pattern must appear in the title
    for keywords, canonical in _ROLE_FUZZY_PATTERNS:
        if all(kw in title_lower for kw in keywords):
            return canonical
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# LOCATION PARSING & LOOKUP FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _normalize(text):
    """Normalize text for matching."""
    return re.sub(r'[^a-z0-9]', '', text.lower().strip())

def _extract_state(location_str):
    """Extract state abbreviation from location string like 'Portland, OR'."""
    location_str = location_str.strip()
    # Check for state abbreviation at end
    m = re.search(r',?\s*([A-Z]{2})\s*$', location_str)
    if m and m.group(1) in STATE_DATA:
        return m.group(1)
    # Check for full state name
    for abbr, data in STATE_DATA.items():
        if data["name"].lower() in location_str.lower():
            return abbr
    return None

def _find_metro(location_str):
    """Find matching metro area from location string."""
    norm = _normalize(location_str)
    # Direct matches
    for key, data in METRO_DATA.items():
        if _normalize(key) in norm or norm in _normalize(key):
            return key, data
    # City name matches
    city_part = re.sub(r',.*$', '', location_str).strip().lower()
    city_norm = _normalize(city_part)
    for key, data in METRO_DATA.items():
        if city_norm == _normalize(key) or _normalize(key).startswith(city_norm):
            return key, data
    # Fuzzy match on metro_name
    for key, data in METRO_DATA.items():
        if city_part.lower() in data.get("metro_name", "").lower():
            return key, data
    return None, None

def get_location_info(location_str):
    """Get comprehensive info for a location. Supports US and international locations."""
    # Check for international location first
    country = _detect_country(location_str)
    if country and country != "United States":
        cd = COUNTRY_DATA[country]
        return {
            "location": location_str,
            "state": None,
            "country": country,
            "region": cd.get("region", "Global"),
            "coli": cd["coli"],
            "population": cd["population"],
            "median_salary": cd["median_salary"],
            "unemployment": cd["unemployment"],
            "currency": cd.get("currency", "USD"),
            "metro_name": f"{location_str} ({country})",
            "major_employers": cd["top_industries"],
            "top_boards": cd.get("top_boards", ""),
            "is_international": True,
        }

    # US location handling
    state = _extract_state(location_str)
    metro_key, metro = _find_metro(location_str)

    info = {"location": location_str, "state": state, "country": "United States", "region": "North America", "is_international": False}

    if metro:
        info["coli"] = metro.get("coli", 100)
        info["population"] = metro.get("population", "")
        info["median_salary"] = metro.get("median_salary", 0)
        info["unemployment"] = metro.get("unemployment", "")
        info["metro_name"] = metro.get("metro_name", "")
        info["major_employers"] = metro.get("major_employers", "")
    elif state and state in STATE_DATA:
        sd = STATE_DATA[state]
        info["coli"] = sd.get("coli", 100)
        info["population"] = sd.get("population", "")
        info["median_salary"] = sd.get("median_household_income", 0)
        info["unemployment"] = sd.get("unemployment", "")
        info["metro_name"] = f"{sd['name']} (statewide)"
        info["major_employers"] = sd.get("top_industries", "")
    else:
        info["coli"] = 100
        info["population"] = "Data not available"
        info["median_salary"] = 60000
        info["unemployment"] = "~3.5%"
        info["metro_name"] = location_str
        info["major_employers"] = "Varies by area"

    return info


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN RESEARCH FUNCTIONS (called by app.py)
# ═══════════════════════════════════════════════════════════════════════════════

def _get_role_salary_range(role, location_coli=100, enrichment_salary_data=None):
    """
    Get salary range for a role, using BLS enrichment data when available,
    with location-based COLI adjustment. Falls back to ROLE_SALARY_RANGES.

    Args:
        role: Role name string
        location_coli: Cost of Living Index for the target location (100 = national avg)
        enrichment_salary_data: Dict from api_enrichment salary_data (keyed by role name)

    Returns:
        Formatted salary range string like "$95,000 - $180,000"
    """
    # Try BLS enrichment data first (exact match or case-insensitive match)
    if enrichment_salary_data:
        bls = enrichment_salary_data.get(role)
        if not bls:
            # Try case-insensitive match
            role_lower = role.strip().lower()
            for k, v in enrichment_salary_data.items():
                if k.strip().lower() == role_lower:
                    bls = v
                    break
        if bls and bls.get("p10") and bls.get("p90"):
            # Apply COLI adjustment: BLS data is national, adjust for location
            coli_factor = location_coli / 100.0
            p10 = int(bls["p10"] * coli_factor)
            p90 = int(bls["p90"] * coli_factor)
            return f"${p10:,} - ${p90:,}"
        elif bls and bls.get("median"):
            coli_factor = location_coli / 100.0
            median = int(bls["median"] * coli_factor)
            low = int(median * 0.75)
            high = int(median * 1.30)
            return f"${low:,} - ${high:,}"

    # Fall back to curated ROLE_SALARY_RANGES (try exact, then normalized match, then fuzzy)
    salary = ROLE_SALARY_RANGES.get(role)
    if not salary:
        role_lower = role.strip().lower()
        for k, v in ROLE_SALARY_RANGES.items():
            if k.lower() == role_lower or k.lower().rstrip('s') == role_lower.rstrip('s'):
                salary = v
                break
    if not salary:
        # Try fuzzy keyword-based matching (e.g. "Uber Driver Partner" -> "Delivery Driver")
        canonical = _fuzzy_match_role(role)
        if canonical:
            salary = ROLE_SALARY_RANGES.get(canonical)
    if not salary:
        # Last resort: generic range based on industry context
        salary = "$45,000 - $80,000"

    # Apply COLI adjustment to the fallback range if location differs significantly
    if location_coli != 100 and salary and "$" in salary:
        import re as _re
        nums = _re.findall(r'[\d,]+', salary.split("+")[0])
        if len(nums) >= 2:
            try:
                low = int(nums[0].replace(",", ""))
                high = int(nums[1].replace(",", ""))
                coli_factor = location_coli / 100.0
                adj_low = int(low * coli_factor)
                adj_high = int(high * coli_factor)
                # Preserve any suffix like "+ commission"
                suffix = ""
                if "+" in salary:
                    suffix = " +" + salary.split("+", 1)[1]
                elif "commission" in salary.lower():
                    suffix = " + commission"
                return f"${adj_low:,} - ${adj_high:,}{suffix}"
            except (ValueError, IndexError):
                pass

    return salary


def get_market_trends(locations, industry, roles, enrichment_salary_data=None):
    """Generate real market trend data for each location (US + international).

    Args:
        locations: List of location strings
        industry: Industry key string
        roles: List of role name strings
        enrichment_salary_data: Optional dict from api_enrichment salary_data (BLS data keyed by role)
    """
    industry_label = {
        "healthcare_medical": "Healthcare & Medical",
        "blue_collar_trades": "Blue Collar / Skilled Trades",
        "maritime_marine": "Maritime & Marine",
        "military_recruitment": "Military Recruitment",
        "tech_engineering": "Technology & Engineering",
        "general_entry_level": "General / Entry-Level",
        "legal_services": "Legal Services",
        "finance_banking": "Finance & Banking",
        "mental_health": "Mental Health & Behavioral",
        "retail_consumer": "Retail & Consumer",
        "aerospace_defense": "Aerospace & Defense",
        "pharma_biotech": "Pharma & Biotech",
    }.get(industry, industry)

    # Build per-role salary text using BLS data when available
    # Use national average COLI (100) for the summary; per-location adjustments happen in factor descriptions
    role_salary_text = []
    for r in (roles or [])[:5]:
        salary = _get_role_salary_range(r, location_coli=100, enrichment_salary_data=enrichment_salary_data)
        role_salary_text.append(f"{r}: {salary}")
    salary_summary = "; ".join(role_salary_text) if role_salary_text else "Varies by position"

    factors = []

    for factor_name in ["Labor Supply & Demand", "Median Salary / Compensation Benchmark",
                         "Cost of Living Index (COLI)", "Labor Market & Demographic Trends",
                         "Media Infrastructure & Platform Access", "Conversion Timeline Risk",
                         "Key Competitive Advantage"]:
        descs = {}
        for loc in locations:
            info = get_location_info(loc)
            coli = info["coli"]
            pop = info["population"]
            unemp = info["unemployment"]
            sal = info["median_salary"]
            employers = info["major_employers"]

            is_intl = info.get("is_international", False)
            currency = info.get("currency", "USD")
            currency_sym = {"USD": "$", "GBP": "£", "EUR": "€", "JPY": "¥", "INR": "₹", "AUD": "A$", "CAD": "C$", "SGD": "S$", "CHF": "CHF ", "AED": "AED ", "ZAR": "R", "BRL": "R$", "MXN": "MX$", "PLN": "PLN ", "SEK": "SEK ", "DKK": "DKK ", "NOK": "NOK ", "CZK": "CZK ", "PHP": "₱", "MYR": "RM ", "NZD": "NZ$", "HKD": "HK$", "KRW": "₩", "ILS": "₪", "CNY": "¥", "IDR": "Rp ", "THB": "฿", "VND": "₫", "KES": "KES ", "COP": "COP ", "CLP": "CLP ", "ARS": "ARS "}.get(currency, "$")

            if factor_name == "Labor Supply & Demand":
                tightness = "tight" if float(unemp.replace('%','').replace('~','')) < 3.5 else "moderate" if float(unemp.replace('%','').replace('~','')) < 5.0 else "looser"
                country_note = f" Country: {info.get('country', '')}. Region: {info.get('region', '')}." if is_intl else ""
                descs[loc] = (
                    f"Population: {pop}.{country_note} Unemployment rate: {unemp} ({tightness} labor market). "
                    f"The {industry_label.lower()} sector faces {'strong' if tightness == 'tight' else 'moderate'} competition for qualified candidates. "
                    f"Key industries in the region: {employers}. "
                    f"{'Aggressive sourcing strategies recommended due to talent scarcity.' if tightness == 'tight' else 'Standard sourcing approaches should yield adequate candidate flow.'}"
                    f"{' International hiring may require local recruitment partners, compliance with local labor laws, and localized job postings.' if is_intl else ''}"
                )
            elif factor_name == "Median Salary / Compensation Benchmark":
                sal_display = f"{currency_sym}{sal:,}" if currency in ("USD", "GBP", "EUR") else f"{currency_sym}{sal:,} ({currency})"
                # Build location-adjusted salary ranges for each role
                loc_role_salary_parts = []
                for r in (roles or [])[:5]:
                    loc_sal = _get_role_salary_range(r, location_coli=coli, enrichment_salary_data=enrichment_salary_data)
                    loc_role_salary_parts.append(f"{r}: {loc_sal}")
                loc_salary_summary = "; ".join(loc_role_salary_parts) if loc_role_salary_parts else salary_summary
                bls_note = " (BLS-sourced, COLI-adjusted)" if enrichment_salary_data else ""
                descs[loc] = (
                    f"Regional median salary: {sal_display}/year. "
                    f"Target role salary ranges{bls_note}: {loc_salary_summary}. "
                    f"{'Compensation packages should be above-market to attract top talent in this competitive metro.' if coli > 110 else 'Compensation aligned with market averages should be competitive.'} "
                    f"{'Consider currency exchange rates and local purchasing power parity when setting compensation.' if is_intl else 'Consider sign-on bonuses and relocation assistance for hard-to-fill positions.'}"
                )
            elif factor_name == "Cost of Living Index (COLI)":
                vs_national = "above" if coli > 100 else "below" if coli < 100 else "at"
                pct = abs(coli - 100)
                descs[loc] = (
                    f"COLI: {coli} ({pct}% {vs_national} national average of 100). "
                    f"{'Housing costs are a significant factor — consider highlighting housing assistance or remote/hybrid options.' if coli > 120 else 'Cost of living is manageable — can be positioned as an advantage vs. higher-cost metros.'  if coli < 95 else 'Cost of living is near national average — standard compensation packages should be competitive.'} "
                    f"Candidates relocating from {'lower' if coli > 110 else 'higher'}-cost areas may need adjustment assistance."
                )
            elif factor_name == "Labor Market & Demographic Trends":
                descs[loc] = (
                    f"Metro area population: {pop}. Labor force participation is {'strong' if float(unemp.replace('%','').replace('~','')) < 4.0 else 'moderate'}. "
                    f"Key demographic trends: aging workforce creating replacement demand, growing diversity in labor pool, "
                    f"{'increasing remote work migration to the region' if coli < 105 else 'some outmigration due to cost pressures'}. "
                    f"{'Strong veteran population in the area provides potential talent pipeline.' if industry in ['military_recruitment', 'maritime_marine'] else 'Focus on upskilling and apprenticeship programs to develop local talent pipeline.'}"
                )
            elif factor_name == "Media Infrastructure & Platform Access":
                has_radio = info.get("state") in STATE_RADIO if not is_intl else False
                top_boards = info.get("top_boards", "")
                if is_intl:
                    descs[loc] = (
                        f"Key job boards in {info.get('country', loc)}: {top_boards}. "
                        f"{'Well-developed digital media market with strong platform infrastructure.' if coli > 60 else 'Emerging digital market — mobile-first strategy recommended with local platform focus.'} "
                        f"{'LinkedIn is widely used for professional recruitment.' if info.get('region') in ['Europe', 'North America'] else 'Local platforms may outperform LinkedIn — prioritize regional job boards.'} "
                        f"Consider localized job descriptions in the native language and culturally appropriate employer branding. "
                        f"Programmatic job advertising available via Joveo Mojo and regional DSPs."
                    )
                else:
                    descs[loc] = (
                        f"{'Well-developed media market with strong local radio, TV, and digital infrastructure.' if pop and ('M' in str(pop) and float(str(pop).replace('M','').replace('+','').replace(' metro','')) > 1.0) else 'Smaller media market — digital channels and targeted social media will be primary reach drivers.'} "
                        f"{'Multiple NPR/local radio stations available for audio advertising.' if has_radio else 'Limited local radio — prioritize digital audio (Spotify, podcasts) and social media.'} "
                        f"Strong mobile penetration supports Meta, Google, and programmatic display campaigns. "
                        f"LinkedIn penetration is {'high' if industry in ['tech_engineering', 'healthcare_medical', 'finance_banking', 'legal_services'] else 'moderate'} for {industry_label.lower()} roles."
                    )
            elif factor_name == "Conversion Timeline Risk":
                urgency = "high" if float(unemp.replace('%','').replace('~','')) < 3.0 else "moderate"
                descs[loc] = (
                    f"{'HIGH RISK: Very tight labor market means candidates have multiple offers. Expect 2-4 week hiring cycles needed.' if urgency == 'high' else 'MODERATE RISK: Standard hiring timelines of 3-6 weeks should be achievable.'} "
                    f"Recommendation: {'Streamline application process to under 5 minutes, enable text-to-apply, and ensure response within 24-48 hours.' if urgency == 'high' else 'Maintain responsive communication cadence and competitive offer timelines.'} "
                    f"{'Consider instant-interview or same-day offer programs for high-volume roles.' if industry in ['general_entry_level', 'blue_collar_trades'] else 'Build relationship-based recruitment pipeline for specialized talent.'}"
                )
            elif factor_name == "Key Competitive Advantage":
                advantages = {
                    "healthcare_medical": "mission-driven work, patient impact, clinical excellence reputation, continuing education support, and work-life balance programs",
                    "blue_collar_trades": "competitive hourly rates, overtime availability, tool allowances, safety culture, career progression pathways, and apprenticeship programs",
                    "maritime_marine": "unique marine/maritime career path, specialized training, competitive pay with sea-pay differentials, travel opportunities, and strong camaraderie culture",
                    "military_recruitment": "unmatched benefits package (healthcare, housing, GI Bill worth $100K+, retirement at 20 years), leadership development, career training in 150+ specialties, and sense of purpose/service",
                    "tech_engineering": "cutting-edge technology stack, flexible/remote work options, equity compensation, professional development budget, and innovative company culture",
                    "general_entry_level": "immediate start dates, flexible scheduling, advancement opportunities, tuition reimbursement, and employee discount programs",
                    "legal_services": "prestigious firm reputation, pro bono opportunities, mentorship from senior partners, competitive billable rate compensation, bar exam support, and professional development CLEs",
                    "finance_banking": "competitive base + bonus structure, signing bonuses, equity/RSU grants, global mobility opportunities, prestigious brand, and fast-track promotion paths",
                    "mental_health": "meaningful patient impact, clinical supervision support, flexible scheduling, loan repayment programs, diverse caseloads, and growing demand ensuring job security",
                    "retail_consumer": "employee discounts, flexible scheduling, rapid advancement from store to district/regional management, performance bonuses, and transferable skills development",
                    "aerospace_defense": "cutting-edge R&D, security clearance career premium, long-term program stability, relocation packages, and contribution to national defense/space exploration",
                    "pharma_biotech": "life-changing drug development impact, competitive total compensation with equity, publication opportunities, state-of-the-art lab facilities, and global collaboration",
                }
                adv = advantages.get(industry, "competitive compensation, benefits, and career growth opportunities")
                descs[loc] = (
                    f"Key differentiators to emphasize in {loc} market: {adv}. "
                    f"{'Position against high cost-of-living competitors by emphasizing total compensation value.' if coli < 100 else 'Leverage strong regional brand recognition and community presence.'} "
                    f"Recommended messaging: highlight authentic employee stories, career progression timelines, and tangible benefit values."
                )

        factors.append({"factor": factor_name, "descriptions": descs})

    return factors


def _filter_self_from_competitors(competitors_str, company_name):
    """Remove the requesting company from a comma-separated competitor string."""
    if not company_name:
        return competitors_str
    company_lower = company_name.lower().strip()
    # Also handle common variations (e.g., "JPMorgan Chase" vs "JPMorgan")
    company_words = [w for w in company_lower.split() if len(w) > 2]
    parts = [c.strip() for c in competitors_str.split(",")]
    filtered = []
    for part in parts:
        part_lower = part.lower().strip()
        # Exact match or company name is a substring of competitor
        if company_lower in part_lower:
            continue
        # Check if any significant company word matches
        if any(w in part_lower for w in company_words if len(w) >= 4):
            continue
        filtered.append(part)
    return ", ".join(filtered) if filtered else competitors_str


def get_competitors(industry, locations, company_name=None):
    """Get real competitor data for the industry (US + international).

    Args:
        industry: The industry key (e.g. 'tech_engineering', 'healthcare_medical')
        locations: List of target locations
        company_name: The requesting company's name (used to filter self from results)
    """
    # Check if any location is international
    has_intl = any(_detect_country(loc) and _detect_country(loc) != "United States" for loc in (locations or []))

    comp_data = INDUSTRY_COMPETITORS.get(industry, INDUSTRY_COMPETITORS.get("general_entry_level", {}))
    result = []
    for category, data in comp_data.items():
        # CRITICAL: Remove self from competitor list
        filtered_competitors = _filter_self_from_competitors(data["competitors"], company_name)
        result.append({
            "category": category,
            "competitors": filtered_competitors,
            "threat": data["threat"],
        })

    # Add international competitor categories if applicable
    if has_intl:
        intl_categories = {
            "healthcare_medical": {"competitors": "NHS (UK), Ramsay Health (AU), Apollo Hospitals (India), Charité (Germany)", "threat": "HIGH"},
            "tech_engineering": {"competitors": "SAP (Germany), Infosys/TCS (India), Rakuten (Japan), Spotify (Sweden)", "threat": "HIGH"},
            "finance_banking": {"competitors": "HSBC (UK), Deutsche Bank (Germany), DBS (Singapore), Nomura (Japan)", "threat": "HIGH"},
            "pharma_biotech": {"competitors": "Roche (Switzerland), Novartis (Switzerland), AstraZeneca (UK), Takeda (Japan)", "threat": "MODERATE"},
            "aerospace_defense": {"competitors": "Airbus (EU), BAE Systems (UK), Thales (France), Leonardo (Italy)", "threat": "HIGH"},
            "legal_services": {"competitors": "Magic Circle firms (UK), De Brauw (Netherlands), Hengeler Mueller (Germany)", "threat": "MODERATE"},
        }
        if industry in intl_categories:
            filtered_intl = _filter_self_from_competitors(intl_categories[industry]["competitors"], company_name)
            result.append({
                "category": "International / Global Competitors",
                "competitors": filtered_intl,
                "threat": intl_categories[industry]["threat"],
            })

    # Filter out the company itself from competitor lists to prevent self-as-competitor
    if company_name:
        _cn_lower = company_name.strip().lower()
        for comp_entry in result:
            comp_str = comp_entry.get("competitors", "")
            # Remove company name from comma-separated competitor list
            comp_parts = [c.strip() for c in comp_str.split(",")]
            filtered_parts = [c for c in comp_parts if _cn_lower not in c.strip().lower()]
            comp_entry["competitors"] = ", ".join(filtered_parts) if filtered_parts else comp_str

    # Fallback if no data found
    if not result:
        result.append({"category": "General Market Competitors", "competitors": "Major employers in the region competing for similar talent pools", "threat": "MODERATE"})

    return result


def get_educational_partners(locations, industry):
    """Get real universities and educational institutions near target locations (US + international)."""
    partners = []
    seen = set()

    industry_focus = {
        "healthcare_medical": "healthcare, nursing, medicine, and allied health programs",
        "blue_collar_trades": "trade skills, diesel technology, welding, HVAC, and manufacturing programs",
        "maritime_marine": "maritime engineering, marine technology, and naval architecture programs",
        "military_recruitment": "ROTC programs, military science, and leadership development",
        "tech_engineering": "computer science, engineering, and technology programs",
        "general_entry_level": "workforce development, career readiness, and professional skills programs",
        "legal_services": "law programs, paralegal studies, and legal research",
        "finance_banking": "finance, accounting, economics, and business administration programs",
        "mental_health": "psychology, social work, counseling, and behavioral health programs",
        "retail_consumer": "business management, marketing, and retail management programs",
        "aerospace_defense": "aerospace engineering, systems engineering, and defense studies programs",
        "pharma_biotech": "pharmaceutical sciences, biotechnology, chemistry, and clinical research programs",
    }.get(industry, "relevant career programs")

    # International university data by country
    INTL_UNIVERSITIES = {
        "United Kingdom": [
            {"name": "University of Oxford", "programs": "Medicine, Law, Engineering, Business", "enrollment": "26,000"},
            {"name": "University of Cambridge", "programs": "Engineering, Sciences, Medicine, Business", "enrollment": "24,000"},
            {"name": "Imperial College London", "programs": "Engineering, Medicine, Business, Technology", "enrollment": "20,000"},
            {"name": "University College London (UCL)", "programs": "Medicine, Engineering, Law, Sciences", "enrollment": "42,000"},
        ],
        "Germany": [
            {"name": "Technical University of Munich (TUM)", "programs": "Engineering, Technology, Sciences, Medicine", "enrollment": "50,000"},
            {"name": "RWTH Aachen University", "programs": "Engineering, Technology, Sciences", "enrollment": "47,000"},
            {"name": "Ludwig Maximilian University (LMU)", "programs": "Medicine, Law, Business, Sciences", "enrollment": "52,000"},
        ],
        "France": [
            {"name": "HEC Paris", "programs": "Business, Finance, Management", "enrollment": "5,000"},
            {"name": "École Polytechnique", "programs": "Engineering, Sciences, Technology", "enrollment": "3,300"},
            {"name": "Sorbonne University", "programs": "Medicine, Sciences, Humanities, Law", "enrollment": "55,000"},
        ],
        "India": [
            {"name": "Indian Institute of Technology (IIT) System", "programs": "Engineering, Technology, Sciences (23 campuses)", "enrollment": "120,000"},
            {"name": "All India Institute of Medical Sciences (AIIMS)", "programs": "Medicine, Nursing, Allied Health", "enrollment": "4,000"},
            {"name": "Indian Institute of Management (IIM) System", "programs": "Business, Management, Finance (20 campuses)", "enrollment": "50,000"},
        ],
        "Australia": [
            {"name": "University of Melbourne", "programs": "Medicine, Engineering, Law, Business", "enrollment": "52,000"},
            {"name": "University of Sydney", "programs": "Medicine, Engineering, Business, Law", "enrollment": "73,000"},
            {"name": "UNSW Sydney", "programs": "Engineering, Business, Medicine, Law", "enrollment": "62,000"},
        ],
        "Japan": [
            {"name": "University of Tokyo", "programs": "Engineering, Medicine, Law, Sciences", "enrollment": "28,000"},
            {"name": "Kyoto University", "programs": "Sciences, Engineering, Medicine", "enrollment": "23,000"},
        ],
        "Singapore": [
            {"name": "National University of Singapore (NUS)", "programs": "Engineering, Business, Medicine, Law", "enrollment": "40,000"},
            {"name": "Nanyang Technological University (NTU)", "programs": "Engineering, Business, Sciences", "enrollment": "33,000"},
        ],
        "Canada": [
            {"name": "University of Toronto", "programs": "Engineering, Medicine, Business, Law", "enrollment": "97,000"},
            {"name": "University of British Columbia (UBC)", "programs": "Engineering, Medicine, Sciences, Business", "enrollment": "67,000"},
            {"name": "McGill University", "programs": "Medicine, Engineering, Law, Business", "enrollment": "40,000"},
        ],
        "Netherlands": [
            {"name": "Delft University of Technology (TU Delft)", "programs": "Engineering, Technology, Architecture", "enrollment": "26,000"},
            {"name": "University of Amsterdam", "programs": "Business, Law, Sciences, Medicine", "enrollment": "39,000"},
        ],
        "Switzerland": [
            {"name": "ETH Zurich", "programs": "Engineering, Sciences, Technology", "enrollment": "24,000"},
            {"name": "EPFL Lausanne", "programs": "Engineering, Technology, Sciences", "enrollment": "17,000"},
        ],
    }

    for loc in locations:
        country = _detect_country(loc)
        if country and country != "United States" and country in INTL_UNIVERSITIES:
            for uni in INTL_UNIVERSITIES[country]:
                if uni["name"] not in seen:
                    seen.add(uni["name"])
                    partners.append({
                        "institution": uni["name"],
                        "fit": f"Country: {country}. Programs: {uni['programs']}. Enrollment: {uni['enrollment']}. Strategic fit for recruiting talent with {industry_focus}. Explore campus partnerships, career fairs, and graduate recruitment programs."
                    })
        elif country and country != "United States":
            partners.append({
                "institution": f"Top Universities in {country}",
                "fit": f"Identify leading institutions in {country} with {industry_focus}. Establish career fair presence and university recruitment partnerships."
            })
        else:
            state = _extract_state(loc)
            if state and state in STATE_UNIVERSITIES:
                unis = STATE_UNIVERSITIES[state]
                for uni in unis:
                    if uni["name"] not in seen:
                        seen.add(uni["name"])
                        partners.append({
                            "institution": uni["name"],
                            "fit": f"Programs: {uni['programs']}. Enrollment: {uni['enrollment']}. Strategic fit for recruiting talent with {industry_focus}. Partner on career fairs, guest lectures, sponsored capstone projects, and internship pipelines."
                        })

    if not partners:
        partners.append({
            "institution": "Regional Universities & Community Colleges",
            "fit": f"Identify local institutions with {industry_focus}. Establish career fair presence, sponsor student organizations, and create internship-to-hire pipelines."
        })

    return partners


def get_events(locations, industry):
    """Generate career fair and event recommendations per location."""
    events = []

    industry_events = {
        "healthcare_medical": [
            ("Healthcare Career Fair", "Career Fair", "Direct access to healthcare professionals, nurses, and allied health candidates", "500-2,000 attendees", "$2,000-$5,000"),
            ("AONE/ACHE Annual Conference", "Industry Conference", "Network with healthcare administrators and leaders; brand visibility among decision-makers", "3,000-5,000 attendees", "$5,000-$15,000"),
            ("Local Hospital Job Fair", "Career Fair", "Targeted recruitment for clinical and support roles at healthcare facilities", "200-800 attendees", "$1,000-$3,000"),
            ("Nursing Student Career Day", "College Partnership", "Early talent pipeline; build relationships with nursing students before graduation", "100-500 students", "$500-$2,000"),
            ("Community Health Fair", "Community Engagement", "Build employer brand awareness in the community while supporting public health initiatives", "1,000-5,000 attendees", "$1,500-$4,000"),
        ],
        "blue_collar_trades": [
            ("SkillsUSA Championship", "STEM Sponsorship", "Access to top trade school students nationally; showcase company and career opportunities", "6,000+ students", "$3,000-$10,000"),
            ("Local Trade School Job Fair", "Career Fair", "Direct access to graduates in welding, electrical, HVAC, diesel mechanics, and manufacturing", "200-600 attendees", "$1,000-$3,000"),
            ("Construction Industry Career Fair", "Industry Conference", "Network with construction workers, equipment operators, and project managers", "300-1,000 attendees", "$2,000-$5,000"),
            ("Union Apprenticeship Open House", "Community Engagement", "Build relationships with union locals and apprenticeship programs for talent pipeline", "100-400 attendees", "$500-$2,000"),
            ("Community College Trades Showcase", "College Partnership", "Engage with students completing trade certificates and associate degree programs", "150-500 students", "$500-$2,000"),
        ],
        "maritime_marine": [
            ("International WorkBoat Show", "Industry Conference", "Premier maritime trade show; recruit marine mechanics, engineers, and naval architects", "8,000+ attendees", "$5,000-$15,000"),
            ("Maritime Career Fair (local port authority)", "Career Fair", "Direct access to maritime workers, dock workers, and marine technicians", "200-500 attendees", "$2,000-$5,000"),
            ("Maritime Academy Career Day", "College Partnership", "Early pipeline access to maritime academy cadets and graduates", "100-300 students", "$1,000-$3,000"),
            ("SNAME Maritime Convention", "Industry Conference", "Network with naval architects and marine engineers at the Society of Naval Architects and Marine Engineers annual event", "2,000+ attendees", "$3,000-$8,000"),
            ("Local Shipyard Open House", "Community Engagement", "Community brand building and recruitment for shipyard trades positions", "500-2,000 attendees", "$2,000-$5,000"),
        ],
        "military_recruitment": [
            ("State Fair Military Booth", "Community Engagement", "High-visibility brand presence reaching diverse demographics in a relaxed setting", "50,000-500,000 fair attendees", "$3,000-$10,000"),
            ("High School Career Day", "College Partnership", "Direct access to 17-18 year olds exploring career options; can present military career paths", "200-800 students", "$500-$1,500"),
            ("Veterans Job Fair (Hire Heroes/RecruitMilitary)", "Career Fair", "Network with transitioning service members and veterans for Guard/Reserve opportunities", "300-1,000 attendees", "$2,000-$5,000"),
            ("College ROTC Open House", "College Partnership", "Build relationships with ROTC cadets and engage college students considering military service", "50-200 students", "$500-$2,000"),
            ("Community 5K/Fitness Challenge Sponsorship", "Community Engagement", "Brand visibility among fitness-oriented, service-minded individuals in target demographic", "500-3,000 participants", "$2,000-$8,000"),
        ],
        "tech_engineering": [
            ("Local Tech Meetup Sponsorship", "Community Engagement", "Build brand awareness among local developers and engineers through meetup sponsorship and tech talks", "50-300 attendees", "$500-$2,000"),
            ("University STEM Career Fair", "College Partnership", "Direct access to CS, engineering, and data science students at top regional universities", "500-2,000 students", "$2,000-$5,000"),
            ("Regional Tech Conference Sponsorship", "Industry Conference", "Employer brand visibility at regional tech events; sponsor talks or host workshops", "1,000-5,000 attendees", "$5,000-$20,000"),
            ("Hackathon Sponsorship", "STEM Sponsorship", "Identify top engineering talent through competitive coding events and showcase company technology", "100-500 participants", "$3,000-$10,000"),
            ("Women in Tech / Diversity Event", "Community Engagement", "DEI-focused recruiting; build pipeline of diverse engineering candidates", "200-1,000 attendees", "$2,000-$5,000"),
        ],
        "general_entry_level": [
            ("Community Job Fair (WorkSource / CareerOneStop)", "Career Fair", "High-volume recruitment for entry-level positions; access to active job seekers", "500-2,000 attendees", "$1,000-$3,000"),
            ("College Campus Recruitment Event", "College Partnership", "Access graduating seniors and current students for entry-level and intern positions", "200-1,000 students", "$1,000-$3,000"),
            ("Community Center / Library Job Workshop", "Community Engagement", "Reach underemployed and career-changing individuals in a supportive community setting", "50-200 attendees", "$500-$1,500"),
            ("Workforce Development Board Job Fair", "Career Fair", "Partnership with state/local workforce agencies for targeted recruitment of eligible candidates", "200-800 attendees", "$500-$2,000"),
            ("High School Career Exploration Day", "College Partnership", "Build long-term brand awareness and early talent pipeline among high school juniors/seniors", "100-500 students", "$500-$1,500"),
        ],
    }

    # Additional industry events for new industries
    industry_events.update({
        "legal_services": [
            ("ABA Annual Meeting", "Industry Conference", "Network with attorneys, legal professionals, and law firm recruiters", "5,000+ attendees", "$5,000-$15,000"),
            ("Law School Career Fair", "College Partnership", "Direct access to law students and recent graduates seeking positions", "200-800 students", "$2,000-$5,000"),
            ("Legal Tech Conference", "Industry Conference", "Recruit tech-savvy legal professionals at the intersection of law and technology", "1,000-3,000 attendees", "$3,000-$8,000"),
            ("State Bar Association Job Fair", "Career Fair", "Target licensed attorneys in specific practice areas and jurisdictions", "300-1,000 attendees", "$2,000-$5,000"),
            ("Pro Bono / Public Interest Career Fair", "Career Fair", "Recruit mission-driven attorneys interested in public interest and nonprofit legal work", "200-500 attendees", "$1,000-$3,000"),
        ],
        "finance_banking": [
            ("CFA Institute Annual Conference", "Industry Conference", "Network with chartered financial analysts and investment professionals", "3,000+ attendees", "$5,000-$15,000"),
            ("Business School Career Fair (MBA)", "College Partnership", "Direct access to MBA candidates from top business schools", "500-2,000 students", "$3,000-$8,000"),
            ("Wall Street Technology Summit", "Industry Conference", "Recruit fintech talent and quantitative analysts", "1,000-3,000 attendees", "$5,000-$12,000"),
            ("Women in Finance Conference", "Community Engagement", "DEI-focused recruiting for female finance professionals", "500-1,500 attendees", "$3,000-$8,000"),
            ("Regional Banking Career Fair", "Career Fair", "Target banking professionals in retail, commercial, and wealth management", "300-800 attendees", "$2,000-$5,000"),
        ],
        "mental_health": [
            ("APA Annual Convention", "Industry Conference", "Largest gathering of psychologists; recruit licensed clinicians and researchers", "10,000+ attendees", "$5,000-$15,000"),
            ("NASW National Conference", "Industry Conference", "Network with social workers and behavioral health professionals", "5,000+ attendees", "$3,000-$10,000"),
            ("Graduate School of Social Work Career Fair", "College Partnership", "Recruit MSW and PhD candidates for clinical and research positions", "200-500 students", "$1,000-$3,000"),
            ("Community Mental Health Fair", "Community Engagement", "Build employer brand awareness and recruit in community mental health settings", "500-2,000 attendees", "$1,500-$4,000"),
            ("Telehealth & Digital Health Expo", "Industry Conference", "Recruit tech-savvy clinicians for teletherapy and digital health platforms", "1,000-3,000 attendees", "$3,000-$8,000"),
        ],
        "retail_consumer": [
            ("NRF Retail's Big Show", "Industry Conference", "Largest retail industry event; recruit store managers and retail executives", "40,000+ attendees", "$10,000-$30,000"),
            ("Retail Job Fair (seasonal)", "Career Fair", "High-volume seasonal hiring for retail positions", "500-2,000 attendees", "$1,000-$3,000"),
            ("Shopping Center / Mall Hiring Event", "Career Fair", "On-site recruitment at high-traffic retail locations", "200-800 attendees", "$500-$2,000"),
            ("College Campus Retail Recruitment", "College Partnership", "Recruit students for part-time and entry-level retail positions", "100-500 students", "$500-$2,000"),
            ("Retail Management Leadership Summit", "Industry Conference", "Recruit district and regional managers from competing retailers", "500-1,500 attendees", "$3,000-$8,000"),
        ],
        "aerospace_defense": [
            ("AUSA Annual Meeting", "Industry Conference", "Defense industry's premier event; recruit engineers and program managers", "30,000+ attendees", "$10,000-$30,000"),
            ("Paris Air Show / Farnborough International", "Industry Conference", "Global aerospace showcase; recruit engineers and technical specialists", "150,000+ attendees", "$15,000-$50,000"),
            ("Cleared Job Fair (ClearedJobs.Net)", "Career Fair", "Direct access to candidates with active security clearances", "500-1,500 attendees", "$3,000-$8,000"),
            ("STEM University Career Fair", "College Partnership", "Recruit aerospace and systems engineering graduates from top programs", "500-2,000 students", "$2,000-$5,000"),
            ("Women in Aerospace Conference", "Community Engagement", "DEI-focused recruitment of female aerospace professionals", "500-1,000 attendees", "$3,000-$8,000"),
        ],
        "pharma_biotech": [
            ("BIO International Convention", "Industry Conference", "Largest biotech event globally; recruit scientists and regulatory professionals", "15,000+ attendees", "$10,000-$30,000"),
            ("ASCO Annual Meeting", "Industry Conference", "Premier oncology conference; recruit clinical researchers and MSLs", "40,000+ attendees", "$10,000-$25,000"),
            ("University Research Career Fair", "College Partnership", "Recruit PhD and postdoc candidates from top research universities", "200-800 attendees", "$2,000-$5,000"),
            ("Drug Discovery & Development Summit", "Industry Conference", "Recruit medicinal chemists, biologists, and formulation scientists", "1,000-3,000 attendees", "$5,000-$15,000"),
            ("Women in Pharma Leadership Forum", "Community Engagement", "DEI-focused recruitment of female pharma professionals and scientists", "300-800 attendees", "$2,000-$6,000"),
        ],
    })

    templates = industry_events.get(industry, industry_events["general_entry_level"])

    for loc in locations:
        country = _detect_country(loc)
        is_intl = country and country != "United States"
        for partner, etype, impact, reach, budget in templates:
            events.append({
                "partner": f"{partner}{' (International)' if is_intl else ''}",
                "location": loc,
                "type": etype,
                "impact": f"{impact}{f'. Note: Adapt for {country} market with local language and cultural considerations.' if is_intl else ''}",
                "reach": reach,
                "budget": budget,
            })

    return events


def get_location_boards(locations):
    """Get real local job boards and resources for each location (US + international)."""
    boards = []
    global_supply = _load_global_supply()

    for loc in locations:
        country = _detect_country(loc)

        # International boards
        if country and country != "United States":
            cd = COUNTRY_DATA.get(country, {})
            top_boards = cd.get("top_boards", "")
            if top_boards:
                boards.extend([b.strip() for b in top_boards.split(",")])
            # Add from global supply data
            country_boards = global_supply.get("country_job_boards", {}).get(country, {})
            if isinstance(country_boards, dict):
                for board_entry in country_boards.get("boards", [])[:5]:
                    if isinstance(board_entry, dict):
                        boards.append(f"{board_entry.get('name', '')} ({board_entry.get('billing_model', 'CPC')})")
                    elif isinstance(board_entry, str):
                        boards.append(board_entry)
            boards.append(f"LinkedIn ({country})")
            boards.append(f"Indeed ({country})")
            continue

        # US boards
        state = _extract_state(loc)
        state_boards = {
            "OR": ["Oregon Employment Department (WorkSource Oregon)", "Mac's List (Portland)", "OregonLive Jobs"],
            "MN": ["MinnesotaWorks.net", "Minnesota Department of Employment", "MN Council of Nonprofits Jobs"],
            "CA": ["CalJOBS (EDD)", "Craigslist (major metros)", "Bay Area Community Jobs"],
            "TX": ["WorkInTexas.com (TWC)", "Texas Workforce Commission", "Austin Digital Jobs"],
            "NY": ["NY State Job Bank", "NYS Department of Labor", "Craigslist NYC"],
            "WA": ["WorkSourceWA.com", "Washington State Employment Security", "Seattle Jobs - Craigslist"],
            "FL": ["Employ Florida (FloridaJobs.org)", "Florida DEO", "South Florida Jobs"],
            "GA": ["Georgia DOL - EmployGeorgia", "Atlanta Jobs Board", "Georgia Diversity Job Board"],
            "IL": ["IllinoisJobLink.com", "Illinois Workforce Innovation Board", "Chicago Jobs (Built In Chicago)"],
            "PA": ["PA CareerLink", "Pennsylvania JobGateway", "Pittsburgh Technology Council Jobs"],
            "OH": ["OhioMeansJobs.com", "Ohio Department of Job & Family Services", "Columbus Tech Jobs"],
            "MI": ["Pure Michigan Talent Connect", "Michigan Works!", "Detroit Regional Jobs"],
            "NC": ["NCWorks.gov", "NC Division of Employment Security", "Charlotte Agenda Jobs"],
            "CO": ["Connecting Colorado", "Colorado Workforce Centers", "Built In Colorado"],
            "VA": ["Virginia Workforce Connection", "Virginia Employment Commission", "ClearanceJobs (NoVA)"],
            "TN": ["Jobs4TN.gov", "Tennessee Dept of Labor & Workforce Development", "Nashville Tech Jobs"],
            "IN": ["IndianaCareerConnect.com", "Indiana DWD", "TechPoint (Indiana Tech)"],
            "MO": ["MoJobs.mo.gov", "Missouri Career Source", "St. Louis Regional Jobs"],
            "AZ": ["Arizona Job Connection", "Arizona @ Work", "Phoenix Business Journal Jobs"],
            "MD": ["Maryland Workforce Exchange", "Maryland Department of Labor", "CyberSecJobs.com (Baltimore)"],
            "MA": ["MassHire JobQuest", "Mass.gov Job Seekers", "Built In Boston"],
        }

        if state and state in state_boards:
            boards.extend(state_boards[state])
        else:
            boards.append(f"State Workforce Agency Job Board ({loc})")
            boards.append(f"Local Craigslist ({loc})")
            boards.append(f"City/County Career Portal ({loc})")

    return list(dict.fromkeys(boards))  # Deduplicate while preserving order


def get_media_platform_audiences(industry):
    """Get target audience descriptions for media/print platforms."""
    audiences = {
        "healthcare_medical": {
            "print": "Physicians, Surgeons, Hospital Administrators",
            "digital": "Nurses, NPs, PAs, Medical Students, Residents",
            "hybrid": "All healthcare professionals — physicians through allied health",
        },
        "blue_collar_trades": {
            "print": "Trade Professionals, Contractors, Shop Owners",
            "digital": "Apprentices, Journeymen, Young Trades Workers",
            "hybrid": "Broad trades & construction audience",
        },
        "maritime_marine": {
            "print": "Marine Engineers, Naval Architects, Ship Officers",
            "digital": "Marine Technicians, Deck Officers, Maritime Students",
            "hybrid": "All maritime professionals and industry stakeholders",
        },
        "military_recruitment": {
            "print": "Veterans, Active Duty, Military Families",
            "digital": "Young Adults 17-24, Fitness Enthusiasts, Patriotic Youth",
            "hybrid": "Broad military-connected and service-oriented audience",
        },
        "tech_engineering": {
            "print": "Senior Engineers, CTOs, Engineering Managers",
            "digital": "Software Developers, Data Scientists, DevOps Engineers",
            "hybrid": "Full spectrum of technology professionals",
        },
        "general_entry_level": {
            "print": "Job Seekers, Career Changers, Community Members",
            "digital": "Young Adults 18-30, Recent Graduates, Gig Workers",
            "hybrid": "Broad workforce audience across experience levels",
        },
        "legal_services": {
            "print": "Attorneys, Partners, Legal Executives, Judges",
            "digital": "Associates, Paralegals, Law Students, Legal Tech Professionals",
            "hybrid": "All legal professionals across practice areas and seniority levels",
        },
        "finance_banking": {
            "print": "CFOs, Portfolio Managers, Investment Directors, Banking Executives",
            "digital": "Financial Analysts, Traders, Risk Managers, Fintech Professionals",
            "hybrid": "Full spectrum of finance and banking professionals",
        },
        "mental_health": {
            "print": "Psychiatrists, Licensed Psychologists, Department Heads",
            "digital": "Therapists (LCSW/LPC), Counselors, Social Workers, Behavioral Analysts",
            "hybrid": "All mental health and behavioral health professionals",
        },
        "retail_consumer": {
            "print": "Regional/District Managers, Retail Executives, Buyers",
            "digital": "Store Managers, Sales Associates, Merchandisers, E-commerce Specialists",
            "hybrid": "Broad retail workforce from store floor to corporate",
        },
        "aerospace_defense": {
            "print": "Chief Engineers, Program Directors, Defense Executives",
            "digital": "Aerospace Engineers, Systems Engineers, Test Engineers, Avionics Technicians",
            "hybrid": "All aerospace and defense professionals across clearance levels",
        },
        "pharma_biotech": {
            "print": "Chief Science Officers, VP R&D, Medical Directors",
            "digital": "Research Scientists, Clinical Research Associates, Lab Technicians, Regulatory Affairs",
            "hybrid": "Full spectrum of pharmaceutical and biotech professionals",
        },
    }
    return audiences.get(industry, audiences["general_entry_level"])


def get_global_supply_data(locations, industry):
    """Get global supply data for international media plans."""
    global_supply = _load_global_supply()
    if not global_supply:
        return {}

    result = {
        "country_boards": [],
        "dei_boards": [],
        "innovative_channels": [],
        "billing_models": global_supply.get("billing_models", {}),
        "commission_tiers": global_supply.get("commission_tiers", {}),
        "push_pull_strategy": global_supply.get("push_vs_pull_strategy", {}),
        "niche_boards": [],
    }

    _seen_countries = set()
    for loc in (locations or []):
        country = _detect_country(loc)
        if not country:
            # _detect_country returns None for US locations — include US data from global_supply
            country = "United States"
        if country in _seen_countries:
            continue
        _seen_countries.add(country)

        # Country-specific boards
        cb = global_supply.get("country_job_boards", {}).get(country, {})
        if cb:
            result["country_boards"].append({"country": country, "data": cb})

        # Region for DEI boards
        region = COUNTRY_DATA.get(country, {}).get("region", "")
        dei = global_supply.get("dei_boards_by_country", {})
        for dei_region, dei_boards in dei.items():
            if country.lower() in dei_region.lower() or (region and region.lower() in dei_region.lower()):
                result["dei_boards"].extend(dei_boards if isinstance(dei_boards, list) else [dei_boards])

    # Innovative channels (always include)
    result["innovative_channels"] = global_supply.get("innovative_channels_2025", [])

    # Niche industry boards
    industry_map = {
        "legal_services": "legal",
        "finance_banking": "finance",
        "mental_health": "mental_health",
        "retail_consumer": "retail",
        "aerospace_defense": "aerospace",
        "pharma_biotech": "pharma",
    }
    niche_key = industry_map.get(industry, "")
    niche_data = global_supply.get("niche_industry_boards", {})
    for key, boards in niche_data.items():
        if niche_key and niche_key in key.lower():
            result["niche_boards"] = boards if isinstance(boards, list) else [boards]
            break

    return result


def get_radio_podcasts(locations, industry):
    """Get real radio stations for locations plus industry-relevant podcasts."""
    result = []
    seen = set()

    # Add local radio stations (US only)
    for loc in locations:
        country = _detect_country(loc)
        if country and country != "United States":
            # International: add podcast recommendations only
            result.append({"name": f"Local Radio Advertising ({country})", "listeners": "Varies by market", "genre": "Local Radio", "audience": f"General audience in {country}"})
            continue
        state = _extract_state(loc)
        if state and state in STATE_RADIO:
            for station in STATE_RADIO[state]:
                if station["name"] not in seen:
                    seen.add(station["name"])
                    result.append(station)

    # Add industry podcasts
    podcasts = INDUSTRY_PODCASTS.get(industry, INDUSTRY_PODCASTS.get("general_entry_level", []))
    for pod in podcasts:
        if pod["name"] not in seen:
            seen.add(pod["name"])
            result.append(pod)

    # If no radio stations found, add generic recommendation
    if not result:
        result.append({"name": "Local NPR Affiliate", "listeners": "Varies by market", "genre": "Public Radio/NPR", "audience": "Educated Professionals 25-64"})
        result.append({"name": "Local News/Talk AM Station", "listeners": "Varies by market", "genre": "News/Talk", "audience": "Adults 25-64"})
        result.extend(podcasts)

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# LABOUR MARKET INTELLIGENCE (BLS/JOLTS-STYLE CURATED DATA)
# ═══════════════════════════════════════════════════════════════════════════════

INDUSTRY_LABOUR_MARKET = {
    "healthcare_medical": {
        "sector_name": "Healthcare & Social Assistance",
        "bls_sector_code": "NAICS 62",
        "total_employment_us": "21.5M",
        "projected_growth_2024_2034": "+15.4% (Much faster than average)",
        "annual_openings": "~2.1M (due to growth + replacement needs)",
        "median_annual_wage": "$48,820 (all healthcare practitioners & technical: $77,600)",
        "job_openings_rate_jolts": "8.2% (well above national 5.8% average)",
        "quits_rate_jolts": "2.8% (high voluntary turnover, especially nursing)",
        "hires_rate_jolts": "5.9%",
        "layoffs_rate_jolts": "0.7% (very low — strong job security)",
        "vacancy_fill_time_avg": "49 days (RNs: 84 days, Physicians: 120+ days)",
        "talent_shortage_severity": "CRITICAL — 200K+ nursing shortage projected by 2030; 37.8K physician shortfall by 2034",
        "key_trends": [
            "Travel nursing demand remains elevated (30% above pre-pandemic)",
            "Telehealth expanding clinical roles — 38% of outpatient visits now hybrid",
            "Burnout crisis: 47% of healthcare workers plan to leave roles by 2025",
            "AI/automation augmenting diagnostic roles but not displacing clinicians",
            "International recruitment accelerating (Philippines, India, Caribbean pipeline)",
            "Sign-on bonuses now standard for RNs ($5K-$30K) and specialists ($25K-$100K)",
        ],
        "wage_growth_yoy": "+4.8% (outpacing national 4.1%)",
        "unionization_rate": "7.1% (growing, especially among nurses)",
        "remote_work_pct": "12% (primarily telehealth/admin roles)",
    },
    "blue_collar_trades": {
        "sector_name": "Construction & Skilled Trades",
        "bls_sector_code": "NAICS 23 + Manufacturing (31-33)",
        "total_employment_us": "8.2M (Construction) + 12.9M (Manufacturing)",
        "projected_growth_2024_2034": "+4.7% (Construction), -1.2% (Manufacturing automation offset)",
        "annual_openings": "~900K (construction) + ~700K (manufacturing)",
        "median_annual_wage": "$52,350 (construction) / $44,900 (production)",
        "job_openings_rate_jolts": "5.3%",
        "quits_rate_jolts": "2.3%",
        "hires_rate_jolts": "5.0%",
        "layoffs_rate_jolts": "1.4% (seasonal fluctuations)",
        "vacancy_fill_time_avg": "35 days (electricians: 56 days, welders: 42 days)",
        "talent_shortage_severity": "HIGH — 546K additional workers needed in 2024 alone per ABC; aging workforce (avg age 42.3)",
        "key_trends": [
            "Infrastructure Investment & Jobs Act driving $1.2T in new construction demand",
            "Apprenticeship enrollment up 64% since 2020 but still insufficient",
            "Average skilled trades worker retiring at 61 — 40% of workforce nearing retirement",
            "Wages rising rapidly: electricians +6.2% YoY, welders +5.8% YoY",
            "Women in construction growing (11% of workforce, up from 9.1% in 2019)",
            "Automation/robotics adoption creating new technician roles while changing existing ones",
        ],
        "wage_growth_yoy": "+5.4% (construction trades leading all sectors)",
        "unionization_rate": "12.6% (construction) — key differentiator for recruitment",
        "remote_work_pct": "2% (on-site essential)",
    },
    "maritime_marine": {
        "sector_name": "Maritime, Shipbuilding & Marine Services",
        "bls_sector_code": "NAICS 3366 (Shipbuilding) + 4883 (Marine Cargo)",
        "total_employment_us": "~400K (direct) + 650K (indirect maritime economy)",
        "projected_growth_2024_2034": "+3.2% (driven by Navy shipbuilding + offshore wind)",
        "annual_openings": "~65K",
        "median_annual_wage": "$56,800 (shipbuilding) / $68,500 (marine engineers)",
        "job_openings_rate_jolts": "6.1%",
        "quits_rate_jolts": "2.1%",
        "hires_rate_jolts": "4.8%",
        "layoffs_rate_jolts": "1.0%",
        "vacancy_fill_time_avg": "52 days (marine diesel mechanics: 68 days)",
        "talent_shortage_severity": "HIGH — Navy's 30-year shipbuilding plan requires 100K+ additional workers; Gulf Coast shipyards at 85% capacity",
        "key_trends": [
            "Navy Columbia-class submarine program creating sustained demand through 2042",
            "Offshore wind farms (30 GW by 2030 target) driving new maritime jobs",
            "Jones Act compliance maintaining domestic maritime workforce requirements",
            "Average age of shipyard worker: 47 — succession planning critical",
            "Welding, pipe fitting, and marine electrical skills most in demand",
            "Maritime academies producing only ~1,200 graduates/year vs. 5K+ needed",
        ],
        "wage_growth_yoy": "+5.1%",
        "unionization_rate": "18.2% (higher than national average)",
        "remote_work_pct": "3% (engineering/design roles only)",
    },
    "military_recruitment": {
        "sector_name": "Military / Armed Forces Recruitment",
        "bls_sector_code": "Federal Government (Military)",
        "total_employment_us": "1.3M active duty + 800K reserve/guard",
        "projected_growth_2024_2034": "+0.5% (steady state with retention focus)",
        "annual_openings": "~170K (enlisted) + ~20K (officer accessions)",
        "median_annual_wage": "E-3 with 2 yrs: $30,000 base + $22K benefits = $52K total compensation",
        "job_openings_rate_jolts": "N/A (recruitment target-based)",
        "quits_rate_jolts": "N/A (contract-based service)",
        "hires_rate_jolts": "N/A",
        "layoffs_rate_jolts": "N/A",
        "vacancy_fill_time_avg": "45 days (MEPS to ship date); recruiting cycle: 3-6 months",
        "talent_shortage_severity": "CRITICAL — All branches missed FY2023 recruiting goals by 25%+; eligible population shrinking (only 23% of 17-24 meet requirements)",
        "key_trends": [
            "FY2023 worst recruiting year since all-volunteer force (1973) — Army missed by 15K",
            "Only 9% of eligible youth show propensity to serve (down from 13% in 2018)",
            "67% of youth disqualified (obesity, drugs, mental health, education)",
            "GI Bill value: $100K+ (full tuition + housing at most universities)",
            "New bonuses: up to $50K enlistment bonuses for critical MOSs",
            "Social media/digital recruiting investment up 40% — TikTok, Instagram leading",
        ],
        "wage_growth_yoy": "+4.6% (2024 military pay raise)",
        "unionization_rate": "N/A (military does not unionize)",
        "remote_work_pct": "N/A",
    },
    "tech_engineering": {
        "sector_name": "Technology & Software Engineering",
        "bls_sector_code": "NAICS 5112 (Software) + 5415 (Computer Systems)",
        "total_employment_us": "5.4M (computing & IT occupations)",
        "projected_growth_2024_2034": "+13.1% (Much faster than average)",
        "annual_openings": "~580K",
        "median_annual_wage": "$104,420 (all computing occupations)",
        "job_openings_rate_jolts": "5.1% (recovered from 2023 correction)",
        "quits_rate_jolts": "3.1% (high mobility)",
        "hires_rate_jolts": "4.7%",
        "layoffs_rate_jolts": "1.6% (2022-23 tech layoffs now stabilized)",
        "vacancy_fill_time_avg": "42 days (senior engineers: 62 days, AI/ML: 75+ days)",
        "talent_shortage_severity": "HIGH — 1.2M unfilled computing jobs vs. 80K annual CS graduates; AI specialization gap widening",
        "key_trends": [
            "AI/ML engineer demand up 74% YoY — highest growth job category globally",
            "Remote/hybrid work now standard: 68% of tech roles offer remote options",
            "2022-23 FAANG layoffs reabsorbed — industry employment surpassed pre-layoff peaks",
            "Cybersecurity workforce gap: 3.4M globally (500K in US alone)",
            "Boot camp graduates now 15% of new engineer hires (up from 8% in 2019)",
            "Total compensation compression: mid-level roles gaining relative to senior",
        ],
        "wage_growth_yoy": "+3.8% (base) / +6.2% (total comp with equity)",
        "unionization_rate": "2.1% (very low but growing awareness)",
        "remote_work_pct": "68% (hybrid or fully remote)",
    },
    "general_entry_level": {
        "sector_name": "General / Entry-Level & Hourly Workforce",
        "bls_sector_code": "Cross-sector (Retail 44-45, Accommodation/Food 72, Admin/Support 56)",
        "total_employment_us": "45M+ (hourly/frontline workers)",
        "projected_growth_2024_2034": "+4.0% (in line with average)",
        "annual_openings": "~6.5M (highest turnover sector)",
        "median_annual_wage": "$35,070 (food service) / $33,680 (retail) / $38,560 (admin support)",
        "job_openings_rate_jolts": "7.1% (highest across all sectors)",
        "quits_rate_jolts": "4.1% (highest voluntary turnover sector)",
        "hires_rate_jolts": "6.8%",
        "layoffs_rate_jolts": "0.9%",
        "vacancy_fill_time_avg": "18 days (fastest fill rates but highest re-vacancy)",
        "talent_shortage_severity": "MODERATE-HIGH — Not a skills shortage but a wage/conditions shortage; 65% of hourly workers cite pay as top concern",
        "key_trends": [
            "Federal minimum wage debate continues ($7.25 federal vs. $15+ in 30 states)",
            "Average fast food/retail starting wage now $15.35/hr (up 25% since 2020)",
            "Scheduling flexibility now #2 retention factor after pay",
            "Gig economy capturing 36% of working-age adults in some capacity",
            "Gen Z entering workforce — prioritize purpose, flexibility, mental health support",
            "Automation threatening 25% of current entry-level roles by 2030 (self-checkout, chatbots, kiosks)",
        ],
        "wage_growth_yoy": "+4.9% (entry-level wages outpacing salaried growth)",
        "unionization_rate": "4.2% (retail) / 1.2% (food service)",
        "remote_work_pct": "5% (primarily customer service/admin)",
    },
    "legal_services": {
        "sector_name": "Legal Services",
        "bls_sector_code": "NAICS 5411",
        "total_employment_us": "1.8M",
        "projected_growth_2024_2034": "+5.9%",
        "annual_openings": "~120K",
        "median_annual_wage": "$135,740 (lawyers) / $59,200 (paralegals)",
        "job_openings_rate_jolts": "4.3%",
        "quits_rate_jolts": "2.5%",
        "hires_rate_jolts": "4.0%",
        "layoffs_rate_jolts": "0.8%",
        "vacancy_fill_time_avg": "45 days (associate attorneys: 60 days)",
        "talent_shortage_severity": "MODERATE — Oversupply of new JD graduates but shortage in specialized areas (IP, cybersecurity law, healthcare compliance)",
        "key_trends": [
            "AI/LegalTech disruption: 44% of law firms adopting AI document review",
            "BigLaw starting salaries: $215K (Cravath scale) attracting top talent",
            "Alternative legal service providers (ALSPs) growing 12% annually",
            "In-house legal departments expanding — 30% of attorneys now in-house",
            "Law school enrollment declining 3% but JD advantage careers growing",
            "Remote/hybrid work now accepted: 58% of firms offer flexible arrangements",
        ],
        "wage_growth_yoy": "+4.2%",
        "unionization_rate": "0.8% (very low)",
        "remote_work_pct": "42% (hybrid for associates, higher for partners)",
    },
    "finance_banking": {
        "sector_name": "Financial Services & Banking",
        "bls_sector_code": "NAICS 52",
        "total_employment_us": "6.8M",
        "projected_growth_2024_2034": "+6.2%",
        "annual_openings": "~910K",
        "median_annual_wage": "$79,050 (financial specialists) / $131,710 (financial managers)",
        "job_openings_rate_jolts": "4.8%",
        "quits_rate_jolts": "2.2%",
        "hires_rate_jolts": "4.1%",
        "layoffs_rate_jolts": "1.1%",
        "vacancy_fill_time_avg": "38 days (analysts) / 55 days (compliance/risk)",
        "talent_shortage_severity": "MODERATE — General roles well-supplied, but acute shortage in quantitative finance, fintech, and regulatory compliance",
        "key_trends": [
            "Fintech disruption: 45% of banking transactions now digital-only",
            "ESG/Sustainable finance roles growing 28% annually",
            "Crypto/blockchain talent demand stabilized after 2022 correction",
            "Regulatory compliance hiring up 15% (AML, BSA, sanctions expertise)",
            "AI in finance: 60% of firms using AI for risk assessment, driving quantitative hiring",
            "Return-to-office mandates higher in finance (72% hybrid, 18% full office)",
        ],
        "wage_growth_yoy": "+4.0% (base) / +7.5% (bonus-inclusive)",
        "unionization_rate": "1.1%",
        "remote_work_pct": "35% (higher for fintech, lower for banking)",
    },
    "mental_health": {
        "sector_name": "Mental Health & Behavioral Services",
        "bls_sector_code": "NAICS 6211-6219 (subset of Healthcare)",
        "total_employment_us": "1.2M (licensed clinicians) + 400K (support staff)",
        "projected_growth_2024_2034": "+18.2% (Fastest growing healthcare subsector)",
        "annual_openings": "~190K",
        "median_annual_wage": "$53,710 (substance abuse counselors) / $86,510 (psychologists)",
        "job_openings_rate_jolts": "9.1% (highest in healthcare)",
        "quits_rate_jolts": "3.2% (burnout-driven)",
        "hires_rate_jolts": "6.1%",
        "layoffs_rate_jolts": "0.4% (extremely low — insatiable demand)",
        "vacancy_fill_time_avg": "56 days (psychiatrists: 90+ days)",
        "talent_shortage_severity": "CRITICAL — 160M Americans live in mental health professional shortage areas (HPSAs); need 10K+ additional psychiatrists",
        "key_trends": [
            "Post-pandemic demand surge: 42% increase in patients seeking mental health services",
            "Telehealth now 40% of all mental health visits (up from 8% pre-pandemic)",
            "Parity enforcement increasing insurance coverage — expanding accessible care",
            "988 Suicide & Crisis Lifeline driving need for crisis counselors nationwide",
            "School-based mental health funding up 300% — creating K-12 counselor demand",
            "Licensed Professional Counselor (LPC) recognition expanding to all 50 states",
        ],
        "wage_growth_yoy": "+5.6% (driven by telehealth platform competition)",
        "unionization_rate": "4.8% (primarily in hospital/CMHC settings)",
        "remote_work_pct": "40% (telehealth)",
    },
    "retail_consumer": {
        "sector_name": "Retail & Consumer Services",
        "bls_sector_code": "NAICS 44-45",
        "total_employment_us": "15.6M",
        "projected_growth_2024_2034": "-1.8% (automation + e-commerce shift)",
        "annual_openings": "~2.1M (high turnover replacement)",
        "median_annual_wage": "$33,680 (retail sales) / $49,900 (supervisors)",
        "job_openings_rate_jolts": "6.5%",
        "quits_rate_jolts": "3.6% (second highest after food service)",
        "hires_rate_jolts": "5.8%",
        "layoffs_rate_jolts": "1.2%",
        "vacancy_fill_time_avg": "21 days (managers: 35 days)",
        "talent_shortage_severity": "MODERATE — No skills gap but high turnover challenge; average retail tenure: 2.4 years",
        "key_trends": [
            "Omnichannel creating new roles: BOPIS specialists, social commerce managers",
            "Self-checkout/automation reducing cashier demand 15% but creating tech roles",
            "Employee experience investment up: Walmart $1B, Target $300M in worker programs",
            "Peak season hiring competition intensifying — Q4 seasonal hiring starts August",
            "Retail theft/shrinkage driving loss prevention tech hiring up 22%",
            "Experiential retail growing — customer experience roles up 18%",
        ],
        "wage_growth_yoy": "+4.3%",
        "unionization_rate": "4.5% (UFCW primary union)",
        "remote_work_pct": "8% (corporate/e-commerce roles)",
    },
    "aerospace_defense": {
        "sector_name": "Aerospace & Defense",
        "bls_sector_code": "NAICS 3364 (Aerospace) + 9271 (National Security)",
        "total_employment_us": "2.2M (direct) + 1.5M (supply chain)",
        "projected_growth_2024_2034": "+5.8%",
        "annual_openings": "~210K",
        "median_annual_wage": "$92,120 (aerospace engineers) / $77,650 (aircraft mechanics)",
        "job_openings_rate_jolts": "5.5%",
        "quits_rate_jolts": "1.8% (low — high retention industry)",
        "hires_rate_jolts": "3.9%",
        "layoffs_rate_jolts": "0.9%",
        "vacancy_fill_time_avg": "55 days (cleared roles: 75+ days due to security process)",
        "talent_shortage_severity": "HIGH — 25% of A&D workforce eligible for retirement in 5 years; security clearance backlog adds 3-6 months to hiring",
        "key_trends": [
            "FY2024 DoD budget: $886B driving sustained contractor demand",
            "Space sector hiring boom: 80% increase in commercial space roles since 2020",
            "Hypersonics, directed energy, and autonomous systems creating new specialties",
            "STEM pipeline concern: only 20% of A&D interns convert to full-time (down from 35%)",
            "Security clearance processing: 150 days average (down from 230, still a bottleneck)",
            "SpaceX, Anduril, Shield AI luring talent with startup culture + defense mission",
        ],
        "wage_growth_yoy": "+4.5%",
        "unionization_rate": "8.9% (IAM primary union for production workers)",
        "remote_work_pct": "22% (engineering; classified work on-site only)",
    },
    "pharma_biotech": {
        "sector_name": "Pharmaceutical & Biotechnology",
        "bls_sector_code": "NAICS 3254 (Pharma) + 5417 (R&D)",
        "total_employment_us": "3.4M (direct + CRO/CMO)",
        "projected_growth_2024_2034": "+9.8%",
        "annual_openings": "~320K",
        "median_annual_wage": "$95,000 (R&D scientists) / $130,000 (medical directors)",
        "job_openings_rate_jolts": "5.9%",
        "quits_rate_jolts": "2.4%",
        "hires_rate_jolts": "4.5%",
        "layoffs_rate_jolts": "1.3% (periodic pipeline-driven restructuring)",
        "vacancy_fill_time_avg": "48 days (clinical researchers) / 65 days (regulatory affairs)",
        "talent_shortage_severity": "MODERATE-HIGH — Acute shortage in bioinformatics, cell/gene therapy, and regulatory affairs; PhD oversupply in traditional biology",
        "key_trends": [
            "mRNA platform expansion beyond COVID creating new biologics manufacturing roles",
            "Cell & gene therapy sector growing 25% annually — specialized manufacturing talent scarce",
            "AI-driven drug discovery reducing R&D timelines by 30% but creating new computational roles",
            "GLP-1/obesity drug market ($100B+ projected) driving massive commercial hiring",
            "CRO consolidation: top 5 CROs now employ 250K+ clinical research professionals",
            "Biosimilars market expansion creating regulatory and market access hiring demand",
        ],
        "wage_growth_yoy": "+4.7%",
        "unionization_rate": "2.8% (primarily manufacturing)",
        "remote_work_pct": "35% (office/regulatory roles; lab work on-site)",
    },
}

# National-level JOLTS summary data (BLS reference)
NATIONAL_JOLTS_SUMMARY = {
    "total_nonfarm_openings": "8.8M (Jan 2024 JOLTS)",
    "total_hires": "5.7M/month",
    "total_separations": "5.3M/month",
    "national_quits_rate": "2.3%",
    "national_openings_rate": "5.4%",
    "national_unemployment_rate": "3.7%",
    "labour_force_participation": "62.5%",
    "u6_underemployment": "7.2%",
    "avg_hourly_earnings_all": "$34.55",
    "avg_hourly_earnings_yoy_change": "+4.1%",
    "jobs_to_unemployed_ratio": "1.4 openings per unemployed person",
}


def get_labour_market_intelligence(industry, locations):
    """
    Return rich, curated labour market intelligence data combining
    BLS/JOLTS-style industry metrics with location-specific context.
    """
    ind_data = INDUSTRY_LABOUR_MARKET.get(industry, INDUSTRY_LABOUR_MARKET.get("general_entry_level", {}))
    national = NATIONAL_JOLTS_SUMMARY

    # Build location-specific context
    location_contexts = []
    for loc in (locations or ["United States"]):
        info = get_location_info(loc)
        is_intl = info.get("is_international", False)
        country = _detect_country(loc)

        if is_intl and country and country in COUNTRY_DATA:
            c = COUNTRY_DATA[country]
            location_contexts.append({
                "location": loc,
                "country": country,
                "unemployment_rate": c.get("unemployment", "N/A"),
                "median_salary": f"${c.get('median_salary', 0):,} ({c.get('currency', 'USD')})",
                "population": c.get("population", "N/A"),
                "top_industries": c.get("top_industries", ""),
                "top_job_boards": c.get("top_boards", ""),
                "region": c.get("region", ""),
                "coli": c.get("coli", 100),
                "context_note": f"International market — {country} in {c.get('region', 'Global')} region. Local labour laws and recruitment practices apply.",
            })
        else:
            location_contexts.append({
                "location": loc,
                "country": "United States",
                "unemployment_rate": info.get("unemployment", "3.7%"),
                "median_salary": f"${info.get('median_salary', 50000):,} USD",
                "population": info.get("population", "N/A"),
                "top_industries": info.get("major_employers", ""),
                "coli": info.get("coli", 100),
                "context_note": f"US market — {info.get('state', '')} {info.get('metro_name', loc)}.",
            })

    return {
        "industry_metrics": ind_data,
        "national_summary": national,
        "location_contexts": location_contexts,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PER-COMPETITOR DIFFERENTIATED INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════════

# Known major employer hiring profiles (curated competitive intelligence)
KNOWN_EMPLOYER_PROFILES = {
    # Healthcare
    "hca healthcare": {"industry": "healthcare_medical", "size": "182 hospitals, 2,300+ sites, 283K employees", "hiring_channels": "Indeed (primary volume), LinkedIn, CareerBuilder, Glassdoor, Direct Career Site (high traffic)", "employer_brand": "Strong — 'Be Part of Something Bigger' campaign; heavy Indeed/LinkedIn investment", "known_strategies": "Relocation packages up to $10K, sign-on bonuses $5K-$30K for RNs, employee referral bonuses $3K-$5K, residency programs for new grads", "glassdoor_rating": "3.5/5", "talent_focus": "RNs, surgical techs, respiratory therapists, medical coders"},
    "kaiser permanente": {"industry": "healthcare_medical", "size": "39 hospitals, 12.6M members, 300K+ employees", "hiring_channels": "Direct career site (dominant), LinkedIn, Indeed, Glassdoor, internal mobility platform", "employer_brand": "Very Strong — 'Thrive' brand, known for work-life balance and culture", "known_strategies": "Comprehensive benefits from day 1, tuition reimbursement $10K/yr, pension plan, union partnership (SEIU-UHW), strong internal promotion", "glassdoor_rating": "4.0/5", "talent_focus": "Physicians, NPs, PAs, nurses, behavioral health clinicians"},
    "commonspirit health": {"industry": "healthcare_medical", "size": "140 hospitals, 150K employees", "hiring_channels": "Indeed, LinkedIn, Glassdoor, Catholic health network referrals", "employer_brand": "Mission-driven — faith-based healthcare system", "known_strategies": "Student loan assistance, sign-on bonuses, focus on community health mission", "glassdoor_rating": "3.4/5", "talent_focus": "Nurses, allied health, chaplains, community health workers"},
    "unitedhealth group": {"industry": "healthcare_medical", "size": "400K employees, largest US health company", "hiring_channels": "LinkedIn (heavy investment), Indeed, Direct career site, Handshake for campus", "employer_brand": "Strong corporate — 'Caring. Connecting. Growing together.'", "known_strategies": "Equity grants, tuition reimbursement, hybrid/remote for eligible roles, massive campus recruiting", "glassdoor_rating": "3.7/5", "talent_focus": "Data scientists, clinicians, actuaries, IT, nurses"},

    # Tech
    "google": {"industry": "tech_engineering", "size": "182K employees", "hiring_channels": "Direct career site, LinkedIn, employee referrals (50%+ of hires), university recruiting", "employer_brand": "Iconic — consistently #1 employer brand globally", "known_strategies": "TC $150-$400K+ for SWEs, 20% time for innovation, on-campus perks, equity refresh grants, L3-L7 career ladder", "glassdoor_rating": "4.3/5", "talent_focus": "SWEs, ML engineers, product managers, SREs"},
    "amazon": {"industry": "tech_engineering", "size": "1.5M+ employees globally", "hiring_channels": "Indeed (warehouse volume), LinkedIn (corporate), Direct site, university job fairs, military programs", "employer_brand": "Polarizing but powerful — 'Earth's Best Employer' initiative", "known_strategies": "Career Choice program (tuition pre-paid), $19-25/hr warehouse base, SDE TC $150K-$350K, high bar/leadership principles interviews", "glassdoor_rating": "3.8/5", "talent_focus": "SDEs, operations managers, warehouse associates, PMs"},
    "microsoft": {"industry": "tech_engineering", "size": "221K employees", "hiring_channels": "LinkedIn (owned), Direct career site, GitHub (owned), employee referrals, university programs", "employer_brand": "Very Strong — 'growth mindset' culture under Satya Nadella", "known_strategies": "Equity-heavy comp ($100K-$300K+ TC), inclusive hiring programs, neurodiversity program, hybrid work standard", "glassdoor_rating": "4.2/5", "talent_focus": "Cloud engineers, AI researchers, PMs, security engineers"},
    "meta": {"industry": "tech_engineering", "size": "67K employees", "hiring_channels": "LinkedIn, Direct site, employee referrals, coding competitions, university career fairs", "employer_brand": "Recovering from layoff perception — rebuilding with AI/metaverse focus", "known_strategies": "Top-of-market comp for AI/ML ($200K-$500K+ TC), flat hierarchy, rapid promotion to E5/E6, generous RSU grants", "glassdoor_rating": "4.0/5", "talent_focus": "AI/ML engineers, AR/VR engineers, infrastructure engineers"},
    "apple": {"industry": "tech_engineering", "size": "164K employees", "hiring_channels": "Direct career site (primary), LinkedIn, employee referrals, university programs, Apple Stores (retail)", "employer_brand": "Premium brand — secrecy and product pride", "known_strategies": "RSU grants, product discounts, secretive culture attracts those who want to build at scale, Apple Stores as talent pipeline", "glassdoor_rating": "4.1/5", "talent_focus": "Hardware engineers, iOS developers, ML engineers, retail"},

    # Recruitment marketing / programmatic
    "radancy": {"industry": "recruitment_marketing", "size": "1,500+ employees, 200+ enterprise clients", "hiring_channels": "LinkedIn, Direct site, industry conferences (HR Tech, SHRM)", "employer_brand": "B2B recruitment marketing leader — TalentCloud platform", "known_strategies": "Enterprise SaaS model, career site + CRM + media, programmatic job advertising, strong content marketing", "glassdoor_rating": "3.6/5", "talent_focus": "Account managers, developers, data analysts, media strategists"},
    "appcast": {"industry": "recruitment_marketing", "size": "600+ employees (StepStone Group)", "hiring_channels": "LinkedIn, Indeed, direct recruiting", "employer_brand": "Leading programmatic job advertising platform", "known_strategies": "Pay-per-applicant model, data-driven job ad distribution, 10K+ publisher network, Appcast Xcelerate platform", "glassdoor_rating": "3.8/5", "talent_focus": "Data engineers, client success, sales, product"},
    "recruitics": {"industry": "recruitment_marketing", "size": "300+ employees", "hiring_channels": "LinkedIn, direct outreach, industry events", "employer_brand": "Analytics-driven recruitment marketing", "known_strategies": "Fusion Analytics platform, programmatic + social + search, managed media services", "glassdoor_rating": "3.5/5", "talent_focus": "Media analysts, engineers, client strategists"},
    "joveo": {"industry": "recruitment_marketing", "size": "200+ employees globally", "hiring_channels": "LinkedIn, direct career site, referrals", "employer_brand": "AI-powered programmatic job advertising at scale", "known_strategies": "Mojo platform, 1200+ publisher network, AI optimization, CPA/CPC bidding, global reach", "glassdoor_rating": "4.0/5", "talent_focus": "AI/ML, data science, product, client success"},
    "pandologic": {"industry": "recruitment_marketing", "size": "200+ employees (Veritone subsidiary)", "hiring_channels": "LinkedIn, direct site, AI/tech conferences", "employer_brand": "AI-powered talent acquisition platform", "known_strategies": "pandoIQ programmatic engine, AI bidding optimization, ATS integrations", "glassdoor_rating": "3.4/5", "talent_focus": "AI engineers, sales, account management"},

    # Retail
    "walmart": {"industry": "retail_consumer", "size": "1.6M US employees, world's largest private employer", "hiring_channels": "Indeed (massive volume), Direct career site/app, in-store kiosks, social media, college recruiting", "employer_brand": "Improving — $1B+ investment in wages/training; Walmart Academy", "known_strategies": "$14-19/hr base, Live Better U (free college), fast-track management program, $200K+ store manager comp", "glassdoor_rating": "3.4/5", "talent_focus": "Store associates, supply chain, tech, pharmacists"},
    "target": {"industry": "retail_consumer", "size": "440K employees", "hiring_channels": "Indeed, LinkedIn, Direct career site, social media (Instagram focus), college campus events", "employer_brand": "Strong consumer brand translating to employer brand", "known_strategies": "$15-24/hr range, tuition-free education (250+ programs), team member discount, health benefits from day 1", "glassdoor_rating": "3.5/5", "talent_focus": "Store team, distribution, merchandising, tech/digital"},
    "costco": {"industry": "retail_consumer", "size": "310K+ employees", "hiring_channels": "Direct career site (primary), employee referrals (very strong), limited job board spend", "employer_brand": "Excellent — known as best retailer to work for", "known_strategies": "$17-29/hr range, top-tier benefits, employee-first culture, 90%+ internal promotion for management", "glassdoor_rating": "3.9/5", "talent_focus": "Warehouse associates, supervisors, buyers, logistics"},

    # Finance
    "jpmorgan chase": {"industry": "finance_banking", "size": "293K employees", "hiring_channels": "LinkedIn (major sponsor), Direct career site, university OCR, Handshake, diversity partnerships (MLT, SEO)", "employer_brand": "Premier financial services brand", "known_strategies": "Analyst program ($100K+ first year), rotational programs, tuition assistance, strong ERGs, return-to-work program", "glassdoor_rating": "3.9/5", "talent_focus": "Analysts, technologists, compliance, wealth advisors"},
    "goldman sachs": {"industry": "finance_banking", "size": "46K employees, $400K+ avg compensation", "hiring_channels": "Direct career site, university OCR (target schools), LinkedIn, employee referrals", "employer_brand": "Prestige brand — 'Our People Are Our Greatest Asset'", "known_strategies": "Top-of-market comp ($110K base for analysts), Goldman Sachs University training, strong alumni network, resilience-focused culture", "glassdoor_rating": "3.8/5", "talent_focus": "IB analysts, traders, engineers, compliance"},

    # Defense
    "lockheed martin": {"industry": "aerospace_defense", "size": "116K employees", "hiring_channels": "Direct career site, LinkedIn, military transition programs (SkillBridge), university STEM fairs", "employer_brand": "Strong mission-driven — 'Your Mission is Ours'", "known_strategies": "Clearance sponsorship, tuition reimbursement $10K/yr, veteran preference, STEM scholarships, profit sharing", "glassdoor_rating": "4.0/5", "talent_focus": "Systems engineers, software devs, program managers, cleared professionals"},
    "raytheon": {"industry": "aerospace_defense", "size": "185K employees (RTX)", "hiring_channels": "LinkedIn, Direct site, military hiring events, STEM career fairs, employee referrals", "employer_brand": "Strong — 'One RTX' merger integration", "known_strategies": "Security clearance fast-track, veteran transition programs, STEM early career programs, 9/80 schedules", "glassdoor_rating": "3.8/5", "talent_focus": "RF/EW engineers, cyber, missile systems, avionics"},
    "northrop grumman": {"industry": "aerospace_defense", "size": "90K employees", "hiring_channels": "Direct site, LinkedIn, STEM conferences, military SkillBridge, HBCUs", "employer_brand": "Space/stealth focused — 'Defining Possible'", "known_strategies": "B-21 program prestige hiring, clearance sponsorship, 4-day workweeks at some sites, strong intern conversion", "glassdoor_rating": "3.9/5", "talent_focus": "Space systems, autonomous systems, cyber, cleared SW engineers"},
}


def get_client_competitor_intelligence(competitors, industry):
    """
    Generate differentiated, per-competitor intelligence for client-specified competitors.
    Uses KNOWN_EMPLOYER_PROFILES for recognized companies, and generates
    industry-tailored intelligence for unknown companies.
    """
    results = []

    # Industry-specific default channel sets
    industry_default_channels = {
        "healthcare_medical": "Indeed (primary), LinkedIn, Glassdoor, Direct Career Site, Nurse.com, Health eCareers, hospital system referral programs",
        "blue_collar_trades": "Indeed (primary), Craigslist, Facebook Jobs, local union boards, trade school partnerships, industry job fairs",
        "maritime_marine": "Indeed, Maritime Jobs, MarineLink, MARAD job boards, maritime academy career services, industry associations",
        "military_recruitment": "Military.com, GoArmy.com, branch-specific sites, social media (Instagram/TikTok), high school/college events, recruiters in field",
        "tech_engineering": "LinkedIn (primary), Direct career site, HackerRank, Stack Overflow Talent, GitHub, Wellfound, employee referrals, university OCR",
        "general_entry_level": "Indeed (primary), Snagajob, Facebook Jobs, Craigslist, in-store applications, local job fairs, walk-in hiring events",
        "legal_services": "LinkedIn, Direct career site, LawCrossing, Above The Law job board, law school OCI programs, Lateral Link",
        "finance_banking": "LinkedIn (primary), eFinancialCareers, Direct career site, university OCR, Handshake, CFA Institute career center",
        "mental_health": "Indeed, Psychology Today therapist directory, LinkedIn, NASW job board, university practicum sites, state licensing board listings",
        "retail_consumer": "Indeed (primary), Snagajob, Direct career site/kiosks, Facebook Jobs, social media, seasonal hiring events",
        "aerospace_defense": "LinkedIn, Direct career site, ClearedJobs.net, military transition programs, STEM job fairs, university partnerships",
        "pharma_biotech": "LinkedIn (primary), BioSpace, Direct career site, Nature Jobs, Science Careers, MedReps, CRA job boards",
    }

    industry_default_strategies = {
        "healthcare_medical": "Sign-on bonuses ($5K-$50K), relocation packages, tuition reimbursement, flexible scheduling, loan repayment programs",
        "blue_collar_trades": "Competitive hourly wages, overtime availability, tool allowances, apprenticeship programs, referral bonuses, safety gear provided",
        "maritime_marine": "Sea-pay differentials, specialized training/certifications paid, rotation schedules, housing allowances, maritime credential sponsorship",
        "military_recruitment": "GI Bill ($100K+ value), signing bonuses ($5K-$50K), healthcare/housing/food, career training in 150+ fields, retirement at 20 years",
        "tech_engineering": "Equity/RSU grants, remote/hybrid work, learning stipends, modern tech stack, flat hierarchy, transparent comp bands",
        "general_entry_level": "Flexible scheduling, tuition assistance, rapid advancement, employee discounts, day-1 benefits, weekly pay options",
        "legal_services": "Competitive base + bonus, bar exam support, CLE funding, pro bono opportunities, mentorship programs, partnership track",
        "finance_banking": "Base + bonus structure ($50K-$200K bonuses), signing bonuses, rotational programs, global mobility, strong alumni network",
        "mental_health": "Clinical supervision provided, caseload management, telehealth flexibility, loan repayment (NHSC), conference attendance, CEU funding",
        "retail_consumer": "Employee discounts, flexible hours, seasonal bonuses, internal promotion paths, training programs, performance bonuses",
        "aerospace_defense": "Security clearance sponsorship, 9/80 or 4/10 schedules, tuition reimbursement, profit sharing, veteran preference, STEM scholarships",
        "pharma_biotech": "Publication support, conference attendance, equity grants, lab equipment budgets, global R&D collaboration, patent bonuses",
    }

    for comp_name in (competitors or []):
        comp_key = comp_name.lower().strip()
        profile = KNOWN_EMPLOYER_PROFILES.get(comp_key, None)

        if profile:
            results.append({
                "competitor": comp_name,
                "company_size": profile.get("size", "Unknown"),
                "primary_hiring_channels": profile.get("hiring_channels", ""),
                "employer_brand_strength": profile.get("employer_brand", ""),
                "known_recruitment_strategies": profile.get("known_strategies", ""),
                "glassdoor_rating": profile.get("glassdoor_rating", "N/A"),
                "talent_focus": profile.get("talent_focus", ""),
                "strategic_recommendation": _generate_competitive_recommendation(comp_name, profile, industry),
            })
        else:
            # Generate intelligent defaults based on industry
            default_channels = industry_default_channels.get(industry, "Indeed, LinkedIn, Direct Career Site, Glassdoor, employee referrals")
            default_strategies = industry_default_strategies.get(industry, "Competitive compensation, benefits, career development, employee referral programs")

            results.append({
                "competitor": comp_name,
                "company_size": "Research recommended — check LinkedIn company page for employee count",
                "primary_hiring_channels": default_channels,
                "employer_brand_strength": f"Monitor {comp_name}'s Glassdoor profile, career site, and social media for employer brand positioning",
                "known_recruitment_strategies": default_strategies,
                "glassdoor_rating": f"Check glassdoor.com/Reviews/{comp_name.replace(' ', '-')}-Reviews",
                "talent_focus": f"Likely competing for similar {industry.replace('_', ' ')} talent pools",
                "strategic_recommendation": f"1) Audit {comp_name}'s career site and active job postings to understand their volume and focus areas. "
                    f"2) Monitor their Glassdoor reviews to identify employer brand weaknesses you can exploit. "
                    f"3) Target candidates who follow {comp_name} on LinkedIn with competitor-targeted campaigns. "
                    f"4) Differentiate your employer brand with specific advantages (compensation, culture, mission, flexibility) that {comp_name} may lack.",
            })

    return results


def _generate_competitive_recommendation(comp_name, profile, industry):
    """Generate strategic recommendation for competing against a known employer."""
    rating = profile.get("glassdoor_rating", "3.5/5")
    try:
        rating_num = float(rating.split("/")[0])
    except (ValueError, IndexError):
        rating_num = 3.5

    recs = []

    if rating_num >= 4.0:
        recs.append(f"{comp_name} has strong employer brand ({rating}). Avoid head-to-head brand competition; instead differentiate on specific factors (compensation, location, mission, flexibility) where you excel.")
    else:
        recs.append(f"{comp_name}'s Glassdoor rating ({rating}) suggests employer brand opportunities. Highlight your cultural strengths and employee satisfaction in job ads and careers content.")

    # Channel-specific recommendation
    channels = profile.get("hiring_channels", "").lower()
    if "linkedin" in channels and "primary" in channels:
        recs.append("They invest heavily in LinkedIn — consider alternative high-ROI channels (programmatic, niche boards, social) to reach candidates before LinkedIn saturation.")
    if "indeed" in channels and ("primary" in channels or "volume" in channels):
        recs.append("They use Indeed for volume hiring — optimize your Indeed presence with premium postings and sponsor key roles, or out-bid via programmatic platforms like Joveo Mojo.")
    if "referral" in channels:
        recs.append("They leverage employee referrals significantly — build or strengthen your referral program with competitive bounties ($2K-$5K+) to compete for passive talent.")

    # Strategies recommendation
    strategies = profile.get("known_strategies", "").lower()
    if "sign-on bonus" in strategies or "signing bonus" in strategies:
        recs.append("They offer sign-on bonuses — match or exceed with creative compensation (retention bonuses, spot bonuses, accelerated reviews) to attract candidates weighing multiple offers.")
    if "tuition" in strategies or "education" in strategies:
        recs.append("They invest in education/tuition benefits — if you offer similar, promote aggressively; if not, emphasize on-the-job training, certifications, and career development paths.")

    return " ".join(recs)


# ═══════════════════════════════════════════════════════════════════════════════
# COMPANY INTELLIGENCE DATABASE  
# ═══════════════════════════════════════════════════════════════════════════════

COMPANY_INTELLIGENCE = {
    "walmart": {"size": "2.1M employees", "glassdoor": "3.4/5", "brand_strength": "Strong employer brand in retail/hourly", "hiring_volume": "High (500K+/year)", "benefits_highlight": "College tuition, $15+ min wage", "attrition": "High (60-80% hourly)"},
    "amazon": {"size": "1.5M employees", "glassdoor": "3.5/5", "brand_strength": "Strong tech brand, mixed warehouse perception", "hiring_volume": "Very High (800K+/year)", "benefits_highlight": "Day-1 benefits, Career Choice program", "attrition": "Very High (>100% warehouse)"},
    "uber": {"size": "32K employees + 5M drivers", "glassdoor": "3.8/5", "brand_strength": "Strong tech brand, driver satisfaction varies", "hiring_volume": "Continuous driver onboarding", "benefits_highlight": "Flexible schedule, earnings transparency", "attrition": "Very High driver churn"},
    "fedex": {"size": "500K employees", "glassdoor": "3.5/5", "brand_strength": "Trusted logistics brand", "hiring_volume": "High (seasonal peaks)", "benefits_highlight": "Tuition reimbursement, health benefits", "attrition": "Moderate (40-50% hourly)"},
    "marriott": {"size": "290K employees", "glassdoor": "3.9/5", "brand_strength": "Top hospitality employer brand", "hiring_volume": "Moderate-High", "benefits_highlight": "Hotel discounts, career mobility", "attrition": "High in hourly roles"},
    "jpmorgan": {"size": "300K employees", "glassdoor": "3.8/5", "brand_strength": "Top-tier finance employer", "hiring_volume": "Moderate (20K+/year)", "benefits_highlight": "Competitive comp, training programs", "attrition": "Low-Moderate"},
    "deloitte": {"size": "415K employees", "glassdoor": "3.9/5", "brand_strength": "Big 4 prestige", "hiring_volume": "High (campus + experienced)", "benefits_highlight": "Professional development, flex work", "attrition": "Moderate (25-30%)"},
    "tesla": {"size": "140K employees", "glassdoor": "3.3/5", "brand_strength": "Strong mission-driven brand, mixed reviews", "hiring_volume": "High (factory expansion)", "benefits_highlight": "Stock options, mission appeal", "attrition": "High (manufacturing)"},
    "starbucks": {"size": "380K employees", "glassdoor": "3.6/5", "brand_strength": "Strong employer brand in food service", "hiring_volume": "Very High (100K+/year)", "benefits_highlight": "ASU tuition, healthcare for part-time", "attrition": "High (65% barista)"},
    "google": {"size": "180K employees", "glassdoor": "4.3/5", "brand_strength": "Top tech employer globally", "hiring_volume": "Moderate (15K+/year)", "benefits_highlight": "Best-in-class perks, 20% time", "attrition": "Low (10-12%)"},
    "microsoft": {"size": "220K employees", "glassdoor": "4.2/5", "brand_strength": "Top tech employer", "hiring_volume": "Moderate (20K+/year)", "benefits_highlight": "RSUs, excellent benefits", "attrition": "Low (10-15%)"},
    "apple": {"size": "164K employees", "glassdoor": "4.1/5", "brand_strength": "Premium tech brand", "hiring_volume": "Moderate", "benefits_highlight": "Product discounts, health programs", "attrition": "Low"},
    "unitedhealth": {"size": "400K employees", "glassdoor": "3.7/5", "brand_strength": "Largest health company", "hiring_volume": "High (50K+/year)", "benefits_highlight": "Health benefits, tuition assistance", "attrition": "Moderate"},
    "target": {"size": "440K employees", "glassdoor": "3.5/5", "brand_strength": "Strong retail employer brand", "hiring_volume": "High (seasonal peaks)", "benefits_highlight": "$15+ min wage, tuition free", "attrition": "High (50-70% hourly)"},
    "hca": {"size": "280K employees", "glassdoor": "3.4/5", "brand_strength": "Largest hospital system", "hiring_volume": "Very High (nursing shortage)", "benefits_highlight": "Sign-on bonuses, tuition reimbursement", "attrition": "High (nursing turnover 20-25%)"},
}

def get_company_intelligence(company_name: str) -> dict:
    """Look up company intelligence data. Returns dict with company data or generic defaults."""
    if not company_name:
        return {}
    
    company_lower = company_name.lower().strip()
    
    # Direct match
    for key, data in COMPANY_INTELLIGENCE.items():
        if key in company_lower or company_lower in key:
            return {**data, "matched": True}
    
    # Partial match
    for key, data in COMPANY_INTELLIGENCE.items():
        if any(word in company_lower for word in key.split()):
            return {**data, "matched": True}
    
    return {"matched": False, "note": "Company not in intelligence database - using industry defaults"}


# ═══════════════════════════════════════════════════════════════════════════════
# HIRING COMPLIANCE & REGULATORY INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════════

STATE_HIRING_REGULATIONS = {
    "CA": ["Salary transparency required (SB 1162)", "Ban-the-box (Fair Chance Act)", "CCPA privacy compliance for candidates", "Paid sick leave mandate"],
    "NY": ["NYC salary range disclosure (Local Law 32)", "Ban-the-box", "Paid family leave", "NYC AI hiring law (Local Law 144)"],
    "CO": ["Salary range required in all postings (EPEW Act)", "Equal pay transparency", "Paid family leave"],
    "WA": ["Salary range disclosure required (SB 5761)", "Ban-the-box", "Paid family leave"],
    "IL": ["AI Video Interview Act", "Equal Pay Act amendments", "Chicago fair workweek ordinance"],
    "MA": ["Salary history ban", "Equal Pay Act", "Predictive scheduling discussions"],
    "NJ": ["Salary history ban", "Ban-the-box", "Paid sick leave", "Temp worker protections"],
    "CT": ["Salary range disclosure required", "Ban-the-box", "Pay equity"],
    "MD": ["Salary history ban", "Equal pay for equal work", "Ban-the-box (statewide)"],
    "TX": ["At-will employment", "No state salary transparency mandate", "E-Verify for state contractors"],
    "FL": ["At-will employment", "E-Verify requirement for employers >25", "No state salary transparency mandate"],
    "GA": ["At-will employment", "Limited salary transparency requirements"],
    "PA": ["Philadelphia ban-the-box", "Philadelphia salary history ban (local)"],
    "OH": ["At-will employment", "Limited state-level hiring regulations"],
    "MI": ["Ban-the-box (public employers)", "Paid medical leave act"],
    "VA": ["Salary transparency discussions in progress", "At-will employment"],
    "NC": ["At-will employment", "Limited state-level hiring mandates"],
    "AZ": ["At-will employment", "E-Verify requirement", "Proposition 206 min wage"],
    "TN": ["At-will employment", "Tennessee Lawful Employment Act (E-Verify)"],
    "IN": ["At-will employment", "E-Verify for state agencies", "Limited salary transparency"],
    "MN": ["Salary range disclosure required (2024+)", "Ban-the-box", "Paid sick leave"],
    "HI": ["Salary range disclosure required", "Temp staffing regulations"],
    "DC": ["Salary range transparency required", "Ban-the-box", "Paid family leave"],
    "RI": ["Salary range disclosure required", "Ban-the-box"],
    "NV": ["Salary range disclosure required", "Ban-the-box"],
}

def get_hiring_regulations(locations: list) -> list:
    """Get relevant hiring regulations for the given locations."""
    regulations = []
    seen_states = set()
    
    for loc in locations:
        # Extract state code from location
        loc_upper = loc.upper().strip()
        state = None
        
        # Check if location ends with state code
        parts = loc_upper.replace(",", " ").split()
        for part in reversed(parts):
            if len(part) == 2 and part in STATE_HIRING_REGULATIONS:
                state = part
                break
        
        # Also try to match city to state via _extract_state and METRO_DATA
        if not state:
            extracted = _extract_state(loc)
            if extracted and extracted in STATE_HIRING_REGULATIONS:
                state = extracted
        
        if not state:
            # Try metro data for city-to-state mapping
            metro_key, metro_info = _find_metro(loc)
            if metro_info:
                metro_state = metro_info.get("state", "")
                if metro_state in STATE_HIRING_REGULATIONS:
                    state = metro_state
        
        if state and state not in seen_states:
            seen_states.add(state)
            regs = STATE_HIRING_REGULATIONS.get(state, [])
            if regs:
                regulations.append({
                    "state": state,
                    "location": loc,
                    "regulations": regs,
                    "compliance_note": f"Job postings in {state} must comply with: {'; '.join(regs[:2])}"
                })
    
    # Federal regulations (always applicable)
    regulations.append({
        "state": "Federal",
        "location": "All US",
        "regulations": ["EEOC compliance", "OFCCP for federal contractors", "ADA reasonable accommodations", "I-9 employment eligibility"],
        "compliance_note": "All US hiring must comply with EEOC, ADA, and I-9 requirements"
    })
    
    return regulations


# ═══════════════════════════════════════════════════════════════════════════════
# CAMPUS RECRUITING MODULE
# ═══════════════════════════════════════════════════════════════════════════════

def get_campus_recruiting_recommendations(locations: list, roles: list = None, industry: str = "") -> list:
    """Generate campus recruiting recommendations based on locations and roles."""
    recommendations = []
    
    for loc in locations:
        # Use existing helper functions to extract state
        state = _extract_state(loc)
        
        if not state:
            # Try metro data for city-to-state mapping
            metro_key, metro_info = _find_metro(loc)
            if metro_info:
                state = metro_info.get("state", "")
        
        if not state:
            continue
        
        # Use STATE_UNIVERSITIES if available
        state_unis = STATE_UNIVERSITIES.get(state, [])
        if state_unis:
            for uni in state_unis[:3]:  # Top 3 universities per location
                recommendations.append({
                    "location": loc,
                    "state": state,
                    "university": uni.get("name", ""),
                    "programs": uni.get("programs", ""),
                    "enrollment": uni.get("enrollment", ""),
                    "recruiting_channel": "Campus career fair, On-campus events, University job board"
                })
    
    return recommendations


# ═══════════════════════════════════════════════════════════════════════════════
# SEASONAL HIRING CALENDAR
# ═══════════════════════════════════════════════════════════════════════════════

SEASONAL_HIRING_CALENDAR = {
    "retail": {"peak_months": ["Sep", "Oct", "Nov"], "ramp_start": "Aug", "note": "Holiday hiring season - start campaigns by August for November readiness"},
    "hospitality": {"peak_months": ["Mar", "Apr", "May"], "ramp_start": "Feb", "note": "Summer season prep - hotels/restaurants staff up for Memorial Day through Labor Day"},
    "education": {"peak_months": ["Mar", "Apr", "May"], "ramp_start": "Feb", "note": "Academic hiring cycle - most teaching positions posted Feb-May for fall start"},
    "healthcare": {"peak_months": ["Jan", "Feb", "Jul"], "ramp_start": "Dec", "note": "Nursing residency cycles (Jan, Jul) drive peak hiring. Travel nurse demand spikes in winter"},
    "technology": {"peak_months": ["Jan", "Feb", "Sep"], "ramp_start": "Nov", "note": "New year budget cycles and post-summer campus hire onboarding drive tech hiring peaks"},
    "finance": {"peak_months": ["Jan", "Feb", "Sep"], "ramp_start": "Nov", "note": "New fiscal year budgets and fall campus recruiting drive hiring peaks"},
    "transportation": {"peak_months": ["Sep", "Oct", "Nov"], "ramp_start": "Aug", "note": "Peak shipping season (holiday e-commerce) drives massive driver/warehouse hiring"},
    "manufacturing": {"peak_months": ["Jan", "Mar", "Sep"], "ramp_start": "Dec", "note": "Production ramp-ups for new model years and seasonal demand fluctuations"},
    "construction": {"peak_months": ["Mar", "Apr", "May"], "ramp_start": "Feb", "note": "Spring construction season drives hiring for field workers and project managers"},
    "energy": {"peak_months": ["Mar", "Apr", "Jun"], "ramp_start": "Feb", "note": "Spring/summer field operations and maintenance windows drive seasonal hiring"},
    "government": {"peak_months": ["Oct", "Mar", "Jul"], "ramp_start": "Sep", "note": "Federal fiscal year (Oct 1) and mid-year budget releases drive government hiring"},
}

# Industry key to seasonal calendar key mapping
_INDUSTRY_TO_SEASONAL = {
    "healthcare_medical": "healthcare",
    "tech_engineering": "technology",
    "blue_collar_trades": "manufacturing",
    "finance_banking": "finance",
    "retail_consumer": "retail",
    "hospitality_travel": "hospitality",
    "logistics_supply_chain": "transportation",
    "energy_utilities": "energy",
    "construction_real_estate": "construction",
    "education": "education",
    "automotive": "manufacturing",
    "food_beverage": "hospitality",
    "pharma_biotech": "healthcare",
    "insurance": "finance",
    "telecommunications": "technology",
    "media_entertainment": "technology",
    "aerospace_defense": "manufacturing",
    "legal_services": "finance",
    "mental_health": "healthcare",
    "maritime_marine": "transportation",
}

def get_seasonal_hiring_advice(industry: str) -> dict:
    """Get seasonal hiring calendar advice for the industry."""
    industry_lower = industry.lower().strip() if industry else ""
    
    # Try mapped key first
    mapped = _INDUSTRY_TO_SEASONAL.get(industry_lower, "")
    if mapped and mapped in SEASONAL_HIRING_CALENDAR:
        return SEASONAL_HIRING_CALENDAR[mapped]
    
    # Try direct match
    for key, data in SEASONAL_HIRING_CALENDAR.items():
        if key in industry_lower or industry_lower in key:
            return data
    
    return {"peak_months": ["Jan", "Sep"], "ramp_start": "Dec", "note": "Standard hiring follows Q1 budget releases and fall planning cycles"}
