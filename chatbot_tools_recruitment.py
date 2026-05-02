"""Anthropic-style tool schemas + dispatch table for free recruitment APIs.

Exports:
    - RECRUITMENT_TOOLS_SCHEMA: list[dict]
        Anthropic tool-use schemas (one per function in recruitment_apis).
        Each entry has "name", "description", and "input_schema".
    - RECRUITMENT_TOOL_DISPATCH: dict[str, callable]
        Maps tool name (matching the function name) to the implementation
        callable in recruitment_apis.

The parent agent is expected to wire RECRUITMENT_TOOLS_SCHEMA into the
chatbot tool list and call RECRUITMENT_TOOL_DISPATCH[name](**args) at
execution time. This module does not modify nova.py or any other file.
"""

from __future__ import annotations

from typing import Callable

from recruitment_apis import (
    enrich_person_pdl,
    lookup_company_crunchbase,
    lookup_compensation_levels,
    lookup_country_indicator_worldbank,
    lookup_country_labour_ilostat,
    lookup_healthcare_npi,
    lookup_layoffs_warntracker,
    lookup_occupation_esco,
    lookup_skill_esco,
    lookup_tech_jobs_hnhiring,
    lookup_trucking_carrier,
)


RECRUITMENT_TOOLS_SCHEMA: list[dict] = [
    {
        "name": "lookup_skill_esco",
        "description": (
            "Look up a skill in the ESCO taxonomy (European Commission, free, no key) "
            "and return up to 5 matching skills with URI, title, and description."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "skill": {
                    "type": "string",
                    "description": "Free-text skill name, e.g. 'python', 'welding'.",
                },
                "lang": {
                    "type": "string",
                    "description": "ISO 639-1 language code (e.g. 'en', 'de'). Defaults to 'en'.",
                    "default": "en",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Per-call timeout in seconds.",
                    "default": 10,
                },
            },
            "required": ["skill"],
        },
    },
    {
        "name": "lookup_occupation_esco",
        "description": (
            "Look up an occupation in the ESCO taxonomy (European Commission, free, no key) "
            "and return up to 5 matching occupations with URI, title, and description."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "occupation": {
                    "type": "string",
                    "description": "Free-text occupation name, e.g. 'software developer'.",
                },
                "lang": {
                    "type": "string",
                    "description": "ISO 639-1 language code. Defaults to 'en'.",
                    "default": "en",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Per-call timeout in seconds.",
                    "default": 10,
                },
            },
            "required": ["occupation"],
        },
    },
    {
        "name": "lookup_healthcare_npi",
        "description": (
            "Search the US NPPES NPI Registry for healthcare providers by name or "
            "10-digit NPI number, optionally filtered by US state."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name_or_npi": {
                    "type": "string",
                    "description": "Provider name (e.g. 'Smith') or 10-digit NPI number.",
                },
                "state": {
                    "type": "string",
                    "description": "Optional 2-letter US state abbreviation (e.g. 'CA').",
                    "default": "",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of results (1-200).",
                    "default": 10,
                },
                "timeout": {
                    "type": "integer",
                    "description": "Per-call timeout in seconds.",
                    "default": 10,
                },
            },
            "required": ["name_or_npi"],
        },
    },
    {
        "name": "lookup_trucking_carrier",
        "description": (
            "Look up a US trucking carrier in the FMCSA QC Mobile registry by USDOT "
            "number (digits only) or by carrier name."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dot_or_name": {
                    "type": "string",
                    "description": "USDOT number (digits only) or carrier name string.",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Per-call timeout in seconds.",
                    "default": 10,
                },
            },
            "required": ["dot_or_name"],
        },
    },
    {
        "name": "lookup_country_labour_ilostat",
        "description": (
            "Fetch a country's labour-market series from ILOSTAT (default: annual "
            "unemployment rate) with automatic World Bank fallback on failure."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "country_iso3": {
                    "type": "string",
                    "description": "ISO 3-letter country code (e.g. 'USA', 'DEU').",
                },
                "indicator": {
                    "type": "string",
                    "description": "ILOSTAT dataflow ID. Defaults to UNE_DEAP_SEX_AGE_RT_A (unemployment rate).",
                    "default": "UNE_DEAP_SEX_AGE_RT_A",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Per-call timeout in seconds.",
                    "default": 15,
                },
            },
            "required": ["country_iso3"],
        },
    },
    {
        "name": "lookup_country_indicator_worldbank",
        "description": (
            "Fetch any World Bank Open Data v2 indicator series for a country "
            "(e.g. unemployment SL.UEM.TOTL.ZS, GDP NY.GDP.MKTP.CD)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "country_iso3": {
                    "type": "string",
                    "description": "ISO 3-letter country code (e.g. 'USA').",
                },
                "indicator": {
                    "type": "string",
                    "description": "World Bank indicator code (e.g. 'SL.UEM.TOTL.ZS').",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Per-call timeout in seconds.",
                    "default": 10,
                },
            },
            "required": ["country_iso3", "indicator"],
        },
    },
    {
        "name": "lookup_layoffs_warntracker",
        "description": (
            "Return a citable WARNTracker.com URL for US WARN Act layoff notices "
            "filtered by state and year (no live JSON API; URL only)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "state": {
                    "type": "string",
                    "description": "Optional 2-letter US state filter (e.g. 'CA').",
                    "default": "",
                },
                "since_year": {
                    "type": "integer",
                    "description": "Year filter for the returned URL.",
                    "default": 2026,
                },
                "timeout": {
                    "type": "integer",
                    "description": "Per-call timeout in seconds (unused).",
                    "default": 15,
                },
            },
            "required": [],
        },
    },
    {
        "name": "lookup_tech_jobs_hnhiring",
        "description": (
            "Search Hacker News 'Who is hiring' threads via the Algolia public API "
            "for matching tech-job posts containing the word 'hiring'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Free-text query, e.g. 'python remote senior'.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of hits to return (capped at 50).",
                    "default": 10,
                },
                "timeout": {
                    "type": "integer",
                    "description": "Per-call timeout in seconds.",
                    "default": 10,
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "lookup_compensation_levels",
        "description": (
            "Return a Levels.fyi public embed URL for compensation data on a given "
            "role and location (no public JSON API; embed URL only)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "role": {
                    "type": "string",
                    "description": "Job title, e.g. 'Software Engineer'.",
                },
                "location": {
                    "type": "string",
                    "description": "Optional location filter (e.g. 'San Francisco').",
                    "default": "",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Per-call timeout in seconds (unused).",
                    "default": 10,
                },
            },
            "required": ["role"],
        },
    },
    {
        "name": "lookup_company_crunchbase",
        "description": (
            "Search Crunchbase v4 for a company by name (requires CRUNCHBASE_API_KEY; "
            "returns a stub note when the key is missing)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Company name to search.",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Per-call timeout in seconds.",
                    "default": 10,
                },
            },
            "required": ["name"],
        },
    },
    {
        "name": "enrich_person_pdl",
        "description": (
            "Enrich a person via People Data Labs v5 by email or LinkedIn URL "
            "(requires PDL_API_KEY; returns a stub note when the key is missing)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "linkedin_or_email": {
                    "type": "string",
                    "description": (
                        "Email address (contains '@') or LinkedIn URL/handle."
                    ),
                },
                "timeout": {
                    "type": "integer",
                    "description": "Per-call timeout in seconds.",
                    "default": 10,
                },
            },
            "required": ["linkedin_or_email"],
        },
    },
]


RECRUITMENT_TOOL_DISPATCH: dict[str, Callable[..., dict]] = {
    "lookup_skill_esco": lookup_skill_esco,
    "lookup_occupation_esco": lookup_occupation_esco,
    "lookup_healthcare_npi": lookup_healthcare_npi,
    "lookup_trucking_carrier": lookup_trucking_carrier,
    "lookup_country_labour_ilostat": lookup_country_labour_ilostat,
    "lookup_country_indicator_worldbank": lookup_country_indicator_worldbank,
    "lookup_layoffs_warntracker": lookup_layoffs_warntracker,
    "lookup_tech_jobs_hnhiring": lookup_tech_jobs_hnhiring,
    "lookup_compensation_levels": lookup_compensation_levels,
    "lookup_company_crunchbase": lookup_company_crunchbase,
    "enrich_person_pdl": enrich_person_pdl,
}


# Sanity check: schema names and dispatch keys must match exactly.
assert {t["name"] for t in RECRUITMENT_TOOLS_SCHEMA} == set(
    RECRUITMENT_TOOL_DISPATCH.keys()
), "RECRUITMENT_TOOLS_SCHEMA names must match RECRUITMENT_TOOL_DISPATCH keys"
