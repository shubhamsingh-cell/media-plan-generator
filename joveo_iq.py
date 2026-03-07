"""
Joveo IQ -- AI-powered recruitment marketing intelligence chatbot.

Provides conversational access to:
- Joveo's proprietary supply data (publishers, channels, global supply)
- 25 live API enrichment sources (salary, demand, location, ad platforms)
- Recruitment industry knowledge base (42 sources)
- Data synthesis engine (fused intelligence with confidence scores)
- Budget allocation engine ($ projections)

Works in two modes:
1. Rule-based (default): keyword-matching routes questions to data sources
2. Claude API (optional): uses Anthropic Claude for natural-language reasoning

Enable Claude mode by setting ANTHROPIC_API_KEY environment variable.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent / "data"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
JOVEO_PRIMARY_COLOR = "#0066CC"
MAX_HISTORY_TURNS = 20
MAX_MESSAGE_LENGTH = 4000
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# Country name aliases for fuzzy matching
_COUNTRY_ALIASES: Dict[str, str] = {
    "us": "United States", "usa": "United States", "united states": "United States",
    "america": "United States", "uk": "United Kingdom", "britain": "United Kingdom",
    "united kingdom": "United Kingdom", "england": "United Kingdom",
    "germany": "Germany", "deutschland": "Germany",
    "france": "France", "india": "India", "australia": "Australia",
    "canada": "Canada", "japan": "Japan", "italy": "Italy",
    "netherlands": "Netherlands", "holland": "Netherlands",
    "spain": "Spain", "brazil": "Brazil", "mexico": "Mexico",
    "south africa": "South Africa", "ireland": "Ireland",
    "singapore": "Singapore", "uae": "United Arab Emirates",
    "saudi arabia": "Saudi Arabia", "poland": "Poland",
    "sweden": "Sweden", "norway": "Norway", "denmark": "Denmark",
    "switzerland": "Switzerland", "belgium": "Belgium", "austria": "Austria",
    "south korea": "South Korea", "korea": "South Korea",
    "new zealand": "New Zealand", "china": "China",
    "philippines": "Philippines", "indonesia": "Indonesia",
    "malaysia": "Malaysia", "thailand": "Thailand", "vietnam": "Vietnam",
    "argentina": "Argentina", "colombia": "Colombia", "chile": "Chile",
    "portugal": "Portugal", "czech republic": "Czech Republic",
    "romania": "Romania", "hungary": "Hungary", "turkey": "Turkey",
    "nigeria": "Nigeria", "kenya": "Kenya", "egypt": "Egypt",
    "israel": "Israel", "taiwan": "Taiwan",
}

# Role keywords for intent detection
_ROLE_KEYWORDS: Dict[str, List[str]] = {
    "nursing": ["nurse", "nursing", "rn", "lpn", "cna", "registered nurse"],
    "engineering": ["engineer", "engineering", "developer", "programmer", "coder", "devops", "sre"],
    "technology": ["tech", "software", "data scientist", "data engineer", "ml engineer", "ai engineer"],
    "healthcare": ["doctor", "physician", "therapist", "pharmacist", "medical", "clinical",
                    "dental", "veterinary", "paramedic", "emt"],
    "retail": ["retail", "cashier", "store associate", "merchandiser", "store manager"],
    "hospitality": ["chef", "cook", "waiter", "waitress", "bartender", "hotel", "restaurant"],
    "transportation": ["driver", "trucker", "cdl", "logistics", "warehouse", "forklift"],
    "finance": ["accountant", "analyst", "banker", "financial", "auditor", "actuary"],
    "executive": ["executive", "director", "vp", "vice president", "c-suite", "cfo", "cto", "ceo"],
    "hourly": ["hourly", "part-time", "part time", "entry-level", "entry level", "seasonal", "gig"],
    "education": ["teacher", "professor", "instructor", "educator", "principal", "tutor"],
    "construction": ["construction", "carpenter", "plumber", "electrician", "mason", "welder"],
    "sales": ["sales", "account executive", "business development", "bdr", "sdr"],
    "marketing": ["marketing", "seo", "content", "social media manager", "brand"],
}

# Metric keywords for intent detection
_METRIC_KEYWORDS: Dict[str, List[str]] = {
    "cpc": ["cpc", "cost per click", "cost-per-click"],
    "cpa": ["cpa", "cost per application", "cost-per-application", "cost per apply"],
    "cph": ["cost per hire", "cost-per-hire", "cph", "hiring cost"],
    "salary": ["salary", "compensation", "pay", "wage", "earnings", "income"],
    "budget": ["budget", "spend", "allocation", "investment", "roi"],
    "time_to_fill": ["time to fill", "time-to-fill", "days to fill", "time to hire",
                      "time-to-hire", "ttf"],
    "apply_rate": ["apply rate", "application rate", "conversion rate", "cvr"],
    "benchmark": ["benchmark", "average", "industry average", "standard", "compare", "comparison"],
}

# Industry keywords
_INDUSTRY_KEYWORDS: Dict[str, List[str]] = {
    "healthcare": ["healthcare", "health care", "hospital", "medical", "pharma", "biotech"],
    "technology": ["technology", "tech", "software", "saas", "it", "information technology"],
    "finance": ["finance", "banking", "insurance", "financial", "fintech"],
    "retail": ["retail", "e-commerce", "ecommerce", "store", "shopping"],
    "hospitality": ["hospitality", "hotel", "restaurant", "tourism", "travel"],
    "manufacturing": ["manufacturing", "industrial", "production", "factory", "automotive"],
    "transportation": ["transportation", "logistics", "trucking", "shipping", "supply chain"],
    "construction": ["construction", "real estate", "building", "contractor"],
    "education": ["education", "school", "university", "academic", "k-12"],
    "energy": ["energy", "oil", "gas", "renewable", "solar", "utility"],
    "government": ["government", "federal", "military", "defense", "public sector"],
}


# ═══════════════════════════════════════════════════════════════════════════════
# JOVEO IQ ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class JoveoIQ:
    """Joveo IQ chatbot engine.

    Loads Joveo's proprietary data sources and provides tool-based access
    for answering recruitment marketing questions.
    """

    def __init__(self):
        self._data_cache: Dict[str, Any] = {}
        self._load_data_sources()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_data_sources(self):
        """Load all static data sources into memory."""
        data_files = {
            "global_supply": "global_supply.json",
            "channels_db": "channels_db.json",
            "joveo_publishers": "joveo_publishers.json",
            "knowledge_base": "recruitment_industry_knowledge.json",
            "linkedin_guidewire": "linkedin_guidewire_data.json",
        }
        for key, filename in data_files.items():
            filepath = DATA_DIR / filename
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    self._data_cache[key] = json.load(f)
                logger.info("Loaded %s from %s", key, filepath)
            except FileNotFoundError:
                logger.warning("Data file not found: %s", filepath)
                self._data_cache[key] = {}
            except json.JSONDecodeError as exc:
                logger.error("JSON parse error in %s: %s", filepath, exc)
                self._data_cache[key] = {}
            except Exception as exc:
                logger.error("Failed to load %s: %s", key, exc)
                self._data_cache[key] = {}

    # ------------------------------------------------------------------
    # System prompt (for Claude API mode)
    # ------------------------------------------------------------------

    def get_system_prompt(self) -> str:
        """Build the system prompt for Claude with full context about Joveo's capabilities."""
        kb = self._data_cache.get("knowledge_base", {})
        publishers = self._data_cache.get("joveo_publishers", {})
        total_pubs = publishers.get("total_active_publishers", 0)
        pub_categories = list(publishers.get("by_category", {}).keys())
        pub_countries = list(publishers.get("by_country", {}).keys())

        supply = self._data_cache.get("global_supply", {})
        supply_countries = list(supply.get("country_job_boards", {}).keys())

        channels = self._data_cache.get("channels_db", {})
        channel_industries = list(channels.get("traditional_channels", {}).get("niche_by_industry", {}).keys())

        return f"""You are Joveo IQ, an AI-powered recruitment marketing intelligence assistant built by Joveo.

Joveo is a leader in programmatic recruitment advertising, helping employers optimize their hiring spend across job boards, social channels, and programmatic networks worldwide.

## YOUR CAPABILITIES

You have access to the following proprietary data through tools:

1. **Joveo Publisher Network**: {total_pubs:,} active publishers across {len(pub_countries)} countries
   - Categories: {', '.join(pub_categories[:10])}{'...' if len(pub_categories) > 10 else ''}

2. **Global Supply Intelligence**: Job boards and supply data for {len(supply_countries)} countries
   - Including DEI boards, women-focused boards, and niche industry boards

3. **Channel Database**: Traditional and non-traditional recruitment channels
   - Industry niches: {', '.join(channel_industries[:8])}{'...' if len(channel_industries) > 8 else ''}

4. **Recruitment Industry Knowledge Base**: Comprehensive benchmarks and trends from 42 sources
   - CPC/CPA/CPH benchmarks by platform and industry
   - Apply rates and conversion metrics
   - Market trends (AI in recruiting, programmatic advertising, skills-based hiring)
   - Platform-specific data (Indeed, LinkedIn, ZipRecruiter, Google Ads, Meta, etc.)
   - Regional insights across APAC, EMEA, Americas

5. **Budget Projection Engine**: Can model spend allocation with projected clicks, applications, and hires

6. **Salary Intelligence**: Role-specific compensation data from BLS, O*NET, and commercial sources

7. **Market Demand Signals**: Job posting volumes, growth trends, and competitive intelligence

8. **LinkedIn Hiring Intelligence (Guidewire Case Study)**: Comprehensive hiring value review
   - Influenced hire rates, skill density analysis, AI-assisted recruiter workflows
   - Peer benchmarks: Stripe, GitLab, Coinbase, NerdWallet, Qualtrics, TCS, Sabre, Robinhood, Talkdesk
   - LinkedIn product adoption: Recruiter, InMail, Hiring Manager tools
   - Use as a reference for tech company hiring performance benchmarks

## RESPONSE GUIDELINES

- Always cite your data sources (e.g., "Based on Joveo's publisher network data..." or "According to our industry knowledge base...")
- Communicate confidence levels when data quality varies
- Use specific numbers and benchmarks whenever available
- When data is limited, clearly state assumptions
- Format responses with clear structure using markdown
- For budget questions, provide concrete dollar projections when possible
- Recommend Joveo's programmatic approach when relevant but do not be overtly promotional
- Keep responses focused and actionable for recruitment marketing professionals
"""

    # ------------------------------------------------------------------
    # Tool definitions (for Claude API mode)
    # ------------------------------------------------------------------

    def get_tool_definitions(self) -> list:
        """Define tools that Claude can call to access Joveo's data."""
        return [
            {
                "name": "query_global_supply",
                "description": "Search Joveo's global supply intelligence: country-specific job boards, DEI boards, women-focused boards, and monthly spend data. Use for questions about which job boards to use in specific countries or regions.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "country": {
                            "type": "string",
                            "description": "Country name (e.g., 'United States', 'Germany', 'India')"
                        },
                        "board_type": {
                            "type": "string",
                            "enum": ["general", "dei", "women", "all"],
                            "description": "Type of boards to query"
                        },
                        "category": {
                            "type": "string",
                            "description": "Board category filter (e.g., 'Tech', 'Healthcare', 'General')"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_channels",
                "description": "Search Joveo's channel database: traditional job boards, niche industry boards, non-traditional channels. Use for questions about recruitment channels by industry.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "industry": {
                            "type": "string",
                            "description": "Industry to filter channels (e.g., 'healthcare_medical', 'tech_engineering')"
                        },
                        "channel_type": {
                            "type": "string",
                            "enum": ["regional_local", "global_reach", "niche_by_industry", "non_traditional", "all"],
                            "description": "Type of channels"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_publishers",
                "description": "Search Joveo's active publisher network of 1,238+ publishers by country or category. Use for questions about specific publishers or publisher availability.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "country": {
                            "type": "string",
                            "description": "Country to filter publishers"
                        },
                        "category": {
                            "type": "string",
                            "description": "Publisher category (e.g., 'DEI', 'Health', 'Tech', 'Social Media')"
                        },
                        "search_term": {
                            "type": "string",
                            "description": "Search term to find specific publishers by name"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_knowledge_base",
                "description": "Search Joveo's recruitment industry knowledge base: CPC/CPA/CPH benchmarks, apply rates, market trends, platform insights, industry-specific data. Use for questions about industry benchmarks, costs, and trends.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "enum": ["benchmarks", "trends", "platforms", "regional", "industry_specific", "all"],
                            "description": "Knowledge base topic area"
                        },
                        "metric": {
                            "type": "string",
                            "description": "Specific metric (e.g., 'cpc', 'cpa', 'cost_per_hire', 'apply_rate', 'time_to_fill')"
                        },
                        "industry": {
                            "type": "string",
                            "description": "Industry for industry-specific benchmarks"
                        },
                        "platform": {
                            "type": "string",
                            "description": "Platform name (e.g., 'indeed', 'linkedin', 'google_ads')"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_salary_data",
                "description": "Get salary intelligence for specific roles and locations. Returns compensation ranges from BLS, O*NET, and commercial sources.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "role": {
                            "type": "string",
                            "description": "Job role or title (e.g., 'Registered Nurse', 'Software Engineer')"
                        },
                        "location": {
                            "type": "string",
                            "description": "Location (city, state, or country)"
                        }
                    },
                    "required": ["role"]
                }
            },
            {
                "name": "query_market_demand",
                "description": "Get job market demand signals: posting volumes, growth trends, competition level for specific roles and locations.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "role": {
                            "type": "string",
                            "description": "Job role or title"
                        },
                        "location": {
                            "type": "string",
                            "description": "Location (city, state, or country)"
                        },
                        "industry": {
                            "type": "string",
                            "description": "Industry context"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_budget_projection",
                "description": "Project budget allocation across channels with projected clicks, applications, and hires. Use for questions about budget planning and ROI modeling.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "budget": {
                            "type": "number",
                            "description": "Total budget in USD"
                        },
                        "roles": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of role titles to hire for"
                        },
                        "locations": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of hiring locations"
                        },
                        "industry": {
                            "type": "string",
                            "description": "Industry classification"
                        }
                    },
                    "required": ["budget"]
                }
            },
            {
                "name": "query_location_profile",
                "description": "Get location cost, workforce density, and infrastructure data for a city or country.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string", "description": "City name"},
                        "state": {"type": "string", "description": "State or province"},
                        "country": {"type": "string", "description": "Country name"}
                    },
                    "required": []
                }
            },
            {
                "name": "query_ad_platform",
                "description": "Get ad platform recommendations and cost benchmarks (Google Ads, Meta, LinkedIn, etc.) for recruitment advertising.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "role_type": {
                            "type": "string",
                            "enum": ["executive", "professional", "hourly", "clinical", "trades"],
                            "description": "Type of role to advertise"
                        },
                        "platforms": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Specific platforms to query"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_linkedin_guidewire",
                "description": "Access LinkedIn Hiring Value Review data for Guidewire Software. Contains hiring performance metrics, influenced hire data, skill density analysis, recruiter efficiency benchmarks, peer company comparisons (Stripe, GitLab, Coinbase, etc.), and LinkedIn product adoption rates. Use for questions about Guidewire hiring, LinkedIn ROI, or tech company hiring benchmarks.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "section": {
                            "type": "string",
                            "enum": ["executive_summary", "hiring_performance", "hire_efficiency", "all"],
                            "description": "Which section of the LinkedIn review to query"
                        },
                        "metric": {
                            "type": "string",
                            "description": "Specific metric to look up (e.g., 'influenced_hires', 'skill_density', 'inmail_response_rate')"
                        }
                    },
                    "required": []
                }
            },
        ]

    # ------------------------------------------------------------------
    # Tool execution
    # ------------------------------------------------------------------

    def execute_tool(self, tool_name: str, tool_input: dict) -> str:
        """Execute a tool call and return the result as a JSON string."""
        handlers = {
            "query_global_supply": self._query_global_supply,
            "query_channels": self._query_channels,
            "query_publishers": self._query_publishers,
            "query_knowledge_base": self._query_knowledge_base,
            "query_salary_data": self._query_salary_data,
            "query_market_demand": self._query_market_demand,
            "query_budget_projection": self._query_budget_projection,
            "query_location_profile": self._query_location_profile,
            "query_ad_platform": self._query_ad_platform,
            "query_linkedin_guidewire": self._query_linkedin_guidewire,
        }
        handler = handlers.get(tool_name)
        if not handler:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})
        try:
            result = handler(tool_input)
            return json.dumps(result, default=str)
        except Exception as e:
            logger.error("Tool %s failed: %s", tool_name, e, exc_info=True)
            return json.dumps({"error": str(e)})

    # ------------------------------------------------------------------
    # Tool handlers
    # ------------------------------------------------------------------

    def _query_global_supply(self, params: dict) -> dict:
        """Query global supply data: country boards, DEI boards, spend data."""
        supply = self._data_cache.get("global_supply", {})
        country = params.get("country", "").strip()
        board_type = params.get("board_type", "all")
        category_filter = params.get("category", "").lower().strip()

        result: Dict[str, Any] = {"source": "Joveo Global Supply Intelligence"}

        # Resolve country alias
        country_resolved = _resolve_country(country)

        if board_type in ("general", "all"):
            country_boards = supply.get("country_job_boards", {})
            if country_resolved and country_resolved in country_boards:
                entry = country_boards[country_resolved]
                boards = entry.get("boards", [])
                if category_filter:
                    boards = [b for b in boards if category_filter in b.get("category", "").lower()]
                result["country_boards"] = {
                    "country": country_resolved,
                    "boards": boards,
                    "monthly_spend": entry.get("monthly_spend", "N/A"),
                    "key_metros": entry.get("key_metros", []),
                }
            elif not country:
                # Return summary of all countries
                result["available_countries"] = list(country_boards.keys())
                result["total_countries"] = len(country_boards)
            else:
                result["country_boards"] = {"message": f"No data for country: {country}"}

        if board_type in ("dei", "all"):
            dei_boards = supply.get("dei_boards_by_country", {})
            if country_resolved and country_resolved in dei_boards:
                result["dei_boards"] = {
                    "country": country_resolved,
                    "boards": dei_boards[country_resolved],
                }
            elif not country:
                # Return global DEI boards
                result["dei_boards"] = {
                    "global": dei_boards.get("Global", []),
                    "available_countries": list(dei_boards.keys()),
                }
            else:
                # Check global list
                result["dei_boards"] = {
                    "global": dei_boards.get("Global", []),
                    "note": f"No country-specific DEI boards for {country}; showing global options",
                }

        if board_type in ("women", "all"):
            women_boards = supply.get("women_boards_by_country", {})
            if country_resolved and country_resolved in women_boards:
                result["women_boards"] = {
                    "country": country_resolved,
                    "boards": women_boards[country_resolved],
                }
            elif not country:
                result["women_boards"] = {
                    "global": women_boards.get("Global", []),
                    "available_countries": list(women_boards.keys()),
                }

        return result

    def _query_channels(self, params: dict) -> dict:
        """Query channel database: traditional and non-traditional channels."""
        channels = self._data_cache.get("channels_db", {})
        industry = params.get("industry", "").strip().lower()
        channel_type = params.get("channel_type", "all")

        result: Dict[str, Any] = {"source": "Joveo Channel Database"}

        traditional = channels.get("traditional_channels", {})
        non_traditional = channels.get("non_traditional_channels", {})

        if channel_type in ("regional_local", "all"):
            result["regional_local"] = traditional.get("regional_local", [])

        if channel_type in ("global_reach", "all"):
            result["global_reach"] = traditional.get("global_reach", [])

        if channel_type in ("niche_by_industry", "all"):
            niche = traditional.get("niche_by_industry", {})
            if industry:
                # Find matching industry key
                matched_key = _match_industry_key(industry, list(niche.keys()))
                if matched_key:
                    result["niche_industry_channels"] = {
                        "industry": matched_key,
                        "channels": niche[matched_key],
                    }
                else:
                    result["niche_industry_channels"] = {
                        "message": f"No niche channels for industry: {industry}",
                        "available_industries": list(niche.keys()),
                    }
            else:
                result["niche_industries_available"] = list(niche.keys())

        if channel_type in ("non_traditional", "all"):
            result["non_traditional"] = non_traditional

        return result

    def _query_publishers(self, params: dict) -> dict:
        """Query Joveo publisher network by country, category, or search term."""
        publishers = self._data_cache.get("joveo_publishers", {})
        country = params.get("country", "").strip()
        category = params.get("category", "").strip()
        search_term = params.get("search_term", "").strip().lower()

        result: Dict[str, Any] = {
            "source": "Joveo Publisher Network",
            "total_active_publishers": publishers.get("total_active_publishers", 0),
        }

        country_resolved = _resolve_country(country)
        by_category = publishers.get("by_category", {})
        by_country = publishers.get("by_country", {})

        if search_term:
            # Search across all publishers
            matches = []
            for cat, pubs in by_category.items():
                for pub in pubs:
                    if search_term in pub.lower():
                        matches.append({"name": pub, "category": cat})
            result["search_results"] = matches
            result["search_term"] = search_term
            result["match_count"] = len(matches)

        elif category:
            # Filter by category
            cat_key = _match_category_key(category, list(by_category.keys()))
            if cat_key:
                result["category"] = cat_key
                result["publishers"] = by_category[cat_key]
                result["count"] = len(by_category[cat_key])
            else:
                result["message"] = f"No category match for: {category}"
                result["available_categories"] = list(by_category.keys())

        elif country_resolved:
            # Filter by country
            if country_resolved in by_country:
                pubs = by_country[country_resolved]
                result["country"] = country_resolved
                result["publishers"] = pubs
                result["count"] = len(pubs)
            else:
                result["message"] = f"No publishers specifically listed for: {country_resolved}"
                result["available_countries"] = list(by_country.keys())[:20]

        else:
            # Return overview
            result["categories"] = {k: len(v) for k, v in by_category.items()}
            result["countries_covered"] = len(by_country)

        return result

    def _query_knowledge_base(self, params: dict) -> dict:
        """Query recruitment industry knowledge base."""
        kb = self._data_cache.get("knowledge_base", {})
        topic = params.get("topic", "all")
        metric = params.get("metric", "").strip().lower()
        industry = params.get("industry", "").strip().lower()
        platform = params.get("platform", "").strip().lower()

        result: Dict[str, Any] = {"source": "Recruitment Industry Knowledge Base"}

        benchmarks = kb.get("benchmarks", {})
        trends = kb.get("market_trends", {})
        industry_benchmarks = kb.get("industry_specific_benchmarks", {})

        if topic in ("benchmarks", "all"):
            if metric:
                metric_map = {
                    "cpc": "cost_per_click",
                    "cpa": "cost_per_application",
                    "cph": "cost_per_hire",
                    "cost_per_hire": "cost_per_hire",
                    "apply_rate": "apply_rates",
                    "time_to_fill": "time_to_fill",
                    "source_of_hire": "source_of_hire",
                    "conversion_rate": "conversion_rates",
                }
                bm_key = metric_map.get(metric, metric)
                if bm_key in benchmarks:
                    result["benchmarks"] = {bm_key: benchmarks[bm_key]}
                else:
                    # Try partial match
                    matched = {k: v for k, v in benchmarks.items() if metric in k.lower()}
                    if matched:
                        result["benchmarks"] = matched
                    else:
                        result["benchmarks"] = {"message": f"No benchmark data for metric: {metric}",
                                                "available_metrics": list(benchmarks.keys())}
            elif platform:
                # Extract platform-specific CPC data
                cpc_data = benchmarks.get("cost_per_click", {}).get("by_platform", {})
                if platform in cpc_data:
                    result["platform_benchmarks"] = {platform: cpc_data[platform]}
                else:
                    matched = {k: v for k, v in cpc_data.items() if platform in k.lower()}
                    result["platform_benchmarks"] = matched if matched else {
                        "message": f"No platform data for: {platform}",
                        "available_platforms": list(cpc_data.keys()),
                    }
            else:
                result["benchmark_categories"] = list(benchmarks.keys())

        if topic in ("trends", "all"):
            result["trend_topics"] = list(trends.keys())
            # Return summary of top trends
            trend_summaries = {}
            for tk, tv in trends.items():
                if isinstance(tv, dict):
                    trend_summaries[tk] = {
                        "title": tv.get("title", tk),
                        "description": tv.get("description", ""),
                    }
            result["trend_summaries"] = trend_summaries

        if topic in ("industry_specific", "all") or industry:
            if industry:
                ind_key = _match_industry_key(industry, list(industry_benchmarks.keys()))
                if ind_key:
                    result["industry_benchmarks"] = {ind_key: industry_benchmarks[ind_key]}
                else:
                    result["industry_benchmarks"] = {
                        "message": f"No industry-specific data for: {industry}",
                        "available_industries": list(industry_benchmarks.keys()),
                    }
            else:
                result["industries_available"] = list(industry_benchmarks.keys())

        if topic == "platforms" or platform:
            platform_data = kb.get("platform_insights", {})
            if platform:
                matched = {k: v for k, v in platform_data.items() if platform in k.lower()}
                result["platform_insights"] = matched if matched else {
                    "available_platforms": list(platform_data.keys()),
                }
            else:
                result["platform_insights_available"] = list(platform_data.keys())

        if topic == "regional":
            result["regional_insights"] = kb.get("regional_insights", {})

        return result

    def _query_salary_data(self, params: dict) -> dict:
        """Get salary intelligence for roles and locations."""
        role = params.get("role", "").strip()
        location = params.get("location", "").strip()

        kb = self._data_cache.get("knowledge_base", {})
        benchmarks = kb.get("benchmarks", {})
        cph = benchmarks.get("cost_per_hire", {})

        # Use knowledge base benchmarks for salary context
        result: Dict[str, Any] = {
            "source": "Joveo Salary Intelligence (KB + Industry Data)",
            "role": role,
            "location": location or "National",
        }

        # Determine role tier for cost estimation
        role_lower = role.lower()
        tier = "Professional"
        if any(kw in role_lower for kw in ["nurse", "rn", "lpn", "therapist", "physician", "clinical"]):
            tier = "Clinical"
            result["salary_range_estimate"] = "$45,000 - $120,000"
            result["notes"] = "Healthcare roles vary significantly by specialization and location"
        elif any(kw in role_lower for kw in ["engineer", "developer", "data scientist", "software"]):
            tier = "Professional"
            result["salary_range_estimate"] = "$75,000 - $200,000"
            result["notes"] = "Tech salaries vary widely by specialization, experience, and metro area"
        elif any(kw in role_lower for kw in ["executive", "director", "vp", "chief", "president"]):
            tier = "Executive"
            result["salary_range_estimate"] = "$150,000 - $500,000+"
            result["notes"] = "Executive compensation often includes equity and bonuses"
        elif any(kw in role_lower for kw in ["driver", "warehouse", "construction", "electrician", "welder"]):
            tier = "Trades"
            result["salary_range_estimate"] = "$35,000 - $80,000"
            result["notes"] = "Trades roles in high-demand areas may command premium wages"
        elif any(kw in role_lower for kw in ["cashier", "retail", "hourly", "part-time", "entry"]):
            tier = "Hourly"
            result["salary_range_estimate"] = "$25,000 - $45,000"
            result["notes"] = "Hourly rates vary significantly by state minimum wage laws"
        else:
            result["salary_range_estimate"] = "$50,000 - $120,000"
            result["notes"] = "General professional role range; actual varies by industry and experience"

        result["role_tier"] = tier
        result["cost_per_hire_benchmark"] = {
            "shrm_average": cph.get("shrm_2026", {}).get("average_cost_per_hire", "$4,800"),
            "executive": cph.get("shrm_2025", {}).get("median_executive", "$10,625"),
            "non_executive": cph.get("shrm_2025", {}).get("median_non_executive", "$1,200"),
        }

        return result

    def _query_market_demand(self, params: dict) -> dict:
        """Get job market demand signals for roles and locations."""
        role = params.get("role", "").strip()
        location = params.get("location", "").strip()
        industry = params.get("industry", "").strip()

        kb = self._data_cache.get("knowledge_base", {})
        benchmarks = kb.get("benchmarks", {})
        trends = kb.get("market_trends", {})
        industry_benchmarks = kb.get("industry_specific_benchmarks", {})

        result: Dict[str, Any] = {
            "source": "Joveo Market Demand Intelligence",
            "role": role or "General",
            "location": location or "National",
        }

        # Applicants per opening data
        apo = benchmarks.get("applicants_per_opening", {})
        result["applicants_per_opening"] = apo

        # Source of hire breakdown
        soh = benchmarks.get("source_of_hire", {})
        result["source_of_hire"] = {
            "job_boards_usage": soh.get("job_boards", {}).get("employer_usage", "68.6%"),
            "referrals_usage": soh.get("employee_referrals", {}).get("employer_usage", "82%"),
            "career_sites_usage": soh.get("career_sites", {}).get("employer_usage", "49.5%"),
            "linkedin_usage": soh.get("linkedin_professional_networks", {}).get("employer_usage", "46.1%"),
        }

        # Industry-specific demand signals
        if industry:
            ind_key = _match_industry_key(industry, list(industry_benchmarks.keys()))
            if ind_key:
                ind_data = industry_benchmarks[ind_key]
                result["industry_demand"] = {
                    "industry": ind_key,
                    "hiring_strength": ind_data.get("hiring_strength", "N/A"),
                    "recruitment_difficulty": ind_data.get("recruitment_difficulty", "N/A"),
                }

        # Labor market trends
        labor = trends.get("labor_market_shifts", {})
        if labor:
            result["labor_market"] = {
                "title": labor.get("title", ""),
                "description": labor.get("description", ""),
            }

        return result

    def _query_budget_projection(self, params: dict) -> dict:
        """Project budget allocation for given parameters."""
        budget = params.get("budget", 0)
        roles_list = params.get("roles", [])
        locations_list = params.get("locations", [])
        industry = params.get("industry", "general")

        if budget <= 0:
            return {"error": "Budget must be greater than zero", "source": "Joveo Budget Engine"}

        result: Dict[str, Any] = {
            "source": "Joveo Budget Allocation Engine",
            "total_budget": budget,
            "industry": industry,
        }

        # Try to use the budget engine
        try:
            from budget_engine import calculate_budget_allocation, BASE_BENCHMARKS

            # Build role dicts
            roles = []
            for r in (roles_list or ["General Hire"]):
                role_lower = r.lower() if isinstance(r, str) else ""
                tier = "Professional / White-Collar"
                if any(kw in role_lower for kw in ["nurse", "clinical", "therapist"]):
                    tier = "Clinical / Licensed"
                elif any(kw in role_lower for kw in ["executive", "director", "vp"]):
                    tier = "Executive / Leadership"
                elif any(kw in role_lower for kw in ["driver", "warehouse", "construction"]):
                    tier = "Skilled Trades / Technical"
                elif any(kw in role_lower for kw in ["cashier", "hourly", "retail"]):
                    tier = "Hourly / Entry-Level"
                roles.append({"title": r, "count": 1, "tier": tier})

            # Build location dicts
            locations = []
            for loc in (locations_list or ["United States"]):
                if isinstance(loc, str):
                    locations.append({"city": loc, "state": "", "country": "United States"})

            # Default channel split
            channel_pcts = {
                "Programmatic & DSP": 30,
                "Global Job Boards": 25,
                "Niche & Industry Boards": 15,
                "Social Media Channels": 15,
                "Regional & Local Boards": 10,
                "Employer Branding": 5,
            }

            kb = self._data_cache.get("knowledge_base", {})
            allocation = calculate_budget_allocation(
                total_budget=budget,
                roles=roles,
                locations=locations,
                industry=industry,
                channel_percentages=channel_pcts,
                synthesized_data=None,
                knowledge_base=kb,
            )

            result["channel_allocations"] = allocation.get("channel_allocations", {})
            result["total_projected"] = allocation.get("total_projected", {})
            result["sufficiency"] = allocation.get("sufficiency", {})
            result["recommendations"] = allocation.get("recommendations", [])

        except Exception as e:
            logger.error("Budget engine call failed: %s", e, exc_info=True)
            # Provide a manual estimate
            result["estimated_allocation"] = {
                "programmatic_dsp": {"pct": 30, "amount": round(budget * 0.30, 2)},
                "global_job_boards": {"pct": 25, "amount": round(budget * 0.25, 2)},
                "niche_industry_boards": {"pct": 15, "amount": round(budget * 0.15, 2)},
                "social_media": {"pct": 15, "amount": round(budget * 0.15, 2)},
                "regional_local": {"pct": 10, "amount": round(budget * 0.10, 2)},
                "employer_branding": {"pct": 5, "amount": round(budget * 0.05, 2)},
            }
            result["note"] = "Estimated allocation (budget engine unavailable)"

        return result

    def _query_location_profile(self, params: dict) -> dict:
        """Get location cost, workforce, and supply data."""
        city = params.get("city", "").strip()
        state = params.get("state", "").strip()
        country = params.get("country", "").strip()

        country_resolved = _resolve_country(country) or _resolve_country(city) or "United States"

        result: Dict[str, Any] = {
            "source": "Joveo Location Intelligence",
            "location": {
                "city": city,
                "state": state,
                "country": country_resolved,
            }
        }

        # Pull supply data for this country
        supply = self._data_cache.get("global_supply", {})
        country_boards = supply.get("country_job_boards", {})

        if country_resolved in country_boards:
            entry = country_boards[country_resolved]
            result["supply_data"] = {
                "monthly_spend": entry.get("monthly_spend", "N/A"),
                "key_metros": entry.get("key_metros", []),
                "total_boards": len(entry.get("boards", [])),
            }

        # Pull publisher count for country
        publishers = self._data_cache.get("joveo_publishers", {})
        by_country = publishers.get("by_country", {})
        if country_resolved in by_country:
            result["publisher_count"] = len(by_country[country_resolved])

        return result

    def _query_ad_platform(self, params: dict) -> dict:
        """Get ad platform recommendations and benchmarks."""
        role_type = params.get("role_type", "professional")
        platforms = params.get("platforms", [])

        kb = self._data_cache.get("knowledge_base", {})
        benchmarks = kb.get("benchmarks", {})
        cpc_data = benchmarks.get("cost_per_click", {}).get("by_platform", {})

        result: Dict[str, Any] = {
            "source": "Joveo Ad Platform Intelligence",
            "role_type": role_type,
        }

        # Platform recommendations by role type
        platform_recs = {
            "executive": {
                "primary": ["LinkedIn", "Indeed"],
                "secondary": ["Glassdoor", "ZipRecruiter"],
                "rationale": "Executive roles require targeted professional networks with advanced targeting",
            },
            "professional": {
                "primary": ["LinkedIn", "Indeed", "Google Ads"],
                "secondary": ["ZipRecruiter", "Glassdoor", "Dice"],
                "rationale": "Professional roles benefit from a mix of job boards and search advertising",
            },
            "hourly": {
                "primary": ["Indeed", "Snagajob", "Facebook/Meta"],
                "secondary": ["Craigslist", "Google Ads", "Jobcase"],
                "rationale": "Hourly roles perform best on high-volume, mobile-first platforms",
            },
            "clinical": {
                "primary": ["Indeed", "Health eCareers", "Doximity"],
                "secondary": ["LinkedIn", "Nurse.com", "Vivian Health"],
                "rationale": "Clinical roles require niche healthcare boards for qualified candidates",
            },
            "trades": {
                "primary": ["Indeed", "Facebook/Meta", "CDLlife"],
                "secondary": ["Craigslist", "Jobcase", "Google Ads"],
                "rationale": "Trades roles benefit from local/regional targeting and mobile-first platforms",
            },
        }

        result["recommendations"] = platform_recs.get(role_type, platform_recs["professional"])

        # CPC benchmarks for requested platforms or all
        if platforms:
            for p in platforms:
                p_lower = p.lower().replace(" ", "_")
                for key, data in cpc_data.items():
                    if p_lower in key.lower() or key.lower() in p_lower:
                        result.setdefault("platform_benchmarks", {})[key] = data
        else:
            result["platform_benchmarks"] = cpc_data

        return result

    def _query_linkedin_guidewire(self, params: dict) -> str:
        """Query LinkedIn Hiring Value Review data for Guidewire Software."""
        gw_data = self._data_cache.get("linkedin_guidewire", {})
        if not gw_data:
            return "LinkedIn Guidewire data not available."

        section = params.get("section", "all")
        metric = params.get("metric", "")
        result = ""

        if section == "executive_summary" or section == "all":
            exec_sum = gw_data.get("executive_summary", {})
            result = f"**Guidewire LinkedIn Hiring Review**\n"
            result += f"Headline: {exec_sum.get('headline', 'N/A')}\n"
            result += f"Context: {exec_sum.get('context', 'N/A')}\n\n"
            for theme in exec_sum.get("key_themes", []):
                result += f"**{theme.get('theme', '')}**\n"
                for pt in theme.get("points", []):
                    result += f"- {pt}\n"
                result += "\n"
            if section == "executive_summary":
                return result

        if section == "hiring_performance" or section == "all":
            # Return hiring performance data
            hp = gw_data.get("hiring_performance", gw_data.get("hiring_performance_l12m", {}))
            if isinstance(hp, dict):
                result_hp = "**Hiring Performance (L12M)**\n"
                for key, val in hp.items():
                    if isinstance(val, dict):
                        result_hp += f"\n**{key.replace('_', ' ').title()}**:\n"
                        for k2, v2 in val.items():
                            result_hp += f"  - {k2}: {v2}\n"
                    else:
                        result_hp += f"- {key}: {val}\n"
                if section == "hiring_performance":
                    return result_hp
                result += result_hp

        if section == "hire_efficiency" or section == "all":
            he = gw_data.get("hire_efficiency", {})
            if isinstance(he, dict):
                result_he = "**Hire Efficiency**\n"
                for key, val in he.items():
                    if isinstance(val, dict):
                        result_he += f"\n**{key.replace('_', ' ').title()}**:\n"
                        for k2, v2 in val.items():
                            result_he += f"  - {k2}: {v2}\n"
                    else:
                        result_he += f"- {key}: {val}\n"
                result += result_he

        return result if result else json.dumps(gw_data, indent=2)[:3000]

    # ------------------------------------------------------------------
    # Chat orchestration
    # ------------------------------------------------------------------

    def chat(self, user_message: str, conversation_history: Optional[list] = None,
             enrichment_context: Optional[dict] = None) -> dict:
        """Process a chat message and return a response.

        Args:
            user_message: The user's question.
            conversation_history: List of previous messages [{role, content}].
            enrichment_context: Optional pre-computed enrichment data.

        Returns:
            Dict with response, sources, confidence, tools_used.
        """
        if not user_message or not user_message.strip():
            return {
                "response": "Please ask a question about recruitment marketing, and I will help you with data-driven insights.",
                "sources": [],
                "confidence": 1.0,
                "tools_used": [],
            }

        # Truncate message
        user_message = user_message.strip()[:MAX_MESSAGE_LENGTH]

        # Check for Claude API mode
        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if api_key:
            try:
                return self._chat_with_claude(user_message, conversation_history, enrichment_context, api_key)
            except Exception as e:
                logger.error("Claude API call failed, falling back to rule-based: %s", e)

        # Rule-based fallback
        return self._chat_rule_based(user_message, enrichment_context)

    def _chat_with_claude(self, user_message: str, conversation_history: Optional[list],
                          enrichment_context: Optional[dict], api_key: str) -> dict:
        """Use Claude API for natural-language chat with tool use."""
        import urllib.request
        import urllib.error

        messages = []
        if conversation_history:
            for msg in conversation_history[-MAX_HISTORY_TURNS:]:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                if role in ("user", "assistant") and content:
                    messages.append({"role": role, "content": content})

        messages.append({"role": "user", "content": user_message})

        # Add enrichment context if available
        system_prompt = self.get_system_prompt()
        if enrichment_context:
            context_summary = _summarize_enrichment(enrichment_context)
            system_prompt += f"\n\n## ACTIVE SESSION CONTEXT\n{context_summary}"

        tools_used = []
        sources = set()
        max_iterations = 5

        for iteration in range(max_iterations):
            payload = {
                "model": CLAUDE_MODEL,
                "max_tokens": 2048,
                "system": system_prompt,
                "messages": messages,
                "tools": self.get_tool_definitions(),
            }

            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages",
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                },
            )

            with urllib.request.urlopen(req, timeout=30) as resp:
                resp_data = json.loads(resp.read().decode("utf-8"))

            stop_reason = resp_data.get("stop_reason", "end_turn")
            content_blocks = resp_data.get("content", [])

            if stop_reason == "tool_use":
                # Process tool calls
                tool_results = []
                for block in content_blocks:
                    if block.get("type") == "tool_use":
                        tool_name = block["name"]
                        tool_input = block.get("input", {})
                        tool_id = block.get("id", "")

                        tools_used.append(tool_name)
                        tool_result = self.execute_tool(tool_name, tool_input)
                        result_parsed = json.loads(tool_result)
                        if "source" in result_parsed:
                            sources.add(result_parsed["source"])

                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": tool_id,
                            "content": tool_result,
                        })

                # Add assistant message with tool_use blocks and tool results
                messages.append({"role": "assistant", "content": content_blocks})
                messages.append({"role": "user", "content": tool_results})
            else:
                # Extract text response
                response_text = ""
                for block in content_blocks:
                    if block.get("type") == "text":
                        response_text += block.get("text", "")

                confidence = _estimate_confidence(tools_used, sources)
                return {
                    "response": response_text,
                    "sources": list(sources),
                    "confidence": confidence,
                    "tools_used": tools_used,
                }

        # If we exhausted iterations
        return {
            "response": "I gathered data but could not finalize a response. Please try rephrasing your question.",
            "sources": list(sources),
            "confidence": 0.3,
            "tools_used": tools_used,
        }

    def _chat_rule_based(self, user_message: str, enrichment_context: Optional[dict] = None) -> dict:
        """Rule-based chat engine using keyword matching and data lookups."""
        msg_lower = user_message.lower()
        tools_used = []
        sources = set()
        sections = []

        # Detect intents
        detected_roles = _detect_keywords(msg_lower, _ROLE_KEYWORDS)
        detected_metrics = _detect_keywords(msg_lower, _METRIC_KEYWORDS)
        detected_industries = _detect_keywords(msg_lower, _INDUSTRY_KEYWORDS)
        detected_country = _detect_country(msg_lower)

        # Detect question type
        is_publisher_question = any(kw in msg_lower for kw in ["publisher", "job board", "board", "where to post", "which board"])
        is_channel_question = any(kw in msg_lower for kw in ["channel", "source", "platform", "where to advertise"])
        is_budget_question = any(kw in msg_lower for kw in ["budget", "allocat", "spend", "invest", "roi", "$"])
        is_benchmark_question = any(kw in msg_lower for kw in ["benchmark", "average", "industry average", "typical", "compare"])
        is_salary_question = "salary" in detected_metrics or any(kw in msg_lower for kw in ["salary", "compensation", "pay range", "wage"])
        is_dei_question = any(kw in msg_lower for kw in ["dei", "diversity", "inclusion", "women", "minority", "veteran", "disability"])
        is_trend_question = any(kw in msg_lower for kw in ["trend", "future", "outlook", "forecast", "what's new", "emerging"])
        is_cpc_cpa_question = "cpc" in detected_metrics or "cpa" in detected_metrics or "cph" in detected_metrics

        # Greeting detection
        is_greeting = any(kw in msg_lower for kw in ["hello", "hi", "hey", "good morning", "good afternoon",
                                                      "help", "what can you do", "who are you"])

        if is_greeting and not (is_publisher_question or is_channel_question or is_budget_question
                                or is_benchmark_question or is_salary_question):
            return {
                "response": (
                    "Hello! I'm **Joveo IQ**, your recruitment marketing intelligence assistant. "
                    "I have access to data from **1,238+ publishers**, job boards across **30+ countries**, "
                    "and comprehensive industry benchmarks.\n\n"
                    "Here are some things I can help with:\n\n"
                    "- **Publisher & Board Recommendations**: \"What publishers work best for nursing roles?\"\n"
                    "- **Industry Benchmarks**: \"What's the average CPA for tech roles?\"\n"
                    "- **Budget Planning**: \"How should I allocate a $50K budget for 10 engineering hires?\"\n"
                    "- **Market Intelligence**: \"What's the talent supply for tech roles in Germany?\"\n"
                    "- **DEI Strategy**: \"What DEI-focused job boards are available in the US?\"\n\n"
                    "What would you like to know?"
                ),
                "sources": [],
                "confidence": 1.0,
                "tools_used": [],
            }

        # ── Guidewire / LinkedIn hiring data ──
        if any(kw in msg_lower for kw in ["guidewire", "linkedin hiring", "influenced hire", "skill density", "inmail"]):
            gw_data = self._data_cache.get("linkedin_guidewire", {})
            if gw_data:
                exec_sum = gw_data.get("executive_summary", {})
                response_parts = [f"**Guidewire Software — LinkedIn Hiring Intelligence**\n"]
                response_parts.append(f"{exec_sum.get('headline', '')}\n")
                for theme in exec_sum.get("key_themes", [])[:3]:
                    response_parts.append(f"\n**{theme.get('theme', '')}**")
                    for pt in theme.get("points", [])[:3]:
                        response_parts.append(f"- {pt}")

                # Add peer comparison if available
                peers = gw_data.get("document_metadata", {}).get("peer_companies", [])
                if peers:
                    response_parts.append(f"\n**Peer Companies**: {', '.join(peers)}")

                return {
                    "response": "\n".join(response_parts),
                    "sources": ["LinkedIn Hiring Value Review for Guidewire Software (Jan 2025 - Dec 2025)"],
                    "confidence": 0.95,
                }

        # ── Publisher / Job Board questions ──
        if is_publisher_question or (detected_country and not is_benchmark_question and not is_budget_question):
            country = detected_country or "United States"
            if is_dei_question:
                data = self._query_global_supply({"country": country, "board_type": "dei"})
            else:
                category = ""
                for role_cat in detected_roles:
                    if role_cat in ("nursing", "healthcare"):
                        category = "Healthcare"
                    elif role_cat in ("engineering", "technology"):
                        category = "Tech"
                    break
                data = self._query_global_supply({"country": country, "board_type": "general", "category": category})

            tools_used.append("query_global_supply")
            sources.add("Joveo Global Supply Intelligence")
            sections.append(_format_supply_response(data, country, is_dei_question))

            # Also query publishers
            pub_params = {"country": country}
            if detected_roles:
                role_cat = list(detected_roles)[0]
                cat_map = {
                    "nursing": "Health", "healthcare": "Health", "engineering": "Tech",
                    "technology": "Tech", "finance": "Job Board",
                }
                if role_cat in cat_map:
                    pub_params["category"] = cat_map[role_cat]
            pub_data = self._query_publishers(pub_params)
            tools_used.append("query_publishers")
            sources.add("Joveo Publisher Network")
            sections.append(_format_publisher_response(pub_data))

        # ── Channel questions ──
        if is_channel_question and not is_publisher_question:
            industry = list(detected_industries)[0] if detected_industries else ""
            ch_data = self._query_channels({"industry": industry, "channel_type": "all"})
            tools_used.append("query_channels")
            sources.add("Joveo Channel Database")
            sections.append(_format_channel_response(ch_data, industry))

        # ── CPC / CPA / Benchmark questions ──
        if is_cpc_cpa_question or is_benchmark_question:
            metric = ""
            if "cpc" in detected_metrics:
                metric = "cpc"
            elif "cpa" in detected_metrics:
                metric = "cpa"
            elif "cph" in detected_metrics:
                metric = "cost_per_hire"
            elif "apply_rate" in detected_metrics:
                metric = "apply_rate"
            elif "time_to_fill" in detected_metrics:
                metric = "time_to_fill"
            elif "benchmark" in detected_metrics:
                metric = ""

            industry = list(detected_industries)[0] if detected_industries else ""
            kb_data = self._query_knowledge_base({"topic": "benchmarks", "metric": metric, "industry": industry})
            tools_used.append("query_knowledge_base")
            sources.add("Recruitment Industry Knowledge Base")
            sections.append(_format_benchmark_response(kb_data, metric, industry))

        # ── Salary questions ──
        if is_salary_question:
            role = list(detected_roles)[0] if detected_roles else "general"
            role_titles = {
                "nursing": "Registered Nurse", "engineering": "Software Engineer",
                "technology": "Software Developer", "healthcare": "Healthcare Professional",
                "retail": "Retail Associate", "hospitality": "Hospitality Worker",
                "transportation": "CDL Driver", "finance": "Financial Analyst",
                "executive": "Senior Executive", "hourly": "Hourly Worker",
                "education": "Teacher", "construction": "Construction Worker",
                "sales": "Sales Representative", "marketing": "Marketing Manager",
            }
            role_title = role_titles.get(role, role.title())
            location = detected_country or ""
            sal_data = self._query_salary_data({"role": role_title, "location": location})
            tools_used.append("query_salary_data")
            sources.add("Joveo Salary Intelligence")
            sections.append(_format_salary_response(sal_data))

        # ── Budget questions ──
        if is_budget_question:
            # Extract budget amount from message
            budget_amount = _extract_budget(msg_lower)
            roles_for_budget = []
            for r in detected_roles:
                role_titles = {
                    "nursing": "Registered Nurse", "engineering": "Software Engineer",
                    "technology": "Software Developer", "healthcare": "Healthcare Professional",
                    "retail": "Retail Associate", "transportation": "CDL Driver",
                    "finance": "Financial Analyst", "executive": "Senior Executive",
                    "hourly": "Hourly Worker", "education": "Teacher",
                    "construction": "Construction Worker", "sales": "Sales Representative",
                }
                roles_for_budget.append(role_titles.get(r, r.title()))

            locations_for_budget = [detected_country] if detected_country else ["United States"]
            industry = list(detected_industries)[0] if detected_industries else "general"

            budget_data = self._query_budget_projection({
                "budget": budget_amount,
                "roles": roles_for_budget or ["General Hire"],
                "locations": locations_for_budget,
                "industry": industry,
            })
            tools_used.append("query_budget_projection")
            sources.add("Joveo Budget Allocation Engine")
            sections.append(_format_budget_response(budget_data, budget_amount))

        # ── DEI questions (standalone) ──
        if is_dei_question and not is_publisher_question:
            country = detected_country or ""
            dei_data = self._query_global_supply({"country": country, "board_type": "dei"})
            tools_used.append("query_global_supply")
            sources.add("Joveo Global Supply Intelligence")
            sections.append(_format_dei_response(dei_data, country))

        # ── Trend questions ──
        if is_trend_question:
            trend_data = self._query_knowledge_base({"topic": "trends"})
            tools_used.append("query_knowledge_base")
            sources.add("Recruitment Industry Knowledge Base")
            sections.append(_format_trend_response(trend_data))

        # ── Market demand questions ──
        if detected_roles and not sections:
            role = list(detected_roles)[0]
            role_titles = {
                "nursing": "Registered Nurse", "engineering": "Software Engineer",
                "technology": "Software Developer", "healthcare": "Healthcare Professional",
                "retail": "Retail Associate", "transportation": "CDL Driver",
            }
            role_title = role_titles.get(role, role.title())
            location = detected_country or ""
            industry = list(detected_industries)[0] if detected_industries else ""
            demand_data = self._query_market_demand({"role": role_title, "location": location, "industry": industry})
            tools_used.append("query_market_demand")
            sources.add("Joveo Market Demand Intelligence")
            sections.append(_format_demand_response(demand_data, role_title))

        # ── Fallback ──
        if not sections:
            # Try a general knowledge base search
            kb_data = self._query_knowledge_base({"topic": "all"})
            tools_used.append("query_knowledge_base")
            sources.add("Recruitment Industry Knowledge Base")

            response_text = (
                "I can help you with recruitment marketing intelligence. "
                "Based on Joveo's data across **1,238+ publishers** in **30+ countries**, "
                "I can answer questions about:\n\n"
                "- **Job boards and publishers** for specific countries or industries\n"
                "- **CPC, CPA, and cost-per-hire benchmarks** by industry and platform\n"
                "- **Budget allocation** recommendations with projected outcomes\n"
                "- **Salary intelligence** for specific roles and locations\n"
                "- **DEI recruitment channels** and diversity-focused boards\n"
                "- **Market trends** in recruitment advertising\n\n"
                "Could you rephrase your question with more specifics? "
                "For example, mention a role, location, industry, or metric."
            )
            sections.append(response_text)

        response = "\n\n".join(sections)
        confidence = _estimate_confidence(tools_used, sources)

        return {
            "response": response,
            "sources": list(sources),
            "confidence": confidence,
            "tools_used": tools_used,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _resolve_country(name: str) -> Optional[str]:
    """Resolve a country name or alias to its canonical form."""
    if not name:
        return None
    name_lower = name.lower().strip()
    if name_lower in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[name_lower]
    # Try title case match
    title = name.strip().title()
    # Check if it's already a valid country name in our data
    return title if title != "" else None


def _match_industry_key(query: str, available_keys: List[str]) -> Optional[str]:
    """Find the best matching industry key from available options."""
    query_lower = query.lower().strip()
    # Exact match
    if query_lower in available_keys:
        return query_lower
    # Partial match
    for key in available_keys:
        if query_lower in key or key in query_lower:
            return key
    # Keyword match
    for key in available_keys:
        key_parts = key.replace("_", " ").split()
        if any(part in query_lower for part in key_parts):
            return key
    return None


def _match_category_key(query: str, available_keys: List[str]) -> Optional[str]:
    """Find the best matching category key."""
    query_lower = query.lower().strip()
    for key in available_keys:
        if query_lower == key.lower():
            return key
    for key in available_keys:
        if query_lower in key.lower() or key.lower() in query_lower:
            return key
    return None


def _detect_keywords(text: str, keyword_map: Dict[str, List[str]]) -> set:
    """Detect which keyword categories are present in text."""
    found = set()
    for category, keywords in keyword_map.items():
        for kw in keywords:
            if kw in text:
                found.add(category)
                break
    return found


def _detect_country(text: str) -> Optional[str]:
    """Detect a country name in the text."""
    text_lower = text.lower()
    # Check aliases (longest first to avoid partial matches)
    sorted_aliases = sorted(_COUNTRY_ALIASES.keys(), key=len, reverse=True)
    for alias in sorted_aliases:
        # Use word boundary check to avoid false matches
        pattern = r'\b' + re.escape(alias) + r'\b'
        if re.search(pattern, text_lower):
            return _COUNTRY_ALIASES[alias]
    return None


def _extract_budget(text: str) -> float:
    """Extract a dollar budget amount from text."""
    # Match patterns like $50K, $50,000, 50K, 50000, $1M, $1.5M
    patterns = [
        r'\$\s*([\d,.]+)\s*[mM](?:illion)?',     # $1M, $1.5 million
        r'\$\s*([\d,.]+)\s*[kK]',                  # $50K, $50k
        r'([\d,.]+)\s*[mM](?:illion)?\s*(?:dollar|usd|budget)',  # 1M dollars
        r'([\d,.]+)\s*[kK]\s*(?:dollar|usd|budget)',              # 50K dollars
        r'\$\s*([\d,.]+)',                          # $50,000
        r'([\d,.]+)\s*(?:dollar|usd)',             # 50000 dollars
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            num_str = match.group(1).replace(",", "")
            try:
                val = float(num_str)
                if "m" in text[match.start():match.end()].lower():
                    val *= 1_000_000
                elif "k" in text[match.start():match.end()].lower():
                    val *= 1_000
                return val
            except ValueError:
                continue
    return 50000.0  # Default budget


def _estimate_confidence(tools_used: list, sources: set) -> float:
    """Estimate response confidence based on tools and sources used."""
    if not tools_used:
        return 0.5
    base = 0.6
    # More tools = more comprehensive
    base += min(len(tools_used) * 0.05, 0.2)
    # More sources = higher confidence
    base += min(len(sources) * 0.05, 0.15)
    return min(base, 0.95)


def _summarize_enrichment(context: dict) -> str:
    """Create a brief text summary of enrichment context."""
    parts = []
    if context.get("roles"):
        roles = context["roles"]
        if isinstance(roles, list):
            role_names = [r.get("title", str(r)) if isinstance(r, dict) else str(r) for r in roles[:5]]
            parts.append(f"Roles: {', '.join(role_names)}")
    if context.get("locations"):
        locs = context["locations"]
        if isinstance(locs, list):
            loc_names = []
            for loc in locs[:5]:
                if isinstance(loc, dict):
                    loc_names.append(f"{loc.get('city', '')}, {loc.get('state', '')}, {loc.get('country', '')}".strip(", "))
                else:
                    loc_names.append(str(loc))
            parts.append(f"Locations: {', '.join(loc_names)}")
    if context.get("industry"):
        parts.append(f"Industry: {context['industry']}")
    if context.get("budget"):
        parts.append(f"Budget: ${context['budget']:,.0f}")
    return "\n".join(parts) if parts else "No additional context available."


# ═══════════════════════════════════════════════════════════════════════════════
# RESPONSE FORMATTERS
# ═══════════════════════════════════════════════════════════════════════════════

def _format_supply_response(data: dict, country: str, is_dei: bool = False) -> str:
    """Format global supply data into a readable response."""
    parts = []

    if is_dei:
        dei = data.get("dei_boards", {})
        boards = dei.get("boards", dei.get("global", []))
        if boards:
            parts.append(f"### DEI Job Boards{' for ' + country if country else ''}\n")
            for b in boards[:10]:
                if isinstance(b, dict):
                    parts.append(f"- **{b.get('name', 'N/A')}** - Focus: {b.get('focus', 'General')} ({b.get('regions', 'Global')})")
                else:
                    parts.append(f"- {b}")
            if len(boards) > 10:
                parts.append(f"\n*...and {len(boards) - 10} more DEI boards available*")
        return "\n".join(parts)

    cb = data.get("country_boards", {})
    if cb and "boards" in cb:
        parts.append(f"### Job Boards in {cb.get('country', country)}\n")
        parts.append(f"**Monthly Spend**: {cb.get('monthly_spend', 'N/A')}")
        parts.append(f"**Key Metros**: {', '.join(cb.get('key_metros', []))}\n")

        # Group by tier
        boards = cb["boards"]
        tiers = {}
        for b in boards:
            tier = b.get("tier", "Other")
            tiers.setdefault(tier, []).append(b)

        for tier in ["Tier 1", "Tier 2", "Niche", "Govt"]:
            if tier in tiers:
                parts.append(f"**{tier}:**")
                for b in tiers[tier]:
                    parts.append(f"- {b['name']} ({b.get('billing', 'N/A')}) - {b.get('category', 'General')}")
                parts.append("")

    elif "available_countries" in data:
        parts.append("### Available Countries in Joveo's Global Supply Data\n")
        countries = data["available_countries"]
        parts.append(f"We have job board data for **{len(countries)} countries**: {', '.join(countries[:15])}{'...' if len(countries) > 15 else ''}")

    return "\n".join(parts) if parts else "No supply data available for this query."


def _format_publisher_response(data: dict) -> str:
    """Format publisher network data into a readable response."""
    parts = []
    total = data.get("total_active_publishers", 0)

    if "search_results" in data:
        matches = data["search_results"]
        parts.append(f"### Publisher Search Results ({data.get('match_count', 0)} matches)\n")
        for m in matches[:15]:
            parts.append(f"- **{m['name']}** (Category: {m['category']})")
    elif "publishers" in data:
        pubs = data["publishers"]
        label = data.get("country", data.get("category", ""))
        parts.append(f"### Joveo Publishers{' in ' + label if label else ''} ({data.get('count', len(pubs))} publishers)\n")
        for p in pubs[:15]:
            parts.append(f"- {p}")
        if len(pubs) > 15:
            parts.append(f"\n*...and {len(pubs) - 15} more publishers*")
    elif "categories" in data:
        parts.append(f"### Joveo Publisher Network Overview\n")
        parts.append(f"**Total Active Publishers**: {total:,}\n")
        cats = data["categories"]
        for cat, count in sorted(cats.items(), key=lambda x: x[1], reverse=True)[:12]:
            parts.append(f"- **{cat}**: {count} publishers")

    return "\n".join(parts) if parts else ""


def _format_channel_response(data: dict, industry: str) -> str:
    """Format channel data into a readable response."""
    parts = []
    parts.append("### Recruitment Channels\n")

    if "niche_industry_channels" in data:
        nic = data["niche_industry_channels"]
        parts.append(f"**Niche Channels for {nic.get('industry', industry)}:**")
        for ch in nic.get("channels", [])[:12]:
            parts.append(f"- {ch}")
        parts.append("")

    if "regional_local" in data:
        parts.append(f"**Regional/Local Boards** ({len(data['regional_local'])} channels):")
        for ch in data["regional_local"][:8]:
            parts.append(f"- {ch}")
        parts.append("")

    if "global_reach" in data:
        parts.append(f"**Global Reach** ({len(data['global_reach'])} channels):")
        for ch in data["global_reach"][:8]:
            parts.append(f"- {ch}")

    return "\n".join(parts) if parts else "No channel data available."


def _format_benchmark_response(data: dict, metric: str, industry: str) -> str:
    """Format benchmark data into a readable response."""
    parts = []
    bm = data.get("benchmarks", {})

    if not bm or "message" in bm:
        # Try industry benchmarks
        ind_bm = data.get("industry_benchmarks", {})
        if ind_bm and "message" not in ind_bm:
            parts.append(f"### Industry Benchmarks\n")
            for ind_key, ind_data in ind_bm.items():
                parts.append(f"**{ind_key.replace('_', ' ').title()}:**")
                if isinstance(ind_data, dict):
                    for k, v in list(ind_data.items())[:8]:
                        parts.append(f"- {k.replace('_', ' ').title()}: {v}")
                parts.append("")
            return "\n".join(parts)
        parts.append("No specific benchmark data found. ")
        parts.append("Available metrics: CPC, CPA, Cost per Hire, Apply Rate, Time to Fill.")
        return "\n".join(parts)

    for bm_key, bm_data in bm.items():
        nice_key = bm_key.replace("_", " ").title()
        parts.append(f"### {nice_key} Benchmarks\n")

        if isinstance(bm_data, dict):
            desc = bm_data.get("description", "")
            if desc:
                parts.append(f"*{desc}*\n")

            # Format platform-specific data
            if "by_platform" in bm_data:
                parts.append("**By Platform:**")
                for plat, plat_data in bm_data["by_platform"].items():
                    if isinstance(plat_data, dict):
                        key_val = ""
                        for k in ["average_cpc_range", "job_ad_cpc_range", "average_cpc",
                                   "model", "starting_price", "median_cpc_peak_nov_2025"]:
                            if k in plat_data:
                                key_val = f"{plat_data[k]}"
                                break
                        parts.append(f"- **{plat.replace('_', ' ').title()}**: {key_val}")

            # Format report data
            for rkey in ["appcast_2025_report", "appcast_2026_report", "shrm_2025", "shrm_2026",
                         "google_ads_benchmark", "joveo_historical"]:
                if rkey in bm_data:
                    rdata = bm_data[rkey]
                    parts.append(f"\n**{rkey.replace('_', ' ').title()}:**")
                    if isinstance(rdata, dict):
                        for k, v in list(rdata.items())[:6]:
                            if k not in ("year", "dataset"):
                                parts.append(f"- {k.replace('_', ' ').title()}: {v}")

        parts.append("")

    # Add industry-specific data if available
    if industry:
        ind_bm = data.get("industry_benchmarks", {})
        for ind_key, ind_data in ind_bm.items():
            parts.append(f"\n### Industry-Specific: {ind_key.replace('_', ' ').title()}\n")
            if isinstance(ind_data, dict):
                for k, v in list(ind_data.items())[:8]:
                    parts.append(f"- {k.replace('_', ' ').title()}: {v}")

    return "\n".join(parts)


def _format_salary_response(data: dict) -> str:
    """Format salary data into a readable response."""
    parts = []
    parts.append(f"### Salary Intelligence: {data.get('role', 'N/A')}\n")
    parts.append(f"**Location**: {data.get('location', 'National')}")
    parts.append(f"**Role Tier**: {data.get('role_tier', 'N/A')}")
    parts.append(f"**Estimated Range**: {data.get('salary_range_estimate', 'N/A')}")
    if data.get("notes"):
        parts.append(f"*{data['notes']}*\n")

    cph = data.get("cost_per_hire_benchmark", {})
    if cph:
        parts.append("**Cost-per-Hire Benchmarks:**")
        parts.append(f"- SHRM Average: {cph.get('shrm_average', 'N/A')}")
        parts.append(f"- Executive Median: {cph.get('executive', 'N/A')}")
        parts.append(f"- Non-Executive Median: {cph.get('non_executive', 'N/A')}")

    return "\n".join(parts)


def _format_budget_response(data: dict, budget: float) -> str:
    """Format budget projection data into a readable response."""
    parts = []
    parts.append(f"### Budget Allocation: ${budget:,.0f}\n")

    if "channel_allocations" in data:
        allocs = data["channel_allocations"]
        parts.append("**Channel Spend Breakdown:**\n")
        parts.append("| Channel | Spend | Proj. Clicks | Proj. Applications |")
        parts.append("|---------|-------|-------------|-------------------|")
        for ch_name, ch_data in allocs.items():
            spend = ch_data.get("dollars", ch_data.get("spend", 0))
            clicks = ch_data.get("projected_clicks", 0)
            apps = ch_data.get("projected_applications", 0)
            parts.append(f"| {ch_name} | ${spend:,.0f} | {clicks:,.0f} | {apps:,.0f} |")

        total = data.get("total_projected", {})
        if total:
            parts.append(f"\n**Projected Totals:**")
            parts.append(f"- Total Clicks: {total.get('clicks', 0):,.0f}")
            parts.append(f"- Total Applications: {total.get('applications', 0):,.0f}")
            parts.append(f"- Projected Hires: {total.get('hires', 0):,.0f}")
            cph_val = total.get("cost_per_hire", 0)
            if cph_val:
                parts.append(f"- Estimated Cost per Hire: ${cph_val:,.0f}")

    elif "estimated_allocation" in data:
        allocs = data["estimated_allocation"]
        parts.append("**Estimated Channel Allocation:**\n")
        for ch_name, ch_data in allocs.items():
            nice_name = ch_name.replace("_", " ").title()
            parts.append(f"- **{nice_name}**: ${ch_data['amount']:,.0f} ({ch_data['pct']}%)")

    recs = data.get("recommendations", [])
    if recs:
        parts.append("\n**Optimization Recommendations:**")
        for rec in recs[:4]:
            if isinstance(rec, str):
                parts.append(f"- {rec}")
            elif isinstance(rec, dict):
                parts.append(f"- {rec.get('recommendation', rec.get('message', str(rec)))}")

    return "\n".join(parts)


def _format_dei_response(data: dict, country: str) -> str:
    """Format DEI board data."""
    return _format_supply_response(data, country, is_dei=True)


def _format_trend_response(data: dict) -> str:
    """Format trend data into a readable response."""
    parts = []
    parts.append("### Recruitment Market Trends (2025-2026)\n")

    summaries = data.get("trend_summaries", {})
    for tk, tv in list(summaries.items())[:6]:
        parts.append(f"**{tv.get('title', tk.replace('_', ' ').title())}**")
        desc = tv.get("description", "")
        if desc:
            parts.append(f"{desc}\n")

    return "\n".join(parts) if parts else "No trend data available."


def _format_demand_response(data: dict, role: str) -> str:
    """Format market demand data."""
    parts = []
    parts.append(f"### Market Demand: {role}\n")

    apo = data.get("applicants_per_opening", {})
    if apo:
        icims = apo.get("icims_2025", {})
        if icims:
            parts.append(f"**Applicants per Opening**: {icims.get('ratio', 'N/A')} (iCIMS 2025)")

    soh = data.get("source_of_hire", {})
    if soh:
        parts.append("\n**Source of Hire Breakdown:**")
        parts.append(f"- Job Boards: {soh.get('job_boards_usage', 'N/A')}")
        parts.append(f"- Referrals: {soh.get('referrals_usage', 'N/A')}")
        parts.append(f"- Career Sites: {soh.get('career_sites_usage', 'N/A')}")
        parts.append(f"- LinkedIn: {soh.get('linkedin_usage', 'N/A')}")

    ind = data.get("industry_demand", {})
    if ind:
        parts.append(f"\n**Industry Demand ({ind.get('industry', 'N/A')}):**")
        parts.append(f"- Hiring Strength: {ind.get('hiring_strength', 'N/A')}")
        parts.append(f"- Recruitment Difficulty: {ind.get('recruitment_difficulty', 'N/A')}")

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

# Module-level singleton
_iq_instance: Optional[JoveoIQ] = None


def _get_iq() -> JoveoIQ:
    """Get or create the JoveoIQ singleton."""
    global _iq_instance
    if _iq_instance is None:
        _iq_instance = JoveoIQ()
    return _iq_instance


def handle_chat_request(request_data: dict) -> dict:
    """Handle an incoming chat API request.

    Expected request format::

        {
            "message": "What's the average CPA for nursing roles in Texas?",
            "conversation_id": "optional-session-id",
            "history": [{"role": "user", "content": "..."}, ...],
            "context": {
                "roles": [...],
                "locations": [...],
                "industry": "...",
                "enriched": {...},
                "synthesized": {...}
            }
        }

    Returns::

        {
            "response": "Based on Joveo's data...",
            "sources": ["Joveo Publisher Network", "Recruitment Industry KB"],
            "confidence": 0.85,
            "tools_used": ["query_publishers", "query_knowledge_base"]
        }
    """
    if not isinstance(request_data, dict):
        return {
            "response": "Invalid request format.",
            "sources": [],
            "confidence": 0.0,
            "tools_used": [],
            "error": "Request must be a JSON object",
        }

    message = (request_data.get("message") or "").strip()
    if not message:
        return {
            "response": "Please provide a message.",
            "sources": [],
            "confidence": 0.0,
            "tools_used": [],
            "error": "No message provided",
        }

    history = request_data.get("history", [])
    context = request_data.get("context")

    iq = _get_iq()

    try:
        result = iq.chat(
            user_message=message,
            conversation_history=history if isinstance(history, list) else [],
            enrichment_context=context if isinstance(context, dict) else None,
        )
        return result
    except Exception as e:
        logger.error("Chat request failed: %s", e, exc_info=True)
        return {
            "response": "I encountered an error processing your question. Please try again.",
            "sources": [],
            "confidence": 0.0,
            "tools_used": [],
            "error": str(e),
        }
