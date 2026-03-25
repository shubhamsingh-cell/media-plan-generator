# Comprehensive Audit: Quality Bar Gap Analysis
## Media Planner Generator — Current vs. Gold Standard

**Document Created**: 2026-03-26
**Scope**: Analysis of gap between current media plan output and gold-standard quality
**Status**: Specification Only (Implementation NOT included)

---

## EXECUTIVE SUMMARY

The current media planner generator produces **basic channel allocation + market insights** at the **TIER 1 (Awareness)** level. The gold standard you shared demonstrates **TIER 4 (Executive/CFO-Ready)** output with:

- **City-level granularity** (not national aggregates)
- **Security clearance segmentation** (TS/Secret/Other)
- **Competitor mapping per city/role** (not aggregated)
- **Difficulty level framework** (junior/mid/senior/staff tiers)
- **Multi-tier budget breakdown** (creative/media/contingency/testing)
- **Activation event calendars** (seasonal hiring peaks, industry conferences)

### Current Tier 1 Output:
```json
{
  "channels": [
    {"name": "Programmatic DSP", "budget": 35000, "cpc_range": "$2.50", "projected_clicks": 14000}
  ],
  "budget_summary": {"total": 100000, "by_channel": {...}, "by_category": {...}},
  "market_insights": {"hiring_difficulty": "moderate", "competition_level": "medium"},
  "recommendations": ["Lead with Programmatic DSP...", "Diversify across channels..."]
}
```

### Gold Standard Tier 4 Output:
```json
{
  "plan_metadata": {
    "created_at": "2026-03-26T14:32:00Z",
    "audit_quality_tier": 4,
    "data_sources_used": ["BLS", "Census", "Salary data", "Competitor intel", "Industry benchmarks"],
    "confidence_score": 0.92,
    "methodology": "City-level supply-demand analysis with security clearance segmentation"
  },
  "city_level_supply_demand": [
    {
      "city": "San Francisco, CA",
      "supply": {
        "total_salary_talent": 1240,
        "junior_pct": 18,
        "mid_pct": 48,
        "senior_pct": 28,
        "staff_pct": 6
      },
      "demand": {
        "hiring_volume": 85,
        "open_positions_by_level": {"junior": 15, "mid": 40, "senior": 25, "staff": 5},
        "hiring_trend": "growing",
        "months_to_fill": 2.3
      },
      "salary_bands": {
        "junior": {"median": 95000, "p25": 85000, "p75": 110000},
        "mid": {"median": 135000, "p25": 120000, "p75": 155000},
        "senior": {"median": 175000, "p25": 155000, "p75": 200000},
        "staff": {"median": 210000, "p25": 190000, "p75": 240000}
      }
    }
  ],
  "security_clearance_segmentation": {
    "secret": {"hiring_volume": 12, "difficulty": "hard", "channels": ["ClearedJobs.Net", "Military.com"]},
    "top_secret": {"hiring_volume": 8, "difficulty": "very_hard", "channels": ["ClearedJobs.Net"]},
    "other": {"hiring_volume": 65, "difficulty": "moderate", "channels": ["LinkedIn", "Indeed"]}
  },
  "competitor_mapping": {
    "San Francisco": [
      {"company": "Salesforce", "hiring_volume": 120, "salary_premium": 1.15, "channels": ["LinkedIn", "GitHub"]},
      {"company": "Google", "hiring_volume": 95, "salary_premium": 1.22, "channels": ["LinkedIn", "Google Careers"]}
    ]
  },
  "channel_strategy": {
    "traditional": [
      {"channel": "LinkedIn Jobs", "monthly_spend": 8000, "targeting": "mid-to-senior"},
      {"channel": "Indeed", "monthly_spend": 6000, "targeting": "junior-to-mid"}
    ],
    "non_traditional": [
      {"channel": "GitHub", "monthly_spend": 3000, "targeting": "senior engineers", "engagement_type": "organic"},
      {"channel": "Glassdoor", "monthly_spend": 2000, "targeting": "company_brand"}
    ]
  },
  "budget_breakdown_multi_tier": {
    "total": 100000,
    "creative": {
      "amount": 12000,
      "allocation": {
        "job_ad_copy": 5000,
        "employer_brand_video": 4000,
        "landing_pages": 3000
      }
    },
    "media": {
      "amount": 70000,
      "allocation": {
        "paid_channels": 65000,
        "organic_amplification": 5000
      }
    },
    "contingency": {
      "amount": 10000,
      "purpose": "Reallocate based on 2-week performance data"
    },
    "testing": {
      "amount": 8000,
      "purpose": "A/B test creatives, targeting, messaging"
    }
  },
  "activation_events": {
    "seasonal_hiring_peaks": [
      {"period": "Q1 (Jan-Mar)", "hiring_lift": 1.4, "recommended_spend_increase": 1.25},
      {"period": "Q3 (Jul-Sep)", "hiring_lift": 1.15, "recommended_spend_increase": 1.1}
    ],
    "industry_conferences": [
      {"event": "AWS re:Invent", "date": "2026-11-30", "cities_impacted": ["Las Vegas", "San Francisco"], "recommended_creative": "cloud_engineering_focus"},
      {"event": "Kubernetes Conf", "date": "2026-10-15", "cities_impacted": ["Los Angeles", "San Francisco"], "recommended_creative": "devops_focus"}
    ],
    "company_milestones": [
      {"milestone": "Series C Funding Announcement", "timing": "Q2 2026", "recommended_messaging": "growth_hiring_story"}
    ]
  }
}
```

---

## 1. CURRENT STATE ANALYSIS

### 1.1 Current Data Sources

**Active (api_enrichment.py)**:
- BLS OES (salary via SOC code)
- BLS QCEW (industry employment, aggregate)
- US Census ACS (location demographics, aggregate)
- World Bank Open Data (global economic, aggregate)
- Adzuna (job postings, aggregate market)
- Clearbit (company metadata, logos)
- Wikipedia (company descriptions)
- SEC EDGAR (public company data)
- FRED (economic indicators)
- Google Trends (search interest)
- O*NET (occupation skills, aggregate)
- IMF DataMapper (economic indicators)
- REST Countries v3.1 (country data)
- GeoNames (coordinates, timezone)
- Teleport (quality of life, cost of living)
- DataUSA (occupation wages, state demographics)

**Limitations**:
- ❌ **No city-level salary bands** (only national medians from BLS)
- ❌ **No hiring volume by difficulty tier** (junior/mid/senior/staff)
- ❌ **No security clearance segmentation** (TS/Secret/Other)
- ❌ **No per-city competitor mapping** (only aggregated competitor logos)
- ❌ **No month-by-month hiring seasonality** (only macro trends)
- ❌ **No industry conference calendar** (activation events)
- ❌ **No geopolitical risk scoring** (tariffs, labor laws by city)

### 1.2 Current Synthesis Pipeline

**data_synthesizer.py** currently:
1. Fuses 25 API sources with reliability weights
2. Extracts salary percentiles (10th, median, 90th)
3. Produces hiring difficulty estimate (easy/moderate/hard)
4. Estimates market competition (low/medium/high)
5. Calculates demand trend (growing/stable/declining)

**Missing**:
- ❌ Tier-specific salary bands (e.g., senior vs. staff bands)
- ❌ Per-location difficulty assessment
- ❌ Competitor salary premium analysis
- ❌ Activation event calendar synthesis
- ❌ Channel effectiveness by difficulty tier

### 1.3 Current Output Schema (app.py::_extract_plan_json)

```python
{
  "channels": [
    {
      "name": str,
      "allocation_pct": float,
      "budget": float,
      "cpc_range": str,
      "cpa_range": str,
      "projected_clicks": int,
      "recommended_reason": str
    }
  ],
  "budget_summary": {
    "total": float,
    "by_channel": {channel: budget},
    "by_category": {category: budget}
  },
  "market_insights": {
    "hiring_difficulty": str,
    "competition_level": str,
    "salary_range": str,
    "demand_trend": str
  },
  "recommendations": [str],
  "metadata": {
    "roles": list[str],
    "locations": list[str],
    "industry": str,
    "total_budget": float,
    "generated_at": str
  }
}
```

**Missing keys for Tier 4**:
- ❌ `city_level_supply_demand` (granular by city)
- ❌ `security_clearance_segmentation`
- ❌ `competitor_mapping` (per-city, per-role)
- ❌ `channel_strategy.traditional` vs. `.non_traditional`
- ❌ `budget_breakdown_multi_tier` (creative/media/contingency/testing)
- ❌ `activation_events` (seasonal, conferences, milestones)
- ❌ `audit_quality_tier` (self-assessment: 1-4)
- ❌ `confidence_score` (data quality metric, 0.0-1.0)
- ❌ `data_sources_used` (transparency/auditability)
- ❌ `methodology` (how plan was generated)

### 1.4 Current Excel Output (excel_v2.py)

**Current 5 sheets**:
1. Executive Summary — budget, benchmarks, recommendations
2. Channels & Strategy — channel analysis, niche boards
3. Market Intelligence — labour market, locations, competition, salary, demand
4. Sources & Confidence — data quality, API status
5. ROI Projections — per-channel hire forecasts, CPA, time-to-fill

**Missing sections**:
- ❌ City-level supply/demand breakdown (by city + role + difficulty tier)
- ❌ Security clearance segmentation table
- ❌ Competitor salary premium analysis (by city/role)
- ❌ Activation event calendar (seasonal hiring + conferences)
- ❌ Multi-tier budget breakdown with contingency planning
- ❌ Channel recommendations by difficulty tier
- ❌ Geopolitical risk assessment (visa sponsorship, tax, labor laws)

---

## 2. GOLD STANDARD SPECIFICATION

### 2.1 Data Sources Needed (New/Enhanced)

#### A. City-Level Salary Bands
**Source**: BLS OES + Census Microdata (API: Census Bureau, requires auth)
- Fetch SOC code wages by **Metropolitan Statistical Area (MSA)**
- Extract percentiles: 10th, 25th, median, 75th, 90th
- Map to difficulty tiers: junior/mid/senior/staff
- **Implementation**: Create `_fetch_salary_by_city(soc_code, cities)` in api_enrichment.py

**Data contract**:
```python
{
  "San Francisco, CA": {
    "11-1011": {  # SOC code for IT manager
      "junior": {"median": 95000, "p25": 85000, "p75": 110000},
      "mid": {"median": 135000, "p25": 120000, "p75": 155000},
      "senior": {"median": 175000, "p25": 155000, "p75": 200000},
      "staff": {"median": 210000, "p25": 190000, "p75": 240000}
    }
  }
}
```

#### B. Hiring Volume by Difficulty Tier
**Sources**:
- LinkedIn (via Marketing API, if auth available) — job postings by seniority
- Adzuna (enhanced with seniority level detection)
- Indeed (via scraping or API) — difficulty level estimation
- **Fallback**: Use keyword heuristics (e.g., "Senior" in title → senior tier)

**Data contract**:
```python
{
  "San Francisco, CA": {
    "hiring_volume_total": 85,
    "by_tier": {
      "junior": {"volume": 15, "months_to_fill": 1.2},
      "mid": {"volume": 40, "months_to_fill": 2.1},
      "senior": {"volume": 25, "months_to_fill": 3.5},
      "staff": {"volume": 5, "months_to_fill": 4.2}
    }
  }
}
```

#### C. Security Clearance Segmentation
**Sources**:
- **ClearedJobs.Net API** (if available) or web scraping
- **Military.com job postings** (scrape for clearance requirements)
- **Keyword detection** in job postings (e.g., "Secret", "Top Secret", "TS/SCI")
- **Company vertical detection** (aerospace/defense = higher clearance needs)

**Data contract**:
```python
{
  "secret": {
    "hiring_volume": 12,
    "difficulty_score": 4.2,  # 1-5 scale
    "preferred_channels": ["ClearedJobs.Net", "ClearanceJobs"],
    "salary_premium": 1.08,
    "months_to_fill": 2.8
  },
  "top_secret": {
    "hiring_volume": 8,
    "difficulty_score": 4.8,
    "preferred_channels": ["ClearedJobs.Net"],
    "salary_premium": 1.15,
    "months_to_fill": 4.1
  },
  "other": {
    "hiring_volume": 65,
    "difficulty_score": 2.3,
    "preferred_channels": ["LinkedIn", "Indeed", "Glassdoor"],
    "salary_premium": 1.0,
    "months_to_fill": 2.1
  }
}
```

#### D. Competitor Mapping (Per-City, Per-Role)
**Sources**:
- LinkedIn Company data (via scraping or API) — hiring volumes, salary insight
- Glassdoor (via scraping or API) — company ratings, salary reports
- Crunchbase (if auth available) — competitor funding, growth
- **Internal**: Calculate `salary_premium = competitor_median / your_median`

**Data contract**:
```python
{
  "San Francisco, CA": [
    {
      "company": "Salesforce",
      "hiring_volume": 120,
      "salary_median": 156000,
      "salary_premium": 1.15,  # 15% above market
      "open_positions": ["Software Engineer", "Product Manager"],
      "preferred_channels": ["LinkedIn", "Salesforce Careers"],
      "employer_rating": 4.2
    }
  ]
}
```

#### E. Activation Events Calendar
**Sources**:
- **Seasonal hiring data**: BLS JOLTS data (by month, historical)
- **Industry conferences**: Manual curated list (e.g., AWS re:Invent, KubeCon) — tech focus
- **Company milestones**: Earnings calendar, funding announcements (SEC EDGAR, Crunchbase)
- **Holiday hiring**: Manual rules (holiday hiring peaks in Oct-Nov for Q4 goals)

**Data contract**:
```python
{
  "seasonal_hiring_peaks": [
    {
      "period": "Q1 (Jan-Mar)",
      "hiring_lift": 1.35,  # 35% above baseline
      "recommended_spend_increase": 1.25
    },
    {
      "period": "Q3 (Jul-Sep)",
      "hiring_lift": 1.15,
      "recommended_spend_increase": 1.1
    }
  ],
  "industry_conferences": [
    {
      "event": "AWS re:Invent 2026",
      "date": "2026-11-30",
      "cities_impacted": ["Las Vegas", "San Francisco", "Austin"],
      "role_focus": "AWS Engineers, Cloud Architects",
      "recommended_creative_angle": "cloud_infrastructure",
      "expected_recruiter_attendance": 250
    }
  ]
}
```

---

### 2.2 Synthesis Pipeline Changes (data_synthesizer.py)

#### A. Add `synthesize_city_level_supply_demand()`
**Input**: Enriched data with city salary, hiring volume, employment data
**Output**: Per-city supply/demand object with tiers

```python
def synthesize_city_level_supply_demand(
    enriched: dict,
    kb: dict,
    cities: list[str],
    roles: list[str]
) -> dict[str, Any]:
    """
    Synthesize city-level supply-demand matrix.

    Returns:
        {
            "San Francisco, CA": {
                "supply": {...},
                "demand": {...},
                "salary_bands": {...},
                "employment_trend": str,
                "recommendation": str
            }
        }
    """
```

#### B. Add `synthesize_security_clearance()`
**Input**: Job postings, company vertical, enriched data
**Output**: Clearance-segmented hiring breakdown

```python
def synthesize_security_clearance(
    enriched: dict,
    kb: dict,
    roles: list[str],
    locations: list[str]
) -> dict[str, Any]:
    """
    Estimate hiring by clearance level (secret, TS, other).
    Detect from: job postings, company vertical, industry.

    Returns:
        {
            "secret": {...},
            "top_secret": {...},
            "other": {...},
            "total_clearance_pct": float
        }
    """
```

#### C. Add `synthesize_competitor_map()`
**Input**: Enriched competitor logos/data, city salary, hiring volume
**Output**: Per-city competitor analysis

```python
def synthesize_competitor_map(
    enriched: dict,
    kb: dict,
    locations: list[str],
    roles: list[str]
) -> dict[str, list[dict]]:
    """
    Map competitors by city with salary premium, hiring volume.

    Returns:
        {
            "San Francisco, CA": [
                {"company": "...", "salary_premium": 1.15, ...}
            ]
        }
    """
```

#### D. Add `synthesize_activation_events()`
**Input**: Seasonal data, conference calendar, company milestones
**Output**: Calendar with hiring lift recommendations

```python
def synthesize_activation_events(
    enriched: dict,
    kb: dict,
    industry: str,
    locations: list[str]
) -> dict[str, Any]:
    """
    Generate seasonal hiring lift + conference calendar.

    Returns:
        {
            "seasonal_hiring_peaks": [...],
            "industry_conferences": [...],
            "company_milestones": [...]
        }
    """
```

#### E. Modify `_calculate_difficulty_tier()`
**Current**: Single difficulty score (easy/moderate/hard)
**New**: Per-location, per-tier scoring

```python
def _calculate_difficulty_tier_detailed(
    location: str,
    role: str,
    enriched: dict,
    kb: dict
) -> dict[str, float]:
    """
    Return difficulty scores per tier:
        {
            "junior": 1.2,
            "mid": 2.5,
            "senior": 3.8,
            "staff": 4.5
        }

    Inputs: city salary competitiveness, hiring volume, competitor density.
    """
```

---

### 2.3 Output JSON Schema (New Tier 4 Structure)

**File**: Add `SPEC_PLAN_JSON_SCHEMA_TIER4.json` with:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Tier 4 Media Plan (Executive/CFO-Ready)",
  "type": "object",
  "required": [
    "plan_metadata",
    "city_level_supply_demand",
    "channel_strategy",
    "budget_breakdown_multi_tier",
    "activation_events"
  ],
  "properties": {
    "plan_metadata": {
      "type": "object",
      "properties": {
        "created_at": {"type": "string", "format": "date-time"},
        "audit_quality_tier": {"type": "integer", "minimum": 1, "maximum": 4},
        "confidence_score": {"type": "number", "minimum": 0, "maximum": 1},
        "data_sources_used": {"type": "array", "items": {"type": "string"}},
        "methodology": {"type": "string"}
      }
    },
    "city_level_supply_demand": {
      "type": "object",
      "additionalProperties": {
        "type": "object",
        "properties": {
          "city": {"type": "string"},
          "supply": {
            "type": "object",
            "properties": {
              "total_salary_talent": {"type": "integer"},
              "junior_pct": {"type": "number"},
              "mid_pct": {"type": "number"},
              "senior_pct": {"type": "number"},
              "staff_pct": {"type": "number"}
            }
          },
          "demand": {
            "type": "object",
            "properties": {
              "hiring_volume": {"type": "integer"},
              "open_positions_by_level": {
                "type": "object",
                "properties": {
                  "junior": {"type": "integer"},
                  "mid": {"type": "integer"},
                  "senior": {"type": "integer"},
                  "staff": {"type": "integer"}
                }
              },
              "hiring_trend": {"enum": ["growing", "stable", "declining"]},
              "months_to_fill": {"type": "number"}
            }
          },
          "salary_bands": {
            "type": "object",
            "additionalProperties": {
              "type": "object",
              "properties": {
                "median": {"type": "number"},
                "p25": {"type": "number"},
                "p75": {"type": "number"}
              }
            }
          }
        }
      }
    },
    "security_clearance_segmentation": {
      "type": "object",
      "properties": {
        "secret": {"$ref": "#/definitions/clearance_segment"},
        "top_secret": {"$ref": "#/definitions/clearance_segment"},
        "other": {"$ref": "#/definitions/clearance_segment"}
      }
    },
    "competitor_mapping": {
      "type": "object",
      "additionalProperties": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "company": {"type": "string"},
            "hiring_volume": {"type": "integer"},
            "salary_premium": {"type": "number"},
            "channels": {"type": "array", "items": {"type": "string"}}
          }
        }
      }
    },
    "channel_strategy": {
      "type": "object",
      "properties": {
        "traditional": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "channel": {"type": "string"},
              "monthly_spend": {"type": "number"},
              "targeting": {"type": "string"}
            }
          }
        },
        "non_traditional": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "channel": {"type": "string"},
              "monthly_spend": {"type": "number"},
              "targeting": {"type": "string"},
              "engagement_type": {"type": "string"}
            }
          }
        }
      }
    },
    "budget_breakdown_multi_tier": {
      "type": "object",
      "properties": {
        "total": {"type": "number"},
        "creative": {
          "type": "object",
          "properties": {
            "amount": {"type": "number"},
            "allocation": {"type": "object", "additionalProperties": {"type": "number"}}
          }
        },
        "media": {
          "type": "object",
          "properties": {
            "amount": {"type": "number"},
            "allocation": {"type": "object", "additionalProperties": {"type": "number"}}
          }
        },
        "contingency": {
          "type": "object",
          "properties": {
            "amount": {"type": "number"},
            "purpose": {"type": "string"}
          }
        },
        "testing": {
          "type": "object",
          "properties": {
            "amount": {"type": "number"},
            "purpose": {"type": "string"}
          }
        }
      }
    },
    "activation_events": {
      "type": "object",
      "properties": {
        "seasonal_hiring_peaks": {"type": "array"},
        "industry_conferences": {"type": "array"},
        "company_milestones": {"type": "array"}
      }
    }
  },
  "definitions": {
    "clearance_segment": {
      "type": "object",
      "properties": {
        "hiring_volume": {"type": "integer"},
        "difficulty": {"type": "string"},
        "channels": {"type": "array", "items": {"type": "string"}}
      }
    }
  }
}
```

---

### 2.4 LLM Prompt Changes

#### A. System Prompt Enhancement
**Current** (concise, 2 sentences):
```
You are a senior recruitment marketing strategist. Write concise, actionable recommendations. No fluff.
```

**Required** (Tier 4 audit quality):
```
You are a CFO-grade recruitment intelligence AI. Your role:

1. CITY-LEVEL ANALYSIS: Analyze hiring supply-demand by city, role, and difficulty tier
   (junior/mid/senior/staff). Cite BLS, Census, and Adzuna data.

2. SECURITY CLEARANCE: For aerospace/defense/government verticals, segment hiring by
   clearance level (Secret/TS/Other). Recommend channel-clearance alignment.

3. COMPETITIVE MAPPING: Map top 5 competitors per city with salary premium, hiring volume,
   and channel preferences. Flag salary gaps requiring premium positioning.

4. ACTIVATION EVENTS: Identify seasonal hiring peaks, industry conferences, and company
   milestones that justify spend timing/creative adjustments.

5. BUDGET TIERS: Break budget into creative (job ads, employer brand), media (paid channels),
   contingency (performance-based reallocation), and testing (A/B, new channels).

6. AUDIT TRANSPARENCY: Include data sources used, confidence scores (0-1), and methodology
   notes. Self-assess plan quality tier (1-4).

Output ONLY valid JSON matching the Tier 4 schema. No markdown, no explanation.
```

#### B. Synthesis Prompt for data_synthesizer
**Current**: Generic "fuse all data sources"
**Required**: Role-specific, location-specific prompts

```python
SYNTHESIS_PROMPTS = {
    "city_level_supply_demand": """
        Given:
        - BLS OES salary data by MSA and SOC code
        - Census ACS employment by city
        - Adzuna job postings by location
        - Hiring volume from LinkedIn/Indeed

        Synthesize per-city supply-demand:
        - Count employed talent in role (supply side)
        - Estimate hiring volume by difficulty tier (demand side)
        - Calculate months-to-fill based on supply/demand ratio
        - Flag talent shortages vs. surplus

        Confidence: Use BLS weight=1.0, Census=0.95, Adzuna=0.60, LinkedIn=0.70
        Aggregate by city (MSA if exact city unavailable).
    """,

    "security_clearance": """
        Given:
        - Company industry vertical (aerospace/defense = higher clearance)
        - Job titles and descriptions (keyword: Secret, TS, TS/SCI)
        - Regional hiring concentrations (where cleared roles cluster)

        Estimate hiring breakdown by clearance level:
        - Secret: % of roles requiring baseline clearance
        - Top Secret: % requiring higher clearance
        - Other: uncleared roles

        For each tier:
        - Recommend preferred channels (ClearedJobs.Net, Military.com, etc.)
        - Estimate salary premium (cleared roles cost 8-15% more)
        - Calculate months-to-fill (longer vetting = slower hiring)
    """,

    "competitor_mapping": """
        Given:
        - Top 5-10 competitors in industry/role
        - Glassdoor salary reports, LinkedIn job postings
        - Hiring volume by city

        For each competitor per city:
        - Calculate salary premium vs. your baseline (premium = comp_median / your_median)
        - Identify their primary channels (e.g., LinkedIn 40%, Glassdoor 20%)
        - Assess employer brand strength (rating 3.5-4.5 scale)
        - Flag if premium > 1.15 (you're at disadvantage, need premium positioning)

        Recommend channel mix to compete (e.g., if competitor owns LinkedIn, allocate
        to job boards, GitHub, niche channels to differentiate).
    """,

    "activation_events": """
        Given:
        - Historical BLS JOLTS data (monthly hiring patterns)
        - Your company's fiscal year and goal timing
        - Industry conference calendar (Q4: AWS re:Invent, Oct: KubeCon)

        Identify:
        - Seasonal hiring peaks (Q1 hiring up 35%, Q3 up 15%)
        - Industry conferences relevant to roles (e.g., engineers → tech conferences)
        - Company milestones (fundraising, product launch, acquisition)

        For each event:
        - Recommend spend increase during peak (multiply baseline by 1.25-1.4)
        - Suggest creative angle (e.g., "Join us at AWS re:Invent" for cloud roles)
        - Time spend 2-4 weeks BEFORE peak to maximize lead generation
    """
}
```

---

### 2.5 Template Rendering Changes

#### A. Excel Output (5→8 sheets)
**New sheets**:
1. Executive Summary (enhanced with Tier 4 metrics)
2. City-Level Analysis — per-city supply, demand, salary bands, recommendation
3. Security Clearance Breakdown — hiring by clearance, channels, salary premium
4. Competitor Landscape — top competitors per city, salary premium, channels
5. Activation Calendar — seasonal hiring peaks, conferences, milestones
6. Channel Strategy — traditional vs. non-traditional, targeting, spend
7. Budget Tiers — creative/media/contingency/testing with breakdown
8. Sources & Confidence — data quality, API status, methodology

**Each sheet**:
- Header with `audit_quality_tier`, `confidence_score`, `data_sources_used`
- Methodology notes (how plan was generated)
- Hyperlinks to source reports (BLS, Census, LinkedIn)

#### B. Dashboard JSON Output (app.py::_extract_plan_json)
**New fields**:
```python
{
  "plan_metadata": {...},
  "city_level_supply_demand": {...},
  "security_clearance_segmentation": {...},
  "competitor_mapping": {...},
  "channel_strategy": {...},
  "budget_breakdown_multi_tier": {...},
  "activation_events": {...},
  "audit_quality_tier": 4,
  "confidence_score": 0.92
}
```

---

## 3. IMPLEMENTATION CHANGES CHECKLIST

### 3.1 API Enrichment Layer (api_enrichment.py)

**New functions** (~2,000 lines):
- [ ] `_fetch_salary_by_city(soc_code, cities)` — MSA-level salary via Census API + BLS
- [ ] `_fetch_hiring_volume_by_tier(locations, roles)` — LinkedIn/Adzuna with seniority detection
- [ ] `_fetch_security_clearance_data(locations, industry)` — ClearedJobs.Net scrape + keyword detection
- [ ] `_fetch_competitor_salary_premium(competitors, locations)` — Glassdoor/LinkedIn scrape
- [ ] `_fetch_activation_events(industry, locations)` — Conference calendar + seasonal data

**Modifications**:
- [ ] Extend `enrich_data()` to call 5 new functions above
- [ ] Add results to enriched dict with keys: `city_salary_bands`, `hiring_by_tier`, `clearance_data`, `competitor_premiums`, `activation_events`

### 3.2 Synthesis Layer (data_synthesizer.py)

**New functions** (~3,000 lines):
- [ ] `synthesize_city_level_supply_demand(enriched, kb, cities, roles)`
- [ ] `synthesize_security_clearance(enriched, kb, roles, locations)`
- [ ] `synthesize_competitor_map(enriched, kb, locations, roles)`
- [ ] `synthesize_activation_events(enriched, kb, industry, locations)`
- [ ] `_calculate_difficulty_tier_detailed(location, role, enriched, kb)`

**Modifications**:
- [ ] Update `synthesize()` main function to call 4 new functions
- [ ] Return synthesized dict with keys: `city_supply_demand`, `clearance_segmentation`, `competitors`, `activation_calendar`
- [ ] Add `confidence_score` calculation (average source weights, 0-1)
- [ ] Add `data_sources_used` extraction from source weights

### 3.3 Plan Generation (app.py)

**New LLM prompt** (~500 lines):
- [ ] Replace generic "recruitment strategist" prompt with Tier 4 CFO-grade prompt
- [ ] Add constraints: city-level analysis, clearance segmentation, competitor mapping, activation events
- [ ] Add output schema validation (verify Tier 4 JSON structure)

**Modifications**:
- [ ] Update `_async_generate()` to call enhanced synthesis functions
- [ ] Update `_verify_plan_data()` to validate Tier 4 schema

### 3.4 Output Extraction (app.py::_extract_plan_json)

**New function** (~800 lines):
- [ ] Add `_build_tier4_plan_json(data)` — replaces current simple extraction
- [ ] Extract city-level supply-demand from synthesized data
- [ ] Extract security clearance from synthesized data
- [ ] Extract competitor mapping from synthesized data
- [ ] Extract activation events from synthesized data
- [ ] Add audit metadata: `audit_quality_tier=4`, `confidence_score`, `data_sources_used`, `methodology`

**Modifications**:
- [ ] Modify `_store_plan_result()` to use new `_build_tier4_plan_json()`
- [ ] Ensure Tier 4 schema is returned to dashboard

### 3.5 Excel Generation (excel_v2.py)

**New sheets** (~2,000 lines):
- [ ] Add `_write_city_analysis_sheet(wb, data)` — city-level supply/demand table
- [ ] Add `_write_clearance_sheet(wb, data)` — security segmentation
- [ ] Add `_write_competitor_sheet(wb, data)` — competitor landscape
- [ ] Add `_write_activation_sheet(wb, data)` — activation calendar
- [ ] Modify header sheets to include `audit_quality_tier`, `confidence_score`, `methodology`

**Modifications**:
- [ ] Update `generate_excel_v2()` to call 4 new sheet functions
- [ ] Ensure all sheets follow Sapphire Blue palette and professional layout

### 3.6 HTML/Dashboard

**New sections** (index.html, dashboard.html):
- [ ] Add "City-Level Analysis" section — expandable per-city data
- [ ] Add "Security Clearance" toggle — show clearance-specific recommendations
- [ ] Add "Competitor Landscape" chart — salary premium vs. hiring volume
- [ ] Add "Activation Calendar" timeline — seasonal peaks + conferences

---

## 4. DATA QUALITY & CONFIDENCE SCORING

### 4.1 Confidence Score Calculation

**Formula**:
```
confidence_score = (
    0.30 * bls_coverage_rate +        # BLS data for role/city (0-1)
    0.25 * hiring_volume_recency +    # How recent is hiring data (0-1)
    0.20 * competitor_coverage_rate + # Competitor data available (0-1)
    0.15 * event_relevance_score +    # Industry conferences match roles (0-1)
    0.10 * clarity_penalty             # Penalty if multiple interpretations needed (0-1)
)
```

**Example**:
- BLS data available for 3/4 cities → 0.75 coverage
- Hiring data from past 30 days → 0.95 recency
- 4/5 competitors with salary data → 0.80 coverage
- 2/3 relevant conferences → 0.67 relevance
- Clear inputs, no ambiguity → 1.0 clarity
- **Score**: (0.30×0.75) + (0.25×0.95) + (0.20×0.80) + (0.15×0.67) + (0.10×1.0) = 0.806 ≈ **0.81**

### 4.2 Data Sources Transparency

**Include in plan**:
```python
"data_sources_used": [
    "BLS OES (salary by SOC/MSA)",
    "US Census ACS (employment by city)",
    "Adzuna (job postings, hiring volume)",
    "LinkedIn Jobs (competitor hiring)",
    "Glassdoor (salary reports, employer ratings)",
    "ClearedJobs.Net (cleared hiring)",
    "Industry Conference Calendar (manual curated)",
    "FRED/IMF (economic indicators)"
]
```

---

## 5. TESTING & VALIDATION

### 5.1 Unit Tests (data_synthesizer_test.py)
- [ ] Test city-level aggregation with mock BLS data
- [ ] Test clearance segmentation with 0%, 25%, 100% clearance splits
- [ ] Test competitor premium calculation (premium = 1.0, 1.1, 1.2)
- [ ] Test activation event synthesis with Q1/Q3 peaks
- [ ] Test confidence score calculation with various coverage rates

### 5.2 Integration Tests (api_enrichment_test.py)
- [ ] Test salary fetch by city (mock MSA data)
- [ ] Test hiring volume by tier detection (keyword-based seniority)
- [ ] Test clearance data extraction (mock ClearedJobs.Net API)
- [ ] Test competitor mapping (mock Glassdoor API)
- [ ] Test activation event calendar generation

### 5.3 E2E Tests (app_test.py)
- [ ] Full plan generation with Tier 4 output
- [ ] Verify Tier 4 JSON schema validation
- [ ] Verify Excel generation with 8 sheets
- [ ] Verify dashboard displays city-level data
- [ ] Verify confidence score is 0.7-0.95 range

### 5.4 Gold Standard Validation
- [ ] Compare generated plan against gold standard (salary bands, difficulty tiers, events)
- [ ] Verify city-level breakdowns match manual audit
- [ ] Verify clearance segmentation is reasonable for industry
- [ ] Verify competitor salary premiums within 5-20% of expected range
- [ ] Verify activation event timing aligns with fiscal calendar

---

## 6. ROLLOUT & MIGRATION PLAN

### 6.1 Phase 1: Infrastructure (Week 1)
- [ ] Add 5 new data fetch functions to api_enrichment.py
- [ ] Add 4 new synthesis functions to data_synthesizer.py
- [ ] Test with mock data (no live API calls)

### 6.2 Phase 2: LLM Integration (Week 2)
- [ ] Update plan generation prompt to Tier 4 spec
- [ ] Add Tier 4 output schema validation
- [ ] Test with real API calls, monitor latency
- [ ] Parallel run: generate both Tier 1 + Tier 4 outputs (internal validation)

### 6.3 Phase 3: Output & Rendering (Week 3)
- [ ] Implement Tier 4 Excel sheet generation
- [ ] Update dashboard to display city-level, clearance, competitor data
- [ ] A/B test: show Tier 1 to 50%, Tier 4 to 50% of users

### 6.4 Phase 4: Monitoring & Optimization (Week 4+)
- [ ] Monitor confidence scores, flag low-confidence plans
- [ ] Collect user feedback on plan quality
- [ ] Fine-tune synthesis prompts based on real outputs
- [ ] Optimize API call latency (cache city-level data, batch requests)

---

## 7. RISKS & MITIGATION

| Risk | Likelihood | Mitigation |
|------|-----------|-----------|
| City-level APIs unavailable or rate-limited | Medium | Graceful fallback to national aggregates, cache aggressively |
| Clearance detection via keywords too noisy | High | Combine keyword + company vertical heuristics, manual curate high-revenue clients |
| LLM prompt too complex, generates inconsistent JSON | Medium | Strict schema validation, retry with simpler prompt if needed |
| Competitor salary data unavailable (Glassdoor scrape blocked) | Medium | Fall back to BLS percentiles + salary.com data |
| Conference calendar incomplete, misses key events | Low | Curate manually for top 20 industries, allow user input |
| Performance impact from new API calls | High | Parallel execution via ThreadPoolExecutor (already in place), aggressive caching |

---

## 8. SUCCESS METRICS

### 8.1 Data Quality Metrics
- **City-level coverage**: % of plans with ≥2 cities data-enriched (target: 85%+)
- **Confidence score distribution**: Mean 0.80+, Std Dev <0.15 (tight quality)
- **API success rate**: ≥90% for city salary, hiring volume, competitor data

### 8.2 User Satisfaction Metrics
- **Plan actionability**: % users who rate plan "very detailed" (target: 75%+)
- **Data transparency**: % users who cite data sources in presentations (target: 60%+)
- **Tier 4 adoption**: % users who download Excel after seeing Tier 4 plan (target: 80%+)

### 8.3 Performance Metrics
- **Generation latency**: Plan generation <45s including new API calls (target: <40s)
- **Cache hit rate**: ≥70% for city salary data (reduce repeated API calls)
- **API cost**: <$0.10 per plan (monitor Upstash Redis, Census API costs)

---

## 9. APPENDIX: FILE REFERENCES

### Current Files to Modify
1. `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/api_enrichment.py` (~13,500 lines)
2. `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/data_synthesizer.py` (~3,400 lines)
3. `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/app.py` (~15,000 lines)
4. `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/excel_v2.py` (~1,500 lines)
5. `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/templates/index.html` (~9,000 lines)
6. `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/templates/dashboard.html` (~8,000 lines)

### New Files to Create
1. `SPEC_PLAN_JSON_SCHEMA_TIER4.json` — Formal JSON schema (500 lines)
2. `synthesis_prompts.py` — Tier 4 LLM prompts module (300 lines)
3. Tests: `api_enrichment_city_test.py`, `data_synthesizer_tier4_test.py` (1,000+ lines)

---

## 10. SUMMARY

The gap between current Tier 1 output and gold Tier 4 requires:

1. **Data Enrichment**: 5 new APIs/sources for city salary, hiring by tier, clearance, competitors, events
2. **Synthesis Logic**: 4 new synthesis functions + enhanced difficulty tier calculation
3. **LLM Prompts**: Shift from "general strategist" to "CFO-grade auditor" with structured outputs
4. **Output Schema**: Expand from 4 keys (channels, budget, insights, recs) to 10+ keys (city data, clearance, competitor, events, budget tiers, audit metadata)
5. **Excel Rendering**: 3 new sheets (city analysis, clearance, competitor, activation calendar)
6. **Dashboard**: 4 new sections displaying city-level, clearance, competitor, calendar data

**Effort Estimate**: 4-6 weeks for full implementation + testing + rollout

**Priority**: High — Tier 4 quality is table-stakes for CFO-ready plans (required per gold standard audit)

---

**Document Status**: SPECIFICATION COMPLETE
**Next Step**: Begin implementation Phase 1 (API enrichment layer)
