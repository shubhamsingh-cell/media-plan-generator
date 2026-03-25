# Tier 4 Data Contracts & API Specifications
## Detailed technical specifications for new data sources

**Document Created**: 2026-03-26
**Companion to**: SPEC_AUDIT_QUALITY_BAR.md

---

## 1. CITY-LEVEL SALARY BANDS

### 1.1 Data Source: BLS OES by MSA + Census Microdata

**API Endpoints**:
- BLS OES: `https://api.bls.gov/publicAPI/v2/timeseries/data/` (requires API key)
- Census ACS: `https://api.census.gov/data/2022/acs/acs5` (requires API key)
- Fallback: Local `data/bls_msa_salary_cache.json` (manually curated)

**Python Function Signature**:
```python
def _fetch_salary_by_city(
    soc_codes: list[str],  # e.g., ["11-1021.00", "15-1131.00"]
    cities: list[str],     # e.g., ["San Francisco, CA", "New York, NY"]
    tier_rules: dict = None  # Optional seniority mapping rules
) -> dict[str, dict[str, dict]]:
    """
    Fetch median, 25th, 75th percentile wages by:
    1. Convert city to MSA code (e.g., "San Francisco, CA" → "41860")
    2. Query BLS OES for each SOC code in MSA
    3. Classify percentiles into tiers: junior (p10), mid (median), senior (p75), staff (p90)
    4. Handle MSA → city fallback if exact MSA unavailable

    Returns:
        {
            "San Francisco, CA": {
                "11-1021.00": {  # Computer and Information Systems Managers
                    "junior": {"median": 95000, "p25": 85000, "p75": 110000},
                    "mid": {"median": 135000, "p25": 120000, "p75": 155000},
                    "senior": {"median": 175000, "p25": 155000, "p75": 200000},
                    "staff": {"median": 210000, "p25": 190000, "p75": 240000}
                }
            }
        }

    Implementation:
    1. Normalize city to MSA code via standardizer.py
    2. Build BLS series IDs (OEUN prefix + MSA + SOC + datatype)
    3. Fetch v2 API if BLS_API_KEY set, else fallback to v1
    4. Parse JSON response, extract latest year's data
    5. Map percentiles to tiers using configurable rules:
       - By default: p10→junior, p25→mid, p75→senior, p90→staff
       - Override via tier_rules parameter
    6. Cache for 24 hours (L1 memory, L2 disk, L3 Supabase)
    7. Return empty dict if API fails (graceful degradation)

    Error Handling:
    - If MSA code unknown: return empty dict (don't crash)
    - If BLS rate-limited (429): retry with exponential backoff
    - If network timeout: use fallback data from cache
    """
```

**Tier Classification Rules**:
```python
TIER_CLASSIFICATION_RULES = {
    # Default mapping: percentiles → tiers
    "default": {
        "junior": "p10",      # Entry-level, minimal experience
        "mid": "median",      # 2-5 years experience
        "senior": "p75",      # 5-10+ years experience
        "staff": "p90"        # Principal/staff/leadership roles
    },

    # Role-specific overrides (e.g., software engineering vs. nursing)
    "software_engineer": {
        "junior": "p10",      # < 2 years
        "mid": "p25",         # 2-5 years (shift down, growth is fast)
        "senior": "median",   # 5-8 years
        "staff": "p75"        # 8+ years (skip p90, staff rare)
    },

    "nurse": {
        "junior": "p10",      # RN new grad
        "mid": "p25",         # 2-5 years experience
        "senior": "median",   # 5-10 years
        "staff": "p75"        # Nurse manager, educator, specialist
    }
}
```

**Response Example**:
```json
{
  "San Francisco, CA": {
    "11-1021.00": {
      "soc_title": "Computer and Information Systems Managers",
      "msа_code": "41860",
      "junior": {
        "median": 95000,
        "p25": 85000,
        "p75": 110000,
        "sample_size": 1240,
        "source": "BLS OES 2023"
      },
      "mid": {
        "median": 135000,
        "p25": 120000,
        "p75": 155000,
        "sample_size": 1240,
        "source": "BLS OES 2023"
      },
      "senior": {
        "median": 175000,
        "p25": 155000,
        "p75": 200000,
        "sample_size": 1240,
        "source": "BLS OES 2023"
      },
      "staff": {
        "median": 210000,
        "p25": 190000,
        "p75": 240000,
        "sample_size": 1240,
        "source": "BLS OES 2023"
      }
    }
  }
}
```

---

## 2. HIRING VOLUME BY DIFFICULTY TIER

### 2.1 Data Sources: LinkedIn + Adzuna + Indeed (via scraping or API)

**Python Function Signature**:
```python
def _fetch_hiring_volume_by_tier(
    locations: list[str],  # e.g., ["San Francisco, CA", "New York, NY"]
    roles: list[str],      # e.g., ["Software Engineer", "Product Manager"]
    days_lookback: int = 30  # e.g., past 30 days of hiring activity
) -> dict[str, dict[str, Any]]:
    """
    Estimate hiring volume by difficulty tier using:
    1. LinkedIn Jobs API (if marketing token available) — seniority level in posting
    2. Adzuna API — aggregate job postings with keyword-based seniority detection
    3. Fallback: Keyword heuristics on job titles/descriptions

    Returns:
        {
            "San Francisco, CA": {
                "hiring_volume_total": 85,
                "by_tier": {
                    "junior": {
                        "volume": 15,
                        "job_titles": ["Junior Software Engineer", "Entry-level Product Analyst"],
                        "months_to_fill": 1.2,
                        "avg_days_open": 18,
                        "source": "LinkedIn + Adzuna"
                    },
                    "mid": {
                        "volume": 40,
                        "job_titles": ["Senior Software Engineer", "Product Manager"],
                        "months_to_fill": 2.1,
                        "avg_days_open": 32,
                        "source": "LinkedIn + Adzuna"
                    },
                    "senior": {
                        "volume": 25,
                        "job_titles": ["Staff Engineer", "Director of Product"],
                        "months_to_fill": 3.5,
                        "avg_days_open": 55,
                        "source": "LinkedIn + Adzuna"
                    },
                    "staff": {
                        "volume": 5,
                        "job_titles": ["Principal Engineer", "VP Engineering"],
                        "months_to_fill": 4.2,
                        "avg_days_open": 75,
                        "source": "LinkedIn + Adzuna"
                    }
                },
                "total_open_days_sum": 2850,  # Sum of all posting days
                "median_days_to_fill": 2.1   # Median time-to-hire across tiers
            }
        }

    Implementation:
    1. Query LinkedIn Marketing API if token available:
       - Filter: locations, job functions, seniority levels
       - Aggregate by seniority (Entry-level, Mid-level, Senior, Executive)
       - Extract: posting count, avg days open

    2. Query Adzuna API:
       - Search by location, role
       - Scrape job descriptions for seniority keywords
       - Classification rules:
         * "Junior" / "Entry-level" / "0-2 years" → junior
         * "Mid-level" / "2-5 years" / "Senior" (non-staff) → mid
         * "Senior" / "5-10 years" / "Staff" / "Principal" → senior
         * "Executive" / "VP" / "Director" / "C-suite" → staff
       - Aggregate by tier

    3. Fallback (if APIs unavailable):
       - Use keyword heuristics on cached job postings
       - Classify based on title keywords alone

    4. Calculate months_to_fill:
       - Heuristic: months_to_fill = avg_days_open / 30 * 1.1 (add 10% for recruiting lag)
       - Or: Use historical BLS JOLTS data for industry (if available)

    5. Cache for 7 days (more volatile than salary data)

    Error Handling:
    - If LinkedIn unavailable: skip, continue with Adzuna
    - If both unavailable: use keyword heuristics from cached postings
    - If all fail: return empty dict
    """
```

**Tier Classification Heuristics**:
```python
TIER_KEYWORDS = {
    "junior": [
        "junior", "entry-level", "entry level", "0-2 years",
        "graduate", "fresh", "new grad", "internship", "apprentice"
    ],
    "mid": [
        "mid-level", "mid level", "2-5 years", "intermediate",
        "experienced", "3-5 years", "associate", "specialist"
    ],
    "senior": [
        "senior", "5-10 years", "5+ years", "6+ years",
        "lead engineer", "principal", "staff", "architect"
    ],
    "staff": [
        "principal", "staff", "director", "vp", "vice president",
        "executive", "c-suite", "chief", "head of", "officer"
    ]
}
```

**Response Example**:
```json
{
  "San Francisco, CA": {
    "hiring_volume_total": 85,
    "by_tier": {
      "junior": {
        "volume": 15,
        "job_titles": [
          "Junior Software Engineer",
          "Entry-level Product Analyst",
          "Graduate Program Engineer"
        ],
        "months_to_fill": 1.2,
        "avg_days_open": 18,
        "source": "LinkedIn + Adzuna"
      },
      "mid": {
        "volume": 40,
        "job_titles": [
          "Software Engineer III",
          "Product Manager",
          "Senior Full-Stack Engineer"
        ],
        "months_to_fill": 2.1,
        "avg_days_open": 32,
        "source": "LinkedIn + Adzuna"
      },
      "senior": {
        "volume": 25,
        "job_titles": [
          "Staff Software Engineer",
          "Senior Product Manager",
          "Engineering Manager"
        ],
        "months_to_fill": 3.5,
        "avg_days_open": 55,
        "source": "LinkedIn + Adzuna"
      },
      "staff": {
        "volume": 5,
        "job_titles": [
          "Principal Engineer",
          "Director of Engineering",
          "VP of Product"
        ],
        "months_to_fill": 4.2,
        "avg_days_open": 75,
        "source": "LinkedIn + Adzuna"
      }
    }
  }
}
```

---

## 3. SECURITY CLEARANCE SEGMENTATION

### 3.1 Data Sources: ClearedJobs.Net, Military.com, Keyword Detection

**Python Function Signature**:
```python
def _fetch_security_clearance_data(
    locations: list[str],    # e.g., ["San Jose, CA", "Washington, DC"]
    industry: str,           # e.g., "aerospace_defense"
    company_verticals: list[str] = None  # e.g., ["aerospace", "defense"]
) -> dict[str, Any]:
    """
    Estimate hiring breakdown by security clearance level.

    Returns:
        {
            "clearance_mix": {
                "secret": {
                    "hiring_volume": 12,
                    "hiring_volume_pct": 14.1,
                    "difficulty_score": 4.2,  # 1-5 scale
                    "months_to_fill": 2.8,
                    "salary_premium": 1.08,
                    "preferred_channels": ["ClearedJobs.Net", "ClearanceJobs"],
                    "location_concentration": [
                        {"city": "San Jose, CA", "pct": 40},
                        {"city": "Washington, DC", "pct": 35},
                        {"city": "Arlington, VA", "pct": 25}
                    ]
                },
                "top_secret": {
                    "hiring_volume": 8,
                    "hiring_volume_pct": 9.4,
                    "difficulty_score": 4.8,
                    "months_to_fill": 4.1,
                    "salary_premium": 1.15,
                    "preferred_channels": ["ClearedJobs.Net"],
                    "location_concentration": [
                        {"city": "Washington, DC", "pct": 50},
                        {"city": "San Jose, CA", "pct": 30},
                        {"city": "Arlington, VA", "pct": 20}
                    ]
                },
                "other": {
                    "hiring_volume": 65,
                    "hiring_volume_pct": 76.5,
                    "difficulty_score": 2.3,
                    "months_to_fill": 2.1,
                    "salary_premium": 1.0,
                    "preferred_channels": ["LinkedIn", "Indeed", "Glassdoor"]
                }
            },
            "total_cleared_pct": 23.5,
            "industry_clearance_baseline": 22,  # avg % for aerospace industry
            "geographic_risk": {
                "San Jose, CA": {
                    "export_control_risk": "high",  # Tech export controls
                    "visa_sponsorship_difficulty": "hard",
                    "cleared_talent_density": "high"
                },
                "Washington, DC": {
                    "export_control_risk": "extreme",  # Federal hub
                    "visa_sponsorship_difficulty": "very_hard",
                    "cleared_talent_density": "very_high"
                }
            }
        }

    Implementation:
    1. Industry vertical assessment:
       - If industry in ["aerospace_defense", "military_recruitment", "government"]:
         Assume baseline 20-40% clearance requirement
       - Else: Assume 0-5% baseline

    2. Keyword detection in job postings:
       - Query Adzuna/LinkedIn for postings in locations + roles
       - Search descriptions for keywords:
         * "Top Secret" / "TS/SCI" / "TS" → top_secret
         * "Secret" / "Secret clearance" → secret
         * "Clearance required" (unspecified) → secret (conservative)
       - Percentage of postings with clearance keywords = clearance_mix

    3. ClearedJobs.Net API (if available):
       - Query API for hiring volume by location, clearance level
       - Use as authoritative source if available

    4. Military.com job postings (scraping):
       - Scrape Military.com job board
       - Count postings by clearance level + location
       - Aggregate by difficulty tier

    5. Geographic risk assessment:
       - San Jose, CA: High export control (ITAR/EAR), hard visa sponsorship
       - Washington, DC: Extreme clearance requirement, very hard visa
       - Use lookup table for standard geographic risk profiles

    6. Salary premium calculation:
       - Cleared roles command 8-15% salary premium
       - Rule: secret → 1.08x, top_secret → 1.15x
       - Can override with empirical data from Glassdoor if available

    7. Difficulty score:
       - Composite of: vetting time (4-6 months for TS), interview difficulty, visa sponsorship
       - Rule: secret → 4.2, top_secret → 4.8 (1-5 scale)

    8. Cache for 30 days (slower-moving than hiring volume)

    Error Handling:
    - If ClearedJobs API unavailable: use keyword detection alone
    - If no clearance keywords found: return 0% clearance (don't assume)
    - If industry unknown: default to 2% baseline clearance
    """
```

**Difficulty Score Mapping**:
```python
CLEARANCE_DIFFICULTY_SCORES = {
    "secret": {
        "vetting_months": 2.0,
        "interview_rounds": 2,
        "visa_sponsorship_difficulty": "moderate",  # Can sponsor Secret
        "difficulty_score": 4.2
    },
    "top_secret": {
        "vetting_months": 4.0,
        "interview_rounds": 3,
        "visa_sponsorship_difficulty": "very_hard",  # Rarely sponsor TS
        "difficulty_score": 4.8
    }
}

# Industry clearance baselines
INDUSTRY_CLEARANCE_BASELINES = {
    "aerospace_defense": 0.35,    # 35% of roles need clearance
    "military_recruitment": 0.40, # 40%
    "government": 0.25,           # 25%
    "tech_engineering": 0.05,     # 5% (select ITAR/export control roles)
    "finance": 0.02,              # 2%
    "healthcare": 0.01            # 1% (rare)
}
```

**Response Example**:
```json
{
  "clearance_mix": {
    "secret": {
      "hiring_volume": 12,
      "hiring_volume_pct": 14.1,
      "difficulty_score": 4.2,
      "months_to_fill": 2.8,
      "salary_premium": 1.08,
      "preferred_channels": [
        "ClearedJobs.Net",
        "ClearanceJobs"
      ]
    },
    "top_secret": {
      "hiring_volume": 8,
      "hiring_volume_pct": 9.4,
      "difficulty_score": 4.8,
      "months_to_fill": 4.1,
      "salary_premium": 1.15,
      "preferred_channels": [
        "ClearedJobs.Net"
      ]
    },
    "other": {
      "hiring_volume": 65,
      "hiring_volume_pct": 76.5,
      "difficulty_score": 2.3,
      "months_to_fill": 2.1,
      "salary_premium": 1.0,
      "preferred_channels": [
        "LinkedIn",
        "Indeed",
        "Glassdoor"
      ]
    }
  },
  "total_cleared_pct": 23.5
}
```

---

## 4. COMPETITOR SALARY PREMIUM MAPPING

### 4.1 Data Sources: Glassdoor, LinkedIn, Crunchbase

**Python Function Signature**:
```python
def _fetch_competitor_salary_premium(
    competitors: list[str],      # e.g., ["Salesforce", "Adobe", "Figma"]
    locations: list[str],        # e.g., ["San Francisco, CA", "New York, NY"]
    roles: list[str],            # e.g., ["Software Engineer", "Product Manager"]
    your_median_salary: dict = None  # e.g., {"Software Engineer": 140000}
) -> dict[str, list[dict]]:
    """
    Map competitor salary premiums per city.

    Returns:
        {
            "San Francisco, CA": [
                {
                    "company": "Salesforce",
                    "hiring_volume": 120,
                    "salary_median": 156000,
                    "salary_premium": 1.15,  # 15% above market
                    "salary_range": {
                        "p25": 140000,
                        "median": 156000,
                        "p75": 180000
                    },
                    "open_positions": [
                        {"title": "Software Engineer", "count": 45},
                        {"title": "Product Manager", "count": 12}
                    ],
                    "preferred_channels": [
                        "LinkedIn",
                        "Salesforce Careers"
                    ],
                    "employer_rating": 4.2,
                    "glassdoor_rating": 4.2,
                    "data_source": "Glassdoor + LinkedIn + Crunchbase",
                    "confidence": 0.85  # 0-1 scale based on sample size
                }
            ]
        }

    Implementation:
    1. Competitor identification (already done upstream)
    2. For each competitor:
       - Glassdoor scraping: salary reports by role, city
         * Query: "Salary at {company} for {role} in {city}"
         * Extract: median salary, p25, p75
         * Sample size (e.g., 120 salary reports)

       - LinkedIn scraping: job postings + hiring volume
         * Count open positions by city and role
         * Extract: job titles, salary ranges (if visible)
         * Hiring volume = count of postings in past 30 days

       - Crunchbase (if API available): funding, growth stage
         * Context: pre-IPO companies often pay more to compete

    3. Salary premium calculation:
       - If your_median_salary provided:
         premium = competitor_median / your_median_salary[role]
       - Else:
         premium = competitor_median / market_median_from_bls
       - Example: Comp pays $156K, market pays $135K → premium = 1.15

    4. Risk scoring:
       - If premium > 1.15: "You're at disadvantage, need premium positioning"
       - If premium > 1.25: "Severe disadvantage, consider non-salary benefits"
       - If premium < 0.95: "Competitive advantage, lead with salary"

    5. Channel intelligence:
       - Scrape where competitor posts jobs (LinkedIn, own careers site, etc.)
       - Infer: "Competitor owns LinkedIn, so we should diversify to GitHub, niche boards"

    6. Cache for 14 days (employer brands move slower)

    Error Handling:
    - If Glassdoor scrape blocked: use fallback data
    - If competitor not found: skip that competitor
    - If salary data < 10 samples: mark confidence < 0.7
    """
```

**Response Example**:
```json
{
  "San Francisco, CA": [
    {
      "company": "Salesforce",
      "hiring_volume": 120,
      "salary_median": 156000,
      "salary_premium": 1.15,
      "salary_range": {
        "p25": 140000,
        "median": 156000,
        "p75": 180000
      },
      "open_positions": [
        {
          "title": "Software Engineer",
          "count": 45
        },
        {
          "title": "Product Manager",
          "count": 12
        }
      ],
      "preferred_channels": [
        "LinkedIn",
        "Salesforce Careers"
      ],
      "employer_rating": 4.2,
      "data_source": "Glassdoor + LinkedIn",
      "confidence": 0.85
    }
  ]
}
```

---

## 5. ACTIVATION EVENTS CALENDAR

### 5.1 Data Sources: BLS JOLTS, Conference Calendar, Company Milestones

**Python Function Signature**:
```python
def _fetch_activation_events(
    industry: str,           # e.g., "tech_engineering"
    locations: list[str],    # e.g., ["San Francisco, CA", "New York, NY"]
    roles: list[str],        # e.g., ["Software Engineer", "Product Manager"]
    company_fiscal_year_q1_month: int = 1  # e.g., 1 for calendar year
) -> dict[str, Any]:
    """
    Generate seasonal hiring lift + conference calendar + company milestones.

    Returns:
        {
            "seasonal_hiring_peaks": [
                {
                    "period": "Q1 (Jan-Mar)",
                    "hiring_lift": 1.35,
                    "recommended_spend_increase": 1.25,
                    "reason": "New budget allocation, goal-setting",
                    "channel_focus": ["LinkedIn", "Programmatic DSP"]
                },
                {
                    "period": "Q3 (Jul-Sep)",
                    "hiring_lift": 1.15,
                    "recommended_spend_increase": 1.1,
                    "reason": "Back-to-school hiring, pre-holiday builds",
                    "channel_focus": ["Job Boards", "Social Media"]
                }
            ],
            "industry_conferences": [
                {
                    "event": "AWS re:Invent 2026",
                    "date": "2026-11-30",
                    "cities_impacted": [
                        "Las Vegas",
                        "San Francisco"
                    ],
                    "role_focus": "AWS Engineers, Cloud Architects",
                    "recommended_creative_angle": "cloud_infrastructure",
                    "expected_recruiter_attendance": 250,
                    "recommended_spend_start_date": "2026-10-15",
                    "spend_multiplier": 1.3
                }
            ],
            "company_milestones": [
                {
                    "milestone": "Series C Funding Announcement",
                    "expected_timing": "Q2 2026",
                    "recommended_messaging": "growth_hiring_story",
                    "hiring_lift_expected": 1.4,
                    "channel_focus": ["LinkedIn", "Company Site"]
                }
            ]
        }

    Implementation:
    1. Seasonal hiring peaks (BLS JOLTS data):
       - Query FRED API for monthly hiring by industry
       - Calculate % above/below baseline for each month
       - Identify Q1, Q3 as seasonal peaks (goal-setting season, pre-holiday)
       - Return: period, hiring_lift, recommended_spend_increase

    2. Industry conference calendar:
       - Curated lookup table by industry:
         * tech: AWS re:Invent, KubeCon, PyCon, SXSW
         * healthcare: HIMSS, AABB, ACA Annual Conference
         * finance: Money 20/20, SAS Financial Services
       - For each conference:
         * Extract: date, location, typical attendance
         * Estimate recruiter attendance (% of attendees)
         * Suggest creative angle (e.g., "Cloud Engineering" for AWS re:Invent)
         * Recommend spend start date (4-6 weeks before event)

    3. Company milestones:
       - Pull from enriched data: funding announcements, earnings calendar
       - Infer from context: if Series C expected, hiring lift anticipated
       - Suggest messaging: "Join us on our growth journey"

    4. Channel recommendations:
       - Tie to event: in-person conference → LinkedIn + company site
       - Seasonal peak → diversify across channels

    5. Cache for 90 days (annual calendar, relatively static)

    Error Handling:
    - If no conferences match industry: return empty array
    - If company milestones unknown: skip
    - If JOLTS data unavailable: use hardcoded seasonal factors
    """
```

**Industry Conference Lookup Table**:
```python
INDUSTRY_CONFERENCES = {
    "tech_engineering": [
        {
            "event": "AWS re:Invent",
            "month": 11,
            "typical_location": "Las Vegas",
            "role_focus": "AWS Engineers, Cloud Architects, DevOps",
            "recruiter_attendance_pct": 0.12,
            "creative_angle": "cloud_infrastructure"
        },
        {
            "event": "KubeCon + CloudNativeCon",
            "month": 10,
            "typical_location": "Los Angeles",
            "role_focus": "Kubernetes Engineers, DevOps, Site Reliability",
            "recruiter_attendance_pct": 0.08,
            "creative_angle": "devops_focus"
        },
        {
            "event": "PyCon US",
            "month": 5,
            "typical_location": "Pittsburgh",
            "role_focus": "Python Engineers, Data Scientists",
            "recruiter_attendance_pct": 0.05,
            "creative_angle": "data_engineering"
        }
    ],
    "healthcare": [
        {
            "event": "HIMSS (Health Information and Management Systems Society)",
            "month": 3,
            "typical_location": "Las Vegas",
            "role_focus": "Healthcare IT, Clinical Informaticists, HIM Specialists",
            "recruiter_attendance_pct": 0.15,
            "creative_angle": "digital_health"
        }
    ]
}

SEASONAL_HIRING_PATTERNS = {
    # BLS JOLTS baseline: national hiring by month
    "Q1": {"hiring_lift": 1.35, "spend_multiplier": 1.25},  # New budgets, goal-setting
    "Q2": {"hiring_lift": 1.10, "spend_multiplier": 1.05},  # Mid-year planning
    "Q3": {"hiring_lift": 1.15, "spend_multiplier": 1.1},   # Back-to-school, pre-holiday
    "Q4": {"hiring_lift": 1.05, "spend_multiplier": 1.0}    # Year-end hiring push (varies)
}
```

**Response Example**:
```json
{
  "seasonal_hiring_peaks": [
    {
      "period": "Q1 (Jan-Mar)",
      "hiring_lift": 1.35,
      "recommended_spend_increase": 1.25,
      "reason": "New budget allocation, goal-setting"
    },
    {
      "period": "Q3 (Jul-Sep)",
      "hiring_lift": 1.15,
      "recommended_spend_increase": 1.1,
      "reason": "Back-to-school hiring, pre-holiday builds"
    }
  ],
  "industry_conferences": [
    {
      "event": "AWS re:Invent 2026",
      "date": "2026-11-30",
      "cities_impacted": [
        "Las Vegas",
        "San Francisco"
      ],
      "role_focus": "AWS Engineers, Cloud Architects",
      "recommended_creative_angle": "cloud_infrastructure",
      "expected_recruiter_attendance": 250,
      "recommended_spend_start_date": "2026-10-15",
      "spend_multiplier": 1.3
    }
  ]
}
```

---

## 6. INTEGRATION WITH EXISTING PIPELINE

### 6.1 Enrich Data Flow
```
app.py :: _async_generate()
  ├─ enrich_data(gen_data)  [EXISTING]
  │   ├─ fetch_salary_data()
  │   ├─ fetch_industry_employment()
  │   ├─ fetch_location_demographics()
  │   └─ ... (other 20+ APIs)
  │
  ├─ NEW: _fetch_salary_by_city()
  ├─ NEW: _fetch_hiring_volume_by_tier()
  ├─ NEW: _fetch_security_clearance_data()
  ├─ NEW: _fetch_competitor_salary_premium()
  ├─ NEW: _fetch_activation_events()
  │
  └─ gen_data["_enriched"] = {
       salary_data,
       hiring_by_tier,
       clearance_segmentation,
       competitor_premiums,
       activation_events
     }
```

### 6.2 Synthesis Flow
```
data_synthesizer.py :: synthesize()
  ├─ fuse_salary_intelligence()  [EXISTING]
  ├─ NEW: synthesize_city_level_supply_demand()
  ├─ NEW: synthesize_security_clearance()
  ├─ NEW: synthesize_competitor_map()
  ├─ NEW: synthesize_activation_events()
  │
  └─ synthesized = {
       city_supply_demand,
       clearance_segmentation,
       competitors,
       activation_calendar
     }
```

### 6.3 Output Flow
```
app.py :: _extract_plan_json()
  ├─ Extract from gen_data["_synthesized"]:
  │   ├─ city_supply_demand
  │   ├─ clearance_segmentation
  │   ├─ competitors
  │   └─ activation_calendar
  │
  └─ Return Tier 4 JSON schema with all 10+ keys
```

---

## 7. CACHING STRATEGY

| Data Source | Freshness | Cache Duration | Tier |
|-------------|-----------|-----------------|------|
| City salary bands | Low | 90 days | L1 memory, L2 disk, L3 Supabase |
| Hiring volume by tier | High | 7 days | L1 memory, L2 disk |
| Security clearance | Medium | 30 days | L1 memory, L2 disk |
| Competitor premiums | Medium | 14 days | L1 memory, L2 disk |
| Activation events | Low | 90 days | L1 memory, L2 disk |

---

## 8. ERROR HANDLING & GRACEFUL DEGRADATION

| API | Fallback | Impact |
|-----|----------|--------|
| BLS City Salary | National median from existing BLS OES | City-level detail lost, use nationwide |
| LinkedIn Hiring Volume | Adzuna + keyword heuristics | Less accurate tier classification |
| ClearedJobs API | Keyword detection + industry baseline | Lower confidence in clearance % |
| Glassdoor Salary Scrape | Industry baseline salary premium | Assume premium = 1.0 for all competitors |
| Conference Calendar | Empty (skip activations) | No conference recommendations |

---

**Document Status**: DATA CONTRACTS COMPLETE
**Next Step**: Implement functions in order: salary_by_city → hiring_by_tier → clearance → competitor → activation_events
