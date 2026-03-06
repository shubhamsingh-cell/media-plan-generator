#!/usr/bin/env python3
"""AI Media Planner - Standalone HTTP server with real research data."""

import json
import os
import io
import datetime
import sys
import re
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.chart import BarChart, PieChart, Reference
from openpyxl.chart.label import DataLabelList
from openpyxl.chart.series import DataPoint
from openpyxl.drawing.image import Image as XlImage

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

# Import research module for real data
sys.path.insert(0, BASE_DIR)
import research

# Load global supply data
GLOBAL_SUPPLY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "global_supply.json")
global_supply_data = {}
try:
    with open(GLOBAL_SUPPLY_PATH, "r") as f:
        global_supply_data = json.load(f)
except:
    pass

def load_channels_db():
    with open(os.path.join(DATA_DIR, "channels_db.json"), "r") as f:
        return json.load(f)

def load_joveo_publishers():
    path = os.path.join(DATA_DIR, "joveo_publishers.json")
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {}


def fetch_client_logo(client_name, client_website=""):
    """Try to fetch a client logo using the client website URL (preferred) or name-based domain guess."""
    domain = ""

    # 1. Try to extract domain from the provided client website URL (most accurate)
    if client_website:
        try:
            parsed = urlparse(client_website if "://" in client_website else f"https://{client_website}")
            domain = parsed.hostname or ""
            # Remove www. prefix for cleaner domain
            if domain.startswith("www."):
                domain = domain[4:]
        except Exception:
            pass

    # 2. Fallback: guess domain from client name
    if not domain and client_name:
        name = client_name.lower().strip()
        for suffix in [" inc", " inc.", " llc", " ltd", " corp", " corporation", " co", " company", " group", " international"]:
            name = name.replace(suffix, "")
        domain = re.sub(r'[^a-z0-9]', '', name) + ".com"

    if not domain:
        return None, None

    # Try Clearbit Logo API (high quality, free for logos), then Google favicon
    logo_urls = [
        f"https://logo.clearbit.com/{domain}",
        f"https://www.google.com/s2/favicons?domain={domain}&sz=128",
    ]

    for url in logo_urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            response = urllib.request.urlopen(req, timeout=5)
            if response.status == 200:
                img_data = response.read()
                if len(img_data) > 500:  # Ensure it's not a tiny placeholder
                    return img_data, url
        except Exception:
            continue
    return None, None


def generate_excel(data):
    db = load_channels_db()
    joveo_pubs = load_joveo_publishers()
    gs = global_supply_data  # global supply reference
    # Get global supply data via research module for international locations
    global_research = research.get_global_supply_data(
        data.get("locations", ["United States"]),
        data.get("industry", "general_entry_level"),
    )

    # Industry label mapping
    industry_label_map = {
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
    }

    wb = Workbook()

    # Styles
    header_font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
    header_fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
    subheader_font = Font(name="Calibri", bold=True, size=11, color="1B2A4A")
    subheader_fill = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
    section_font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
    section_fill = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
    body_font = Font(name="Calibri", size=10)
    wrap_alignment = Alignment(wrap_text=True, vertical="top")
    center_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin_border = Border(
        left=Side(style="thin", color="CCCCCC"),
        right=Side(style="thin", color="CCCCCC"),
        top=Side(style="thin", color="CCCCCC"),
        bottom=Side(style="thin", color="CCCCCC"),
    )
    accent_fill = PatternFill(start_color="E8F0FE", end_color="E8F0FE", fill_type="solid")

    def style_body_cell(ws, row, col, val=""):
        cell = ws.cell(row=row, column=col, value=val)
        cell.font = body_font
        cell.alignment = wrap_alignment
        cell.border = thin_border
        return cell

    locations = data.get("locations", ["United States"])
    industry = data.get("industry", "general_entry_level")
    roles = data.get("target_roles", [])
    niche_key = db.get("industries", {}).get(industry, {}).get("niche_channel_key", "")

    # ── Sheet 1: Overview ──
    ws_overview = wb.active
    ws_overview.title = "Overview"
    ws_overview.sheet_properties.tabColor = "1B2A4A"
    ws_overview.column_dimensions["A"].width = 5
    ws_overview.column_dimensions["B"].width = 40
    ws_overview.column_dimensions["C"].width = 60

    ws_overview.merge_cells("B2:C2")
    title_cell = ws_overview["B2"]
    title_cell.value = "AI Media Planner — Overview"
    title_cell.font = Font(name="Calibri", bold=True, size=18, color="1B2A4A")

    # Try to fetch and insert client logo (prefer website URL for accuracy)
    client_name = data.get("client_name", "")
    client_website = data.get("client_website", "")
    logo_data, logo_src = fetch_client_logo(client_name, client_website) if (client_name or client_website) else (None, None)
    if logo_data:
        try:
            logo_stream = io.BytesIO(logo_data)
            logo_img = XlImage(logo_stream)
            logo_img.width = 80
            logo_img.height = 80
            ws_overview.add_image(logo_img, "D2")
            ws_overview.column_dimensions["D"].width = 15
        except Exception:
            pass  # Silently skip if image insertion fails

    job_cat_labels = data.get("job_category_labels", [])
    client_competitors = data.get("competitors", [])

    overview_items = [
        ("Client Name", data.get("client_name", "")),
        ("Client Website", data.get("client_website", "") or "Not specified"),
        ("Client's Use Case", data.get("use_case", "")),
        ("Industry", data.get("industry_label", industry_label_map.get(industry, industry))),
        ("Job Categories", ", ".join(job_cat_labels) if job_cat_labels else "Not specified"),
        ("Target Locations", ", ".join(locations)),
        ("Target Roles", ", ".join(roles)),
        ("Target Demographic", data.get("target_demographic", "")),
        ("Budget Range", data.get("budget_range", "")),
        ("Campaign Duration", data.get("campaign_duration", "")),
        ("Key Competitors", ", ".join(client_competitors) if client_competitors else "Not specified"),
    ]

    row = 4
    for label, value in overview_items:
        ws_overview.cell(row=row, column=2, value=label).font = Font(name="Calibri", bold=True, size=11, color="1B2A4A")
        ws_overview.cell(row=row, column=2).border = thin_border
        ws_overview.cell(row=row, column=3, value=value).font = body_font
        ws_overview.cell(row=row, column=3).alignment = wrap_alignment
        ws_overview.cell(row=row, column=3).border = thin_border
        row += 1

    # Joveo supply network summary
    row += 1
    ws_overview.cell(row=row, column=2, value="Joveo Supply Network").font = Font(name="Calibri", bold=True, size=12, color="2E75B6")
    row += 1
    total_pubs = joveo_pubs.get("total_active_publishers", 1238)
    ws_overview.cell(row=row, column=2, value=f"  Active Supply Partners: {total_pubs:,}+").font = body_font
    row += 1
    ws_overview.cell(row=row, column=2, value=f"  Countries Covered: 200+").font = body_font
    row += 1
    ws_overview.cell(row=row, column=2, value=f"  Regions: Americas, Europe, APAC, LATAM, MEA, Africa").font = body_font

    row += 2
    ws_overview.cell(row=row, column=2, value="Plan Sections").font = Font(name="Calibri", bold=True, size=12, color="2E75B6")
    row += 1
    # Determine if any location is international (not US-only)
    has_international = False
    for loc in locations:
        loc_info = research.get_location_info(loc)
        if loc_info.get("is_international"):
            has_international = True
            break

    sections = ["Market Trends", "Labour Market Intelligence", "Competitor Analysis", "Channel Strategy", "Traditional Channels", "Non-Traditional Channels"]
    if data.get("job_categories"):
        sections.append("Job Category Insights")
    if has_international or data.get("include_global_supply"):
        sections.append("Global Supply Strategy")
    if data.get("include_dei"):
        sections.append("DEI & Diversity Channels")
    if data.get("include_innovative"):
        sections.append("Innovative Channels 2025+")
    if data.get("include_budget_guide"):
        sections.append("Budget & Pricing Guide")
    if data.get("include_educational"):
        sections.append("Educational Partners")
    if data.get("include_events"):
        sections.append("Events & Career Fairs")
    if data.get("include_media_platforms"):
        sections.append("Media/Print Platforms")
    if data.get("include_radio_podcasts"):
        sections.append("Radio/Podcasts")

    for section in sections:
        ws_overview.cell(row=row, column=2, value=f"  {section}").font = body_font
        row += 1

    ws_overview.cell(row=row + 1, column=2, value=f"Generated on: {datetime.datetime.now().strftime('%B %d, %Y')}").font = Font(name="Calibri", italic=True, size=9, color="888888")
    ws_overview.cell(row=row + 2, column=2, value="Powered by Joveo — Programmatic Job Advertising at Scale").font = Font(name="Calibri", italic=True, size=9, color="2E75B6")

    # ── Executive Summary (inserted as FIRST sheet) ──
    ws_exec = wb.create_sheet("Executive Summary")
    ws_exec.sheet_properties.tabColor = "1B2A4A"
    ws_exec.column_dimensions["A"].width = 3
    ws_exec.column_dimensions["B"].width = 30
    ws_exec.column_dimensions["C"].width = 30
    ws_exec.column_dimensions["D"].width = 30
    ws_exec.column_dimensions["E"].width = 30

    navy_fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
    metric_fill = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
    blue_accent_border = Border(
        left=Side(style="medium", color="2E75B6"),
        right=Side(style="thin", color="CCCCCC"),
        top=Side(style="thin", color="CCCCCC"),
        bottom=Side(style="thin", color="CCCCCC"),
    )

    client_name_val = data.get("client_name", "CLIENT")
    industry_label_val = data.get("industry_label", industry_label_map.get(industry, industry))

    # Large merged header
    ws_exec.merge_cells("B2:E2")
    title_cell_exec = ws_exec["B2"]
    title_cell_exec.value = f"AI MEDIA PLANNER \u2014 {client_name_val.upper()}"
    title_cell_exec.font = Font(name="Calibri", bold=True, size=22, color="FFFFFF")
    title_cell_exec.fill = navy_fill
    title_cell_exec.alignment = Alignment(horizontal="center", vertical="center")
    for c in range(3, 6):
        ws_exec.cell(row=2, column=c).fill = navy_fill
    ws_exec.row_dimensions[2].height = 50

    # Subtitle
    ws_exec.merge_cells("B3:E3")
    ws_exec["B3"].value = f"{industry_label_val}  |  Generated {datetime.datetime.now().strftime('%B %d, %Y')}"
    ws_exec["B3"].font = Font(name="Calibri", italic=True, size=11, color="FFFFFF")
    ws_exec["B3"].fill = navy_fill
    ws_exec["B3"].alignment = Alignment(horizontal="center", vertical="center")
    for c in range(3, 6):
        ws_exec.cell(row=3, column=c).fill = navy_fill

    # Insert logo on executive summary if available
    if logo_data:
        try:
            logo_stream2 = io.BytesIO(logo_data)
            logo_img2 = XlImage(logo_stream2)
            logo_img2.width = 60
            logo_img2.height = 60
            ws_exec.add_image(logo_img2, "F2")
            ws_exec.column_dimensions["F"].width = 12
        except Exception:
            pass

    # Campaign Snapshot section
    exec_row = 5
    ws_exec.merge_cells(f"B{exec_row}:E{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Campaign Snapshot").font = Font(name="Calibri", bold=True, size=14, color="1B2A4A")
    exec_row += 1

    # 2x3 metric grid
    budget_range_val = data.get("budget_range", "Not specified")
    campaign_duration_val = data.get("campaign_duration", "Not specified")
    loc_count = len(locations)
    role_count = len(roles)
    hire_volume_val = data.get("hire_volume", "Not specified")

    metric_grid = [
        ("Budget Range", budget_range_val, "Campaign Duration", campaign_duration_val),
        ("Target Locations", f"{loc_count} location(s)", "Target Roles", f"{role_count} role(s)"),
        ("Industry", industry_label_val, "Hire Volume", str(hire_volume_val)),
    ]
    for label1, val1, label2, val2 in metric_grid:
        # Left metric
        cell_l = ws_exec.cell(row=exec_row, column=2, value=label1)
        cell_l.font = Font(name="Calibri", bold=True, size=9, color="666666")
        cell_l.fill = metric_fill
        cell_l.border = thin_border
        cell_v = ws_exec.cell(row=exec_row, column=3, value=val1)
        cell_v.font = Font(name="Calibri", bold=True, size=12, color="1B2A4A")
        cell_v.fill = metric_fill
        cell_v.border = thin_border
        # Right metric
        cell_r = ws_exec.cell(row=exec_row, column=4, value=label2)
        cell_r.font = Font(name="Calibri", bold=True, size=9, color="666666")
        cell_r.fill = metric_fill
        cell_r.border = thin_border
        cell_rv = ws_exec.cell(row=exec_row, column=5, value=val2)
        cell_rv.font = Font(name="Calibri", bold=True, size=12, color="1B2A4A")
        cell_rv.fill = metric_fill
        cell_rv.border = thin_border
        exec_row += 1

    # Plan at a Glance
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:E{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Plan at a Glance").font = Font(name="Calibri", bold=True, size=14, color="1B2A4A")
    exec_row += 1
    all_sheet_names = ["Overview", "Market Trends", "Labour Market Intelligence", "Channel Strategy", "Traditional Channels", "Non-Traditional Channels"]
    if data.get("job_categories"):
        all_sheet_names.append("Job Category Insights")
    if has_international or data.get("include_global_supply"):
        all_sheet_names.append("Global Supply Strategy")
    if data.get("include_dei"):
        all_sheet_names.append("DEI & Diversity Channels")
    if data.get("include_innovative"):
        all_sheet_names.append("Innovative Channels 2025+")
    if data.get("include_budget_guide"):
        all_sheet_names.append("Budget & Pricing Guide")
    if data.get("include_educational"):
        all_sheet_names.append("Educational Partners")
    if data.get("include_events"):
        all_sheet_names.append("Events & Career Fairs")
    if data.get("include_media_platforms"):
        all_sheet_names.append("Media & Print Platforms")
    if data.get("include_radio_podcasts"):
        all_sheet_names.append("Radio & Podcasts")
    all_sheet_names.append("Campaign Timeline")
    for sn in all_sheet_names:
        ws_exec.cell(row=exec_row, column=2, value=f"  \u2022  {sn}").font = Font(name="Calibri", size=10, color="333333")
        exec_row += 1

    # Channel Mix Summary
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:E{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Channel Mix Summary").font = Font(name="Calibri", bold=True, size=14, color="1B2A4A")
    exec_row += 1
    regional_count = len(data.get("selected_regional", db["traditional_channels"]["regional_local"][:25]))
    niche_count = len(data.get("selected_niche", db["traditional_channels"]["niche_by_industry"].get(niche_key, [])[:25]))
    global_count = len(data.get("selected_global", db["traditional_channels"]["global_reach"][:25]))
    ws_exec.cell(row=exec_row, column=2, value=f"{regional_count} Regional  +  {niche_count} Niche  +  {global_count} Global channels").font = Font(name="Calibri", size=11, color="1B2A4A")
    exec_row += 2

    # Labour Market Summary in Executive Summary
    lm_exec = research.get_labour_market_intelligence(industry, locations)
    lm_ind = lm_exec.get("industry_metrics", {})
    ws_exec.merge_cells(f"B{exec_row}:E{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Labour Market Snapshot").font = Font(name="Calibri", bold=True, size=14, color="1B2A4A")
    exec_row += 1
    lm_summary_items = [
        f"Sector: {lm_ind.get('sector_name', '')}",
        f"Employment Growth: {lm_ind.get('projected_growth_2024_2034', '')}",
        f"Talent Shortage: {lm_ind.get('talent_shortage_severity', '')}",
        f"JOLTS Openings Rate: {lm_ind.get('job_openings_rate_jolts', '')}",
        f"Avg Time to Fill: {lm_ind.get('vacancy_fill_time_avg', '')}",
        f"Wage Growth: {lm_ind.get('wage_growth_yoy', '')}",
    ]
    for item in lm_summary_items:
        ws_exec.cell(row=exec_row, column=2, value=f"  {item}").font = Font(name="Calibri", size=10, color="333333")
        ws_exec.merge_cells(f"B{exec_row}:E{exec_row}")
        exec_row += 1
    exec_row += 1

    # Competitive Landscape in Executive Summary
    if client_competitors:
        ws_exec.merge_cells(f"B{exec_row}:E{exec_row}")
        ws_exec.cell(row=exec_row, column=2, value="Competitive Landscape").font = Font(name="Calibri", bold=True, size=14, color="1B2A4A")
        exec_row += 1
        ws_exec.cell(row=exec_row, column=2, value=f"Key Competitors: {', '.join(client_competitors)}").font = Font(name="Calibri", size=11, color="333333")
        exec_row += 1
        ws_exec.cell(row=exec_row, column=2, value="Detailed per-competitor intelligence (hiring channels, employer brand, strategies, and recommendations) included in the Market Trends sheet.").font = Font(name="Calibri", italic=True, size=10, color="596780")
        ws_exec.merge_cells(f"B{exec_row}:E{exec_row}")
        exec_row += 2

    # Key Recommendations
    ws_exec.merge_cells(f"B{exec_row}:E{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Key Recommendations").font = Font(name="Calibri", bold=True, size=14, color="1B2A4A")
    exec_row += 1

    recommendations = []
    if has_international:
        recommendations.append("International recruitment strategy recommended with local job boards")
    budget_str = data.get("budget_range", "")
    # Check if budget exceeds $500K
    if any(x in budget_str.lower() for x in ["500k", "500,000", "1m", "1,000,000", "million"]):
        recommendations.append("Multi-channel programmatic approach with performance tracking")
    # Industry-specific recommendation
    industry_rec_map = {
        "healthcare_medical": "Healthcare: Focus on medical-specific job boards and professional associations",
        "tech_engineering": "Technology: Leverage developer communities and tech-focused platforms",
        "blue_collar_trades": "Trades: Prioritize local community boards and vocational training partnerships",
        "finance_banking": "Finance: Target professional networks and financial industry publications",
        "maritime_marine": "Maritime: Use specialized maritime job boards and port-area media",
        "legal_services": "Legal: Focus on bar association boards and legal publications",
        "retail_consumer": "Retail: Leverage high-volume job boards and social media advertising",
        "aerospace_defense": "Aerospace & Defense: Target cleared-talent networks and defense industry boards",
        "pharma_biotech": "Pharma: Use scientific publications and specialized biotech job platforms",
        "mental_health": "Mental Health: Focus on counseling association boards and healthcare networks",
    }
    if industry in industry_rec_map:
        recommendations.append(industry_rec_map[industry])
    if data.get("include_dei"):
        recommendations.append("DEI-focused channels included to ensure inclusive hiring")
    # Job category specific recommendations
    jc_keys = data.get("job_categories", [])
    jc_db = db.get("job_categories", {})
    if jc_keys:
        for jck in jc_keys[:2]:
            jc_data = jc_db.get(jck, {})
            if jc_data:
                bp = jc_data.get("best_practices", [])
                if bp:
                    recommendations.append(f"{jc_data.get('label', jck)}: {bp[0]}")
    # Always have at least 3 recommendations
    if len(recommendations) < 3:
        recommendations.append("Programmatic job advertising recommended for optimized cost-per-applicant")
    if len(recommendations) < 3:
        recommendations.append("Employer branding investment will improve long-term talent pipeline")

    for rec in recommendations:
        cell_rec = ws_exec.cell(row=exec_row, column=2, value=f"  {rec}")
        cell_rec.font = Font(name="Calibri", size=10, color="333333")
        cell_rec.border = blue_accent_border
        ws_exec.merge_cells(f"B{exec_row}:E{exec_row}")
        exec_row += 1

    # ── Joveo CPA/CPC Benchmark Recommendations ──
    exec_row += 2
    ws_exec.merge_cells(f"B{exec_row}:E{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Joveo CPA/CPC Benchmarks by Job Category & Region").font = Font(name="Calibri", bold=True, size=14, color="1B2A4A")
    ws_exec.cell(row=exec_row, column=2).border = Border(bottom=Side(style="medium", color="2E75B6"))
    exec_row += 1
    ws_exec.cell(row=exec_row, column=2, value="Based on Joveo platform data and industry benchmarks from programmatic recruitment advertising reports.").font = Font(name="Calibri", italic=True, size=10, color="596780")
    exec_row += 2

    # CPA/CPC benchmark data by job category and region
    cpa_cpc_benchmarks = {
        "healthcare_medical": {
            "label": "Healthcare & Medical",
            "benchmarks": {
                "North America": {"cpa": "$25 - $65", "cpc": "$0.80 - $2.50", "notes": "High demand; travel nursing CPA can reach $85+"},
                "Europe (UK/DE/FR)": {"cpa": "$20 - $55", "cpc": "$0.60 - $2.00", "notes": "NHS roles lower CPA; specialist roles premium"},
                "APAC (IN/AU/SG)": {"cpa": "$8 - $35", "cpc": "$0.25 - $1.20", "notes": "India lowest; Australia/Singapore premium market"},
                "LATAM": {"cpa": "$6 - $25", "cpc": "$0.15 - $0.80", "notes": "Growing market; lower competition"},
            }
        },
        "blue_collar_trades": {
            "label": "Blue Collar / Skilled Trades",
            "benchmarks": {
                "North America": {"cpa": "$12 - $35", "cpc": "$0.40 - $1.50", "notes": "High volume; warehouse/logistics most competitive"},
                "Europe (UK/DE/FR)": {"cpa": "$10 - $30", "cpc": "$0.35 - $1.20", "notes": "Trades apprenticeships lower CPA"},
                "APAC (IN/AU/SG)": {"cpa": "$4 - $18", "cpc": "$0.10 - $0.65", "notes": "India high volume, very low CPA"},
                "LATAM": {"cpa": "$3 - $15", "cpc": "$0.08 - $0.50", "notes": "Manufacturing hubs competitive"},
            }
        },
        "tech_engineering": {
            "label": "Technology & Engineering",
            "benchmarks": {
                "North America": {"cpa": "$35 - $95", "cpc": "$1.20 - $4.50", "notes": "Most competitive; senior roles $100+ CPA"},
                "Europe (UK/DE/FR)": {"cpa": "$28 - $75", "cpc": "$0.90 - $3.50", "notes": "Berlin/London hotspots; remote roles lower CPA"},
                "APAC (IN/AU/SG)": {"cpa": "$10 - $45", "cpc": "$0.30 - $1.80", "notes": "India tech hubs competitive; Singapore premium"},
                "LATAM": {"cpa": "$8 - $35", "cpc": "$0.25 - $1.40", "notes": "Nearshore tech hubs growing rapidly"},
            }
        },
        "general_entry_level": {
            "label": "General / Entry-Level",
            "benchmarks": {
                "North America": {"cpa": "$10 - $28", "cpc": "$0.30 - $1.20", "notes": "Highest volume; seasonal spikes in Q4"},
                "Europe (UK/DE/FR)": {"cpa": "$8 - $25", "cpc": "$0.25 - $1.00", "notes": "Retail/hospitality most common"},
                "APAC (IN/AU/SG)": {"cpa": "$3 - $15", "cpc": "$0.08 - $0.50", "notes": "Massive volume in India; quality varies"},
                "LATAM": {"cpa": "$2 - $12", "cpc": "$0.05 - $0.40", "notes": "Lowest CPAs globally; scaling opportunity"},
            }
        },
        "finance_banking": {
            "label": "Finance & Banking",
            "benchmarks": {
                "North America": {"cpa": "$30 - $80", "cpc": "$1.00 - $3.80", "notes": "Compliance roles premium; fintech competitive"},
                "Europe (UK/DE/FR)": {"cpa": "$25 - $65", "cpc": "$0.80 - $3.00", "notes": "London financial district highest CPA"},
                "APAC (IN/AU/SG)": {"cpa": "$12 - $40", "cpc": "$0.35 - $1.60", "notes": "Singapore/HK premium; India BPO lower"},
                "LATAM": {"cpa": "$8 - $30", "cpc": "$0.20 - $1.00", "notes": "Banking sector growing in Brazil/Mexico"},
            }
        },
        "retail_consumer": {
            "label": "Retail & Consumer",
            "benchmarks": {
                "North America": {"cpa": "$8 - $22", "cpc": "$0.25 - $1.00", "notes": "Seasonal hiring drives volume; Q4 peak"},
                "Europe (UK/DE/FR)": {"cpa": "$7 - $20", "cpc": "$0.20 - $0.85", "notes": "High street retail competitive in UK"},
                "APAC (IN/AU/SG)": {"cpa": "$3 - $12", "cpc": "$0.08 - $0.40", "notes": "E-commerce driving demand in India"},
                "LATAM": {"cpa": "$2 - $10", "cpc": "$0.05 - $0.35", "notes": "Retail expansion across region"},
            }
        },
        "pharma_biotech": {
            "label": "Pharma & Biotech",
            "benchmarks": {
                "North America": {"cpa": "$40 - $110", "cpc": "$1.50 - $5.00", "notes": "Highly specialized; clinical roles most expensive"},
                "Europe (UK/DE/FR)": {"cpa": "$35 - $90", "cpc": "$1.20 - $4.00", "notes": "Basel/Cambridge clusters premium"},
                "APAC (IN/AU/SG)": {"cpa": "$15 - $50", "cpc": "$0.45 - $2.00", "notes": "India pharma hub; R&D roles growing"},
                "LATAM": {"cpa": "$10 - $40", "cpc": "$0.30 - $1.50", "notes": "Clinical trials driving demand in Brazil"},
            }
        },
    }

    # Show benchmarks relevant to the client's industry
    client_industry = data.get("industry", "general_entry_level")
    relevant_benchmarks = {}

    # Always show the client's industry first
    if client_industry in cpa_cpc_benchmarks:
        relevant_benchmarks[client_industry] = cpa_cpc_benchmarks[client_industry]

    # Add general entry-level if not already the client's industry
    if client_industry != "general_entry_level" and "general_entry_level" in cpa_cpc_benchmarks:
        relevant_benchmarks["general_entry_level"] = cpa_cpc_benchmarks["general_entry_level"]

    # If industry not in our benchmark data, show general
    if not relevant_benchmarks:
        relevant_benchmarks["general_entry_level"] = cpa_cpc_benchmarks["general_entry_level"]

    # Determine relevant regions based on client locations
    client_locations = data.get("locations", ["United States"])
    relevant_regions = set()
    for loc in client_locations:
        loc_lower = loc.lower()
        if any(x in loc_lower for x in ["united states", "us", "america", "canada", "new york", "california", "texas", "florida", "chicago", "boston", "seattle"]):
            relevant_regions.add("North America")
        elif any(x in loc_lower for x in ["uk", "united kingdom", "germany", "france", "europe", "london", "berlin", "paris", "netherlands", "spain", "italy"]):
            relevant_regions.add("Europe (UK/DE/FR)")
        elif any(x in loc_lower for x in ["india", "australia", "singapore", "japan", "china", "asia", "apac", "hong kong", "korea"]):
            relevant_regions.add("APAC (IN/AU/SG)")
        elif any(x in loc_lower for x in ["brazil", "mexico", "latin", "latam", "colombia", "argentina", "chile"]):
            relevant_regions.add("LATAM")
        else:
            relevant_regions.add("North America")  # Default
    if not relevant_regions:
        relevant_regions.add("North America")

    for ind_key, ind_data in relevant_benchmarks.items():
        ws_exec.cell(row=exec_row, column=2, value=ind_data["label"]).font = Font(name="Calibri", bold=True, size=12, color="2E75B6")
        exec_row += 1

        # Table headers
        bench_headers = ["Region", "Avg CPA Range", "Avg CPC Range", "Market Notes"]
        for i, h in enumerate(bench_headers):
            cell = ws_exec.cell(row=exec_row, column=2 + i, value=h)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
            cell.border = thin_border
            cell.alignment = center_alignment
        exec_row += 1

        for region_name, region_data in ind_data["benchmarks"].items():
            # Highlight relevant regions
            is_relevant = region_name in relevant_regions
            row_fill = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid") if is_relevant else None

            c1 = ws_exec.cell(row=exec_row, column=2, value=("★ " if is_relevant else "") + region_name)
            c1.font = Font(name="Calibri", bold=is_relevant, size=10, color="1B2A4A" if is_relevant else "333333")
            c1.border = thin_border
            if row_fill:
                c1.fill = row_fill

            c2 = ws_exec.cell(row=exec_row, column=3, value=region_data["cpa"])
            c2.font = Font(name="Calibri", bold=is_relevant, size=10)
            c2.border = thin_border
            c2.alignment = center_alignment
            if row_fill:
                c2.fill = row_fill

            c3 = ws_exec.cell(row=exec_row, column=4, value=region_data["cpc"])
            c3.font = Font(name="Calibri", bold=is_relevant, size=10)
            c3.border = thin_border
            c3.alignment = center_alignment
            if row_fill:
                c3.fill = row_fill

            c4 = ws_exec.cell(row=exec_row, column=5, value=region_data["notes"])
            c4.font = Font(name="Calibri", italic=True, size=9, color="596780")
            c4.border = thin_border
            c4.alignment = wrap_alignment
            if row_fill:
                c4.fill = row_fill

            exec_row += 1

        exec_row += 1

    # Source attribution
    ws_exec.cell(row=exec_row, column=2, value="Sources: Joveo Platform Data, industry recruitment media benchmark reports, CPA/CPC aggregated from programmatic recruitment campaigns across 1,200+ publishers.").font = Font(name="Calibri", italic=True, size=8, color="999999")
    exec_row += 1
    ws_exec.cell(row=exec_row, column=2, value="★ = Regions matching your target locations. Actual rates may vary based on job specificity, seasonality, and competition.").font = Font(name="Calibri", italic=True, size=8, color="999999")

    # Move Executive Summary to first position
    wb.move_sheet("Executive Summary", offset=-(len(wb.sheetnames) - 1))

    # ── Sheet 2: Market Trends ──
    ws_trends = wb.create_sheet("Market Trends")
    ws_trends.sheet_properties.tabColor = "2E75B6"
    ws_trends.column_dimensions["A"].width = 5
    ws_trends.column_dimensions["B"].width = 35
    for i, loc in enumerate(locations):
        ws_trends.column_dimensions[get_column_letter(3 + i)].width = 55

    ws_trends.merge_cells(start_row=2, start_column=2, end_row=2, end_column=2 + len(locations))
    ws_trends["B2"].value = "Market Trends & Labor Market Analysis"
    ws_trends["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")

    row = 4
    headers = ["Market Trends Factor"] + locations
    for i, h in enumerate(headers):
        cell = ws_trends.cell(row=row, column=2 + i, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center_alignment
        cell.border = thin_border

    # USE RESEARCH MODULE for real market trends
    market_trends = research.get_market_trends(locations, industry, roles)

    for trend in market_trends:
        row += 1
        style_body_cell(ws_trends, row, 2, trend.get("factor", ""))
        ws_trends.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
        for i, loc in enumerate(locations):
            desc = trend.get("descriptions", {}).get(loc, "")
            style_body_cell(ws_trends, row, 3 + i, desc)
        # Alternate row shading
        if (row % 2) == 0:
            for c in range(2, 3 + len(locations)):
                ws_trends.cell(row=row, column=c).fill = accent_fill

    # Competitor section
    row += 3
    ws_trends.merge_cells(start_row=row, start_column=2, end_row=row, end_column=2 + len(locations))
    ws_trends.cell(row=row, column=2, value="Competitor Analysis").font = Font(name="Calibri", bold=True, size=14, color="1B2A4A")

    row += 2
    comp_headers = ["Competitor Category", "Key Competitors", "Hiring Focus & Threat Level"]
    for i, h in enumerate(comp_headers):
        cell = ws_trends.cell(row=row, column=2 + i, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    # Widen columns for competitor detail
    ws_trends.column_dimensions[get_column_letter(3)].width = 70
    ws_trends.column_dimensions[get_column_letter(4)].width = 70

    # USE RESEARCH MODULE for real competitor data
    competitors = research.get_competitors(industry, locations)

    for comp in competitors:
        row += 1
        style_body_cell(ws_trends, row, 2, comp.get("category", ""))
        ws_trends.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
        style_body_cell(ws_trends, row, 3, comp.get("competitors", ""))
        style_body_cell(ws_trends, row, 4, comp.get("threat", ""))
        # Row height for long text
        ws_trends.row_dimensions[row].height = 80

    # Client-specified competitors section — per-competitor differentiated intelligence
    if client_competitors:
        comp_intel = research.get_client_competitor_intelligence(client_competitors, industry)

        row += 3
        ws_trends.merge_cells(start_row=row, start_column=2, end_row=row, end_column=5)
        ws_trends.cell(row=row, column=2, value="Client-Identified Competitor Intelligence").font = Font(name="Calibri", bold=True, size=14, color="1B2A4A")
        row += 1
        ws_trends.cell(row=row, column=2, value="Detailed competitive intelligence for each client-specified competitor, including hiring channels, employer brand analysis, and strategic recommendations.").font = Font(name="Calibri", italic=True, size=10, color="596780")
        ws_trends.merge_cells(start_row=row, start_column=2, end_row=row, end_column=5)
        row += 2

        ci_headers = ["Competitor", "Hiring Channels & Brand", "Recruitment Strategies", "Strategic Recommendation"]
        for i, h in enumerate(ci_headers):
            cell = ws_trends.cell(row=row, column=2 + i, value=h)
            cell.font = header_font
            cell.fill = PatternFill(start_color="CE9047", end_color="CE9047", fill_type="solid")
            cell.alignment = center_alignment
            cell.border = thin_border
        ws_trends.column_dimensions[get_column_letter(5)].width = 60

        for ci in comp_intel:
            row += 1
            # Competitor name + size
            comp_label = ci["competitor"]
            if ci.get("company_size") and "Research" not in ci["company_size"]:
                comp_label += f"\n({ci['company_size']})"
            style_body_cell(ws_trends, row, 2, comp_label)
            ws_trends.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)

            # Channels + brand + Glassdoor
            channels_brand = f"Channels: {ci['primary_hiring_channels']}\n\nBrand: {ci['employer_brand_strength']}\n\nGlassdoor: {ci['glassdoor_rating']}"
            if ci.get("talent_focus"):
                channels_brand += f"\n\nTalent Focus: {ci['talent_focus']}"
            style_body_cell(ws_trends, row, 3, channels_brand)

            # Recruitment strategies
            style_body_cell(ws_trends, row, 4, ci.get("known_recruitment_strategies", ""))

            # Strategic recommendation
            style_body_cell(ws_trends, row, 5, ci.get("strategic_recommendation", ""))

            ws_trends.row_dimensions[row].height = 120

    # ── Sheet: Labour Market Intelligence ──
    labour_data = research.get_labour_market_intelligence(industry, locations)
    ws_labour = wb.create_sheet("Labour Market Intel")
    ws_labour.sheet_properties.tabColor = "438765"
    ws_labour.column_dimensions["A"].width = 5
    ws_labour.column_dimensions["B"].width = 40
    ws_labour.column_dimensions["C"].width = 60
    ws_labour.column_dimensions["D"].width = 60

    ws_labour.merge_cells("B2:D2")
    ws_labour["B2"].value = "Labour Market Intelligence"
    ws_labour["B2"].font = Font(name="Calibri", bold=True, size=18, color="1B2A4A")

    lm_row = 3
    ws_labour.cell(row=lm_row, column=2, value="BLS/JOLTS-style curated data for the selected industry and target locations. Use this intelligence to inform recruitment strategy, budget allocation, and candidate messaging.").font = Font(name="Calibri", italic=True, size=10, color="596780")
    ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")

    # ── Section 1: National Economic Summary ──
    lm_row += 2
    ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
    ws_labour.cell(row=lm_row, column=2, value="National Economic Snapshot (US)").font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
    ws_labour.cell(row=lm_row, column=2).fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
    lm_row += 1

    national = labour_data.get("national_summary", {})
    national_items = [
        ("Total Nonfarm Job Openings", national.get("total_nonfarm_openings", "")),
        ("National Unemployment Rate", national.get("national_unemployment_rate", "")),
        ("Labour Force Participation Rate", national.get("labour_force_participation", "")),
        ("U-6 Underemployment Rate", national.get("u6_underemployment", "")),
        ("National Quits Rate (JOLTS)", national.get("national_quits_rate", "")),
        ("National Openings Rate (JOLTS)", national.get("national_openings_rate", "")),
        ("Avg Hourly Earnings (All Workers)", national.get("avg_hourly_earnings_all", "")),
        ("Wage Growth YoY", national.get("avg_hourly_earnings_yoy_change", "")),
        ("Jobs-to-Unemployed Ratio", national.get("jobs_to_unemployed_ratio", "")),
    ]
    for label, value in national_items:
        ws_labour.cell(row=lm_row, column=2, value=label).font = Font(name="Calibri", bold=True, size=10, color="1B2A4A")
        ws_labour.cell(row=lm_row, column=2).border = thin_border
        ws_labour.cell(row=lm_row, column=3, value=value).font = body_font
        ws_labour.cell(row=lm_row, column=3).border = thin_border
        lm_row += 1

    # ── Section 2: Industry-Specific Metrics ──
    lm_row += 1
    ind_metrics = labour_data.get("industry_metrics", {})
    sector_name = ind_metrics.get("sector_name", industry_label_map.get(industry, industry))
    ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
    ws_labour.cell(row=lm_row, column=2, value=f"Industry Focus: {sector_name}").font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
    ws_labour.cell(row=lm_row, column=2).fill = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
    lm_row += 1

    ind_items = [
        ("BLS Sector Code", ind_metrics.get("bls_sector_code", "")),
        ("Total US Employment", ind_metrics.get("total_employment_us", "")),
        ("Projected Growth (2024-2034)", ind_metrics.get("projected_growth_2024_2034", "")),
        ("Annual Job Openings", ind_metrics.get("annual_openings", "")),
        ("Median Annual Wage", ind_metrics.get("median_annual_wage", "")),
        ("JOLTS Openings Rate", ind_metrics.get("job_openings_rate_jolts", "")),
        ("JOLTS Quits Rate", ind_metrics.get("quits_rate_jolts", "")),
        ("JOLTS Hires Rate", ind_metrics.get("hires_rate_jolts", "")),
        ("JOLTS Layoffs Rate", ind_metrics.get("layoffs_rate_jolts", "")),
        ("Avg Time to Fill Vacancy", ind_metrics.get("vacancy_fill_time_avg", "")),
        ("Talent Shortage Severity", ind_metrics.get("talent_shortage_severity", "")),
        ("Wage Growth YoY", ind_metrics.get("wage_growth_yoy", "")),
        ("Unionization Rate", ind_metrics.get("unionization_rate", "")),
        ("Remote Work %", ind_metrics.get("remote_work_pct", "")),
    ]
    for label, value in ind_items:
        ws_labour.cell(row=lm_row, column=2, value=label).font = Font(name="Calibri", bold=True, size=10, color="1B2A4A")
        ws_labour.cell(row=lm_row, column=2).border = thin_border
        c = ws_labour.cell(row=lm_row, column=3, value=value)
        c.font = body_font
        c.border = thin_border
        c.alignment = wrap_alignment
        # Highlight severity
        if "CRITICAL" in str(value).upper():
            c.font = Font(name="Calibri", bold=True, size=10, color="CC0000")
        elif "HIGH" in str(value).upper() and "shortage" in label.lower():
            c.font = Font(name="Calibri", bold=True, size=10, color="CE9047")
        lm_row += 1

    # ── Section 3: Key Industry Trends ──
    lm_row += 1
    ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
    ws_labour.cell(row=lm_row, column=2, value="Key Industry Trends & Hiring Implications").font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
    ws_labour.cell(row=lm_row, column=2).fill = PatternFill(start_color="438765", end_color="438765", fill_type="solid")
    lm_row += 1

    for i, trend in enumerate(ind_metrics.get("key_trends", []), 1):
        ws_labour.cell(row=lm_row, column=2, value=f"Trend {i}").font = Font(name="Calibri", bold=True, size=10, color="438765")
        ws_labour.cell(row=lm_row, column=2).border = thin_border
        c = ws_labour.cell(row=lm_row, column=3, value=trend)
        c.font = body_font
        c.alignment = wrap_alignment
        c.border = thin_border
        ws_labour.merge_cells(f"C{lm_row}:D{lm_row}")
        ws_labour.row_dimensions[lm_row].height = 35
        lm_row += 1

    # ── Section 4: Location-Specific Context ──
    loc_contexts = labour_data.get("location_contexts", [])
    if loc_contexts:
        lm_row += 1
        ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
        ws_labour.cell(row=lm_row, column=2, value="Location-Specific Labour Market Context").font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
        ws_labour.cell(row=lm_row, column=2).fill = PatternFill(start_color="CE9047", end_color="CE9047", fill_type="solid")
        lm_row += 1

        loc_headers = ["Location", "Unemployment", "Median Salary", "Population", "COLI", "Notes"]
        for i, h in enumerate(loc_headers):
            cell = ws_labour.cell(row=lm_row, column=2 + i, value=h)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color="CE9047", end_color="CE9047", fill_type="solid")
            cell.alignment = center_alignment
            cell.border = thin_border
        ws_labour.column_dimensions["E"].width = 15
        ws_labour.column_dimensions["F"].width = 15
        ws_labour.column_dimensions["G"].width = 60
        lm_row += 1

        for lctx in loc_contexts:
            style_body_cell(ws_labour, lm_row, 2, lctx.get("location", ""))
            ws_labour.cell(row=lm_row, column=2).font = Font(name="Calibri", bold=True, size=10)
            style_body_cell(ws_labour, lm_row, 3, lctx.get("unemployment_rate", ""))
            style_body_cell(ws_labour, lm_row, 4, lctx.get("median_salary", ""))
            style_body_cell(ws_labour, lm_row, 5, lctx.get("population", ""))
            style_body_cell(ws_labour, lm_row, 6, str(lctx.get("coli", "")))
            style_body_cell(ws_labour, lm_row, 7, lctx.get("context_note", ""))
            ws_labour.row_dimensions[lm_row].height = 35
            lm_row += 1

    lm_row += 1
    ws_labour.cell(row=lm_row, column=2, value="Data Sources: BLS Occupational Employment & Wage Statistics, JOLTS (Job Openings & Labor Turnover Survey), BLS Employment Projections, industry reports. Curated reference data as of 2024.").font = Font(name="Calibri", italic=True, size=9, color="888888")
    ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")

    # ── Sheet 3: Channel Strategy ──
    ws_strategy = wb.create_sheet("Channel Strategy")
    ws_strategy.sheet_properties.tabColor = "2E75B6"
    ws_strategy.column_dimensions["A"].width = 5
    ws_strategy.column_dimensions["B"].width = 30
    ws_strategy.column_dimensions["C"].width = 50
    ws_strategy.column_dimensions["D"].width = 50
    ws_strategy.column_dimensions["E"].width = 20

    ws_strategy.merge_cells("B2:E2")
    ws_strategy["B2"].value = "Channel Strategy"
    ws_strategy["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")

    row = 4
    strat_headers = ["Channel", "Reasoning", "How to Use", "KPIs / Metrics"]
    for i, h in enumerate(strat_headers):
        cell = ws_strategy.cell(row=row, column=2 + i, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center_alignment
        cell.border = thin_border

    selected_strategies = data.get("channel_strategies", [])
    if not selected_strategies:
        all_strats = db.get("channel_strategies", {})
        selected_strategies = all_strats.get("awareness", []) + all_strats.get("hiring", [])

    for strat in selected_strategies:
        row += 1
        style_body_cell(ws_strategy, row, 2, strat.get("channel", ""))
        ws_strategy.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
        style_body_cell(ws_strategy, row, 3, strat.get("reasoning", ""))
        style_body_cell(ws_strategy, row, 4, strat.get("usage", ""))
        style_body_cell(ws_strategy, row, 5, strat.get("kpis", "Reach, Engagement, CTR, Conversions"))

    # ── Bar Chart: Channel Effectiveness Score ──
    row += 3
    ws_strategy.merge_cells(f"B{row}:E{row}")
    ws_strategy.cell(row=row, column=2, value="Channel Effectiveness Scores").font = Font(name="Calibri", bold=True, size=14, color="1B2A4A")
    row += 1

    bar_data_start = row
    bar_headers_list = ["Channel", "Effectiveness Score (0-100)"]
    for i, h in enumerate(bar_headers_list):
        cell = ws_strategy.cell(row=row, column=2 + i, value=h)
        cell.font = subheader_font
        cell.fill = subheader_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    row += 1

    bar_items = [
        ("Indeed", 85),
        ("LinkedIn", 90),
        ("Google Ads", 75),
        ("Facebook/Meta", 70),
        ("Programmatic DSP", 80),
        ("Niche Job Boards", 88),
        ("Social Media", 65),
        ("Events/Career Fairs", 72),
    ]
    for ch_name, score in bar_items:
        style_body_cell(ws_strategy, row, 2, ch_name)
        style_body_cell(ws_strategy, row, 3, score)
        row += 1

    bar_data_end = row - 1

    # Create BarChart
    bar_chart = BarChart()
    bar_chart.type = "bar"  # horizontal bars
    bar_chart.title = "Channel Effectiveness Score (0-100)"
    bar_chart.width = 18
    bar_chart.height = 10
    bar_chart.style = 10
    bar_chart.y_axis.title = None
    bar_chart.x_axis.title = "Score"

    bar_data_ref = Reference(ws_strategy, min_col=3, min_row=bar_data_start, max_row=bar_data_end)
    bar_cats = Reference(ws_strategy, min_col=2, min_row=bar_data_start + 1, max_row=bar_data_end)
    bar_chart.add_data(bar_data_ref, titles_from_data=True)
    bar_chart.set_categories(bar_cats)
    bar_chart.shape = 4

    # Color the bars with primary blue
    if bar_chart.series:
        bar_chart.series[0].graphicalProperties.solidFill = "2E75B6"

    ws_strategy.add_chart(bar_chart, f"B{row + 1}")

    # ── Sheet 4: Traditional Channels ──
    ws_trad = wb.create_sheet("Traditional Channels")
    ws_trad.sheet_properties.tabColor = "4472C4"
    ws_trad.column_dimensions["A"].width = 5
    ws_trad.column_dimensions["B"].width = 30
    ws_trad.column_dimensions["C"].width = 30
    ws_trad.column_dimensions["D"].width = 30
    ws_trad.column_dimensions["E"].width = 5
    ws_trad.column_dimensions["F"].width = 35

    ws_trad.merge_cells("B2:F2")
    ws_trad["B2"].value = "Traditional Channels"
    ws_trad["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")

    ws_trad["B3"].value = f"Target: {', '.join(locations)} | Joveo Supply Network: {joveo_pubs.get('total_active_publishers', 1238):,}+ active publishers"
    ws_trad["B3"].font = Font(name="Calibri", italic=True, size=10, color="666666")

    row = 5
    ws_trad["B4"].value = "Job Boards (PPC + Slot/Posting + DE&I specific + Niche)"
    ws_trad["B4"].font = Font(name="Calibri", bold=True, size=12, color="2E75B6")

    cat_headers = ["Regional/Local Reach", "*Niche", "Global Reach", "", "Location-Specific"]
    for i, h in enumerate(cat_headers):
        cell = ws_trad.cell(row=row, column=2 + i, value=h)
        cell.font = section_font
        cell.fill = section_fill
        cell.alignment = center_alignment
        cell.border = thin_border

    niche_key = db.get("industries", {}).get(industry, {}).get("niche_channel_key", "")

    regional = data.get("selected_regional", db["traditional_channels"]["regional_local"][:25])
    niche_channels = data.get("selected_niche", db["traditional_channels"]["niche_by_industry"].get(niche_key, [])[:25])
    global_channels = data.get("selected_global", db["traditional_channels"]["global_reach"][:25])

    # USE RESEARCH MODULE for real location-specific boards
    location_boards = research.get_location_boards(locations)

    max_rows = max(len(regional), len(niche_channels), len(global_channels), len(location_boards))
    for i in range(max_rows):
        row += 1
        if i < len(regional):
            style_body_cell(ws_trad, row, 2, regional[i])
        if i < len(niche_channels):
            style_body_cell(ws_trad, row, 3, niche_channels[i])
        if i < len(global_channels):
            style_body_cell(ws_trad, row, 4, global_channels[i])
        if i < len(location_boards):
            style_body_cell(ws_trad, row, 6, location_boards[i])

    # Add Joveo publisher categories summary
    row += 2
    ws_trad.merge_cells(start_row=row, start_column=2, end_row=row, end_column=6)
    ws_trad.cell(row=row, column=2, value="Additional Joveo Supply Partners by Category").font = Font(name="Calibri", bold=True, size=12, color="2E75B6")
    row += 1

    joveo_cats = [
        ("DEI / Diversity Job Boards", "DEI"),
        ("University Job Boards", "University Job Board"),
        ("Government Job Boards", "Govt"),
        ("Healthcare / Niche Health", "Health"),
        ("Technology / Niche Tech", "Tech"),
        ("Community Hiring", "Community Hiring"),
    ]
    for label, cat_key in joveo_cats:
        pubs = joveo_pubs.get("by_category", {}).get(cat_key, [])
        if pubs:
            cell = ws_trad.cell(row=row, column=2, value=label)
            cell.font = Font(name="Calibri", bold=True, size=10, color="2E75B6")
            cell.border = thin_border
            style_body_cell(ws_trad, row, 3, ", ".join(pubs[:15]))
            ws_trad.merge_cells(start_row=row, start_column=3, end_row=row, end_column=6)
            row += 1

    # ── Sheet 5: Non-Traditional Channels ──
    ws_nontrad = wb.create_sheet("Non-Traditional Channels")
    ws_nontrad.sheet_properties.tabColor = "4472C4"
    ws_nontrad.column_dimensions["A"].width = 5
    ws_nontrad.column_dimensions["B"].width = 35
    ws_nontrad.column_dimensions["C"].width = 30

    ws_nontrad.merge_cells("B2:C2")
    ws_nontrad["B2"].value = "Non-Traditional Channels"
    ws_nontrad["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")

    ws_nontrad["B3"].value = f"Target: {', '.join(locations)}"
    ws_nontrad["B3"].font = Font(name="Calibri", italic=True, size=10, color="666666")

    row = 5
    nt = db["non_traditional_channels"]
    categories = [
        ("CPQA (Cost Per Qualified Applicant)", nt.get("cpqa", [])),
        ("Data Partners / Candidate Re-engagement", nt.get("data_partners", [])),
        ("Media Channels", nt.get("media_channels", [])),
        ("DSPs (Demand-Side Platforms)", nt.get("dsps", [])),
        ("Government Job Boards", nt.get("gov_job_boards", [])),
        ("Early-Career Channels", nt.get("early_career_channels", [])),
        ("Employer Branding", nt.get("employer_branding", [])),
    ]

    # Enrich with Joveo supply partner data
    joveo_nt_cats = {
        "Staffing Partners": joveo_pubs.get("by_category", {}).get("Staffing Partner", [])[:10],
        "AI-Powered Sourcing Tools": joveo_pubs.get("by_category", {}).get("AI tool", [])[:10],
        "Influencer Marketing Platforms": joveo_pubs.get("by_category", {}).get("Influencer Marketing", [])[:10],
        "Programmatic Audio Partners": joveo_pubs.get("by_category", {}).get("Programmatic Audio", [])[:10],
        "DSP Partners (Joveo Network)": joveo_pubs.get("by_category", {}).get("DSP", [])[:10],
        "Social Media Advertising": joveo_pubs.get("by_category", {}).get("Social Media", [])[:10],
    }

    for cat_name, channels in categories:
        cell = ws_nontrad.cell(row=row, column=2, value=cat_name)
        cell.font = section_font
        cell.fill = section_fill
        cell.border = thin_border
        ws_nontrad.cell(row=row, column=3).fill = section_fill
        ws_nontrad.cell(row=row, column=3).border = thin_border
        row += 1
        for ch in channels:
            style_body_cell(ws_nontrad, row, 3, ch)
            row += 1
        row += 1

    # Add Joveo-specific non-traditional categories
    for cat_name, pubs in joveo_nt_cats.items():
        if pubs:
            cell = ws_nontrad.cell(row=row, column=2, value=cat_name)
            cell.font = section_font
            cell.fill = section_fill
            cell.border = thin_border
            ws_nontrad.cell(row=row, column=3).fill = section_fill
            ws_nontrad.cell(row=row, column=3).border = thin_border
            row += 1
            for ch in pubs:
                style_body_cell(ws_nontrad, row, 3, ch)
                row += 1
            row += 1

    # Add alternate supply categories from channels_db
    alt_supply = nt.get("alternate_supply", {})
    alt_supply_sections = [
        ("Competitor Supply Channels", nt.get("competitor_supply_channels", [])),
        ("Alternate Staffing Partners", alt_supply.get("staffing_partners", [])),
        ("Alternate DSPs", alt_supply.get("dsps", [])),
        ("Influencer Marketing", alt_supply.get("influencer_marketing", [])),
        ("Programmatic Audio", alt_supply.get("programmatic_audio", [])),
        ("Social Media Advertising", alt_supply.get("social_media_ads", [])),
    ]
    for cat_name, items in alt_supply_sections:
        if items:
            cell = ws_nontrad.cell(row=row, column=2, value=cat_name)
            cell.font = section_font
            cell.fill = section_fill
            cell.border = thin_border
            ws_nontrad.cell(row=row, column=3).fill = section_fill
            ws_nontrad.cell(row=row, column=3).border = thin_border
            row += 1
            for ch in items:
                style_body_cell(ws_nontrad, row, 3, ch)
                row += 1
            row += 1

    # Add APAC local social platforms if any location is in APAC
    apac_social = alt_supply.get("local_social_platforms", {})
    apac_classifieds = alt_supply.get("local_classifieds", {})
    if apac_social or apac_classifieds:
        has_apac_data = False
        for loc in locations:
            loc_lower = loc.strip().lower()
            for country_key in list(apac_social.keys()) + list(apac_classifieds.keys()):
                if country_key in loc_lower or loc_lower in country_key:
                    has_apac_data = True
                    break
        # Also include if any international location is present
        if has_international or has_apac_data:
            if apac_social:
                cell = ws_nontrad.cell(row=row, column=2, value="APAC Local Social Platforms")
                cell.font = section_font
                cell.fill = section_fill
                cell.border = thin_border
                ws_nontrad.cell(row=row, column=3).fill = section_fill
                ws_nontrad.cell(row=row, column=3).border = thin_border
                row += 1
                for country, platforms in apac_social.items():
                    style_body_cell(ws_nontrad, row, 2, f"  {country.replace('_', ' ').title()}")
                    style_body_cell(ws_nontrad, row, 3, ", ".join(platforms))
                    row += 1
                row += 1
            if apac_classifieds:
                cell = ws_nontrad.cell(row=row, column=2, value="APAC Local Classifieds")
                cell.font = section_font
                cell.fill = section_fill
                cell.border = thin_border
                ws_nontrad.cell(row=row, column=3).fill = section_fill
                ws_nontrad.cell(row=row, column=3).border = thin_border
                row += 1
                for country, platforms in apac_classifieds.items():
                    style_body_cell(ws_nontrad, row, 2, f"  {country.replace('_', ' ').title()}")
                    style_body_cell(ws_nontrad, row, 3, ", ".join(platforms))
                    row += 1
                row += 1

    # ── Sheet 6: Global Supply Strategy (if international or explicitly requested) ──
    if has_international or data.get("include_global_supply"):
        ws_global = wb.create_sheet("Global Supply Strategy")
        ws_global.sheet_properties.tabColor = "00B050"
        ws_global.column_dimensions["A"].width = 5
        ws_global.column_dimensions["B"].width = 25
        ws_global.column_dimensions["C"].width = 25
        ws_global.column_dimensions["D"].width = 20
        ws_global.column_dimensions["E"].width = 15
        ws_global.column_dimensions["F"].width = 20
        ws_global.column_dimensions["G"].width = 30

        ws_global.merge_cells("B2:G2")
        ws_global["B2"].value = "Global Supply Strategy"
        ws_global["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")
        ws_global["B3"].value = f"Markets: {', '.join(locations)}"
        ws_global["B3"].font = Font(name="Calibri", italic=True, size=10, color="666666")

        row = 5
        # Country-Specific Job Boards
        cell = ws_global.cell(row=row, column=2, value="Country-Specific Job Boards")
        cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        for c in range(3, 8):
            ws_global.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            ws_global.cell(row=row, column=c).border = thin_border
        row += 1

        gs_headers = ["Country", "Board Name", "Billing Model", "Category", "Tier", "Monthly Spend"]
        for i, h in enumerate(gs_headers):
            cell = ws_global.cell(row=row, column=2 + i, value=h)
            cell.font = subheader_font
            cell.fill = subheader_fill
            cell.alignment = center_alignment
            cell.border = thin_border
        row += 1

        country_boards = global_research.get("country_boards", [])
        for cb in country_boards:
            country_name = cb.get("country", "")
            board_data = cb.get("data", {})
            boards = board_data.get("boards", [])
            monthly = board_data.get("monthly_spend", "N/A")
            for idx, board in enumerate(boards):
                style_body_cell(ws_global, row, 2, country_name if idx == 0 else "")
                if idx == 0:
                    ws_global.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_global, row, 3, board.get("name", ""))
                style_body_cell(ws_global, row, 4, board.get("billing", ""))
                style_body_cell(ws_global, row, 5, board.get("category", ""))
                style_body_cell(ws_global, row, 6, board.get("tier", ""))
                style_body_cell(ws_global, row, 7, monthly if idx == 0 else "")
                if row % 2 == 0:
                    for c in range(2, 8):
                        ws_global.cell(row=row, column=c).fill = accent_fill
                row += 1
            row += 1  # gap between countries

        # Push vs Pull Strategy
        row += 1
        cell = ws_global.cell(row=row, column=2, value="Push vs Pull Strategy Recommendations")
        cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        for c in range(3, 8):
            ws_global.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            ws_global.cell(row=row, column=c).border = thin_border
        row += 1

        push_pull = global_research.get("push_pull_strategy", {})
        for strategy_type in ["pull_advertising", "push_advertising"]:
            strat = push_pull.get(strategy_type, {})
            if strat:
                cell = ws_global.cell(row=row, column=2, value=strategy_type.replace("_", " ").title())
                cell.font = Font(name="Calibri", bold=True, size=11, color="1F4E79")
                cell.fill = subheader_fill
                cell.border = thin_border
                for c in range(3, 8):
                    ws_global.cell(row=row, column=c).fill = subheader_fill
                    ws_global.cell(row=row, column=c).border = thin_border
                row += 1
                style_body_cell(ws_global, row, 2, "Description")
                ws_global.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_global, row, 3, strat.get("description", ""))
                ws_global.merge_cells(start_row=row, start_column=3, end_row=row, end_column=7)
                row += 1
                style_body_cell(ws_global, row, 2, "Best For")
                ws_global.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_global, row, 3, strat.get("best_for", ""))
                ws_global.merge_cells(start_row=row, start_column=3, end_row=row, end_column=7)
                row += 1
                style_body_cell(ws_global, row, 2, "Channels")
                ws_global.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                channels_list = strat.get("channels", [])
                style_body_cell(ws_global, row, 3, ", ".join(channels_list) if isinstance(channels_list, list) else str(channels_list))
                ws_global.merge_cells(start_row=row, start_column=3, end_row=row, end_column=7)
                row += 1
                style_body_cell(ws_global, row, 2, "KPIs")
                ws_global.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                kpis_list = strat.get("kpis", [])
                style_body_cell(ws_global, row, 3, ", ".join(kpis_list) if isinstance(kpis_list, list) else str(kpis_list))
                ws_global.merge_cells(start_row=row, start_column=3, end_row=row, end_column=7)
                row += 1
                row += 1

        # NOTE: Commission Tiers intentionally excluded — internal Joveo data, not for client-facing output

    # ── Sheet 7: DEI & Diversity Channels ──
    ws_dei = wb.create_sheet("DEI & Diversity Channels")
    ws_dei.sheet_properties.tabColor = "7030A0"
    ws_dei.column_dimensions["A"].width = 5
    ws_dei.column_dimensions["B"].width = 30
    ws_dei.column_dimensions["C"].width = 35
    ws_dei.column_dimensions["D"].width = 25
    ws_dei.column_dimensions["E"].width = 30

    ws_dei.merge_cells("B2:E2")
    ws_dei["B2"].value = "DEI & Diversity Channels"
    ws_dei["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")
    ws_dei["B3"].value = f"Target Markets: {', '.join(locations)}"
    ws_dei["B3"].font = Font(name="Calibri", italic=True, size=10, color="666666")

    row = 5
    # DEI Boards by Region
    cell = ws_dei.cell(row=row, column=2, value="DEI-Focused Job Boards by Region")
    cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    for c in range(3, 6):
        ws_dei.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        ws_dei.cell(row=row, column=c).border = thin_border
    row += 1

    dei_headers = ["Board Name", "Focus Area", "Regions Covered"]
    for i, h in enumerate(dei_headers):
        cell = ws_dei.cell(row=row, column=2 + i, value=h)
        cell.font = subheader_font
        cell.fill = subheader_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    row += 1

    dei_by_country = gs.get("dei_boards_by_country", {})
    for region_name, boards in dei_by_country.items():
        # Section header for each region
        cell = ws_dei.cell(row=row, column=2, value=region_name)
        cell.font = section_font
        cell.fill = section_fill
        cell.border = thin_border
        for c in range(3, 5):
            ws_dei.cell(row=row, column=c).fill = section_fill
            ws_dei.cell(row=row, column=c).border = thin_border
        row += 1

        board_list = boards if isinstance(boards, list) else boards.get("boards", boards) if isinstance(boards, dict) else []
        if isinstance(board_list, list):
            for board in board_list:
                if isinstance(board, dict):
                    style_body_cell(ws_dei, row, 2, board.get("name", ""))
                    style_body_cell(ws_dei, row, 3, board.get("focus", ""))
                    style_body_cell(ws_dei, row, 4, board.get("regions", region_name))
                    if row % 2 == 0:
                        for c in range(2, 5):
                            ws_dei.cell(row=row, column=c).fill = accent_fill
                    row += 1
        row += 1  # gap between regions

    # Women-Specific Boards
    row += 1
    cell = ws_dei.cell(row=row, column=2, value="Women-Specific Job Boards")
    cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    for c in range(3, 6):
        ws_dei.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        ws_dei.cell(row=row, column=c).border = thin_border
    row += 1

    women_headers = ["Board Name", "Focus", "Regions"]
    for i, h in enumerate(women_headers):
        cell = ws_dei.cell(row=row, column=2 + i, value=h)
        cell.font = subheader_font
        cell.fill = subheader_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    row += 1

    women_boards = gs.get("women_specific_boards", [])
    for board in women_boards:
        style_body_cell(ws_dei, row, 2, board.get("name", ""))
        style_body_cell(ws_dei, row, 3, board.get("focus", ""))
        style_body_cell(ws_dei, row, 4, board.get("regions", ""))
        if row % 2 == 0:
            for c in range(2, 5):
                ws_dei.cell(row=row, column=c).fill = accent_fill
        row += 1

    # Industry-Specific Diversity Channels from channels_db
    row += 2
    cell = ws_dei.cell(row=row, column=2, value="Industry-Specific Diversity Channels")
    cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    for c in range(3, 6):
        ws_dei.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        ws_dei.cell(row=row, column=c).border = thin_border
    row += 1

    dei_channels = db.get("traditional_channels", {}).get("niche_by_industry", {}).get("diversity_dei", [])
    for ch in dei_channels:
        style_body_cell(ws_dei, row, 2, ch)
        style_body_cell(ws_dei, row, 3, "Multi-diversity / DEI-focused")
        style_body_cell(ws_dei, row, 4, "US / Global")
        row += 1

    # ── Sheet 8: Innovative Channels 2025+ (always) ──
    ws_innov = wb.create_sheet("Innovative Channels 2025+")
    ws_innov.sheet_properties.tabColor = "FF6600"
    ws_innov.column_dimensions["A"].width = 5
    ws_innov.column_dimensions["B"].width = 30
    ws_innov.column_dimensions["C"].width = 50
    ws_innov.column_dimensions["D"].width = 45
    ws_innov.column_dimensions["E"].width = 20
    ws_innov.column_dimensions["F"].width = 35

    ws_innov.merge_cells("B2:F2")
    ws_innov["B2"].value = "Innovative Channels 2025+"
    ws_innov["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")
    ws_innov["B3"].value = "Emerging recruitment channels: CTV, DOOH, Retail Media, Gaming, Podcasts, Messaging Apps & more"
    ws_innov["B3"].font = Font(name="Calibri", italic=True, size=10, color="666666")

    row = 5
    innov_headers = ["Channel", "Description", "Best Use Case", "Billing Model", "Best For / Industries"]
    for i, h in enumerate(innov_headers):
        cell = ws_innov.cell(row=row, column=2 + i, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    row += 1

    innovative = gs.get("innovative_channels_2025", {})
    for channel_key, channel_data in innovative.items():
        if isinstance(channel_data, dict):
            style_body_cell(ws_innov, row, 2, channel_key.replace("_", " ").title())
            ws_innov.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
            style_body_cell(ws_innov, row, 3, channel_data.get("description", ""))
            style_body_cell(ws_innov, row, 4, channel_data.get("use_case", ""))
            style_body_cell(ws_innov, row, 5, channel_data.get("billing", ""))
            best_for = channel_data.get("best_for", [])
            style_body_cell(ws_innov, row, 6, ", ".join(best_for) if isinstance(best_for, list) else str(best_for))
            ws_innov.row_dimensions[row].height = 50
            if row % 2 == 0:
                for c in range(2, 7):
                    ws_innov.cell(row=row, column=c).fill = accent_fill
            row += 1

    # Platforms sub-section
    row += 2
    cell = ws_innov.cell(row=row, column=2, value="Platform Details by Channel")
    cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    for c in range(3, 7):
        ws_innov.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        ws_innov.cell(row=row, column=c).border = thin_border
    row += 1

    for i, h in enumerate(["Channel", "Platforms"]):
        cell = ws_innov.cell(row=row, column=2 + i, value=h)
        cell.font = subheader_font
        cell.fill = subheader_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    row += 1

    for channel_key, channel_data in innovative.items():
        if isinstance(channel_data, dict):
            platforms = channel_data.get("platforms", [])
            if platforms:
                style_body_cell(ws_innov, row, 2, channel_key.replace("_", " ").title())
                ws_innov.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_innov, row, 3, ", ".join(platforms))
                ws_innov.merge_cells(start_row=row, start_column=3, end_row=row, end_column=6)
                row += 1

    # ── Sheet 9: Budget & Pricing Guide (always) ──
    ws_budget = wb.create_sheet("Budget & Pricing Guide")
    ws_budget.sheet_properties.tabColor = "C00000"
    ws_budget.column_dimensions["A"].width = 5
    ws_budget.column_dimensions["B"].width = 25
    ws_budget.column_dimensions["C"].width = 50
    ws_budget.column_dimensions["D"].width = 25
    ws_budget.column_dimensions["E"].width = 40

    ws_budget.merge_cells("B2:E2")
    ws_budget["B2"].value = "Budget & Pricing Guide"
    ws_budget["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")

    row = 4
    # Billing Models
    cell = ws_budget.cell(row=row, column=2, value="Billing Models Explained")
    cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    for c in range(3, 6):
        ws_budget.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        ws_budget.cell(row=row, column=c).border = thin_border
    row += 1

    billing_headers = ["Model", "Description", "Typical Rate Range"]
    for i, h in enumerate(billing_headers):
        cell = ws_budget.cell(row=row, column=2 + i, value=h)
        cell.font = subheader_font
        cell.fill = subheader_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    row += 1

    billing_models = gs.get("billing_models", {})
    for model_name, model_data in billing_models.items():
        style_body_cell(ws_budget, row, 2, model_name)
        ws_budget.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
        style_body_cell(ws_budget, row, 3, model_data.get("description", ""))
        style_body_cell(ws_budget, row, 4, model_data.get("typical_range", ""))
        if row % 2 == 0:
            for c in range(2, 5):
                ws_budget.cell(row=row, column=c).fill = accent_fill
        row += 1

    # NOTE: Commission Tiers intentionally excluded — internal Joveo data, not for client-facing output

    # Monthly Buying Recommendations by Region
    row += 2
    cell = ws_budget.cell(row=row, column=2, value="Monthly Buying by Region")
    cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    for c in range(3, 6):
        ws_budget.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        ws_budget.cell(row=row, column=c).border = thin_border
    row += 1

    region_headers = ["Region", "Monthly Spend", "Top Countries"]
    for i, h in enumerate(region_headers):
        cell = ws_budget.cell(row=row, column=2 + i, value=h)
        cell.font = subheader_font
        cell.fill = subheader_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    row += 1

    monthly_buying = gs.get("monthly_buying_by_region", {})
    for region_name, region_data in monthly_buying.items():
        style_body_cell(ws_budget, row, 2, region_name)
        ws_budget.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
        style_body_cell(ws_budget, row, 3, region_data.get("spend", ""))
        top_countries = region_data.get("top_countries", [])
        style_body_cell(ws_budget, row, 4, ", ".join(top_countries))
        if row % 2 == 0:
            for c in range(2, 5):
                ws_budget.cell(row=row, column=c).fill = accent_fill
        row += 1

    # CPA Rate Benchmarks by region (sourced from channels_db channel_strategies)
    row += 2
    cell = ws_budget.cell(row=row, column=2, value="CPA Rate Benchmarks (Typical Ranges)")
    cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    for c in range(3, 6):
        ws_budget.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        ws_budget.cell(row=row, column=c).border = thin_border
    row += 1

    # Try to read CPA benchmarks from channels_db; fall back to hardcoded values
    cpa_from_db = db.get("cpa_rate_benchmarks", {})
    if cpa_from_db:
        cpa_benchmarks = [
            (region_name, region_info.get("range", "N/A"), region_info.get("notes", ""))
            for region_name, region_info in cpa_from_db.items()
            if isinstance(region_info, dict)
        ]
    else:
        cpa_benchmarks = [
            ("North America (US/Canada)", "$15 - $45", "High competition, strong CPC performance on Indeed/ZipRecruiter"),
            ("Europe (UK/DE/FR/NL)", "$12 - $40", "Mixed CPC/Posting models; StepStone, Reed, Totaljobs common"),
            ("APAC (India/Japan/AU/SG)", "$5 - $30", "Lower CPAs in India; premium in Japan/AU/Singapore"),
            ("LATAM (Brazil/Mexico/Argentina)", "$3 - $20", "Cost-effective; CompuTrabajo, OCC Mundial popular"),
            ("MEA (UAE/South Africa/Kenya)", "$5 - $25", "Growing market; Bayt.com, CareerJunction dominant"),
        ]
    for i, h in enumerate(["Region", "CPA Range", "Notes"]):
        cell = ws_budget.cell(row=row, column=2 + i, value=h)
        cell.font = subheader_font
        cell.fill = subheader_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    row += 1

    for region, cpa_range, notes in cpa_benchmarks:
        style_body_cell(ws_budget, row, 2, region)
        ws_budget.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
        style_body_cell(ws_budget, row, 3, cpa_range)
        style_body_cell(ws_budget, row, 4, notes)
        ws_budget.row_dimensions[row].height = 35
        if row % 2 == 0:
            for c in range(2, 5):
                ws_budget.cell(row=row, column=c).fill = accent_fill
        row += 1

    # ── Pie Chart: Recommended Budget Allocation ──
    row += 2
    cell = ws_budget.cell(row=row, column=2, value="Recommended Budget Allocation by Channel Type")
    cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    for c in range(3, 6):
        ws_budget.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        ws_budget.cell(row=row, column=c).border = thin_border
    row += 1

    # Write pie chart data table
    pie_data_start = row
    pie_headers = ["Channel Type", "Allocation %"]
    for i, h in enumerate(pie_headers):
        cell = ws_budget.cell(row=row, column=2 + i, value=h)
        cell.font = subheader_font
        cell.fill = subheader_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    row += 1

    pie_items = [
        ("Job Boards (Programmatic)", 35),
        ("Social Media Advertising", 20),
        ("Niche/Industry Boards", 15),
        ("Employer Branding", 10),
        ("Events & Career Fairs", 8),
        ("Innovative/Emerging", 7),
        ("DEI Channels", 5),
    ]
    for label, pct in pie_items:
        style_body_cell(ws_budget, row, 2, label)
        style_body_cell(ws_budget, row, 3, pct)
        row += 1

    pie_data_end = row - 1

    # Create PieChart
    pie_chart = PieChart()
    pie_chart.title = "Recommended Budget Allocation by Channel Type"
    pie_chart.width = 18
    pie_chart.height = 12
    pie_chart.style = 10

    pie_labels = Reference(ws_budget, min_col=2, min_row=pie_data_start + 1, max_row=pie_data_end)
    pie_values = Reference(ws_budget, min_col=3, min_row=pie_data_start, max_row=pie_data_end)
    pie_chart.add_data(pie_values, titles_from_data=True)
    pie_chart.set_categories(pie_labels)

    pie_chart.dataLabels = DataLabelList()
    pie_chart.dataLabels.showPercent = True
    pie_chart.dataLabels.showCatName = True

    ws_budget.add_chart(pie_chart, f"B{row + 1}")
    row += 20  # leave space for chart

    # ── Job Category Insights Sheet (if categories selected) ──
    job_categories = data.get("job_categories", [])
    job_cat_db = db.get("job_categories", {})
    if job_categories and job_cat_db:
        ws_jc = wb.create_sheet("Job Category Insights")
        ws_jc.sheet_properties.tabColor = "00B0F0"
        ws_jc.column_dimensions["A"].width = 3
        ws_jc.column_dimensions["B"].width = 22
        ws_jc.column_dimensions["C"].width = 28
        ws_jc.column_dimensions["D"].width = 28
        ws_jc.column_dimensions["E"].width = 28
        ws_jc.column_dimensions["F"].width = 28

        ws_jc.merge_cells("B2:F2")
        jc_title = ws_jc["B2"]
        jc_title.value = "Job Category Insights & Channel Recommendations"
        jc_title.font = Font(name="Calibri", bold=True, size=18, color="FFFFFF")
        jc_title.fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
        jc_title.alignment = Alignment(horizontal="center", vertical="center")
        for c in range(3, 7):
            ws_jc.cell(row=2, column=c).fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
        ws_jc.row_dimensions[2].height = 40

        ws_jc.merge_cells("B3:F3")
        ws_jc["B3"].value = f"Categories: {', '.join(data.get('job_category_labels', []))}  |  Industry: {data.get('industry_label', industry)}"
        ws_jc["B3"].font = Font(name="Calibri", italic=True, size=10, color="FFFFFF")
        ws_jc["B3"].fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
        ws_jc["B3"].alignment = Alignment(horizontal="center")
        for c in range(3, 7):
            ws_jc.cell(row=3, column=c).fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")

        jc_row = 5

        for cat_key in job_categories:
            cat = job_cat_db.get(cat_key)
            if not cat:
                continue

            # Category header
            ws_jc.merge_cells(f"B{jc_row}:F{jc_row}")
            cell = ws_jc.cell(row=jc_row, column=2, value=f"{cat.get('icon', '')} {cat.get('label', cat_key)}")
            cell.font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
            cell.fill = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
            cell.alignment = Alignment(horizontal="center", vertical="center")
            for c in range(3, 7):
                ws_jc.cell(row=jc_row, column=c).fill = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
            ws_jc.row_dimensions[jc_row].height = 30
            jc_row += 1

            # Description
            ws_jc.merge_cells(f"B{jc_row}:F{jc_row}")
            ws_jc.cell(row=jc_row, column=2, value=cat.get("description", "")).font = Font(name="Calibri", italic=True, size=10, color="666666")
            jc_row += 1

            # Key Metrics Row
            jc_row += 1
            metric_items = [
                ("Avg CPA Range", cat.get("avg_cpa_range", "N/A")),
                ("Time to Fill", cat.get("avg_time_to_fill", "N/A")),
                ("Awareness %", str(cat.get("strategy_emphasis", {}).get("awareness", "")) + "%" if cat.get("strategy_emphasis") else "N/A"),
                ("Hiring %", str(cat.get("strategy_emphasis", {}).get("hiring", "")) + "%" if cat.get("strategy_emphasis") else "N/A"),
            ]
            for i, (label, val) in enumerate(metric_items):
                cell_l = ws_jc.cell(row=jc_row, column=2 + i, value=label)
                cell_l.font = Font(name="Calibri", bold=True, size=9, color="666666")
                cell_l.fill = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
                cell_l.border = thin_border
                cell_v = ws_jc.cell(row=jc_row + 1, column=2 + i, value=val)
                cell_v.font = Font(name="Calibri", bold=True, size=12, color="1B2A4A")
                cell_v.fill = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
                cell_v.border = thin_border
                cell_v.alignment = Alignment(horizontal="center")
            jc_row += 2

            # Example Roles
            jc_row += 1
            ws_jc.cell(row=jc_row, column=2, value="Typical Roles").font = Font(name="Calibri", bold=True, size=11, color="1B2A4A")
            example_roles = cat.get("example_roles", [])
            ws_jc.cell(row=jc_row, column=3, value=", ".join(example_roles)).font = body_font
            ws_jc.merge_cells(f"C{jc_row}:F{jc_row}")
            ws_jc.cell(row=jc_row, column=2).border = thin_border
            ws_jc.cell(row=jc_row, column=3).border = thin_border
            ws_jc.cell(row=jc_row, column=3).alignment = wrap_alignment
            jc_row += 1

            # Recommended Channels Table
            jc_row += 1
            rec_ch = cat.get("recommended_channels", {})
            channel_sections = [
                ("Primary Channels (Joveo Supply)", rec_ch.get("primary", []), "00B050"),
                ("Secondary Channels", rec_ch.get("secondary", []), "4472C4"),
                ("Social & Paid Media", rec_ch.get("social", []), "ED7D31"),
                ("Niche / Specialized", rec_ch.get("niche", []), "7030A0"),
            ]
            ch_headers = ["Category", "Channels", "Strategy Notes", "Data Source"]
            for i, h in enumerate(ch_headers):
                cell = ws_jc.cell(row=jc_row, column=2 + i, value=h)
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = center_alignment
                cell.border = thin_border
            jc_row += 1

            for ch_label, ch_list, ch_color in channel_sections:
                if ch_list:
                    cell = ws_jc.cell(row=jc_row, column=2, value=ch_label)
                    cell.font = Font(name="Calibri", bold=True, size=10, color=ch_color)
                    cell.border = thin_border
                    style_body_cell(ws_jc, jc_row, 3, ", ".join(ch_list))
                    # Add strategy notes based on category
                    if "Primary" in ch_label:
                        style_body_cell(ws_jc, jc_row, 4, "High-volume, proven ROI — allocate 40-50% of budget")
                        style_body_cell(ws_jc, jc_row, 5, "Joveo Supply Repository")
                    elif "Secondary" in ch_label:
                        style_body_cell(ws_jc, jc_row, 4, "Supplementary reach — allocate 20-25% of budget")
                        style_body_cell(ws_jc, jc_row, 5, "Past Media Plan Data")
                    elif "Social" in ch_label:
                        style_body_cell(ws_jc, jc_row, 4, "Brand awareness + retargeting — allocate 15-20% of budget")
                        style_body_cell(ws_jc, jc_row, 5, "Competitor Analysis")
                    else:
                        style_body_cell(ws_jc, jc_row, 4, "Targeted specialists — allocate 10-15% of budget")
                        style_body_cell(ws_jc, jc_row, 5, "Industry Research")
                    ws_jc.row_dimensions[jc_row].height = 35
                    jc_row += 1

            # Joveo Supply Fit
            jc_row += 1
            ws_jc.cell(row=jc_row, column=2, value="Joveo Supply Fit").font = Font(name="Calibri", bold=True, size=11, color="00B050")
            ws_jc.cell(row=jc_row, column=2).border = thin_border
            joveo_fit = cat.get("joveo_supply_fit", [])
            ws_jc.cell(row=jc_row, column=3, value=", ".join(joveo_fit)).font = body_font
            ws_jc.cell(row=jc_row, column=3).border = thin_border
            ws_jc.merge_cells(f"C{jc_row}:F{jc_row}")
            ws_jc.cell(row=jc_row, column=3).alignment = wrap_alignment
            jc_row += 1

            # Competitor Channels
            ws_jc.cell(row=jc_row, column=2, value="Competitor Channels").font = Font(name="Calibri", bold=True, size=11, color="ED7D31")
            ws_jc.cell(row=jc_row, column=2).border = thin_border
            comp_ch = cat.get("competitor_channels", [])
            ws_jc.cell(row=jc_row, column=3, value=", ".join(comp_ch)).font = body_font
            ws_jc.cell(row=jc_row, column=3).border = thin_border
            ws_jc.merge_cells(f"C{jc_row}:F{jc_row}")
            ws_jc.cell(row=jc_row, column=3).alignment = wrap_alignment
            jc_row += 1

            # Best Practices
            jc_row += 1
            ws_jc.merge_cells(f"B{jc_row}:F{jc_row}")
            ws_jc.cell(row=jc_row, column=2, value="Best Practices & Recommendations").font = Font(name="Calibri", bold=True, size=11, color="1B2A4A")
            jc_row += 1
            for bp in cat.get("best_practices", []):
                ws_jc.cell(row=jc_row, column=2, value=f"  \u2022  {bp}").font = body_font
                ws_jc.merge_cells(f"B{jc_row}:F{jc_row}")
                ws_jc.cell(row=jc_row, column=2).border = thin_border
                jc_row += 1

            jc_row += 2  # gap between categories

    # ── Campaign Timeline Sheet ──
    ws_timeline = wb.create_sheet("Campaign Timeline")
    ws_timeline.sheet_properties.tabColor = "2E75B6"
    ws_timeline.column_dimensions["A"].width = 3
    ws_timeline.column_dimensions["B"].width = 22
    ws_timeline.column_dimensions["C"].width = 16
    ws_timeline.column_dimensions["D"].width = 40
    ws_timeline.column_dimensions["E"].width = 25
    ws_timeline.column_dimensions["F"].width = 28
    ws_timeline.column_dimensions["G"].width = 12

    ws_timeline.merge_cells("B2:G2")
    ws_timeline["B2"].value = "Campaign Timeline"
    ws_timeline["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")

    ws_timeline["B3"].value = f"Client: {data.get('client_name', '')}  |  Duration: {data.get('campaign_duration', 'Standard 12 Weeks')}"
    ws_timeline["B3"].font = Font(name="Calibri", italic=True, size=10, color="666666")

    tl_row = 5
    ws_timeline.merge_cells(f"B{tl_row}:G{tl_row}")
    ws_timeline.cell(row=tl_row, column=2, value="Campaign Phases").font = Font(name="Calibri", bold=True, size=14, color="1B2A4A")
    tl_row += 1

    tl_headers = ["Phase", "Timeline", "Activities", "Channels", "KPIs", "Budget %"]
    for i, h in enumerate(tl_headers):
        cell = ws_timeline.cell(row=tl_row, column=2 + i, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    tl_row += 1

    phase_colors = {
        "Phase 1": PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid"),  # blue
        "Phase 2": PatternFill(start_color="D9EAD3", end_color="D9EAD3", fill_type="solid"),  # green
        "Phase 3": PatternFill(start_color="FCE5CD", end_color="FCE5CD", fill_type="solid"),  # orange
        "Phase 4": PatternFill(start_color="E4D1F0", end_color="E4D1F0", fill_type="solid"),  # purple
    }

    phases = [
        ("Phase 1", "Research & Setup", "Weeks 1-2", "Market analysis, account setup, creative development", "N/A", "Setup completion", "5%"),
        ("Phase 2", "Launch & Optimize", "Weeks 3-6", "Initial campaign launch, A/B testing, bid optimization", "All programmatic", "CPC, CPA, Apply rate", "30%"),
        ("Phase 3", "Scale & Expand", "Weeks 7-12", "Increase spend on top performers, add new channels", "Top 5 channels", "Cost per hire, Quality of hire", "40%"),
        ("Phase 4", "Sustain & Refine", "Ongoing", "Maintain performance, quarterly reviews, seasonal adjustments", "Proven channels", "ROI, Time to fill", "25%"),
    ]

    for phase_key, phase_name, timeline, activities, channels, kpis, budget_pct in phases:
        phase_fill = phase_colors.get(phase_key, accent_fill)
        vals = [f"{phase_key}: {phase_name}", timeline, activities, channels, kpis, budget_pct]
        for i, v in enumerate(vals):
            cell = ws_timeline.cell(row=tl_row, column=2 + i, value=v)
            cell.font = body_font
            cell.alignment = wrap_alignment
            cell.border = thin_border
            cell.fill = phase_fill
        ws_timeline.cell(row=tl_row, column=2).font = Font(name="Calibri", bold=True, size=10)
        ws_timeline.row_dimensions[tl_row].height = 45
        tl_row += 1

    # Key Milestones section
    tl_row += 2
    ws_timeline.merge_cells(f"B{tl_row}:G{tl_row}")
    ws_timeline.cell(row=tl_row, column=2, value="Key Milestones").font = Font(name="Calibri", bold=True, size=14, color="1B2A4A")
    tl_row += 1

    milestone_headers = ["Milestone", "Description"]
    for i, h in enumerate(milestone_headers):
        cell = ws_timeline.cell(row=tl_row, column=2 + i, value=h)
        cell.font = subheader_font
        cell.fill = subheader_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    tl_row += 1

    milestones = [
        ("Week 1", "Kick-off meeting & channel setup"),
        ("Week 2", "Creative assets approved, campaigns live"),
        ("Week 4", "First performance review"),
        ("Week 8", "Mid-campaign optimization review"),
        ("Week 12", "Full performance report & recommendations"),
    ]
    for ms_label, ms_desc in milestones:
        cell_l = ws_timeline.cell(row=tl_row, column=2, value=ms_label)
        cell_l.font = Font(name="Calibri", bold=True, size=10)
        cell_l.border = thin_border
        cell_d = ws_timeline.cell(row=tl_row, column=3, value=ms_desc)
        cell_d.font = body_font
        cell_d.border = thin_border
        ws_timeline.merge_cells(f"C{tl_row}:G{tl_row}")
        if tl_row % 2 == 0:
            cell_l.fill = accent_fill
            cell_d.fill = accent_fill
        tl_row += 1

    # ── Optional: Educational Partners ──
    if data.get("include_educational"):
        ws_edu = wb.create_sheet("Educational Partners")
        ws_edu.sheet_properties.tabColor = "70AD47"
        ws_edu.column_dimensions["A"].width = 5
        ws_edu.column_dimensions["B"].width = 45
        ws_edu.column_dimensions["C"].width = 70
        ws_edu.merge_cells("B2:C2")
        ws_edu["B2"].value = "Educational Partners & Training Programs"
        ws_edu["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")
        row = 4
        for i, h in enumerate(["Institution", "Talent Focus / Strategic Fit"]):
            cell = ws_edu.cell(row=row, column=2 + i, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center_alignment
            cell.border = thin_border

        # USE RESEARCH MODULE for real universities
        partners = research.get_educational_partners(locations, industry)

        for p in partners:
            row += 1
            style_body_cell(ws_edu, row, 2, p.get("institution", ""))
            ws_edu.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
            style_body_cell(ws_edu, row, 3, p.get("fit", ""))
            ws_edu.row_dimensions[row].height = 45

        # Add Joveo university job board partners
        uni_pubs = joveo_pubs.get("by_category", {}).get("University Job Board", [])
        if uni_pubs:
            row += 2
            ws_edu.merge_cells(start_row=row, start_column=2, end_row=row, end_column=3)
            ws_edu.cell(row=row, column=2, value="Joveo University Job Board Partners (for campus recruitment)").font = Font(name="Calibri", bold=True, size=12, color="2E75B6")
            row += 1
            for i, h in enumerate(["Platform", "Type"]):
                cell = ws_edu.cell(row=row, column=2 + i, value=h)
                cell.font = subheader_font
                cell.fill = subheader_fill
                cell.alignment = center_alignment
                cell.border = thin_border
            row += 1
            for pub in uni_pubs[:20]:
                style_body_cell(ws_edu, row, 2, pub)
                style_body_cell(ws_edu, row, 3, "University Job Board — Campus Recruiting Pipeline")
                row += 1

    # ── Optional: Events & Career Fairs ──
    if data.get("include_events"):
        ws_events = wb.create_sheet("Events & Career Fairs")
        ws_events.sheet_properties.tabColor = "70AD47"
        for col, w in [("A",5),("B",40),("C",22),("D",22),("E",45),("F",18),("G",18)]:
            ws_events.column_dimensions[col].width = w
        ws_events.merge_cells("B2:G2")
        ws_events["B2"].value = "Events & Career Fairs"
        ws_events["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")
        row = 4
        for i, h in enumerate(["Primary Partners", "Location", "Type", "Branding & Recruitment Impact", "Reach", "Budget Est."]):
            cell = ws_events.cell(row=row, column=2 + i, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center_alignment
            cell.border = thin_border

        # USE RESEARCH MODULE for real events
        events = research.get_events(locations, industry)

        for evt in events:
            row += 1
            style_body_cell(ws_events, row, 2, evt.get("partner", ""))
            ws_events.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
            style_body_cell(ws_events, row, 3, evt.get("location", ""))
            style_body_cell(ws_events, row, 4, evt.get("type", ""))
            style_body_cell(ws_events, row, 5, evt.get("impact", ""))
            style_body_cell(ws_events, row, 6, evt.get("reach", ""))
            style_body_cell(ws_events, row, 7, evt.get("budget", ""))

    # ── Optional: Radio/Podcasts ──
    if data.get("include_radio_podcasts"):
        ws_radio = wb.create_sheet("Radio & Podcasts")
        ws_radio.sheet_properties.tabColor = "ED7D31"
        for col, w in [("A",5),("B",45),("C",25),("D",30),("E",35)]:
            ws_radio.column_dimensions[col].width = w
        ws_radio.merge_cells("B2:E2")
        ws_radio["B2"].value = "Radio & Podcasts"
        ws_radio["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")
        row = 4
        for i, h in enumerate(["Channel / Station", "Weekly Listeners / Downloads", "Format / Genre", "Audience Type"]):
            cell = ws_radio.cell(row=row, column=2 + i, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center_alignment
            cell.border = thin_border

        # USE RESEARCH MODULE for real radio/podcast data
        radio_data = research.get_radio_podcasts(locations, industry)

        # Separate local radio and podcasts
        local_stations = [r for r in radio_data if "downloads" not in r.get("listeners", "").lower()]
        podcasts = [r for r in radio_data if "downloads" in r.get("listeners", "").lower()]

        if local_stations:
            row += 1
            cell = ws_radio.cell(row=row, column=2, value="LOCAL RADIO STATIONS")
            cell.font = section_font
            cell.fill = section_fill
            cell.border = thin_border
            for c in range(3, 6):
                ws_radio.cell(row=row, column=c).fill = section_fill
                ws_radio.cell(row=row, column=c).border = thin_border

            for station in local_stations:
                row += 1
                style_body_cell(ws_radio, row, 2, station.get("name", ""))
                ws_radio.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_radio, row, 3, station.get("listeners", ""))
                style_body_cell(ws_radio, row, 4, station.get("genre", ""))
                style_body_cell(ws_radio, row, 5, station.get("audience", ""))

        if podcasts:
            row += 2
            cell = ws_radio.cell(row=row, column=2, value="INDUSTRY PODCASTS")
            cell.font = section_font
            cell.fill = section_fill
            cell.border = thin_border
            for c in range(3, 6):
                ws_radio.cell(row=row, column=c).fill = section_fill
                ws_radio.cell(row=row, column=c).border = thin_border

            for pod in podcasts:
                row += 1
                style_body_cell(ws_radio, row, 2, pod.get("name", ""))
                ws_radio.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_radio, row, 3, pod.get("listeners", ""))
                style_body_cell(ws_radio, row, 4, pod.get("genre", ""))
                style_body_cell(ws_radio, row, 5, pod.get("audience", ""))

    # ── Optional: Media/Print Platforms ──
    if data.get("include_media_platforms"):
        ws_media = wb.create_sheet("Media & Print Platforms")
        ws_media.sheet_properties.tabColor = "ED7D31"
        for col, w in [("A",5),("B",45),("C",40),("D",22),("E",18)]:
            ws_media.column_dimensions[col].width = w
        ws_media.merge_cells("B2:E2")
        ws_media["B2"].value = "Media & Print Platforms"
        ws_media["B2"].font = Font(name="Calibri", bold=True, size=16, color="1B2A4A")
        industry_data = db.get("industries", {}).get(industry, {})
        media_platforms = industry_data.get("media_platforms", {})

        # USE RESEARCH MODULE for real audience descriptions
        audiences = research.get_media_platform_audiences(industry)

        row = 4
        for platform_type in ["print", "digital", "hybrid"]:
            platforms = media_platforms.get(platform_type, [])
            if platforms:
                cell = ws_media.cell(row=row, column=2, value=f"{platform_type.upper()} PLATFORMS")
                cell.font = section_font
                cell.fill = section_fill
                cell.border = thin_border
                for c in range(3, 6):
                    ws_media.cell(row=row, column=c).fill = section_fill
                    ws_media.cell(row=row, column=c).border = thin_border
                row += 1
                for i, h in enumerate(["Platform Name", "Target Audience", "Audience Type", "Reach"]):
                    cell = ws_media.cell(row=row, column=2 + i, value=h)
                    cell.font = subheader_font
                    cell.fill = subheader_fill
                    cell.alignment = center_alignment
                    cell.border = thin_border
                row += 1
                audience_desc = audiences.get(platform_type, "Practitioners")
                for p in platforms:
                    style_body_cell(ws_media, row, 2, p)
                    style_body_cell(ws_media, row, 3, audience_desc)
                    style_body_cell(ws_media, row, 4, "Practitioners & Decision-Makers")
                    style_body_cell(ws_media, row, 5, "US + Global")
                    row += 1
                row += 1

    # Remove optional sheets the user didn't request
    # NOTE: "Executive Summary" and "Campaign Timeline" are ALWAYS included — never remove them.
    always_keep = {"Executive Summary", "Campaign Timeline"}
    optional_sheets = {
        "DEI & Diversity Channels": data.get("include_dei", False),
        "Innovative Channels 2025+": data.get("include_innovative", False),
        "Budget & Pricing Guide": data.get("include_budget_guide", False),
    }
    for sheet_name, included in optional_sheets.items():
        if sheet_name in always_keep:
            continue
        if not included and sheet_name in wb.sheetnames:
            wb.remove(wb[sheet_name])

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output.getvalue()


class MediaPlanHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {format % args}", file=sys.stderr)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/" or parsed.path == "":
            self._serve_file(os.path.join(TEMPLATES_DIR, "index.html"), "text/html")
        elif parsed.path == "/api/channels":
            db = load_channels_db()
            # Inject the full industry options list for frontend consumption
            db["industry_options"] = [
                {"value": "healthcare_medical", "label": "Healthcare & Medical"},
                {"value": "blue_collar_trades", "label": "Blue Collar / Skilled Trades"},
                {"value": "maritime_marine", "label": "Maritime & Marine"},
                {"value": "military_recruitment", "label": "Military Recruitment"},
                {"value": "tech_engineering", "label": "Technology & Engineering"},
                {"value": "general_entry_level", "label": "General / Entry-Level"},
                {"value": "legal_services", "label": "Legal Services"},
                {"value": "finance_banking", "label": "Finance & Banking"},
                {"value": "mental_health", "label": "Mental Health & Behavioral"},
                {"value": "retail_consumer", "label": "Retail & Consumer"},
                {"value": "aerospace_defense", "label": "Aerospace & Defense"},
                {"value": "pharma_biotech", "label": "Pharma & Biotech"},
            ]
            self._send_json(db)
        else:
            self.send_error(404)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/generate":
            content_len = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_len)
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
                return
            excel_bytes = generate_excel(data)
            client_name = data.get("client_name", "Client").replace(" ", "_")
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            self.send_header("Content-Disposition", f'attachment; filename="{client_name}_Media_Plan.xlsx"')
            self.send_header("Content-Length", str(len(excel_bytes)))
            self.end_headers()
            self.wfile.write(excel_bytes)
        else:
            self.send_error(404)

    def _serve_file(self, filepath, content_type):
        try:
            with open(filepath, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_error(404)

    def _send_json(self, data):
        body = json.dumps(data).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 5001))
    server = HTTPServer(("0.0.0.0", port), MediaPlanHandler)
    print(f"AI Media Planner running at http://localhost:{port}", file=sys.stderr)
    server.serve_forever()
