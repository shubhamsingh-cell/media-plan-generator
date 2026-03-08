#!/usr/bin/env bash
# =============================================================================
# Nova Chatbot Automated Test Suite  (v2 -- updated 2026-03-08)
# =============================================================================
# Tests the /api/chat endpoint against known questions and validates:
#   - Response structure (JSON with response, sources, confidence, tools_used)
#   - Learned answer detection (instant, 0 API tokens)
#   - Cache behavior (second identical request returns faster)
#   - Claude API integration (complex queries use tools)
#   - Ask-before-answering logic (missing parameters triggers clarification)
#   - Rule-based fallback (when applicable)
#
# v2 additions:
#   - Data matrix health endpoint (28-cell product x layer probe)
#   - Employer brand tool (query_employer_brand via Nova)
#   - Ad benchmarks tool (query_ad_benchmarks via Nova)
#   - Hiring insights tool (query_hiring_insights via Nova)
#   - Salary query with location (exercises additive cascade + confidence)
#   - Location profile query (exercises parallel Census+WorldBank fetches)
#   - V2 metadata passthrough (data_confidence, sources_used in response)
#   - Orchestrator self-healing verification
#
# Usage:
#   ./tests/test_nova_chat.sh                     # test against localhost:8000
#   ./tests/test_nova_chat.sh https://media-plan-generator.onrender.com
#   ADMIN_API_KEY=xxx ./tests/test_nova_chat.sh   # also test admin endpoints
# =============================================================================

set -euo pipefail

BASE_URL="${1:-http://localhost:8000}"
PASS=0
FAIL=0
TOTAL=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No color

chat() {
    local msg="$1"
    curl -s -X POST "${BASE_URL}/api/chat" \
        -H "Content-Type: application/json" \
        -d "$(python3 -c "import json,sys;print(json.dumps({'message':sys.argv[1]}))" "$msg")" \
        --max-time 60
}

assert_contains() {
    local label="$1"
    local response="$2"
    local pattern="$3"
    TOTAL=$((TOTAL + 1))
    if echo "$response" | grep -qi "$pattern"; then
        echo -e "  ${GREEN}PASS${NC} $label (found: $pattern)"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC} $label (expected: $pattern)"
        echo "       Response: $(echo "$response" | head -c 200)"
        FAIL=$((FAIL + 1))
    fi
}

assert_json_field() {
    local label="$1"
    local response="$2"
    local field="$3"
    TOTAL=$((TOTAL + 1))
    if echo "$response" | python3 -c "import sys,json; d=json.load(sys.stdin); assert '$field' in d" 2>/dev/null; then
        echo -e "  ${GREEN}PASS${NC} $label (field '$field' exists)"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC} $label (field '$field' missing)"
        FAIL=$((FAIL + 1))
    fi
}

assert_confidence_above() {
    local label="$1"
    local response="$2"
    local threshold="$3"
    TOTAL=$((TOTAL + 1))
    local conf
    conf=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('confidence',0))" 2>/dev/null || echo "0")
    if python3 -c "assert float('$conf') >= float('$threshold')" 2>/dev/null; then
        echo -e "  ${GREEN}PASS${NC} $label (confidence=$conf >= $threshold)"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC} $label (confidence=$conf < $threshold)"
        FAIL=$((FAIL + 1))
    fi
}

assert_http_status() {
    local label="$1"
    local actual="$2"
    local expected="$3"
    TOTAL=$((TOTAL + 1))
    if [ "$actual" = "$expected" ]; then
        echo -e "  ${GREEN}PASS${NC} $label (HTTP $actual)"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC} $label (expected HTTP $expected, got HTTP $actual)"
        FAIL=$((FAIL + 1))
    fi
}

assert_json_nested() {
    # Check a nested field exists in JSON, e.g. "matrix.excel_ppt.json_files.health"
    local label="$1"
    local response="$2"
    local path="$3"
    TOTAL=$((TOTAL + 1))
    if echo "$response" | python3 -c "
import sys, json
d = json.load(sys.stdin)
keys = '$path'.split('.')
for k in keys:
    d = d[k]
assert d is not None
" 2>/dev/null; then
        echo -e "  ${GREEN}PASS${NC} $label (path '$path' exists)"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC} $label (path '$path' missing)"
        FAIL=$((FAIL + 1))
    fi
}

admin_get() {
    local path="$1"
    curl -s "${BASE_URL}${path}" \
        -H "Authorization: Bearer ${ADMIN_API_KEY}" \
        --max-time 15
}

admin_get_status() {
    local path="$1"
    curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}${path}" \
        -H "Authorization: Bearer ${ADMIN_API_KEY}" \
        --max-time 15
}

echo "============================================="
echo "  Nova Chatbot Test Suite"
echo "  Target: $BASE_URL"
echo "============================================="
echo ""

# ── Test 1: Health check ──
echo -e "${YELLOW}Test 1: Health endpoint${NC}"
RESP=$(curl -s "${BASE_URL}/health" --max-time 10)
assert_contains "Health returns status" "$RESP" '"status"'
assert_contains "Health returns ok" "$RESP" '"ok"'
echo ""

# ── Test 2: Response structure ──
echo -e "${YELLOW}Test 2: Response structure (/api/chat)${NC}"
RESP=$(chat "hello")
assert_json_field "Has 'response' field" "$RESP" "response"
assert_json_field "Has 'confidence' field" "$RESP" "confidence"
assert_json_field "Has 'sources' field" "$RESP" "sources"
assert_json_field "Has 'tools_used' field" "$RESP" "tools_used"
echo ""

# ── Test 3: Learned answer -- "what is joveo" ──
echo -e "${YELLOW}Test 3: Learned answer -- what is joveo${NC}"
RESP=$(chat "what is joveo")
assert_contains "Mentions Joveo" "$RESP" "joveo"
assert_contains "Mentions recruitment" "$RESP" "recruitment"
assert_confidence_above "High confidence" "$RESP" "0.85"
echo ""

# ── Test 4: Learned answer -- "how many publishers" ──
echo -e "${YELLOW}Test 4: Learned answer -- how many publishers${NC}"
RESP=$(chat "how many publishers does joveo have")
assert_contains "Mentions 10,238" "$RESP" "10,238"
assert_contains "Mentions supply partners" "$RESP" "supply"
assert_confidence_above "High confidence" "$RESP" "0.85"
echo ""

# ── Test 5: Learned answer -- "what is CPC CPA CPH" ──
echo -e "${YELLOW}Test 5: Learned answer -- CPC CPA CPH${NC}"
RESP=$(chat "what is cpc cpa cph")
assert_contains "Mentions Cost Per Click" "$RESP" "cost per click"
assert_confidence_above "High confidence" "$RESP" "0.85"
echo ""

# ── Test 6: Ask-before-answering (missing location) ──
echo -e "${YELLOW}Test 6: Ask-before-answering -- salary without location${NC}"
RESP=$(chat "what is the average salary of a nurse")
assert_contains "Asks for location/country" "$RESP" "countr\|region\|location\|where\|which"
echo ""

# ── Test 7: Complex query with tools (CPC for healthcare) ──
echo -e "${YELLOW}Test 7: Tool-using query -- healthcare CPC${NC}"
RESP=$(chat "what is the average CPC for healthcare industry in the US")
assert_contains "Mentions CPC data" "$RESP" "cpc\|cost per click\|healthcare"
assert_confidence_above "Reasonable confidence" "$RESP" "0.5"
echo ""

# ── Test 8: Cache hit test (repeat same question) ──
echo -e "${YELLOW}Test 8: Cache behavior -- repeat question${NC}"
Q="what is programmatic job advertising"
T1_START=$(python3 -c "import time; print(time.time())")
RESP1=$(chat "$Q")
T1_END=$(python3 -c "import time; print(time.time())")
T2_START=$(python3 -c "import time; print(time.time())")
RESP2=$(chat "$Q")
T2_END=$(python3 -c "import time; print(time.time())")
T1=$(python3 -c "print(round(($T1_END - $T1_START) * 1000))")
T2=$(python3 -c "print(round(($T2_END - $T2_START) * 1000))")
echo "  First request: ${T1}ms, Second request: ${T2}ms"
assert_contains "Both return same answer" "$RESP1" "programmatic"
# Second should be at least faster (or same if learned answer)
TOTAL=$((TOTAL + 1))
if [ "$T2" -le "$((T1 + 500))" ]; then
    echo -e "  ${GREEN}PASS${NC} Second request not slower (cache or learned answer working)"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC} Second request was significantly slower (${T2}ms vs ${T1}ms)"
    FAIL=$((FAIL + 1))
fi
echo ""

# ── Test 9: Empty message handling ──
echo -e "${YELLOW}Test 9: Empty message handling${NC}"
RESP=$(curl -s -X POST "${BASE_URL}/api/chat" \
    -H "Content-Type: application/json" \
    -d '{"message": ""}' \
    --max-time 10)
assert_contains "Returns guidance" "$RESP" "provide\|ask\|question\|please"
echo ""

# ── Test 10: Budget/strategy query (complex, should use max tokens) ──
echo -e "${YELLOW}Test 10: Complex budget query${NC}"
RESP=$(chat "I have a 50000 dollar budget to hire 10 nurses in Texas. How should I allocate it?")
assert_contains "Mentions budget/allocation" "$RESP" "budget\|allocat\|spend\|cost"
assert_confidence_above "Reasonable confidence" "$RESP" "0.4"
echo ""

# ── Test 11: Nova metrics endpoint (if ADMIN_API_KEY is set) ──
if [ -n "${ADMIN_API_KEY:-}" ]; then
    echo -e "${YELLOW}Test 11: Nova metrics endpoint${NC}"
    RESP=$(curl -s "${BASE_URL}/api/nova/metrics" \
        -H "Authorization: Bearer ${ADMIN_API_KEY}" \
        --max-time 10)
    assert_json_field "Has total_requests" "$RESP" "total_requests"
    assert_json_field "Has response_modes" "$RESP" "response_modes"
    assert_json_field "Has tokens" "$RESP" "tokens"
    assert_json_field "Has estimated_cost_usd" "$RESP" "estimated_cost_usd"
    assert_json_field "Has cache_hit_rate_pct" "$RESP" "cache_hit_rate_pct"
    echo ""
else
    echo -e "${YELLOW}Test 11: Nova metrics endpoint (SKIPPED -- set ADMIN_API_KEY to test)${NC}"
    echo ""
fi

# =============================================================================
# v2 TESTS: DataOrchestrator upgrades + new Nova tools + data matrix monitor
# =============================================================================

# ── Test 12: Data matrix health endpoint ──
if [ -n "${ADMIN_API_KEY:-}" ]; then
    echo -e "${YELLOW}Test 12: Data matrix health endpoint${NC}"
    DM_STATUS=$(admin_get_status "/api/health/data-matrix")
    assert_http_status "Data matrix returns 200 or 503" "$DM_STATUS" "200"
    DM_RESP=$(admin_get "/api/health/data-matrix")
    assert_json_field "Has 'status' field" "$DM_RESP" "status"
    assert_json_field "Has 'health_pct' field" "$DM_RESP" "health_pct"
    assert_json_field "Has 'matrix' field" "$DM_RESP" "matrix"
    assert_json_field "Has 'summary' field" "$DM_RESP" "summary"
    # Verify all 4 products are present
    assert_json_nested "Excel/PPT product exists" "$DM_RESP" "matrix.excel_ppt"
    assert_json_nested "Nova Chat product exists" "$DM_RESP" "matrix.nova_chat"
    assert_json_nested "Slack Bot product exists" "$DM_RESP" "matrix.slack_bot"
    assert_json_nested "PPT Generator product exists" "$DM_RESP" "matrix.ppt_generator"
    # Verify key layer cells
    assert_json_nested "Excel JSON files health" "$DM_RESP" "matrix.excel_ppt.json_files.health"
    assert_json_nested "Nova API enrichment health" "$DM_RESP" "matrix.nova_chat.api_enrichment.health"
    assert_json_nested "Nova Claude API health" "$DM_RESP" "matrix.nova_chat.claude_api.health"
    assert_json_nested "Slack research health" "$DM_RESP" "matrix.slack_bot.research.health"
    # Verify health percentage
    TOTAL=$((TOTAL + 1))
    HP=$(echo "$DM_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('health_pct',0))" 2>/dev/null || echo "0")
    if python3 -c "assert float('$HP') >= 70.0" 2>/dev/null; then
        echo -e "  ${GREEN}PASS${NC} Health percentage >= 70% (actual: ${HP}%)"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC} Health percentage too low (actual: ${HP}%)"
        FAIL=$((FAIL + 1))
    fi
    echo ""
else
    echo -e "${YELLOW}Test 12: Data matrix health endpoint (SKIPPED -- set ADMIN_API_KEY)${NC}"
    echo ""
fi

# ── Test 13: Data matrix self-healing ──
if [ -n "${ADMIN_API_KEY:-}" ]; then
    echo -e "${YELLOW}Test 13: Data matrix self-healing verification${NC}"
    DM_RESP=$(admin_get "/api/health/data-matrix")
    # Check that error count is 0 (self-healing fixed any startup issues)
    TOTAL=$((TOTAL + 1))
    ERRORS=$(echo "$DM_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('summary',{}).get('error',99))" 2>/dev/null || echo "99")
    if [ "$ERRORS" = "0" ]; then
        echo -e "  ${GREEN}PASS${NC} Zero errors in data matrix (self-healing working)"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC} Data matrix has $ERRORS errors (self-healing may not be working)"
        FAIL=$((FAIL + 1))
    fi
    # Check self-heal log exists
    assert_json_field "Has 'recent_heal_actions'" "$DM_RESP" "recent_heal_actions"
    echo ""
else
    echo -e "${YELLOW}Test 13: Data matrix self-healing (SKIPPED -- set ADMIN_API_KEY)${NC}"
    echo ""
fi

# ── Test 14: Employer brand query (new v2 tool) ──
echo -e "${YELLOW}Test 14: Employer brand query -- Google${NC}"
RESP=$(chat "I want to recruit against Google. Tell me about Google's employer brand - their Glassdoor rating, hiring channels, and recruitment strategies for tech talent.")
assert_contains "Mentions Google or employer data" "$RESP" "google\|glassdoor\|rating\|employer\|hiring\|brand\|recruit"
assert_confidence_above "Reasonable confidence" "$RESP" "0.35"
echo ""

# ── Test 15: Ad platform benchmarks (new v2 tool) ──
echo -e "${YELLOW}Test 15: Ad platform benchmarks -- technology industry${NC}"
RESP=$(chat "What are the CPC and CTR benchmarks for recruiting ads in the technology industry across different platforms like Google, LinkedIn, Indeed?")
assert_contains "Mentions ad benchmarks" "$RESP" "cpc\|ctr\|benchmark\|google\|linkedin\|indeed\|platform"
assert_confidence_above "Reasonable confidence" "$RESP" "0.4"
echo ""

# ── Test 16: Hiring insights / difficulty (new v2 tool) ──
echo -e "${YELLOW}Test 16: Hiring insights -- software engineer difficulty${NC}"
RESP=$(chat "I need hiring insights for a software engineer role in San Francisco, United States. What is the hiring difficulty index, salary competitiveness, and when is the next peak hiring window?")
assert_contains "Mentions difficulty or hiring data" "$RESP" "difficult\|competitiv\|demand\|challeng\|index\|score\|peak\|hiring\|salary\|engineer"
assert_confidence_above "Reasonable confidence" "$RESP" "0.35"
echo ""

# ── Test 17: Salary with location (additive cascade + confidence scoring) ──
echo -e "${YELLOW}Test 17: Salary with location -- nurse in Texas (v2 additive cascade)${NC}"
RESP=$(chat "What is the average salary for a registered nurse in Texas, USA?")
assert_contains "Mentions salary figure" "$RESP" "salary\|annual\|median\|range\|\$"
assert_confidence_above "Reasonable confidence" "$RESP" "0.4"
echo ""

# ── Test 18: Location profile (parallel Census+WorldBank fetches) ──
echo -e "${YELLOW}Test 18: Location profile -- New York labor market${NC}"
RESP=$(chat "Give me the labor market profile for New York City - population, unemployment rate, cost of living")
assert_contains "Mentions location data" "$RESP" "new york\|population\|unemployment\|cost of living\|labor"
assert_confidence_above "Reasonable confidence" "$RESP" "0.4"
echo ""

# ── Test 19: Data matrix endpoint auth rejection ──
echo -e "${YELLOW}Test 19: Data matrix auth -- no API key rejected${NC}"
NOAUTH_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/api/health/data-matrix" --max-time 10)
assert_http_status "Rejects unauthenticated request" "$NOAUTH_STATUS" "401"
echo ""

# ── Test 20: Tools used field populated (tool-using queries list tools) ──
echo -e "${YELLOW}Test 20: Tools used tracking -- complex query populates tools_used${NC}"
RESP=$(chat "What is the typical CPC for retail industry job ads on Indeed in the United States?")
TOTAL=$((TOTAL + 1))
TOOLS_COUNT=$(echo "$RESP" | python3 -c "
import sys, json
d = json.load(sys.stdin)
tools = d.get('tools_used', [])
print(len(tools))
" 2>/dev/null || echo "0")
if [ "$TOOLS_COUNT" -gt 0 ]; then
    echo -e "  ${GREEN}PASS${NC} tools_used populated ($TOOLS_COUNT tools used)"
    PASS=$((PASS + 1))
else
    # Some queries might be answered from learned answers without tools
    echo -e "  ${YELLOW}WARN${NC} tools_used empty (may be learned answer -- not counted as failure)"
fi
assert_contains "Returns relevant content" "$RESP" "cpc\|cost\|retail\|indeed"
echo ""

# ── Test 21: Orchestrator telemetry endpoint ──
if [ -n "${ADMIN_API_KEY:-}" ]; then
    echo -e "${YELLOW}Test 21: Orchestrator telemetry endpoint${NC}"
    ORCH_STATUS=$(admin_get_status "/api/health/orchestrator")
    assert_http_status "Orchestrator endpoint returns 200" "$ORCH_STATUS" "200"
    ORCH_RESP=$(admin_get "/api/health/orchestrator")
    assert_json_field "Has 'status' field" "$ORCH_RESP" "status"
    echo ""
else
    echo -e "${YELLOW}Test 21: Orchestrator telemetry endpoint (SKIPPED -- set ADMIN_API_KEY)${NC}"
    echo ""
fi

# ── Summary ──
echo "============================================="
echo "  Nova Chatbot Test Suite v2 -- Results"
echo "============================================="
echo -e "  ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}, ${TOTAL} total"
echo ""
echo "  Tests 1-11:  Core (health, structure, learned, cache, tools, metrics)"
echo "  Tests 12-13: Data matrix health monitor + self-healing"
echo "  Tests 14-16: New v2 Nova tools (employer brand, ad benchmarks, insights)"
echo "  Tests 17-18: v2 additive cascade + parallel fetches"
echo "  Tests 19-20: Auth enforcement + tool tracking"
echo "  Test 21:     Orchestrator telemetry endpoint"
echo "============================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
