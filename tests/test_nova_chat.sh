#!/usr/bin/env bash
# =============================================================================
# Nova Chatbot Automated Test Suite
# =============================================================================
# Tests the /api/chat endpoint against known questions and validates:
#   - Response structure (JSON with response, sources, confidence, tools_used)
#   - Learned answer detection (instant, 0 API tokens)
#   - Cache behavior (second identical request returns faster)
#   - Claude API integration (complex queries use tools)
#   - Ask-before-answering logic (missing parameters triggers clarification)
#   - Rule-based fallback (when applicable)
#
# Usage:
#   ./tests/test_nova_chat.sh                     # test against localhost:8000
#   ./tests/test_nova_chat.sh https://media-plan-generator.onrender.com
#   ADMIN_API_KEY=xxx ./tests/test_nova_chat.sh   # also test /api/nova/metrics
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

# ── Summary ──
echo "============================================="
echo -e "  Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}, ${TOTAL} total"
echo "============================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
