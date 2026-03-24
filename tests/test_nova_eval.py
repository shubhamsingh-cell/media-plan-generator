#!/usr/bin/env python3
"""Nova AI Evaluation Framework.

Golden dataset of question-answer pairs to measure chatbot quality.
Run periodically to detect quality regressions.
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Golden test cases: (question, expected_keywords, min_quality_score)
GOLDEN_DATASET = [
    {
        "question": "What is the average CPC for LinkedIn job ads?",
        "expected_keywords": ["linkedin", "cpc", "cost"],
        "category": "salary_data",
        "min_quality": 0.5,
    },
    {
        "question": "Compare Indeed vs LinkedIn for software engineer hiring",
        "expected_keywords": ["indeed", "linkedin", "engineer"],
        "category": "channel_comparison",
        "min_quality": 0.5,
    },
    {
        "question": "What compliance rules apply to job ads in California?",
        "expected_keywords": ["california", "compliance"],
        "category": "compliance",
        "min_quality": 0.4,
    },
    {
        "question": "Create a $50,000 media plan for hiring nurses in Texas",
        "expected_keywords": ["nurse", "texas", "budget", "channel"],
        "category": "plan_generation",
        "min_quality": 0.5,
    },
    {
        "question": "What are the current hiring trends in tech?",
        "expected_keywords": ["tech", "hiring", "trend"],
        "category": "market_intelligence",
        "min_quality": 0.4,
    },
    {
        "question": "How much does Joveo spend on category advertising?",
        "expected_keywords": ["joveo", "spend", "category"],
        "category": "internal_data",
        "min_quality": 0.4,
    },
    {
        "question": "What is the best channel for blue collar hiring?",
        "expected_keywords": ["blue collar", "channel"],
        "category": "channel_recommendation",
        "min_quality": 0.5,
    },
    {
        "question": "Set my campaign budget to $75,000",
        "expected_keywords": ["budget", "75"],
        "category": "context_setting",
        "min_quality": 0.6,
    },
    {
        "question": "Show me salary benchmarks for data scientists in New York",
        "expected_keywords": ["salary", "data scientist", "new york"],
        "category": "salary_benchmark",
        "min_quality": 0.5,
    },
    {
        "question": "What recruitment channels have the lowest CPA?",
        "expected_keywords": ["cpa", "channel", "cost"],
        "category": "roi_analysis",
        "min_quality": 0.5,
    },
]


def evaluate_response(response_text: str, test_case: dict) -> dict:
    """Evaluate a single response against expected criteria."""
    if not response_text:
        return {"passed": False, "score": 0.0, "reason": "Empty response"}

    lower = response_text.lower()

    # Keyword coverage
    keywords = test_case["expected_keywords"]
    matches = sum(1 for kw in keywords if kw.lower() in lower)
    keyword_score = matches / len(keywords) if keywords else 0

    # Length quality (too short = bad)
    length_score = min(len(response_text) / 200, 1.0)

    # Refusal detection
    refusal_signals = [
        "i cannot",
        "i'm sorry",
        "i don't have",
        "unable to",
        "no data available",
    ]
    refusal_penalty = 0.5 if any(s in lower for s in refusal_signals) else 0

    # Combined score
    score = keyword_score * 0.5 + length_score * 0.3 + (1 - refusal_penalty) * 0.2

    passed = score >= test_case.get("min_quality", 0.5)

    return {
        "passed": passed,
        "score": round(score, 3),
        "keyword_coverage": f"{matches}/{len(keywords)}",
        "response_length": len(response_text),
        "refusal_detected": refusal_penalty > 0,
    }


def run_evaluation(base_url: str = "http://localhost:10000") -> dict:
    """Run full evaluation against golden dataset."""
    import urllib.request

    results = []
    passed = 0
    failed = 0

    print(f"\n{'='*60}")
    print(f"  NOVA AI EVALUATION FRAMEWORK")
    print(f"  Target: {base_url}")
    print(f"  Test cases: {len(GOLDEN_DATASET)}")
    print(f"{'='*60}\n")

    for i, test in enumerate(GOLDEN_DATASET):
        try:
            payload = json.dumps(
                {
                    "message": test["question"],
                    "conversation_history": [],
                }
            ).encode()

            req = urllib.request.Request(
                f"{base_url}/api/chat",
                data=payload,
                method="POST",
            )
            req.add_header("Content-Type", "application/json")

            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                response_text = data.get("response", "")

            evaluation = evaluate_response(response_text, test)
            evaluation["question"] = test["question"]
            evaluation["category"] = test["category"]
            results.append(evaluation)

            status = "PASS" if evaluation["passed"] else "FAIL"
            if evaluation["passed"]:
                passed += 1
            else:
                failed += 1

            print(
                f"  [{status}] {test['category']}: {evaluation['score']:.2f} ({evaluation['keyword_coverage']} keywords)"
            )

        except Exception as e:
            results.append(
                {
                    "question": test["question"],
                    "category": test["category"],
                    "passed": False,
                    "score": 0.0,
                    "error": str(e),
                }
            )
            failed += 1
            print(f"  [ERROR] {test['category']}: {e}")

    avg_score = sum(r.get("score", 0) for r in results) / len(results) if results else 0

    print(f"\n{'─'*60}")
    print(f"  Results: {passed}/{len(results)} passed")
    print(f"  Average quality score: {avg_score:.3f}")
    print(
        f"  Grade: {'A' if avg_score > 0.8 else 'B' if avg_score > 0.6 else 'C' if avg_score > 0.4 else 'D'}"
    )
    print(f"{'='*60}\n")

    return {
        "total": len(results),
        "passed": passed,
        "failed": failed,
        "avg_score": round(avg_score, 3),
        "results": results,
    }


if __name__ == "__main__":
    url = os.environ.get("TEST_BASE_URL", "http://localhost:10000")
    run_evaluation(url)
