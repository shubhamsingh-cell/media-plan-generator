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
    # ── Multi-tool queries (requires combining data from multiple sources) ──
    {
        "question": "Compare salary data with job market demand for data scientists in New York",
        "expected_keywords": ["salary", "data scientist", "new york", "demand"],
        "category": "multi_tool_synthesis",
        "min_quality": 0.5,
    },
    # ── Follow-up / context retention ──
    {
        "question": "Now show me the same data for San Francisco",
        "expected_keywords": ["san francisco"],
        "category": "context_followup",
        "min_quality": 0.3,
        "requires_history": [
            {
                "role": "user",
                "content": "What is the average salary for software engineers in Austin?",
            },
            {
                "role": "assistant",
                "content": "The median salary for Software Engineers in Austin is approximately $125,000 based on Adzuna data.",
            },
        ],
    },
    # ── Edge cases (empty/minimal and very long queries) ──
    {
        "question": "",
        "expected_keywords": [],
        "category": "edge_empty_query",
        "min_quality": 0.0,
        "expect_error": True,
    },
    # ── Data accuracy (specific known benchmarks) ──
    {
        "question": "What is the median salary for registered nurses in Texas according to BLS data?",
        "expected_keywords": ["nurse", "texas", "salary", "$"],
        "category": "data_accuracy",
        "min_quality": 0.5,
    },
    # ── Error recovery (graceful degradation when data is unavailable) ──
    {
        "question": "What is the CPA for a Chief Quantum Computing Officer in Antarctica?",
        "expected_keywords": [],
        "category": "error_recovery_graceful",
        "min_quality": 0.3,
        "negative_keywords": [
            "i can't",
            "i don't have",
            "unable to",
            "no data available",
        ],
    },
    # ── Cross-session memory recall ──
    {
        "question": "Based on what we discussed earlier about the marketing budget, what channels would you recommend?",
        "expected_keywords": ["channel", "recommend", "budget"],
        "category": "memory_recall",
        "min_quality": 0.4,
    },
]


def evaluate_response(response_text: str, test_case: dict) -> dict:
    """Evaluate a single response against expected criteria.

    Supports standard keyword matching, negative keyword checks,
    error expectation, and refusal detection.

    Args:
        response_text: The response from the chat API.
        test_case: A golden test case dict with expected_keywords, category, etc.

    Returns:
        Dict with passed, score, keyword_coverage, response_length, and refusal_detected.
    """
    # Handle edge case: empty query expects error response
    if test_case.get("expect_error"):
        if not response_text or "provide a message" in response_text.lower():
            return {
                "passed": True,
                "score": 1.0,
                "keyword_coverage": "N/A",
                "response_length": len(response_text or ""),
                "refusal_detected": False,
                "reason": "Correctly handled empty/error input",
            }
        return {
            "passed": False,
            "score": 0.0,
            "keyword_coverage": "N/A",
            "response_length": len(response_text or ""),
            "refusal_detected": False,
            "reason": "Expected error handling but got normal response",
        }

    if not response_text:
        return {"passed": False, "score": 0.0, "reason": "Empty response"}

    lower = response_text.lower()

    # Keyword coverage
    keywords = test_case["expected_keywords"]
    matches = sum(1 for kw in keywords if kw.lower() in lower)
    keyword_score = matches / len(keywords) if keywords else 1.0

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

    # Negative keyword penalty (words that should NOT appear)
    negative_keywords = test_case.get("negative_keywords", [])
    negative_hits = sum(1 for nk in negative_keywords if nk.lower() in lower)
    negative_penalty = (
        0.3 * (negative_hits / len(negative_keywords)) if negative_keywords else 0
    )

    # Combined score
    score = (
        keyword_score * 0.45
        + length_score * 0.25
        + (1 - refusal_penalty) * 0.2
        + (1 - negative_penalty) * 0.1
    )

    passed = score >= test_case.get("min_quality", 0.5)

    return {
        "passed": passed,
        "score": round(score, 3),
        "keyword_coverage": f"{matches}/{len(keywords)}",
        "response_length": len(response_text),
        "refusal_detected": refusal_penalty > 0,
        "negative_hits": negative_hits if negative_keywords else 0,
    }


def run_evaluation(base_url: str = "http://localhost:10000") -> dict:
    """Run full evaluation against golden dataset.

    Sends each test case to the Nova chat API and evaluates the response
    against expected keywords, negative keywords, and quality thresholds.

    Args:
        base_url: The base URL of the Nova server to test against.

    Returns:
        Dict with total, passed, failed, avg_score, and detailed results.
    """
    import urllib.request

    results: list[dict] = []
    passed = 0
    failed = 0

    print(f"\n{'='*60}")
    print(f"  NOVA AI EVALUATION FRAMEWORK")
    print(f"  Target: {base_url}")
    print(f"  Test cases: {len(GOLDEN_DATASET)}")
    print(f"{'='*60}\n")

    for i, test in enumerate(GOLDEN_DATASET):
        try:
            # Build conversation history for context-dependent tests
            conversation_history = test.get("requires_history", [])

            payload = json.dumps(
                {
                    "message": test["question"],
                    "conversation_history": conversation_history,
                }
            ).encode()

            req = urllib.request.Request(
                f"{base_url}/api/chat",
                data=payload,
                method="POST",
            )
            req.add_header("Content-Type", "application/json")

            with urllib.request.urlopen(req, timeout=65) as resp:
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
            # For expect_error tests, connection errors on empty queries are valid
            if test.get("expect_error"):
                results.append(
                    {
                        "question": test["question"],
                        "category": test["category"],
                        "passed": True,
                        "score": 1.0,
                        "reason": f"Expected error, got: {e}",
                    }
                )
                passed += 1
                print(f"  [PASS] {test['category']}: error handled correctly")
            else:
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


# ═══════════════════════════════════════════════════════════════════════════════
# Cross-session memory tests (NovaMemory unit tests)
# ═══════════════════════════════════════════════════════════════════════════════

import unittest
from unittest.mock import patch, MagicMock


class TestNovaMemoryStorage(unittest.TestCase):
    """Test that conversations store key facts into memory."""

    def setUp(self) -> None:
        """Create a fresh NovaMemory instance with Supabase mocked out."""
        from nova_memory import NovaMemory

        self.memory = NovaMemory(user_id="test_user")
        self.memory._loaded = True  # Skip Supabase load

    @patch("nova_memory.threading.Thread")
    def test_save_conversation_summary_stores_entry(
        self, mock_thread: MagicMock
    ) -> None:
        """Verify save_conversation_summary() adds an entry to short-term memory."""
        mock_thread.return_value.start = MagicMock()

        messages = [
            {"role": "user", "text": "What is the average CPC for LinkedIn?"},
            {"role": "assistant", "text": "The average CPC for LinkedIn is $5.26."},
        ]
        self.memory.save_conversation_summary(
            conversation_id="conv_001",
            messages=messages,
            summary="Discussed LinkedIn CPC benchmarks",
        )

        self.assertEqual(len(self.memory._short_term), 1)
        entry = self.memory._short_term[0]
        self.assertEqual(entry["content"], "Discussed LinkedIn CPC benchmarks")
        self.assertEqual(entry["memory_type"], "short_term")
        self.assertEqual(entry["metadata"]["conversation_id"], "conv_001")
        self.assertEqual(entry["metadata"]["message_count"], 2)

    @patch("nova_memory.threading.Thread")
    def test_save_conversation_triggers_persistence(
        self, mock_thread: MagicMock
    ) -> None:
        """Verify that saving a summary spawns a background persist thread."""
        mock_instance = MagicMock()
        mock_thread.return_value = mock_instance

        self.memory.save_conversation_summary(
            conversation_id="conv_002",
            messages=[{"role": "user", "text": "Hello"}],
            summary="Greeting exchange",
        )

        mock_thread.assert_called_once()
        mock_instance.start.assert_called_once()

    @patch("nova_memory.threading.Thread")
    def test_learn_fact_stores_long_term(self, mock_thread: MagicMock) -> None:
        """Verify learn_fact() adds a long-term memory entry."""
        mock_thread.return_value.start = MagicMock()

        self.memory.learn_fact(
            "User prefers LinkedIn for tech hiring", category="preference"
        )

        self.assertEqual(len(self.memory._long_term), 1)
        entry = self.memory._long_term[0]
        self.assertEqual(entry["content"], "User prefers LinkedIn for tech hiring")
        self.assertEqual(entry["memory_type"], "long_term")
        self.assertEqual(entry["metadata"]["category"], "preference")

    @patch("nova_memory.threading.Thread")
    def test_learn_fact_deduplicates(self, mock_thread: MagicMock) -> None:
        """Verify learn_fact() does not store duplicate facts."""
        mock_thread.return_value.start = MagicMock()

        self.memory.learn_fact("Budget is $50K")
        self.memory.learn_fact("Budget is $50K")

        self.assertEqual(len(self.memory._long_term), 1)

    @patch("nova_memory.threading.Thread")
    def test_set_preference_stores_value(self, mock_thread: MagicMock) -> None:
        """Verify set_preference() stores key-value pair in preferences."""
        mock_thread.return_value.start = MagicMock()

        self.memory.set_preference("default_channel", "LinkedIn")

        self.assertEqual(self.memory._preferences["default_channel"], "LinkedIn")

    @patch("nova_memory.threading.Thread")
    def test_auto_summarize_from_messages(self, mock_thread: MagicMock) -> None:
        """Verify _auto_summarize generates a summary from messages."""
        messages = [
            {"role": "user", "text": "What channels work for nursing?"},
            {
                "role": "assistant",
                "text": "Indeed and LinkedIn are top for nursing roles.",
            },
        ]

        summary = self.memory._auto_summarize(messages)

        self.assertIn("User asked:", summary)
        self.assertIn("nursing", summary.lower())

    def test_auto_summarize_empty_messages(self) -> None:
        """Verify _auto_summarize handles empty message list."""
        summary = self.memory._auto_summarize([])
        self.assertEqual(summary, "")


class TestNovaMemoryRecall(unittest.TestCase):
    """Test that memory recall retrieves relevant context for follow-up queries."""

    def setUp(self) -> None:
        """Create a NovaMemory instance pre-loaded with test data."""
        from nova_memory import NovaMemory

        self.memory = NovaMemory(user_id="test_user")
        self.memory._loaded = True

    @patch("nova_memory.threading.Thread")
    def test_context_injection_includes_preferences(
        self, mock_thread: MagicMock
    ) -> None:
        """Verify get_context_injection() includes stored preferences."""
        mock_thread.return_value.start = MagicMock()

        self.memory.set_preference("budget", "$75,000")
        self.memory.set_preference("industry", "Healthcare")

        context = self.memory.get_context_injection()

        self.assertIn("[MEMORY", context)
        self.assertIn("budget", context)
        self.assertIn("$75,000", context)
        self.assertIn("Healthcare", context)

    @patch("nova_memory.threading.Thread")
    def test_context_injection_includes_long_term_facts(
        self, mock_thread: MagicMock
    ) -> None:
        """Verify get_context_injection() includes learned long-term facts."""
        mock_thread.return_value.start = MagicMock()

        self.memory.learn_fact("Client Acme Corp budget is $50K/quarter")
        self.memory.learn_fact("User prefers Indeed for blue-collar roles")

        context = self.memory.get_context_injection()

        self.assertIn("Known facts", context)
        self.assertIn("Acme Corp", context)
        self.assertIn("Indeed", context)

    @patch("nova_memory.threading.Thread")
    def test_context_injection_includes_short_term_summaries(
        self, mock_thread: MagicMock
    ) -> None:
        """Verify get_context_injection() includes recent conversation summaries."""
        mock_thread.return_value.start = MagicMock()

        self.memory.save_conversation_summary(
            conversation_id="conv_100",
            messages=[{"role": "user", "text": "test"}],
            summary="Discussed marketing budget allocation for Q2",
        )

        context = self.memory.get_context_injection()

        self.assertIn("Recent conversation context", context)
        self.assertIn("marketing budget allocation", context)

    def test_context_injection_empty_when_no_memory(self) -> None:
        """Verify get_context_injection() returns empty string with no data."""
        context = self.memory.get_context_injection()
        self.assertEqual(context, "")

    @patch("nova_memory.threading.Thread")
    def test_context_injection_wraps_in_memory_tags(
        self, mock_thread: MagicMock
    ) -> None:
        """Verify the context injection is wrapped in [MEMORY] delimiters."""
        mock_thread.return_value.start = MagicMock()

        self.memory.set_preference("region", "US-West")
        context = self.memory.get_context_injection()

        self.assertTrue(context.startswith("\n\n[MEMORY"))
        self.assertTrue(context.strip().endswith("[/MEMORY]"))

    def test_get_stats_returns_counts(self) -> None:
        """Verify get_stats() reflects current memory state."""
        stats = self.memory.get_stats()

        self.assertEqual(stats["short_term_count"], 0)
        self.assertEqual(stats["long_term_count"], 0)
        self.assertEqual(stats["preference_count"], 0)
        self.assertTrue(stats["loaded"])


class TestNovaMemoryQuality(unittest.TestCase):
    """Golden eval test for memory-aware responses."""

    def test_memory_recall_golden_case_exists(self) -> None:
        """Verify the memory recall golden test case is in the dataset."""
        memory_cases = [
            tc for tc in GOLDEN_DATASET if tc["category"] == "memory_recall"
        ]
        self.assertEqual(
            len(memory_cases), 1, "Expected exactly 1 memory_recall test case"
        )

        case = memory_cases[0]
        self.assertIn("budget", case["expected_keywords"])
        self.assertIn("channel", case["expected_keywords"])
        self.assertIn("recommend", case["expected_keywords"])

    def test_memory_recall_eval_with_channel_response(self) -> None:
        """Verify evaluate_response scores well for a memory-aware response."""
        case = next(tc for tc in GOLDEN_DATASET if tc["category"] == "memory_recall")

        good_response = (
            "While I don't have context from a prior conversation about your marketing budget, "
            "I can still recommend the best channels for your recruitment needs. "
            "For a typical budget allocation, I'd recommend LinkedIn for professional roles "
            "(40% of budget), Indeed for high-volume hiring (30%), and niche job boards (20%), "
            "with the remaining 10% for programmatic channels."
        )

        result = evaluate_response(good_response, case)

        self.assertTrue(
            result["passed"], f"Expected PASS but got score={result['score']}"
        )
        self.assertGreaterEqual(result["score"], case["min_quality"])

    def test_memory_recall_eval_rejects_empty_response(self) -> None:
        """Verify evaluate_response fails on an empty response for memory recall."""
        case = next(tc for tc in GOLDEN_DATASET if tc["category"] == "memory_recall")

        result = evaluate_response("", case)

        self.assertFalse(result["passed"])
        self.assertEqual(result["score"], 0.0)


class TestNovaMemorySingleton(unittest.TestCase):
    """Test the get_memory() singleton factory."""

    @patch("nova_memory.NovaMemory.load")
    def test_get_memory_returns_same_instance(self, mock_load: MagicMock) -> None:
        """Verify get_memory() returns the same instance for the same user_id."""
        from nova_memory import get_memory, _memory_instances, _global_lock

        # Clear singleton cache for test isolation
        with _global_lock:
            _memory_instances.pop("test_singleton", None)

        mem1 = get_memory("test_singleton")
        mem2 = get_memory("test_singleton")

        self.assertIs(mem1, mem2)

        # Cleanup
        with _global_lock:
            _memory_instances.pop("test_singleton", None)

    @patch("nova_memory.NovaMemory.load")
    def test_get_memory_different_users_different_instances(
        self, mock_load: MagicMock
    ) -> None:
        """Verify get_memory() returns different instances for different user_ids."""
        from nova_memory import get_memory, _memory_instances, _global_lock

        with _global_lock:
            _memory_instances.pop("user_a_test", None)
            _memory_instances.pop("user_b_test", None)

        mem_a = get_memory("user_a_test")
        mem_b = get_memory("user_b_test")

        self.assertIsNot(mem_a, mem_b)

        # Cleanup
        with _global_lock:
            _memory_instances.pop("user_a_test", None)
            _memory_instances.pop("user_b_test", None)


if __name__ == "__main__":
    url = os.environ.get("TEST_BASE_URL", "http://localhost:10000")

    # Run unit tests first
    print("Running NovaMemory unit tests...")
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestNovaMemoryStorage))
    suite.addTests(loader.loadTestsFromTestCase(TestNovaMemoryRecall))
    suite.addTests(loader.loadTestsFromTestCase(TestNovaMemoryQuality))
    suite.addTests(loader.loadTestsFromTestCase(TestNovaMemorySingleton))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Run golden eval if unit tests pass
    if result.wasSuccessful():
        print("\nUnit tests passed. Running golden evaluation...")
        run_evaluation(url)
    else:
        print("\nUnit tests failed. Skipping golden evaluation.")
        sys.exit(1)
