"""
Unit tests for EscalationHandler (Layer 5: Human Review).
Tests routing logic: simple → auto-proceed, medium/complex → escalate.
Uses a mock LLM to avoid real API calls.
"""

import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.layer5_human_review.escalation import EscalationHandler


class MockLLM:
    """Mock LLM client that returns predictable responses."""
    def complete(self, system: str, user: str, agent_name: str = "") -> str:
        return (
            "• This job contains complex SCD Type 2 logic requiring manual verification.\n"
            "• The join between ORDERS and CUSTOMERS uses a LeftOuter that could produce NULLs.\n"
            "• The reviewer should verify the BigQuery MERGE statement."
        )


@pytest.fixture
def mock_llm():
    return MockLLM()


@pytest.fixture
def handler_with_llm(mock_llm):
    return EscalationHandler(config={}, llm=mock_llm)


@pytest.fixture
def handler_no_llm():
    return EscalationHandler(config={})


def _make_classified_job(complexity: str, confidence: float = 0.85):
    """Helper to build a classified job dict."""
    return {
        "job_name": "test_job",
        "stages": [
            {"id": "SRC_1", "stage_type": "PxOracleConnector", "category": "source"},
            {"id": "TRX_1", "stage_type": "PxTransformer", "category": "transformer"},
        ],
        "classification": {
            "complexity": complexity,
            "confidence_score": confidence,
            "reasoning": "Test reasoning text.",
            "ambiguity_flags": ["test_flag"],
            "risk_areas": ["SRC_1"],
            "recommendations": ["Review source SQL"],
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Routing Logic Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestRoutingLogic:
    """Verify escalation routing based on complexity."""

    def test_simple_auto_proceeds(self, handler_with_llm):
        """Simple jobs should auto-proceed without escalation."""
        job = _make_classified_job("simple")
        should_continue, result = handler_with_llm.check(job)
        assert should_continue is True
        assert result["escalated"] is False
        assert result["complexity"] == "simple"
        assert result["suggested_action"] == "proceed"

    def test_medium_escalates(self, handler_with_llm):
        """Medium jobs should be escalated for human review."""
        job = _make_classified_job("medium")
        should_continue, result = handler_with_llm.check(job)
        assert should_continue is False
        assert result["escalated"] is True
        assert result["complexity"] == "medium"

    def test_complex_escalates(self, handler_with_llm):
        """Complex jobs should be escalated for human review."""
        job = _make_classified_job("complex")
        should_continue, result = handler_with_llm.check(job)
        assert should_continue is False
        assert result["escalated"] is True
        assert result["complexity"] == "complex"


# ─────────────────────────────────────────────────────────────────────────────
# Escalation Result Structure
# ─────────────────────────────────────────────────────────────────────────────

class TestEscalationResult:
    """Verify the structure of escalation results."""

    def test_simple_result_structure(self, handler_with_llm):
        """Simple result should have all required fields."""
        job = _make_classified_job("simple")
        _, result = handler_with_llm.check(job)
        assert "complexity" in result
        assert "escalated" in result
        assert "instructions" in result
        assert "suggested_action" in result

    def test_escalated_result_has_llm_explanation(self, handler_with_llm):
        """Escalated result should contain the LLM-generated explanation."""
        job = _make_classified_job("medium")
        _, result = handler_with_llm.check(job)
        assert "llm_explanation" in result
        assert len(result["llm_explanation"]) > 0, "LLM explanation should not be empty"

    def test_escalated_result_has_risk_areas(self, handler_with_llm):
        """Escalated result should pass through risk areas from classification."""
        job = _make_classified_job("complex")
        _, result = handler_with_llm.check(job)
        assert "risk_areas" in result
        assert "SRC_1" in result["risk_areas"]

    def test_escalated_result_has_recommendations(self, handler_with_llm):
        """Escalated result should pass through recommendations."""
        job = _make_classified_job("medium")
        _, result = handler_with_llm.check(job)
        assert "recommendations" in result
        assert len(result["recommendations"]) > 0


# ─────────────────────────────────────────────────────────────────────────────
# LLM Requirement Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestLLMRequirement:
    """Verify LLM is required for non-simple jobs."""

    def test_no_llm_simple_succeeds(self, handler_no_llm):
        """Simple jobs should work even without LLM configured."""
        job = _make_classified_job("simple")
        should_continue, result = handler_no_llm.check(job)
        assert should_continue is True

    def test_no_llm_medium_raises(self, handler_no_llm):
        """Medium jobs without LLM should raise RuntimeError."""
        job = _make_classified_job("medium")
        with pytest.raises(RuntimeError, match="LLM client is required"):
            handler_no_llm.check(job)

    def test_no_llm_complex_raises(self, handler_no_llm):
        """Complex jobs without LLM should raise RuntimeError."""
        job = _make_classified_job("complex")
        with pytest.raises(RuntimeError, match="LLM client is required"):
            handler_no_llm.check(job)
