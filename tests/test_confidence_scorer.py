"""
Unit tests for ConfidenceScorer (Layer 2: Reasoning Engine).
Tests score ranges, complexity classification, penalty factors,
and that scoring constants behave correctly.
"""

import sys
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.layer2_reasoning_engine.confidence_scorer import (
    ConfidenceScorer,
    ConfidenceResult,
    FULLY_SUPPORTED_STAGES,
    PARTIAL_STAGES,
    COMPLEX_STAGES,
)


@pytest.fixture
def scorer():
    return ConfidenceScorer()


def _make_job(stages=None, links=None, hints=None, summary=None):
    """Helper to build a minimal parsed job dict."""
    return {
        "job_name": "test_job",
        "stages": stages or [],
        "links": links or [],
        "complexity_hints": hints or [],
        "summary": summary or {},
    }


def _make_stage(stage_id, stage_type, category="transformer", output_pins=1, transformations=None):
    """Helper to build a minimal stage dict."""
    return {
        "id": stage_id,
        "stage_type": stage_type,
        "category": category,
        "output_pins": output_pins,
        "transformations": transformations or [],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Score Range Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestScoreRanges:
    """Verify score stays in valid range and classification is correct."""

    def test_score_between_0_and_1(self, scorer):
        """Score should always be between 0.0 and 1.0."""
        job = _make_job(stages=[_make_stage("s1", "PxTransformer")])
        result = scorer.score(job)
        assert 0.0 <= result.score <= 1.0

    def test_empty_job_scores_high(self, scorer):
        """Empty job (no stages) should score 1.0."""
        result = scorer.score(_make_job())
        assert result.score == 1.0

    def test_result_has_complexity(self, scorer):
        """Result should have a complexity classification string."""
        result = scorer.score(_make_job())
        assert result.complexity in ("simple", "medium", "complex")

    def test_result_is_confidence_result(self, scorer):
        """score() should return a ConfidenceResult dataclass."""
        result = scorer.score(_make_job())
        assert isinstance(result, ConfidenceResult)

    def test_to_dict_works(self, scorer):
        """ConfidenceResult.to_dict() should produce a serializable dict."""
        result = scorer.score(_make_job())
        d = result.to_dict()
        assert "score" in d
        assert "complexity" in d
        assert "factors" in d
        assert isinstance(d["factors"], list)


# ─────────────────────────────────────────────────────────────────────────────
# Classification Thresholds
# ─────────────────────────────────────────────────────────────────────────────

class TestClassification:
    """Verify complexity classification follows thresholds."""

    def test_simple_job(self, scorer):
        """Job with only fully-supported stages should classify as 'simple'."""
        stages = [
            _make_stage("s1", "PxOracleConnector", "source"),
            _make_stage("s2", "PxTransformer"),
            _make_stage("s3", "PxBigQueryConnector", "target"),
        ]
        result = scorer.score(_make_job(stages=stages))
        assert result.complexity == "simple", f"Expected 'simple', got '{result.complexity}' (score={result.score})"

    def test_complex_job(self, scorer):
        """Job with complex/unknown stages should classify as 'complex'."""
        stages = [
            _make_stage("s1", "PxChangeCapture"),
            _make_stage("s2", "PxSurrogateKey"),
            _make_stage("s3", "CustomStage"),
            _make_stage("s4", "UnknownPlugin123"),
        ]
        result = scorer.score(_make_job(stages=stages))
        assert result.complexity == "complex", f"Expected 'complex', got '{result.complexity}' (score={result.score})"


# ─────────────────────────────────────────────────────────────────────────────
# Penalty Factor Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestPenaltyFactors:
    """Verify each scoring factor applies the correct penalty."""

    def test_fully_supported_no_penalty(self, scorer):
        """Fully supported stages should not reduce score."""
        stages = [_make_stage("s1", "PxTransformer")]
        result = scorer.score(_make_job(stages=stages))
        # Should be near 1.0 (only small stage count bonus possible)
        assert result.score >= 0.95

    def test_partial_stage_penalty(self, scorer):
        """Partial stages should reduce score."""
        stages = [_make_stage("s1", "PxLookup")]
        result = scorer.score(_make_job(stages=stages))
        assert result.score < 1.0, "Partial stage should reduce score"

    def test_complex_stage_heavy_penalty(self, scorer):
        """Complex stages should have a larger penalty than partial ones."""
        partial_stages = [_make_stage("s1", "PxLookup")]
        complex_stages = [_make_stage("s1", "PxChangeCapture")]
        partial_result = scorer.score(_make_job(stages=partial_stages))
        complex_result = scorer.score(_make_job(stages=complex_stages))
        assert complex_result.score < partial_result.score, (
            "Complex stage penalty should be larger than partial stage penalty"
        )

    def test_unknown_stage_penalty(self, scorer):
        """Unknown stage types should be penalized and flagged."""
        stages = [_make_stage("s1", "TotallyUnknownPlugin")]
        result = scorer.score(_make_job(stages=stages))
        assert result.score < 1.0
        assert len(result.ambiguity_flags) > 0, "Unknown stage should add ambiguity flag"

    def test_high_stage_count_penalty(self, scorer):
        """Jobs with > 15 stages should receive a penalty."""
        stages = [_make_stage(f"s{i}", "PxTransformer") for i in range(20)]
        result = scorer.score(_make_job(stages=stages))
        small_job = scorer.score(_make_job(stages=[_make_stage("s1", "PxTransformer")]))
        assert result.score < small_job.score, "High stage count should reduce score"

    def test_multi_output_stage_penalty(self, scorer):
        """Stages with multiple output pins should receive a branching penalty."""
        stages = [
            _make_stage(f"s{i}", "PxTransformer", output_pins=3)
            for i in range(6)  # 6 stages avoids small_stage_count bonus
        ]
        result = scorer.score(_make_job(stages=stages))
        single_out_stages = [
            _make_stage(f"s{i}", "PxTransformer", output_pins=1)
            for i in range(6)
        ]
        single_out = scorer.score(_make_job(stages=single_out_stages))
        assert result.score < single_out.score

    def test_complex_transformer_penalty(self, scorer):
        """Transformers with > 8 expressions should be penalized."""
        transforms = [{"output_column": f"col_{i}", "expression": f"expr_{i}"} for i in range(12)]
        stages = [_make_stage("s1", "PxTransformer", transformations=transforms)]
        result = scorer.score(_make_job(stages=stages))
        assert result.score < 1.0
        flag_names = [f.name for f in result.factors]
        assert "complex_transformer" in flag_names

    def test_complexity_hints_penalty(self, scorer):
        """Complexity hints like SCD Type 2 should reduce score."""
        hints = [{"hint": "scd_type2", "weight": 0.2, "stage": "s1"}]
        result = scorer.score(_make_job(hints=hints))
        assert result.score < 1.0
        assert any("SCD" in f for f in result.ambiguity_flags)


# ─────────────────────────────────────────────────────────────────────────────
# Recommendations Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestRecommendations:
    """Verify recommendations are generated based on complexity."""

    def test_simple_gets_auto_recommendation(self, scorer):
        result = scorer.score(_make_job(stages=[_make_stage("s1", "PxTransformer")]))
        if result.complexity == "simple":
            assert any("automated" in r.lower() for r in result.recommendations)

    def test_complex_gets_review_recommendation(self, scorer):
        stages = [_make_stage(f"s{i}", "CustomStage") for i in range(5)]
        result = scorer.score(_make_job(stages=stages))
        assert any("review" in r.lower() for r in result.recommendations)
