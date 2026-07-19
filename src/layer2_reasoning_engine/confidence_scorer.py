"""
Layer 2: Confidence Scorer
Computes a migration confidence score (0.0–1.0) based on parsed job structure.
Higher score = simpler migration with lower risk. Lower score = complex/ambiguous.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# Stage types that are fully supported (high confidence)
FULLY_SUPPORTED_STAGES = {
    "PxOracleConnector", "PxDB2Connector", "PxMSSQLConnector",
    "PxPostgresConnector", "PxSequentialFile", "PxCsvConnector",
    "PxBigQueryConnector", "PxSnowflakeConnector",
    "PxTransformer", "PxFilter", "PxJoin", "PxAggregate",
    "PxSort", "PxSortStage", "PxFunnel", "PxCopyStage",
}

# Partially supported (medium confidence deduction)
PARTIAL_STAGES = {
    "PxLookup", "PxMerge", "PxPivot", "PxUnpivot",
    "PxRemoveDup", "PxPeekStage", "PxS3Connector",
}

# Complex / risky stages (high confidence penalty)
COMPLEX_STAGES = {
    "PxChangeCapture", "PxSurrogateKey", "PxDataSet",
    "PxLocalParallelism", "CustomStage",
}


@dataclass
class ScoringFactor:
    """A single factor contributing to the confidence score."""
    name: str
    impact: float  # positive = boosts score, negative = penalizes
    reason: str


@dataclass
class ConfidenceResult:
    """Result of confidence scoring for a DataStage job."""
    score: float  # 0.0 (very complex) to 1.0 (fully automated)
    complexity: str  # "simple" | "medium" | "complex"
    factors: list[ScoringFactor] = field(default_factory=list)
    ambiguity_flags: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "score": round(self.score, 3),
            "complexity": self.complexity,
            "factors": [{"name": f.name, "impact": f.impact, "reason": f.reason} for f in self.factors],
            "ambiguity_flags": self.ambiguity_flags,
            "recommendations": self.recommendations,
        }


class ConfidenceScorer:
    """
    Calculates migration confidence for a parsed DataStage job.
    Uses a weighted factor system modeled on experienced ETL migration judgment.
    """

    SIMPLE_THRESHOLD = 0.75
    MEDIUM_THRESHOLD = 0.45

    def __init__(self, config: dict | None = None):
        self.config = config or {}
        self._simple_threshold = self.config.get("simple_threshold", self.SIMPLE_THRESHOLD)
        self._medium_threshold = self.config.get("medium_threshold", self.MEDIUM_THRESHOLD)

    def score(self, parsed_job: dict) -> ConfidenceResult:
        """
        Score a parsed DataStage job and return a ConfidenceResult.
        """
        factors: list[ScoringFactor] = []
        flags: list[str] = []
        recommendations: list[str] = []

        stages = parsed_job.get("stages", [])
        links = parsed_job.get("links", [])
        hints = parsed_job.get("complexity_hints", [])
        summary = parsed_job.get("summary", {})

        # ── Base score ────────────────────────────────────────────────────────
        base = 1.0

        # ── Factor 1: Stage type support ──────────────────────────────────────
        for stage in stages:
            st = stage.get("stage_type", "")
            if st in FULLY_SUPPORTED_STAGES:
                pass  # no penalty
            elif st in PARTIAL_STAGES:
                base -= 0.08
                factors.append(ScoringFactor(
                    name="partial_support_stage",
                    impact=-0.08,
                    reason=f"Stage '{stage['id']}' type '{st}' has partial migration support",
                ))
            elif st in COMPLEX_STAGES:
                base -= 0.20
                factors.append(ScoringFactor(
                    name="complex_stage",
                    impact=-0.20,
                    reason=f"Stage '{stage['id']}' type '{st}' requires custom implementation",
                ))
                flags.append(f"Complex stage: {st} in {stage['id']}")
            else:
                base -= 0.15
                factors.append(ScoringFactor(
                    name="unknown_stage",
                    impact=-0.15,
                    reason=f"Unknown stage type '{st}' in '{stage['id']}' - manual review needed",
                ))
                flags.append(f"Unknown stage type: {st}")
                recommendations.append(f"Manually review stage '{stage['id']}' of type '{st}'")

        # ── Factor 2: Complexity hints ─────────────────────────────────────────
        for hint in hints:
            weight = hint.get("weight", 0.1)
            hint_type = hint.get("hint", "")
            base -= weight
            factors.append(ScoringFactor(
                name=f"hint_{hint_type}",
                impact=-weight,
                reason=f"Complexity hint: {hint_type} in stage '{hint.get('stage', '?')}'",
            ))
            if hint_type == "scd_type2":
                flags.append("SCD Type 2 logic detected — requires MERGE/UPSERT pattern")
                recommendations.append("Review SCD Type 2 logic and validate MERGE statement output")
            elif hint_type == "cdc_or_merge":
                flags.append("CDC or MERGE stage detected — complex state management required")
                recommendations.append("Implement change data capture using Spark Structured Streaming or BQ MERGE")
            elif hint_type == "lookup_join":
                flags.append("Lookup join detected — review join key and no-match handling")

        # ── Factor 3: Total stage count ────────────────────────────────────────
        stage_count = len(stages)
        if stage_count > 15:
            penalty = min(0.15, (stage_count - 15) * 0.01)
            base -= penalty
            factors.append(ScoringFactor(
                name="high_stage_count",
                impact=-penalty,
                reason=f"Job has {stage_count} stages (large jobs are harder to verify)",
            ))
        elif stage_count <= 5:
            factors.append(ScoringFactor(
                name="small_stage_count",
                impact=0.05,
                reason=f"Small job with {stage_count} stages — easy to review",
            ))
            base += 0.05

        # ── Factor 4: Multiple outputs / branching ─────────────────────────────
        multi_out_stages = [s for s in stages if s.get("output_pins", 0) > 1]
        if multi_out_stages:
            penalty = len(multi_out_stages) * 0.05
            base -= penalty
            factors.append(ScoringFactor(
                name="multi_output_stages",
                impact=-penalty,
                reason=f"{len(multi_out_stages)} stage(s) with multiple outputs (branching DAG)",
            ))

        # ── Factor 5: Complex transformers ────────────────────────────────────
        for stage in stages:
            txf_count = len(stage.get("transformations", []))
            if txf_count > 8:
                base -= 0.10
                factors.append(ScoringFactor(
                    name="complex_transformer",
                    impact=-0.10,
                    reason=f"Stage '{stage['id']}' has {txf_count} transformations",
                ))
                flags.append(f"Complex transformer: {stage['id']} ({txf_count} expressions)")

        # ── Factor 6: Source/target diversity ─────────────────────────────────
        source_count = summary.get("source_count", 0)
        target_count = summary.get("target_count", 0)
        if source_count >= 3:
            base -= 0.05
            factors.append(ScoringFactor(
                name="multi_source",
                impact=-0.05,
                reason=f"Job reads from {source_count} sources",
            ))
        if target_count >= 2:
            base -= 0.05
            factors.append(ScoringFactor(
                name="multi_target",
                impact=-0.05,
                reason=f"Job writes to {target_count} targets",
            ))

        # ── Clamp score ────────────────────────────────────────────────────────
        score = max(0.0, min(1.0, base))

        # ── Classify complexity ────────────────────────────────────────────────
        if score >= self._simple_threshold:
            complexity = "simple"
        elif score >= self._medium_threshold:
            complexity = "medium"
        else:
            complexity = "complex"

        # ── Default recommendations ────────────────────────────────────────────
        if complexity == "simple":
            recommendations.append("Fully automated migration recommended")
        elif complexity == "medium":
            recommendations.append("Review flagged sections before deployment")
            recommendations.append("Run validation suite against sample data")
        else:
            recommendations.append("Full human review required before deployment")
            recommendations.append("Consider breaking this job into smaller migration units")
            recommendations.append("Validate all SCD/CDC logic manually")

        result = ConfidenceResult(
            score=score,
            complexity=complexity,
            factors=factors,
            ambiguity_flags=flags,
            recommendations=recommendations,
        )

        logger.info(
            f"[Scorer] {parsed_job.get('job_name', '?')} → "
            f"score={score:.3f} complexity={complexity}"
        )
        return result
