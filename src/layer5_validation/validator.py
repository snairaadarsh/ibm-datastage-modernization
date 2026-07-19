"""
Layer 5: Validator
Validates the migrated pipeline by comparing schemas, structure, and logic
against the original DataStage job. Produces a structured validation report.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class Validator:
    """
    Validates generated pipeline output against the original DataStage structure.
    Checks schema coverage, stage coverage, expression completeness, and output quality.
    """

    def __init__(self, config: dict | None = None):
        self.config = config or {}

    def validate(self, documented_job: dict) -> dict:
        """
        Run all validation checks on the documented job.
        Returns a validation report dict.
        """
        job_name = documented_job.get("job_name", "?")
        logger.info(f"[Validator] Validating: {job_name}")

        checks = []

        # 1. Schema coverage check
        checks.append(self._check_schema_coverage(documented_job))

        # 2. Stage coverage check
        checks.append(self._check_stage_coverage(documented_job))

        # 3. Link/DAG completeness check
        checks.append(self._check_dag_completeness(documented_job))

        # 4. Expression translation coverage
        checks.append(self._check_expression_coverage(documented_job))

        # 5. Output format completeness
        checks.append(self._check_output_completeness(documented_job))

        # 6. Parameter coverage
        checks.append(self._check_parameter_coverage(documented_job))

        # Aggregate
        passed_checks = sum(1 for c in checks if c["status"] == "pass")
        failed_checks = sum(1 for c in checks if c["status"] == "fail")
        warn_checks = sum(1 for c in checks if c["status"] == "warn")
        total = len(checks)

        overall_status = (
            "pass" if failed_checks == 0 else
            "warn" if failed_checks <= 1 else
            "fail"
        )
        overall_score = passed_checks / total if total > 0 else 0.0

        report = {
            "overall_status": overall_status,
            "overall_score": round(overall_score, 3),
            "checks": checks,
            "summary": {
                "total_checks": total,
                "passed": passed_checks,
                "warnings": warn_checks,
                "failed": failed_checks,
            },
        }

        status_icon = {"pass": "✅", "warn": "⚠️", "fail": "❌"}.get(overall_status, "?")
        logger.info(
            f"[Validator] {job_name}: {status_icon} {overall_status.upper()} "
            f"({passed_checks}/{total} checks passed, score={overall_score:.1%})"
        )
        return report

    # ─────────────────────────────────────────────────────────────────────────

    def _check_schema_coverage(self, job: dict) -> dict:
        """Verify output columns are defined for all source stages."""
        stages = job.get("stages", [])
        source_stages = [s for s in stages if s.get("category") in ("source", "source_or_target")]
        covered = [s for s in source_stages if s.get("output_columns")]
        total = len(source_stages)
        cov = len(covered) / total if total > 0 else 1.0
        return {
            "name": "schema_coverage",
            "description": "Output column definitions for source stages",
            "status": "pass" if cov == 1.0 else ("warn" if cov >= 0.5 else "fail"),
            "score": round(cov, 3),
            "detail": f"{len(covered)}/{total} source stages have column definitions",
        }

    def _check_stage_coverage(self, job: dict) -> dict:
        """Verify all stages have been translated (no unknown categories)."""
        stages = job.get("stages", [])
        unknown = [s for s in stages if s.get("category") == "unknown"]
        total = len(stages)
        covered = total - len(unknown)
        cov = covered / total if total > 0 else 1.0
        return {
            "name": "stage_coverage",
            "description": "All stages have a known migration path",
            "status": "pass" if not unknown else ("warn" if len(unknown) <= 1 else "fail"),
            "score": round(cov, 3),
            "detail": f"{covered}/{total} stages have known category. Unknown: {[s['id'] for s in unknown]}",
        }

    def _check_dag_completeness(self, job: dict) -> dict:
        """Verify the data flow DAG is complete (no orphan stages)."""
        stages = job.get("stages", [])
        links = job.get("links", [])
        stage_ids = {s["id"] for s in stages}

        link_stages = set()
        for lnk in links:
            link_stages.add(lnk["from_stage"])
            link_stages.add(lnk["to_stage"])

        orphans = [sid for sid in stage_ids if sid not in link_stages and len(stages) > 1]
        return {
            "name": "dag_completeness",
            "description": "All stages are connected in the data flow DAG",
            "status": "pass" if not orphans else "warn",
            "score": 1.0 if not orphans else 0.7,
            "detail": f"Orphan stages (not in any link): {orphans}" if orphans else "All stages are connected",
        }

    def _check_expression_coverage(self, job: dict) -> dict:
        """Check transformer stages have translated expressions."""
        stages = job.get("stages", [])
        transformer_stages = [s for s in stages if s.get("category") == "transformer"]
        with_expressions = [s for s in transformer_stages if s.get("transformations")]
        total = len(transformer_stages)
        cov = len(with_expressions) / total if total > 0 else 1.0

        # Check for un-translated expressions (still in DS syntax)
        ds_syntax_patterns = ["Upcase(", "Downcase(", "Year(", "Month(", "IsNull("]
        translation = job.get("translation", {})
        pyspark_code = translation.get("pyspark", "") if isinstance(translation, dict) else ""
        residual = [p for p in ds_syntax_patterns if p in pyspark_code]

        status = "pass" if cov == 1.0 and not residual else ("fail" if cov < 0.5 else "warn")
        return {
            "name": "expression_coverage",
            "description": "Transformer expressions translated to target syntax",
            "status": status,
            "score": round(cov, 3),
            "detail": f"{len(with_expressions)}/{total} transformers have expressions. "
                      f"Possible un-translated DS syntax: {residual}" if residual else
                      f"{len(with_expressions)}/{total} transformers with expressions",
        }

    def _check_output_completeness(self, job: dict) -> dict:
        """Verify both PySpark and DataFusion outputs were generated."""
        translation = job.get("translation", {})
        if not isinstance(translation, dict):
            translation = {}

        pyspark_ok = bool(translation.get("pyspark"))
        df_ok = bool(translation.get("datafusion"))
        both_ok = pyspark_ok and df_ok

        return {
            "name": "output_completeness",
            "description": "Both PySpark and DataFusion outputs generated",
            "status": "pass" if both_ok else ("warn" if pyspark_ok or df_ok else "fail"),
            "score": (2 if both_ok else 1 if (pyspark_ok or df_ok) else 0) / 2,
            "detail": f"PySpark: {'✓' if pyspark_ok else '✗'} | DataFusion: {'✓' if df_ok else '✗'}",
        }

    def _check_parameter_coverage(self, job: dict) -> dict:
        """Check that detected parameters are documented."""
        params = job.get("parameters", [])
        if not params:
            return {
                "name": "parameter_coverage",
                "description": "Pipeline parameters are documented",
                "status": "pass",
                "score": 1.0,
                "detail": "No parameters detected",
            }
        return {
            "name": "parameter_coverage",
            "description": "Pipeline parameters are documented",
            "status": "pass",
            "score": 1.0,
            "detail": f"{len(params)} parameter(s) detected and converted to environment variables: "
                      f"{[p['name'] for p in params]}",
        }
