"""
Agent 5: Tester Agent — Fully LLM-driven (No Fallbacks)
Sends the generated PySpark code and the original DataStage transformation
logic to the LLM to generate unit tests. Strictly requires LLM completion.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class TesterAgent:
    """
    Generates a real pytest test suite using an LLM.
    Strictly requires LLM and raises a RuntimeError on failure.
    """

    def __init__(self, engine=None, llm=None):
        self.engine = engine
        self.llm = llm

    def run(self, reviewed_job: dict) -> dict:
        job_name = reviewed_job.get("job_name", "etl_job")
        logger.info(f"[TesterAgent] Generating tests with LLM: {job_name}")

        if not self.llm:
            raise RuntimeError("[TesterAgent] LLM client is required but not configured.")

        try:
            test_code = self._llm_generate_tests(reviewed_job)
        except Exception as e:
            logger.error(f"[TesterAgent] LLM test generation failed: {e}")
            raise RuntimeError(f"TesterAgent LLM generation failed: {e}")

        result = dict(reviewed_job)
        result["tests"] = {
            "test_code":      test_code,
            "test_file_name": f"test_{self._safe(job_name)}.py",
        }
        logger.info(f"[TesterAgent] Test suite generated for '{job_name}'")
        return result

    def _llm_generate_tests(self, job: dict) -> str:
        job_name     = job.get("job_name", "etl_job")
        stages       = job.get("stages", [])
        translation  = job.get("translation", {})
        pyspark_code = (translation.get("pyspark") or "")[:5000]
        review       = job.get("review", {})

        transform_summary = []
        filter_summary    = []
        agg_summary       = []

        for s in stages:
            sid = s.get("id", "?")
            cat = s.get("category", "")
            if cat == "transformer":
                for t in s.get("transformations", [])[:6]:
                    transform_summary.append(
                        f"  Stage={sid}: {t.get('output_column','')} = {t.get('expression','')}"
                    )
            elif cat == "filter":
                filter_summary.append(f"  Stage={sid}: {s.get('filter_condition','')}")
            elif cat == "aggregation":
                agg_summary.append(f"  Stage={sid}: groupBy={s.get('group_by_keys','')} agg={s.get('aggregation_string','')}")

        system = (
            "You are a senior data engineer writing pytest tests for a PySpark ETL pipeline "
            "migrated from IBM DataStage.\n\n"
            "Write a complete pytest test file that:\n"
            "  • Imports pytest and pyspark correctly with a session-scoped SparkSession fixture\n"
            "  • Tests EACH transformation expression with real input data and expected output\n"
            "  • Tests EACH filter condition — data that should pass and data that should be filtered\n"
            "  • Tests EACH aggregation with known input rows and expected totals\n"
            "  • Tests null handling for columns that appear in expressions\n"
            "  • Has at least one integration-level test that chains multiple stages\n"
            "  • Uses spark.createDataFrame() with explicit schemas\n"
            "  • Assertions use .collect()[0] pattern or .count() depending on what's tested\n"
            "  • Test data values are derived from the actual DataStage expressions (not generic)\n\n"
            "Return ONLY the Python code. No markdown fences, no explanations."
        )

        user = (
            f"Write a pytest test suite for this DataStage→PySpark migration.\n\n"
            f"Job: {job_name}\n\n"
            f"Transformer expressions to test:\n" + ("\n".join(transform_summary) or "  None") + "\n\n"
            f"Filter conditions to test:\n" + ("\n".join(filter_summary) or "  None") + "\n\n"
            f"Aggregations to test:\n" + ("\n".join(agg_summary) or "  None") + "\n\n"
            f"Review issues to test against:\n"
            + "\n".join(f"  - {i.get('issue','')}" for i in review.get("issues",[])[:5]) + "\n\n"
            f"Generated PySpark code (for reference):\n{pyspark_code}"
        )

        return self.llm.complete(system, user, agent_name="TesterAgent")

    def _safe(self, name: str) -> str:
        return name.lower().replace(" ", "_").replace("-", "_").replace(".", "_")
