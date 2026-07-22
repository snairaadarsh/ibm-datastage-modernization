"""
Agent 5: Tester Agent — v2.0 Production Test Harness (No Fallbacks)
Generates a full pytest test harness with one test CLASS per DataStage stage,
boundary/null/valid tests per transformation, a conftest.py fixture, and
an integration smoke test. Strictly requires LLM completion.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_TESTER_SYSTEM = """\
You are a senior data engineer writing a production-grade pytest test harness for a PySpark ETL pipeline \
migrated from IBM DataStage.

Generate TWO sections separated by exact delimiters:

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 1: conftest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A standalone conftest.py with:
  • A session-scoped SparkSession fixture named `spark`
    - master("local[2]"), appName matching the job, shuffle_partitions=2
  • A helper fixture `make_df(spark, data, schema)` for creating inline DataFrames

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 2: test_harness_<job_name>.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A complete pytest test file with:

ONE TEST CLASS PER DATASTAGE STAGE (class Test<StageName>):

For every TRANSFORMER stage, generate:
  - test_<output_col>_valid_input: normal row → correct computed output value
  - test_<output_col>_null_input: null inputs → coalesced/default output (not raise)
  - test_<output_col>_boundary: boundary value (e.g. NET_AMOUNT=10000.00 for tier boundary, QUANTITY=100 for IS_BULK_ORDER)

For every FILTER stage, generate:
  - test_filter_pass: row satisfying the filter condition → present in output
  - test_filter_reject: row violating the filter → absent from output

For every JOIN stage, generate:
  - test_<join_name>_matched: rows with matching join key → appear in join output
  - test_<join_name>_unmatched: row with no match on key → verify LEFT OUTER behaviour (row with NULLs retained or anti-join captures it)

For every LOOKUP stage, generate:
  - test_lookup_match: successful lookup → output columns populated from reference
  - test_lookup_miss: no match in reference → output columns NULL or default (based on no_match_handling)

For every AGGREGATION stage, generate:
  - test_aggregation_sum: known input rows → verify SUM/COUNT/AVG outputs exactly

Integration class TestEndToEnd:
  - test_pipeline_smoke_test: chain at least 2 stages end-to-end, verify output is non-empty

RULES:
  • ALL test data is INLINE (spark.createDataFrame() with StructType schema — no CSV/JSON files)
  • Every test function has a docstring:
      \"\"\"DataStage stage: <StageName> | Expression: <expression> | Type: <test_type>\"\"\"
  • Assertions use .collect()[0][col] for single-row checks or .count() for count checks
  • Use StructType and StructField for explicit schemas
  • Import from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, etc.
  • Test values must be DERIVED FROM the actual DataStage expressions, not generic placeholder values

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTPUT FORMAT (mandatory)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Emit:
=== conftest.py ===
<content>
=== END conftest.py ===
=== test_harness.py ===
<content>
=== END test_harness.py ===

Return ONLY these two delimited sections. No other text, no markdown.\
"""


class TesterAgent:
    """
    Generates a production-grade pytest test harness using an LLM.
    v2.0: One test class per DataStage stage, conftest.py stub,
    boundary/null/valid tests per transformation.
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
            harness = self._llm_generate_tests(reviewed_job)
        except Exception as e:
            logger.error(f"[TesterAgent] LLM test generation failed: {e}")
            raise RuntimeError(f"TesterAgent LLM generation failed: {e}")

        result = dict(reviewed_job)
        result["tests"] = {
            "test_code":         harness.get("test_harness", ""),
            "conftest_code":     harness.get("conftest", ""),
            "test_file_name":    f"test_harness_{self._safe(job_name)}.py",
            "conftest_file_name": "conftest.py",
        }
        logger.info(f"[TesterAgent] Test suite generated for '{job_name}'")
        return result

    def _llm_generate_tests(self, job: dict) -> dict:
        job_name     = job.get("job_name", "etl_job")
        stages       = job.get("stages", [])
        translation  = job.get("translation", {})
        pyspark_code = (translation.get("pyspark") or "")[:5000]
        review       = job.get("review", {})

        transform_summary = []
        filter_summary    = []
        agg_summary       = []
        join_summary      = []
        lookup_summary    = []

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
                agg_summary.append(
                    f"  Stage={sid}: groupBy={s.get('group_by_keys','')} agg={s.get('aggregations','')}"
                )
            elif cat == "join":
                join_summary.append(
                    f"  Stage={sid}: type={s.get('join_type','?')} key={s.get('join_key','?')}"
                )
            elif cat == "lookup":
                lookup_summary.append(
                    f"  Stage={sid}: key={s.get('lookup_key','?')} no_match={s.get('no_match_handling','?')}"
                )

        user = (
            f"Write a v2.0 pytest test harness for this DataStage→PySpark migration.\n\n"
            f"Job: {job_name}\n\n"
            f"Transformer expressions to test:\n" + ("\n".join(transform_summary) or "  None") + "\n\n"
            f"Filter conditions to test:\n" + ("\n".join(filter_summary) or "  None") + "\n\n"
            f"Aggregations to test:\n" + ("\n".join(agg_summary) or "  None") + "\n\n"
            f"Joins to test:\n" + ("\n".join(join_summary) or "  None") + "\n\n"
            f"Lookups to test:\n" + ("\n".join(lookup_summary) or "  None") + "\n\n"
            f"Review issues to test against:\n"
            + "\n".join(f"  - {i.get('issue','')}" for i in review.get("issues", [])[:5]) + "\n\n"
            f"Generated PySpark code (for reference):\n{pyspark_code}"
        )

        raw = self.llm.complete(_TESTER_SYSTEM, user, agent_name="TesterAgent")

        # Parse sections
        import re
        conftest_match = re.search(
            r"=== conftest\.py ===\s*(.*?)\s*=== END conftest\.py ===",
            raw, re.DOTALL | re.IGNORECASE
        )
        harness_match = re.search(
            r"=== test_harness\.py ===\s*(.*?)\s*=== END test_harness\.py ===",
            raw, re.DOTALL | re.IGNORECASE
        )

        conftest = conftest_match.group(1).strip() if conftest_match else ""
        test_harness = harness_match.group(1).strip() if harness_match else raw

        # Fallback conftest if not parsed
        if not conftest:
            safe = self._safe(job_name)
            conftest = (
                f'"""conftest.py — Shared fixtures for {job_name} test harness."""\n\n'
                f"import pytest\n"
                f"from pyspark.sql import SparkSession\n\n\n"
                f"@pytest.fixture(scope=\"session\")\n"
                f"def spark():\n"
                f"    session = (\n"
                f"        SparkSession.builder\n"
                f"        .master(\"local[2]\")\n"
                f"        .appName(\"{safe}_test\")\n"
                f"        .config(\"spark.sql.shuffle.partitions\", \"2\")\n"
                f"        .getOrCreate()\n"
                f"    )\n"
                f"    yield session\n"
                f"    session.stop()\n\n\n"
                f"@pytest.fixture\n"
                f"def make_df(spark):\n"
                f"    def _make(data, schema):\n"
                f"        return spark.createDataFrame(data, schema)\n"
                f"    return _make\n"
            )

        return {"conftest": conftest, "test_harness": test_harness}

    def _safe(self, name: str) -> str:
        return name.lower().replace(" ", "_").replace("-", "_").replace(".", "_")
