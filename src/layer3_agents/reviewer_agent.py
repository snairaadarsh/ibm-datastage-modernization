"""
Agent 4: Reviewer Agent — Fully LLM-driven with structural diff
Sends the generated PySpark and DataFusion code to the LLM for a real
senior-engineer code review. Includes a structural diff against the original
DSX stage/column/branch checklist to catch missing outputs, wrong join types,
and dropped computed columns.
"""

from __future__ import annotations

import json
import logging
from .classifier_agent import safe_json_loads

logger = logging.getLogger(__name__)


REVIEWER_SYSTEM = """\
You are a principal data engineer performing a migration review of a DataStage-to-cloud ETL job.

You will receive:
  1. A SOURCE CHECKLIST extracted directly from the original DataStage DSX/ISX file
  2. The generated PySpark code
  3. The generated DataFusion pipeline JSON

Your review has TWO parts:

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PART A — STRUCTURAL DIFF (mandatory)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For each item in the source checklist, verify it is present in the generated outputs.
Report MISSING or WRONG items as errors.

Check in order:
  1. Sources       — is every source stage implemented?
  2. Joins         — is every join using the CORRECT type (inner/left/right)?
                     A LeftOuter implemented as inner is a critical bug.
  3. Branches      — is every output branch of every multi-output stage implemented?
                     A missing exception branch is a critical bug.
  4. Computed cols — is EVERY column in the checklist present as withColumn() / directive?
                     Missing computed columns are errors even if they seem minor.
  5. Sinks         — is every sink (main + exception) implemented?
  6. Source SQL    — does the JDBC source use the original table name and WHERE clause?
  7. DataFusion    — does the Wrangler stage avoid window functions (lead, row_number)?
                     If SCD2 is needed, is a SparkSQL plugin or BQ MERGE used instead?

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PART B — CODE QUALITY REVIEW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  1. Null safety — coalesce, isNull checks where needed
  2. Performance — broadcast hints, partition coalescing, avoiding unnecessary shuffles
  3. Security    — no hardcoded credentials, secrets, or connection strings
  4. Completeness — no TODO stubs remaining
  5. SCD2 correctness — window spec, EFF_END_DATE, IS_CURRENT flag

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RETURN FORMAT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Return ONLY a valid JSON object:
{
  "passed": true|false,
  "score": 0.0-1.0,
  "summary": "2-3 sentence assessment covering both structural diff and code quality",
  "structural_diff": {
    "sources_ok":             true|false,
    "joins_ok":               true|false,
    "branches_ok":            true|false,
    "computed_cols_ok":       true|false,
    "sinks_ok":               true|false,
    "source_sql_ok":          true|false,
    "datafusion_wrangler_ok": true|false,
    "missing_items": ["list every missing column, branch, or stage"]
  },
  "issues": [
    {
      "severity": "error"|"warning"|"info",
      "part": "structural"|"quality",
      "location": "stage or line description",
      "issue": "what is wrong",
      "suggestion": "exact fix"
    }
  ],
  "strengths": ["what the code does well"],
  "critical_fixes_required": true|false
}
passed=false if ANY structural_diff item is false OR any error-severity issue exists.
Return ONLY the JSON, no markdown.\
"""


class ReviewerAgent:
    """
    Reviews generated PySpark and DataFusion code using an LLM.
    Performs a structural diff against the original DSX checklist in addition
    to a standard code-quality review.
    """

    def __init__(self, engine=None, llm=None):
        self.engine = engine
        self.llm = llm

    def run(self, translated_job: dict) -> dict:
        job_name        = translated_job.get("job_name", "?")
        translation     = translated_job.get("translation", {})
        stages          = translated_job.get("stages", [])
        cls             = translated_job.get("classification", {})
        logger.info(f"[ReviewerAgent] Reviewing with LLM: {job_name}")

        if not self.llm:
            raise RuntimeError("[ReviewerAgent] LLM client is required but not configured.")

        pyspark_code    = translation.get("pyspark", "") or ""
        datafusion_json = translation.get("datafusion", {})

        review = self._llm_review(job_name, pyspark_code, datafusion_json, stages, cls)

        result = dict(translated_job)
        result["review"] = review
        logger.info(f"[ReviewerAgent] Review done: passed={review['passed']}, score={review['score']:.2f}")
        return result

    def _llm_review(
        self,
        job_name: str,
        pyspark_code: str,
        datafusion_json,
        stages: list,
        cls: dict,
    ) -> dict:
        # ── Build source checklist from parsed stages ─────────────────────────
        source_types    = {"pxsequentialfile", "pxdb2connector", "pxoracleconnector",
                           "pxjdbcconnector", "database_source", "sequential_file", "source"}
        target_types    = {"pxbigqueryconnector", "pxsequentialfile_target", "pxdb2connector_target",
                           "database_target", "sequential_file_target", "target"}
        join_types_set  = {"pxjoin", "pxlookup", "joiner"}

        sources       = [s["id"] for s in stages if s.get("category", "").lower() in source_types
                         or s.get("input_pin_count", 1) == 0]
        sinks         = [s["id"] for s in stages if s.get("category", "").lower() in target_types
                         or s.get("output_pin_count", 1) == 0]
        joins         = {s["id"]: s.get("join_type", "unknown")
                         for s in stages if s.get("stage_type", "").lower() in join_types_set}
        branches      = {s["id"]: s.get("output_pin_count", 1)
                         for s in stages if int(s.get("output_pin_count", 1)) > 1}
        computed_cols = [
            t.get("output_column", "")
            for s in stages
            for t in s.get("transformations", [])
            if t.get("output_column", "")
        ]
        exception_sinks = [
            s["id"] for s in stages
            if "exception" in s.get("id", "").lower()
            or "peek" in s.get("stage_type", "").lower()
        ]
        source_sqls = {
            s["id"]: s.get("sql_query", "")[:200]
            for s in stages if s.get("sql_query", "")
        }

        checklist = {
            "sources":              sources,
            "joins":                joins,
            "branches":             branches,
            "computed_cols":        computed_cols,
            "sinks":                sinks,
            "exception_sinks":      exception_sinks,
            "source_sql_snippets":  source_sqls,
        }

        df_str          = json.dumps(datafusion_json, indent=2) if isinstance(datafusion_json, dict) else str(datafusion_json)
        # Send full code to LLM — relies on large context window models (Gemini, Claude)
        pyspark_preview = pyspark_code
        df_preview      = df_str
        risk_areas      = cls.get("risk_areas", [])
        ambiguity       = cls.get("ambiguity_flags", [])

        user = (
            f"Review the migration output for IBM DataStage job '{job_name}'.\n\n"
            f"=== SOURCE CHECKLIST (extracted from original DSX — diff against this) ===\n"
            f"{json.dumps(checklist, indent=2)}\n\n"
            f"Known risk areas from classifier: {risk_areas}\n"
            f"Ambiguity flags: {ambiguity}\n\n"
            f"=== GENERATED PySpark Code ===\n{pyspark_preview}\n\n"
            f"=== GENERATED DataFusion Pipeline JSON (truncated) ===\n{df_preview}"
        )

        try:
            raw  = self.llm.complete(REVIEWER_SYSTEM, user, agent_name="ReviewerAgent")
            data = safe_json_loads(raw)
            return {
                "passed":                  bool(data.get("passed", False)),
                "score":                   float(data.get("score", 0.5)),
                "summary":                 data.get("summary", ""),
                "structural_diff":         data.get("structural_diff", {}),
                "issues":                  data.get("issues", []),
                "strengths":               data.get("strengths", []),
                "critical_fixes_required": bool(data.get("critical_fixes_required", True)),
            }
        except Exception as e:
            logger.error(f"[ReviewerAgent] LLM review/parsing failed: {e}")
            raise RuntimeError(
                f"ReviewerAgent LLM review/parsing failed: {e}. "
                "Please ensure the LLM response is valid JSON."
            )


class ReviewIssue:
    def __init__(self, severity: str, stage_id: str, code_type: str, issue: str, suggestion: str):
        self.severity  = severity
        self.stage_id  = stage_id
        self.code_type = code_type
        self.issue     = issue
        self.suggestion = suggestion

    def to_dict(self):
        return self.__dict__
