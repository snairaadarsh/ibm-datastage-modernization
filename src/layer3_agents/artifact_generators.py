"""
Artifact Generators — Version 2.0
Produces the 6 new mandatory output artifacts defined in the
datastage_migration_agent_enhancement.md specification:

  3. <job>_migration_report.json
  4. <job>_validation_report.json
  5. <job>_metadata.json
  6. <job>_lineage.json
  7. <job>_ddl.sql
  8. <job>_test_harness.py  (conftest + per-stage test classes)

All generators call the LLM with a schema-specific system prompt and
return the artifact as a Python string (JSON/SQL/Python code).
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# System prompts
# ─────────────────────────────────────────────────────────────────────────────

_MIGRATION_REPORT_SYSTEM = """\
You are a DataStage migration architect producing a JSON migration report.

Return ONLY a valid JSON object matching this exact schema (no markdown):
{
  "schema_version": "2.0",
  "job_name": "<name>",
  "migration_timestamp": "<ISO-8601>",
  "summary": {
    "total_stages": N,
    "total_links": N,
    "total_columns": N,
    "total_transformations": N,
    "sources": [],
    "targets": [],
    "lookups": [],
    "joins": [],
    "aggregations": [],
    "scd_stages": [],
    "reject_paths": [],
    "custom_routines": [],
    "shared_containers": [],
    "sequence_activities": []
  },
  "generated_files": [
    { "filename": "<name>", "type": "<type>", "size_bytes": 0, "checksum": "sha256-placeholder" }
  ],
  "unsupported_components": [
    {
      "feature_name": "<name>",
      "stage_name": "<stage>",
      "datastage_version_specific": "false",
      "description": "<what this feature does>",
      "reason_unsupported": "<why it cannot be auto-translated>",
      "recommended_workaround": "<suggested approach>",
      "estimated_effort_hours": 0,
      "blocking": "false",
      "priority": "LOW"
    }
  ],
  "warnings": [
    { "code": "W-001", "stage": "<stage>", "message": "<message>", "recommendation": "<action>" }
  ],
  "manual_interventions_required": [
    {
      "id": "MI-001",
      "stage": "<stage>",
      "severity": "MEDIUM",
      "description": "<what needs human review>",
      "estimated_effort_hours": 1,
      "owner": "UNASSIGNED"
    }
  ],
  "migration_metrics": {
    "overall_confidence": 0.0,
    "migration_completeness_pct": 0,
    "stages_fully_migrated": 0,
    "stages_partially_migrated": 0,
    "stages_not_migrated": 0,
    "transformations_auto_translated": 0,
    "transformations_requiring_review": 0,
    "transformations_not_translated": 0
  },
  "phase0_analysis": {
    "job_classification": "<ServerJob|ParallelJob|SequenceJob>",
    "stage_inventory": [],
    "link_inventory": [],
    "transformation_inventory": [],
    "connectivity_inventory": [],
    "complexity_flags": []
  }
}

Rules:
- migration_completeness_pct = ((fully*1.0 + partially*0.5) / total_stages) * 100, rounded to 1 decimal
- overall_confidence = weighted average of stage translation confidence scores
- Every unsupported component must have blocking and estimated_effort_hours
- phase0_analysis must enumerate EVERY stage, link, and transformation
- Return ONLY the JSON. No markdown, no code fences.\
"""

_VALIDATION_REPORT_SYSTEM = """\
You are a DataStage migration validator producing a structured JSON validation report.

Return ONLY a valid JSON object matching this schema (no markdown):
{
  "schema_version": "2.0",
  "job_name": "<name>",
  "validation_timestamp": "<ISO-8601>",
  "overall_result": "PASS|WARN|FAIL",
  "overall_confidence": 0.0,
  "quality_gates": {
    "QG-01": { "description": "Every stage has a DataFusion stages[] entry", "result": "PASS|FAIL", "detail": "" },
    "QG-02": { "description": "Every link has a DataFusion connections[] entry", "result": "PASS|FAIL", "detail": "" },
    "QG-03": { "description": "Every derivation in metadata.column_metadata[*].derivation_expression", "result": "PASS|FAIL", "detail": "" },
    "QG-04": { "description": "Every reject path has a quarantine write in PySpark", "result": "PASS|FAIL", "detail": "" },
    "QG-05": { "description": "No plain-text credentials in any artifact", "result": "PASS|FAIL", "detail": "" },
    "QG-06": { "description": "Every column has datastage_type and spark_type", "result": "PASS|FAIL", "detail": "" },
    "QG-07": { "description": "Every transformation has a confidence score", "result": "PASS|FAIL", "detail": "" },
    "QG-08": { "description": "Every unsupported feature has blocking flag and estimated_effort_hours", "result": "PASS|FAIL", "detail": "" },
    "QG-09": { "description": "lineage.json graph is acyclic", "result": "PASS|FAIL", "detail": "" },
    "QG-10": { "description": "Every PySpark stage function has a DataStage-origin docstring", "result": "PASS|FAIL", "detail": "" },
    "QG-11": { "description": "migration_completeness_pct is calculated and present", "result": "PASS|FAIL", "detail": "" },
    "QG-12": { "description": "All 8 artifacts generated", "result": "PASS|FAIL", "detail": "" }
  },
  "categories": [
    {
      "category": "<category_name>",
      "result": "PASS|WARN|FAIL",
      "confidence": 0.0,
      "checks": [
        {
          "check_id": "CHK-001",
          "description": "<what was checked>",
          "expected": "<expected>",
          "actual": "<found or UNKNOWN>",
          "result": "PASS|WARN|FAIL",
          "message": "<explanation>",
          "recommendation": "<fix or action>"
        }
      ]
    }
  ],
  "missing_objects": [
    { "type": "Stage|Link|Column|Expression|Parameter", "name": "<name>", "reason": "<why missing>" }
  ]
}

Mandatory categories to include:
stage_count, link_count, schema_count, column_count, column_types, expressions,
join_types, join_keys, lookup_logic, partitioning, sorting, aggregation_functions,
scd_logic, reject_paths, exception_handling, business_rules, parameters,
environment_variables, source_queries, target_tables

overall_result=FAIL if any category is FAIL. overall_result=WARN if any category is WARN. Otherwise PASS.
Return ONLY the JSON. No markdown, no code fences.\
"""

_METADATA_SYSTEM = """\
You are a DataStage migration metadata specialist. Produce a complete JSON metadata package.

Return ONLY a valid JSON object matching this schema (no markdown):
{
  "schema_version": "2.0",
  "job_metadata": {
    "job_name": "<name>",
    "job_type": "<type>",
    "description": "<description or UNKNOWN>",
    "category": "<path or UNKNOWN>",
    "project": "<project or UNKNOWN>",
    "created_by": "UNKNOWN",
    "created_date": "UNKNOWN",
    "last_modified_by": "UNKNOWN",
    "last_modified_date": "UNKNOWN",
    "version": "UNKNOWN"
  },
  "stage_inventory": [
    {
      "id": "<uuid>",
      "name": "<stage name>",
      "type": "<stage type>",
      "plugin": "<plugin>",
      "execution_mode": "Parallel|Sequential|UNKNOWN",
      "position": { "x": "UNKNOWN", "y": "UNKNOWN" }
    }
  ],
  "link_inventory": [
    {
      "id": "<uuid>",
      "name": "<link name>",
      "source_stage": "<stage name>",
      "target_stage": "<stage name>",
      "type": "Main|Reference|Reject|Lookup"
    }
  ],
  "column_metadata": [
    {
      "stage": "<stage name>",
      "link": "<link name>",
      "column_name": "<name>",
      "datastage_type": "<type>",
      "spark_type": "<type>",
      "nullable": "true|false",
      "length": null,
      "precision": null,
      "scale": null,
      "description": "UNKNOWN",
      "is_key": "false",
      "is_derived": "false",
      "derivation_expression": null
    }
  ],
  "parameters": [],
  "environment_variables": [],
  "dependencies": {
    "shared_containers": [],
    "routines": [],
    "table_definitions": [],
    "file_sets": [],
    "data_elements": [],
    "sequences": []
  },
  "lookups": [],
  "joins": [],
  "aggregations": [],
  "custom_routines": [],
  "unsupported_features": [],
  "stage_type_mapping": [
    {
      "datastage_type": "<type>",
      "pyspark_equivalent": "<class/method or UNKNOWN>",
      "datafusion_equivalent": "<node type or UNKNOWN>",
      "notes": ""
    }
  ]
}

Rules:
- Enumerate EVERY column from EVERY stage in column_metadata
- Mark is_derived=true and populate derivation_expression for all transformer output columns
- Mark is_key=true for join key columns and lookup key columns
- Use UUIDs for all id fields (generate realistic UUID strings)
- Return ONLY the JSON. No markdown, no code fences.\
"""

_LINEAGE_SYSTEM = """\
You are a DataStage column-level lineage specialist. Produce a directed acyclic lineage graph in JSON.

Return ONLY a valid JSON object matching this schema (no markdown):
{
  "schema_version": "2.0",
  "job_name": "<name>",
  "lineage_timestamp": "<ISO-8601>",
  "nodes": [
    {
      "id": "<uuid>",
      "type": "SourceColumn|DerivedColumn|TargetColumn|Intermediate",
      "stage_name": "<stage name>",
      "column_name": "<column name>",
      "data_type": "<spark type>",
      "is_pii": "false|UNKNOWN",
      "tags": []
    }
  ],
  "edges": [
    {
      "id": "<uuid>",
      "source_node_id": "<uuid>",
      "target_node_id": "<uuid>",
      "transformation_type": "PassThrough|Derivation|Aggregation|Lookup|Join|Filter|Rename|Cast|UNKNOWN",
      "expression": "<original DataStage expression or passthrough>",
      "translated_expression": "<PySpark equivalent or passthrough>",
      "confidence": 0.0,
      "stage_name": "<stage where transformation occurs>"
    }
  ],
  "column_lineage_summary": [
    {
      "target_column": "<fully qualified: stage.link.column>",
      "source_columns": ["<fully qualified>"],
      "transformation_path": ["<stage1>", "<stage2>"],
      "lineage_type": "Direct|Derived|Aggregated|Lookup|Joined|UNKNOWN",
      "end_to_end_confidence": 0.0
    }
  ]
}

Rules:
- One node per unique (stage, column) pair across all stages
- Every edge must reference valid node IDs (no dangling references)
- The graph MUST be acyclic — no circular dependencies allowed
- Source columns (stage category=source) → type=SourceColumn
- Target columns (stage category=target) → type=TargetColumn
- Transformer output columns → type=DerivedColumn
- Intermediate columns (join outputs, filter pass-throughs) → type=Intermediate
- PassThrough edges have confidence=1.0
- Derivation edges confidence from expression complexity
- Return ONLY the JSON. No markdown, no code fences.\
"""

_DDL_SYSTEM = """\
You are a DataStage migration DDL specialist. Generate SQL DDL for all target sink tables.

Rules:
- Generate CREATE TABLE IF NOT EXISTS for EVERY target sink table identified in the job
- Use correct SQL types mapped from DataStage types (VarChar→VARCHAR, Integer→INT, Float→DOUBLE, Timestamp→TIMESTAMP, etc.)
- Preserve nullability: NOT NULL where nullable=false, otherwise allow NULL
- Include PRIMARY KEY constraint if merge keys are identified
- Add COMMENT on every column using standard SQL COMMENT syntax
- Generate TWO DDL statements per sink:
    1. Production target table (as-is)
    2. Reject/quarantine table with _reject_reason VARCHAR(1000) and _reject_date DATE appended to the end
- Include a header comment block per table:
  -- Source DataStage stage: <stage_name>
  -- Original connection: <connection_name>
  -- Migration date: <today>
  -- Target system: BigQuery / PostgreSQL (adapt syntax as noted)
- If BigQuery target: add `-- BigQuery syntax: use backtick quoting, no PRIMARY KEY`
- Separate each statement with a blank line and a divider comment

Return ONLY valid SQL. No markdown, no code fences, no explanations.\
"""

_TEST_HARNESS_SYSTEM = """\
You are a senior data engineer writing a production-grade pytest test harness for a PySpark ETL pipeline \
migrated from IBM DataStage.

Generate TWO sections:

SECTION 1: conftest.py
A standalone conftest.py file with:
- A session-scoped SparkSession fixture named `spark`
- Configured with master("local[2]") and appName matching the job
- A helper fixture `make_df(spark, data, schema)` for creating test DataFrames inline

SECTION 2: test_harness_<job_name>.py
A complete pytest file with:
- One test CLASS per DataStage stage (class named Test<StageName>)
- For every transformer expression, generate:
    - test_<col>_valid_input: normal row that should produce correct output
    - test_<col>_null_input: null input producing coalesced or default output
    - test_<col>_boundary: boundary value test (e.g., NET_AMOUNT=10000.00 for tier boundary)
- For every filter condition:
    - test_filter_pass: row that satisfies the filter
    - test_filter_reject: row that fails the filter
- For every join stage:
    - test_join_matched: rows matching on join key
    - test_join_unmatched: row with no match (verify LEFT OUTER behaviour)
- For every aggregation stage:
    - test_aggregation_sum: rows that produce correct SUM/COUNT totals
- Integration test class TestEndToEnd with test_pipeline_smoke_test chaining all stages
- All test data is INLINE (no external CSV/JSON files)
- Every test function has a docstring: "DataStage stage: <stage_name> | Expression: <expression>"
- Use spark.createDataFrame() with explicit StructType schema
- Assertions use .collect()[0][column] or .count() as appropriate

Emit both files in this format:
=== conftest.py ===
<content>
=== END conftest.py ===
=== test_harness.py ===
<content>
=== END test_harness.py ===

Return ONLY these two delimited sections. No other text.\
"""


# ─────────────────────────────────────────────────────────────────────────────
# ArtifactGenerators class
# ─────────────────────────────────────────────────────────────────────────────

class ArtifactGenerators:
    """
    Generates the 6 mandatory v2.0 output artifacts via LLM calls.
    Each method builds a targeted prompt, calls the LLM, and returns
    the artifact as a Python string (to be written to disk by the caller).
    """

    def __init__(self, llm):
        self.llm = llm

    # ── Artifact 3: Migration Report ──────────────────────────────────────────

    def migration_report(self, job: dict) -> str:
        """Generate <job>_migration_report.json as a JSON string."""
        job_name = job.get("job_name", "?")
        logger.info(f"[ArtifactGenerators] Generating migration_report for '{job_name}'")
        user = self._build_job_summary_prompt(job, artifact="migration_report")
        try:
            raw = self.llm.complete(_MIGRATION_REPORT_SYSTEM, user, agent_name="ArtifactGen-MigrationReport")
            raw = _strip_fences(raw)
            # Validate JSON parse
            parsed = json.loads(raw)
            return json.dumps(parsed, indent=2)
        except Exception as e:
            logger.warning(f"[ArtifactGenerators] migration_report parse failed: {e} — returning raw")
            return raw if "raw" in dir() else "{}"

    # ── Artifact 4: Validation Report ─────────────────────────────────────────

    def validation_report(self, job: dict, pyspark_code: str = "", datafusion_json: Any = None) -> str:
        """Generate <job>_validation_report.json as a JSON string."""
        job_name = job.get("job_name", "?")
        logger.info(f"[ArtifactGenerators] Generating validation_report for '{job_name}'")
        user = (
            f"{self._build_job_summary_prompt(job, artifact='validation_report')}\n\n"
            f"PySpark code snippet (first 4000 chars):\n{pyspark_code[:4000]}\n\n"
            f"DataFusion JSON snippet (first 2000 chars):\n"
            f"{json.dumps(datafusion_json, indent=2)[:2000] if isinstance(datafusion_json, dict) else str(datafusion_json)[:2000]}"
        )
        try:
            raw = self.llm.complete(_VALIDATION_REPORT_SYSTEM, user, agent_name="ArtifactGen-ValidationReport")
            raw = _strip_fences(raw)
            parsed = json.loads(raw)
            return json.dumps(parsed, indent=2)
        except Exception as e:
            logger.warning(f"[ArtifactGenerators] validation_report parse failed: {e}")
            return raw if "raw" in dir() else "{}"

    # ── Artifact 5: Metadata ──────────────────────────────────────────────────

    def metadata(self, job: dict) -> str:
        """Generate <job>_metadata.json as a JSON string."""
        job_name = job.get("job_name", "?")
        logger.info(f"[ArtifactGenerators] Generating metadata for '{job_name}'")
        user = self._build_job_summary_prompt(job, artifact="metadata")
        try:
            raw = self.llm.complete(_METADATA_SYSTEM, user, agent_name="ArtifactGen-Metadata")
            raw = _strip_fences(raw)
            parsed = json.loads(raw)
            return json.dumps(parsed, indent=2)
        except Exception as e:
            logger.warning(f"[ArtifactGenerators] metadata parse failed: {e}")
            return raw if "raw" in dir() else "{}"

    # ── Artifact 6: Lineage ───────────────────────────────────────────────────

    def lineage(self, job: dict) -> str:
        """Generate <job>_lineage.json as a JSON string."""
        job_name = job.get("job_name", "?")
        logger.info(f"[ArtifactGenerators] Generating lineage for '{job_name}'")
        user = self._build_job_summary_prompt(job, artifact="lineage")
        try:
            raw = self.llm.complete(_LINEAGE_SYSTEM, user, agent_name="ArtifactGen-Lineage")
            raw = _strip_fences(raw)
            parsed = json.loads(raw)
            return json.dumps(parsed, indent=2)
        except Exception as e:
            logger.warning(f"[ArtifactGenerators] lineage parse failed: {e}")
            return raw if "raw" in dir() else "{}"

    # ── Artifact 7: DDL ───────────────────────────────────────────────────────

    def ddl(self, job: dict) -> str:
        """Generate <job>_ddl.sql as a SQL string."""
        job_name = job.get("job_name", "?")
        logger.info(f"[ArtifactGenerators] Generating DDL for '{job_name}'")
        user = self._build_job_summary_prompt(job, artifact="ddl")
        try:
            raw = self.llm.complete(_DDL_SYSTEM, user, agent_name="ArtifactGen-DDL")
            return _strip_sql_fences(raw)
        except Exception as e:
            logger.warning(f"[ArtifactGenerators] DDL generation failed: {e}")
            return f"-- DDL generation failed: {e}\n"

    # ── Artifact 8: Test Harness ──────────────────────────────────────────────

    def test_harness(self, job: dict, pyspark_code: str = "") -> dict:
        """
        Generate <job>_test_harness.py and conftest.py.
        Returns { "conftest": str, "test_harness": str }.
        """
        job_name = job.get("job_name", "?")
        logger.info(f"[ArtifactGenerators] Generating test harness for '{job_name}'")
        user = (
            f"{self._build_job_summary_prompt(job, artifact='test_harness')}\n\n"
            f"PySpark code (for reference, first 4000 chars):\n{pyspark_code[:4000]}"
        )
        try:
            raw = self.llm.complete(_TEST_HARNESS_SYSTEM, user, agent_name="ArtifactGen-TestHarness")
            return _parse_test_harness_sections(raw, job_name)
        except Exception as e:
            logger.warning(f"[ArtifactGenerators] test_harness generation failed: {e}")
            return {"conftest": _stub_conftest(job_name), "test_harness": f"# Test harness generation failed: {e}\n"}

    # ── Shared prompt builder ─────────────────────────────────────────────────

    def _build_job_summary_prompt(self, job: dict, artifact: str) -> str:
        """Build a rich job summary prompt for artifact generation."""
        stages = job.get("stages", [])
        links  = job.get("links", [])
        params = job.get("parameters", [])
        conns  = job.get("connections", [])
        cls    = job.get("classification", {})
        rev    = job.get("review", {})
        trans  = job.get("translation", {})

        stage_lines = []
        for s in stages:
            sid    = s.get("id", "")
            stype  = s.get("stage_type", "")
            cat    = s.get("category", "")
            cols   = [c.get("name", "") for c in s.get("output_columns", [])[:8]]
            txf    = [(t.get("output_column", ""), t.get("expression", "")) for t in s.get("transformations", [])[:6]]
            jtype  = s.get("join_type", "")
            jkey   = s.get("join_key", "")
            sql    = s.get("sql_query", "")[:200]
            lkup   = s.get("lookup_key", "")
            grp    = s.get("group_by_keys", "")
            agg    = s.get("aggregations", "")
            wmode  = s.get("write_mode", "")
            bqp    = s.get("bq_project", "")
            bqd    = s.get("bq_dataset", "")
            tbl    = s.get("table_name", "")
            conn   = s.get("connection_name", "")
            fp     = s.get("file_path", "")

            line = f"[{sid}] type={stype} cat={cat}"
            if cols:
                line += f" cols={cols}"
            if txf:
                line += f" transforms={txf}"
            if jtype:
                line += f" join_type={jtype} join_key={jkey}"
            if sql:
                line += f" sql={sql!r}"
            if lkup:
                line += f" lookup_key={lkup}"
            if grp:
                line += f" group_by={grp}"
            if agg:
                line += f" agg={agg}"
            if wmode:
                line += f" write_mode={wmode}"
            if bqp:
                line += f" bq_project={bqp} bq_dataset={bqd} table={tbl}"
            if conn:
                line += f" connection={conn}"
            if fp:
                line += f" file_path={fp}"

            stage_lines.append(line)

        link_lines = [
            f"  {lnk.get('from_stage','?')} → {lnk.get('to_stage','?')} (from_pin={lnk.get('from_pin','?')} to_pin={lnk.get('to_pin','?')})"
            for lnk in links
        ]

        return (
            f"Generate the {artifact} artifact for this IBM DataStage job migration.\n\n"
            f"Job: {job.get('job_name','?')}\n"
            f"Description: {job.get('job_description','')}\n"
            f"Complexity: {cls.get('complexity','?')} | Score: {cls.get('confidence_score',0):.0%}\n"
            f"Risk areas: {cls.get('risk_areas',[])}\n"
            f"Ambiguity flags: {cls.get('ambiguity_flags',[])}\n"
            f"Recommendations: {cls.get('recommendations',[])}\n\n"
            f"Sources: {job.get('sources', [])}\n"
            f"Targets: {job.get('targets', [])}\n"
            f"Parameters: {[p.get('name','') for p in params]}\n"
            f"Connections: {[(c.get('name',''), c.get('stage_type','')) for c in conns]}\n\n"
            f"STAGES ({len(stages)}):\n" + "\n".join(stage_lines) + "\n\n"
            f"LINKS ({len(links)}):\n" + "\n".join(link_lines) + "\n\n"
            f"Review: passed={rev.get('passed',True)}, score={rev.get('score',1.0):.0%}\n"
            f"Review issues: {[i.get('issue','') for i in rev.get('issues',[])[:5]]}\n"
            f"Review strengths: {rev.get('strengths',[])[:3]}\n\n"
            f"PySpark lines: {len((trans.get('pyspark') or '').splitlines())}\n"
            f"DataFusion: {'Present' if trans.get('datafusion') else 'Not generated'}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _strip_fences(text: str) -> str:
    """Remove markdown code fences from LLM output."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Drop first line (```json or ```) and last line (```)
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    return text.strip()


def _strip_sql_fences(text: str) -> str:
    """Remove markdown SQL fences from LLM output."""
    text = text.strip()
    text = re.sub(r"^```(?:sql)?\s*\n", "", text, flags=re.MULTILINE)
    text = re.sub(r"\n```\s*$", "", text, flags=re.MULTILINE)
    return text.strip()


def _parse_test_harness_sections(raw: str, job_name: str) -> dict:
    """
    Parse the test harness LLM output into conftest and test_harness sections.
    Expected format:
      === conftest.py ===
      <content>
      === END conftest.py ===
      === test_harness.py ===
      <content>
      === END test_harness.py ===
    """
    conftest = ""
    test_harness = ""

    conftest_match = re.search(
        r"=== conftest\.py ===\s*(.*?)\s*=== END conftest\.py ===",
        raw, re.DOTALL | re.IGNORECASE
    )
    harness_match = re.search(
        r"=== test_harness\.py ===\s*(.*?)\s*=== END test_harness\.py ===",
        raw, re.DOTALL | re.IGNORECASE
    )

    if conftest_match:
        conftest = conftest_match.group(1).strip()
    if harness_match:
        test_harness = harness_match.group(1).strip()

    # Fallback: if delimiters not found, return the whole thing as test_harness
    if not conftest and not test_harness:
        logger.warning("[ArtifactGenerators] Could not parse test harness sections — returning raw output")
        test_harness = raw
        conftest = _stub_conftest(job_name)

    return {"conftest": conftest, "test_harness": test_harness}


def _stub_conftest(job_name: str) -> str:
    safe = job_name.lower().replace(" ", "_").replace("-", "_")
    return (
        f'"""conftest.py — Shared fixtures for {job_name} test harness."""\n\n'
        f"import pytest\n"
        f"from pyspark.sql import SparkSession\n\n\n"
        f"@pytest.fixture(scope=\"session\")\n"
        f"def spark():\n"
        f"    \"\"\"Session-scoped SparkSession fixture.\"\"\"\n"
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
        f"    \"\"\"Helper fixture for creating inline test DataFrames.\"\"\"\n"
        f"    def _make(data, schema):\n"
        f"        return spark.createDataFrame(data, schema)\n"
        f"    return _make\n"
    )
