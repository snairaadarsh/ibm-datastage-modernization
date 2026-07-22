"""
Agent 3: Translator Agent — LLM-only code generation (v2.0)
Sends the full parsed DataStage job JSON to the LLM and asks it to generate
production-quality PySpark and Cloud DataFusion code directly.
Rule-based generators are kept only as an emergency fallback if the LLM fails.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Context builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_rich_job_context(job: dict) -> str:
    """
    Build a maximally detailed job description for the Translator prompt.
    Includes output-pin counts, all computed columns, join types, and
    the full topological execution order.
    """
    stages = job.get("stages", [])
    links  = job.get("links", [])
    params = job.get("parameters", [])
    conns  = job.get("connections", [])
    dag    = job.get("dag", {})
    topo   = dag.get("topological_order", [s["id"] for s in stages])

    stage_details = []
    for s in stages:
        sid       = s.get("id", "")
        stype     = s.get("stage_type", "")
        category  = s.get("category", "")
        n_in      = s.get("input_pin_count",  s.get("InputPins", "?"))
        n_out     = s.get("output_pin_count", s.get("OutputPins", "?"))
        join_type = s.get("join_type", "")
        join_key  = s.get("join_key", "")
        table     = s.get("table_name", "")
        conn      = s.get("connection_name", "")
        sql       = s.get("sql_query", "")
        out_cols  = [c.get("name", "") for c in s.get("output_columns", [])]
        transforms = s.get("transformations", [])
        txf_lines  = [
            f"    {t.get('output_column', '')} = {t.get('expression', '')}"
            for t in transforms
        ]
        filter_cond   = s.get("filter_condition", "")
        group_by_keys = s.get("group_by_keys", "")
        aggregations  = s.get("aggregations", "")
        lookup_key    = s.get("lookup_key", "")
        no_match      = s.get("no_match_handling", "")
        write_mode    = s.get("write_mode", "")
        bq_project    = s.get("bq_project", "")
        bq_dataset    = s.get("bq_dataset", "")
        file_path     = s.get("file_path", "")
        sort_keys     = s.get("sort_keys", "")

        lines = [
            f"[{sid}]",
            f"  type     : {stype}",
            f"  category : {category}",
            f"  pins     : {n_in} in → {n_out} out",
        ]
        if join_type:
            lines.append(f"  join_type: {join_type}")
        if join_key:
            lines.append(f"  join_key : {join_key}")
        if lookup_key:
            lines.append(f"  lookup_key: {lookup_key}")
        if no_match:
            lines.append(f"  no_match_handling: {no_match}")
        if table:
            lines.append(f"  table    : {table}")
        if conn:
            lines.append(f"  conn     : {conn}")
        if sql:
            lines.append(f"  sql      : {sql[:300]}")
        if out_cols:
            lines.append(f"  out_cols : {out_cols}")
        if txf_lines:
            lines.append("  transforms:")
            lines.extend(txf_lines)
        if filter_cond:
            lines.append(f"  filter   : {filter_cond}")
        if group_by_keys:
            lines.append(f"  group_by : {group_by_keys}")
        if aggregations:
            lines.append(f"  agg      : {aggregations}")
        if sort_keys:
            lines.append(f"  sort_keys: {sort_keys}")
        if write_mode:
            lines.append(f"  write_mode: {write_mode}")
        if bq_project:
            lines.append(f"  bq_project: {bq_project}  bq_dataset: {bq_dataset}")
        if file_path:
            lines.append(f"  file_path: {file_path}")

        stage_details.append("\n".join(lines))

    link_details = []
    for lnk in links:
        link_details.append(
            f"  {lnk.get('from_stage', '')} → {lnk.get('to_stage', '')} "
            f"[pin {lnk.get('from_pin', '?')}→{lnk.get('to_pin', '?')}] "
            f"on={lnk.get('join_key', '')} "
            f"cols={[c.get('source_col', '') for c in lnk.get('mapped_columns', [])[:8]]}"
        )

    cls = job.get("classification", {})
    return (
        f"Job: {job.get('job_name', '?')}\n"
        f"Description: {job.get('job_description', '')}\n"
        f"Complexity: {cls.get('complexity', '?')} | Confidence: {cls.get('confidence_score', 0):.0%}\n"
        f"Risk areas: {cls.get('risk_areas', [])}\n\n"
        f"Execution order (topological): {topo}\n\n"
        f"{'=' * 60}\n"
        f"STAGES ({len(stages)}) — with full column and expression detail:\n"
        f"{'=' * 60}\n"
        + "\n\n".join(stage_details)
        + f"\n\n{'=' * 60}\n"
        f"DATA FLOW LINKS ({len(links)}) — includes secondary/exception branches:\n"
        f"{'=' * 60}\n"
        + "\n".join(link_details)
        + f"\n\nParameters: {[p.get('name', '') for p in params]}\n"
        f"Connections: {[(c.get('name', ''), c.get('stage_type', '')) for c in conns]}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# System prompts — v2.0 Production Standard
# ─────────────────────────────────────────────────────────────────────────────

PYSPARK_SYSTEM = """\
You are a world-class senior data engineer specialising in migrating IBM DataStage \
ETL jobs to production-quality Apache PySpark on Google Cloud.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 1 — STRUCTURAL CHECKLIST (emit this first, as a Python comment block)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Before writing any executable code, emit a comment block:
  # MIGRATION CHECKLIST
  # Sources      : <count> — <list of stage IDs>
  # Joins        : <count> — <stage ID : join type (Inner/LeftOuter/etc.)>
  # Branches     : <count> — <stage IDs with output_pins > 1 and where each branch goes>
  # Computed cols: <every output_column from every transformer stage>
  # Sinks        : <count> — <list of target stage IDs>
  # Exceptions   : <yes/no — list exception sink stage IDs if yes>

This checklist is mandatory. It will be diffed against the source DSX by the Reviewer.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 2 — FILE STRUCTURE (follow this exact order)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. Module docstring (job name, description, DataStage source, migration date)
2. Imports block (os, logging, argparse, pyspark imports, pyspark.sql.window)
3. CONFIG dict — every configurable value, ALL from os.environ:
   CONFIG = {
       "app_name": os.environ.get("APP_NAME", "<job_name>"),
       "source_db_url": os.environ["SOURCE_DB_URL"],          # REQUIRED
       "source_db_user": os.environ["SOURCE_DB_USER"],        # REQUIRED
       "source_db_password": os.environ["SOURCE_DB_PASSWORD"],# REQUIRED — use secrets manager in prod
       "target_db_url": os.environ.get("TARGET_DB_URL", ""),
       "quarantine_path": os.environ.get("QUARANTINE_PATH", "/tmp/quarantine"),
       "broadcast_threshold": int(os.environ.get("BROADCAST_THRESHOLD", "10000000")),
       "shuffle_partitions": int(os.environ.get("SHUFFLE_PARTITIONS", "200")),
       "enable_row_count_validation": os.environ.get("ENABLE_ROW_COUNT_VALIDATION","false").lower() == "true",
   }
4. Logging setup (JSON structured logging — NOT print())
5. startup_validation() — checks all REQUIRED env vars are set before run
6. SparkSession builder:
   spark = SparkSession.builder \
       .appName(CONFIG["app_name"]) \
       .config("spark.sql.shuffle.partitions", CONFIG["shuffle_partitions"]) \
       .config("spark.sql.adaptive.enabled", "true") \
       .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
       .getOrCreate()
7. Helper / UDF definitions (one per DataStage custom routine)
8. Stage functions (one function per DataStage stage) — see rules below
9. main() orchestration function
10. CLI entry point:
    if __name__ == "__main__":
        import argparse
        parser = argparse.ArgumentParser(description="<job_name> PySpark Migration")
        parser.add_argument("--run-mode", choices=["full","incremental","rerun"], default="full")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--log-level", default="INFO")
        args = parser.parse_args()
        main(run_mode=args.run_mode, dry_run=args.dry_run, log_level=args.log_level)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 3 — STAGE FUNCTION RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Every DataStage stage = one named Python function with this structure:

def stage_<StageName>(df_input) -> DataFrame:
    \"\"\"
    DataStage Stage: <StageName>
    Type: <stage_type>
    Plugin: <plugin>
    Input Links: [<links>]
    Output Links: [<links>]
    \"\"\"
    import time
    t0 = time.time()
    logger.info({"stage": "<StageName>", "event": "entry", "input_rows": df_input.count()})
    # ... stage logic ...
    logger.info({"stage": "<StageName>", "event": "exit", "output_rows": df_out.count(),
                 "duration_ms": round((time.time() - t0) * 1000)})
    return df_out

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 4 — PYSPARK CODE RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

JOIN TYPES — critical:
  • LeftOuter DataStage join → PySpark how="left"
    - Matched records flow to the main branch
    - Unmatched records MUST be captured with a separate left_anti join
      and written to the exception sink with:
        df_rejected.withColumn("_reject_reason", F.lit("No match on <key>")) \
                   .withColumn("_reject_date", F.current_date())
        df_rejected.write.partitionBy("_reject_date").mode("append") \
                   .parquet(CONFIG["quarantine_path"])
  • Inner DataStage join → how="inner" (no exception branch needed)
  • Always use the exact join type declared in each stage's join_type field.
    Never assume inner unless explicitly declared.

REJECT / EXCEPTION PATHS:
  • Every reject link = separate DataFrame written to CONFIG["quarantine_path"]
  • Rejected rows MUST include: _reject_reason (string), _reject_date (date)
  • Write with .partitionBy("_reject_date").mode("append").parquet(...)
  • QUARANTINE_PATH comes from CONFIG — never hardcoded

BRANCHES / MULTI-OUTPUT STAGES:
  • Any stage with output_pins > 1 produces multiple output DataFrames.
  • Implement every branch with a comment:
      # ── Branch 0 (matched) → <next stage>
      # ── Branch 1 (unmatched/exception) → <exception sink>
  • Exception sinks must be written as a separate DataFrame write, not dropped.

COMPUTED COLUMNS — zero omissions:
  • Every entry in the CHECKLIST "Computed cols" row MUST appear as a withColumn().
  • Translate DataStage expressions faithfully:
      - If/Then/Else              → F.when().when().otherwise()
      - Checksum(...)             → F.sha2(F.concat_ws("|", ...), 256)
      - CurrentDate()             → F.current_date()
      - DateFromComponents(9999,12,31) → F.lit("9999-12-31").cast("date")
      - TRIM(x)                   → F.trim(F.col(x))
      - UPCASE(x)                 → F.upper(F.col(x))
      - LEFT(x, n)                → F.substring(F.col(x), 1, n)
      - ISNULL(x)                 → F.col(x).isNull()
      - Year(x)                   → F.year(F.col(x))
      - Month(x)                  → F.month(F.col(x))
      - MOD(x, y)                 → F.col(x) % y
  • Do NOT invent columns. Do NOT drop columns. Compare to checklist before finishing.

SCD TYPE 1:
  • Implement as a Delta Lake MERGE INTO if Delta available.
  • If not: overwrite the target partition keyed on the merge key.

SCD TYPE 2:
  • Use Window.partitionBy(...).orderBy(...) with F.lead() for EFF_END_DATE.
  • IS_CURRENT_FLAG = 1 when EFF_END_DATE is null, 0 otherwise.
  • Hard-code sentinel open-end date as F.lit("9999-12-31").cast("date") when lead() is null.

SCD TYPE 3:
  • Preserve current_value and previous_value columns explicitly.

PARTITIONING / SORTING:
  • Hash → repartition(n, col)
  • Range → repartition(n).sortWithinPartitions(col)
  • RoundRobin → repartition(n)
  • Same → no repartition (add comment: # DS: Same partitioning — no repartition)
  • Entire → coalesce(1)  # WARNING: performance risk — single-partition write
  • Preserve every DataStage sort: ascending/descending, nulls first/last.

LOOKUPS:
  • Every PxLookup → F.broadcast(ref_df) join
  • If reference_rows > BROADCAST_THRESHOLD: fall back to sort-merge, log WARNING
  • Lookup failure modes: Continue → left join + coalesce nulls; Reject → filter + quarantine; Fail → raise ValueError

AGGREGATIONS:
  • Preserve all group-by keys and aggregation functions exactly
  • SUM→F.sum(), COUNT→F.count(), AVG→F.avg(), MAX→F.max(), MIN→F.min(),
    FIRST→F.first(), LAST→F.last(), STDDEV→F.stddev(), VARIANCE→F.variance()

ROW COUNT VALIDATION:
  • If CONFIG["enable_row_count_validation"] is True, after each stage assert:
      assert output_count > 0, f"Stage <name> produced 0 rows — check pipeline"

GENERAL:
  • DataFrame API only — no RDD.
  • All credentials and paths from CONFIG — zero hardcoded values.
  • F.broadcast() for any stage classified as lookup (PxLookup / small ref table).
  • F.coalesce() for null-safe arithmetic.
  • orderBy() before the final BQ write to match DataStage sort stages.
  • End with spark.stop().
  • One comment per stage mapping the DataStage stage ID to the PySpark block.
  • No TODO stubs — every stage must be fully implemented.

SOURCE QUERIES:
  • Preserve the EXACT SQL from each stage's sql field, including WHERE clauses
    and table names.
  • For JDBC sources use .option("query", "<sql>") not dbtable.

Return ONLY the Python code — no markdown fences, no explanations.\
"""

DATAFUSION_SYSTEM = """\
You are a Google Cloud DataFusion (CDAP) expert specialising in migrating IBM \
DataStage ETL jobs to production DataFusion pipelines. Generate schema_version 2.0 artifacts.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 1 — STRUCTURAL CHECKLIST (emit as top-level "checklist" key in the JSON)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The JSON must include a "checklist" object at the top level:
{
  "checklist": {
    "sources":         ["<stage IDs>"],
    "joins":           {"<stage ID>": "<join type>"},
    "branches":        {"<stage ID with >1 output>": ["<branch 0 target>", "<branch 1 target>"]},
    "computed_cols":   ["<every output_column from every transformer>"],
    "sinks":           ["<stage IDs>"],
    "exception_sinks": ["<stage IDs or empty list>"]
  },
  ...
}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 2 — TOP-LEVEL SCHEMA (v2.0)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{
  "schema_version": "2.0",
  "job": {
    "id": "<uuid>",
    "name": "<job_name>",
    "description": "<description or UNKNOWN>",
    "category": "<DataStage category path or UNKNOWN>",
    "job_type": "<ServerJob|ParallelJob|SequenceJob>",
    "source_platform": "IBM DataStage",
    "source_version": "<version or UNKNOWN>",
    "target_platform": "Apache DataFusion",
    "target_version": "latest",
    "migration_timestamp": "<ISO-8601>",
    "migration_version": "2.0",
    "generator_version": "datastage-migrator-2.0",
    "tags": [],
    "annotations": {}
  },
  "checklist": { ... },
  "artifactType": "cdap-data-pipeline",
  "config": {
    "stages": [ ... ],
    "connections": [ ... ]
  },
  "parameters": [],
  "environment_variables": [],
  "shared_containers": [],
  "execution_plan": {
    "topological_order": [],
    "parallel_stages": [],
    "estimated_duration_minutes": "UNKNOWN"
  }
}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 3 — STAGE OBJECT REQUIREMENTS (every stage)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Every stage must include:
{
  "id": "<uuid>",
  "name": "<original DataStage stage name — never renamed>",
  "stage_type": "<DataStage stage type>",
  "plugin": "<plugin name>",
  "category": "<Source|Target|Transform|Lookup|Join|Aggregation|Sort|Filter|UNKNOWN>",
  "description": "<stage description or UNKNOWN>",
  "execution_mode": "<Parallel|Sequential|UNKNOWN>",
  "node_pool": "UNKNOWN",
  "properties": { "<all stage-specific properties verbatim>" },
  "runtime_parameters": {},
  "partitioning": { "method": "<Hash|Range|RoundRobin|Same|Entire|UNKNOWN>", "keys": [], "partition_count": "UNKNOWN" },
  "sorting": { "keys": [], "sort_order": [], "nulls_order": [], "stable_sort": "UNKNOWN" },
  "schema": {
    "columns": [
      {
        "name": "<column_name>",
        "datastage_type": "<original DataStage type>",
        "spark_type": "<mapped Spark type>",
        "nullable": "true|false",
        "length": null,
        "precision": null,
        "scale": null,
        "description": "UNKNOWN",
        "tags": []
      }
    ]
  },
  "input_links": ["<link_id>"],
  "output_links": ["<link_id>"],
  "generated_from": {
    "datastage_stage_id": "<internal DataStage ID or UNKNOWN>",
    "datastage_stage_type": "<type>",
    "datastage_plugin": "<plugin>",
    "confidence": 1.0
  },
  "transformations": [
    {
      "id": "<uuid>",
      "column_name": "<output column>",
      "transformation_type": "Derivation|Constraint|Aggregation|Filter|SCD|Custom|UNKNOWN",
      "original_expression": "<exact DataStage expression verbatim>",
      "translated_expression": "<DataFusion/Wrangler equivalent>",
      "confidence": 0.0,
      "confidence_reason": "<why this score>",
      "requires_manual_review": false,
      "notes": ""
    }
  ],
  "annotations": {},
  "warnings": []
}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 4 — CONNECTION / LINK OBJECTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Every link must include:
{
  "id": "<uuid>",
  "name": "<original DataStage link name>",
  "source_stage_id": "<uuid>",
  "source_stage_name": "<name>",
  "target_stage_id": "<uuid>",
  "target_stage_name": "<name>",
  "link_type": "Main|Reference|Reject|Lookup|UNKNOWN",
  "schema": { "<same column structure as stage schema>" },
  "partitioning": { "<same as stage partitioning>" },
  "sorting": { "<same as stage sorting>" },
  "row_count_hint": "UNKNOWN",
  "metadata": {}
}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 5 — DATAFUSION PIPELINE JSON RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

JOIN TYPES — critical:
  • LeftOuter DataStage join → Joiner plugin with "joinType": "Left" and
    "requiredInputs" set to only the left/primary input.
  • A LeftOuter join MUST produce two connections:
      - One from Joiner to main processing stage (matched records)
      - One from Joiner to exception sink (unmatched records)
  • Inner join → standard Joiner with all inputs required.

BRANCHES / MULTI-OUTPUT STAGES:
  • Every stage with output_pins > 1 must appear as a SOURCE in multiple
    connection entries — one per output branch.
  • Exception sinks must appear as stages AND as connection targets.

COMPUTED COLUMNS — zero omissions:
  • Every column in checklist.computed_cols must appear as a Wrangler directive
    OR in a SparkSQL / BigQueryExecute plugin.
  • PLATFORM CONSTRAINT — Wrangler cannot do window functions:
    - lead(), row_number(), rank() are NOT available in Wrangler directives.
    - SCD Type-2 EFF_END_DATE / IS_CURRENT using lead() MUST go in a
      "sparksql" plugin (type: "transform", plugin name: "Spark SQL") or
      be offloaded to BigQuery via a post-load MERGE in a BigQueryExecute sink.
    - Do NOT put window logic in a Wrangler stage — it will fail at runtime.

SINK STAGES — additional fields:
  Every sink stage must include a "sink" object:
  {
    "sink": {
      "system_type": "<Oracle|DB2|Parquet|Delta|CSV|BigQuery|UNKNOWN>",
      "connection_name": "<DataStage connection name>",
      "target_table": "<fully qualified table name>",
      "write_mode": "<Overwrite|Append|Upsert|Merge|UNKNOWN>",
      "merge_keys": [],
      "partition_columns": [],
      "pre_sql": null,
      "post_sql": null,
      "array_size": "UNKNOWN",
      "isolation_level": "UNKNOWN"
    }
  }

SOURCE STAGES — additional fields:
  Every source stage must include a "source" object:
  {
    "source": {
      "system_type": "<Oracle|DB2|File|S3|UNKNOWN>",
      "connection_name": "<DataStage connection name>",
      "host": "MASKED",
      "port": "MASKED",
      "database": "<database name or UNKNOWN>",
      "schema": "<schema name or UNKNOWN>",
      "query": "<full SQL query or table name verbatim>",
      "query_type": "<Table|SQL|StoredProcedure|UNKNOWN>",
      "incremental_column": null,
      "connection_properties": "MASKED — see environment variables"
    }
  }

CONFIDENCE SCALE (mandatory for every transformation):
  1.0   = Direct syntactic equivalent; no assumptions
  0.85+ = Semantic equivalent; minor behavioural assumption documented
  0.70+ = Approximate; recommend manual review
  0.50+ = Partial translation; significant assumptions; manual review required
  <0.50 = Cannot translate reliably; emit as UNKNOWN; escalate

SOURCE QUERIES:
  • Preserve the EXACT SQL from the source DSX including table names and WHERE clauses.
  • Use the "importQuery" property on DatabaseQuery plugins.

SORT STAGES:
  • DataStage PxSortStage → "Sorter" plugin (type: "transform", plugin: "Sorter")
    with the same sort keys and order, placed immediately before the sink.

GENERAL:
  • ${VARIABLE} macro syntax for all env-specific values.
  • No hardcoded credentials, project IDs, or hostnames.
  • execution_plan.topological_order must match the DataStage DAG.

Return ONLY valid JSON — no markdown fences, no explanations.\
"""


# ─────────────────────────────────────────────────────────────────────────────
# TranslatorAgent
# ─────────────────────────────────────────────────────────────────────────────

class TranslatorAgent:
    """
    Generates PySpark scripts and DataFusion pipeline JSON using an LLM.
    The LLM receives the complete parsed job structure and generates both
    outputs in a single, context-aware pass.
    Version 2.0: Produces production-standard artifacts with CONFIG dict,
    JSON logging, reject paths, SCD support, and CLI entry point.
    """

    def __init__(self, engine=None, llm=None):
        self.engine = engine
        self.llm = llm

    def run(self, classified_job: dict, output_formats: list[str] | None = None) -> dict:
        formats  = output_formats or ["pyspark", "datafusion"]
        job_name = classified_job.get("job_name", "unknown_job")
        logger.info(f"[TranslatorAgent] Translating '{job_name}' → {formats} via LLM")

        # Read raw XML directly if source file path is present in the job dict
        raw_xml = None
        source_path = classified_job.get("source_file") or classified_job.get("dsx_file")
        if source_path:
            try:
                from pathlib import Path
                raw_xml = Path(source_path).read_text(encoding="utf-8", errors="ignore")
                logger.info(f"[TranslatorAgent] Read {len(raw_xml)} chars of raw XML from {source_path}")
            except Exception as e:
                logger.debug(f"[TranslatorAgent] Could not read raw XML from {source_path}: {e}")

        translation: dict[str, Any] = {
            "pyspark":         None,
            "datafusion":      None,
            "inline_comments": [],
            "warnings":        [],
        }

        if self.llm:
            if "pyspark" in formats:
                translation["pyspark"] = self._llm_generate_pyspark(classified_job, raw_xml)
            if "datafusion" in formats:
                translation["datafusion"] = self._llm_generate_datafusion(classified_job, raw_xml)
        else:
            logger.warning("[TranslatorAgent] No LLM configured — outputs will be empty stubs.")
            translation["pyspark"]    = self._stub_pyspark(classified_job)
            translation["datafusion"] = {}

        result = dict(classified_job)
        result["translation"] = translation
        logger.info(f"[TranslatorAgent] Translation complete for '{job_name}'")
        return result

    # ── LLM generation ────────────────────────────────────────────────────────

    def _llm_generate_pyspark(self, job: dict, raw_xml: str | None = None) -> str:
        context = _build_rich_job_context(job)
        if raw_xml:
            context += f"\n\n{'=' * 60}\nRAW SOURCE DSX XML:\n{'=' * 60}\n{raw_xml}"
        user = (
            f"Generate a complete, production-ready PySpark v2.0 migration script for the IBM DataStage job below.\n"
            f"Follow ALL rules in the system prompt exactly — CONFIG dict, JSON logging, reject paths, CLI entry point.\n\n"
            f"{context}"
        )
        try:
            return self.llm.complete(PYSPARK_SYSTEM, user, agent_name="TranslatorAgent-PySpark")
        except Exception as e:
            logger.error(f"[TranslatorAgent] PySpark LLM generation failed: {e}")
            return self._stub_pyspark(job)

    def _llm_generate_datafusion(self, job: dict, raw_xml: str | None = None) -> dict:
        context = _build_rich_job_context(job)
        if raw_xml:
            context += f"\n\n{'=' * 60}\nRAW SOURCE DSX XML:\n{'=' * 60}\n{raw_xml}"
        user = (
            f"Generate a complete Cloud DataFusion pipeline JSON (schema_version 2.0) for the IBM DataStage job below.\n"
            f"Follow ALL rules in the system prompt exactly — v2.0 stage objects, transformation confidence scores, "
            f"source/sink additional fields, execution_plan.\n\n"
            f"{context}"
        )
        try:
            raw = self.llm.complete(DATAFUSION_SYSTEM, user, agent_name="TranslatorAgent-DataFusion")
            raw = raw.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            return json.loads(raw)
        except json.JSONDecodeError as e:
            logger.warning(f"[TranslatorAgent] DataFusion JSON parse failed: {e} — returning raw string wrapped")
            return {"raw": raw, "parse_error": str(e)}
        except Exception as e:
            logger.error(f"[TranslatorAgent] DataFusion LLM generation failed: {e}")
            return {}

    # ── Emergency stub (only used when LLM is unavailable) ───────────────────

    def _stub_pyspark(self, job: dict) -> str:
        job_name = job.get("job_name", "etl_job")
        stages   = job.get("stages", [])
        return (
            f'#!/usr/bin/env python3\n'
            f'"""\n{job_name} — PySpark stub (LLM was not available)\n"""\n\n'
            f'import os\n'
            f'import logging\n'
            f'from pyspark.sql import SparkSession\n'
            f'from pyspark.sql import functions as F\n\n'
            f'CONFIG = {{\n'
            f'    "app_name": os.environ.get("APP_NAME", "{job_name}"),\n'
            f'    "quarantine_path": os.environ.get("QUARANTINE_PATH", "/tmp/quarantine"),\n'
            f'}}\n\n'
            f'spark = SparkSession.builder\\\n'
            f'    .appName(CONFIG["app_name"])\\\n'
            f'    .config("spark.sql.adaptive.enabled", "true")\\\n'
            f'    .getOrCreate()\n\n'
            + "\n".join(
                f'# TODO: Implement stage [{s.get("id", "")}] ({s.get("stage_type", "")})'
                for s in stages
            )
            + "\n\nspark.stop()\n"
        )
