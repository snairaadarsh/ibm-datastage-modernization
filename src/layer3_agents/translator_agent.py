"""
Agent 3: Translator Agent — LLM-only code generation
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

        stage_details.append("\n".join(lines))

    link_details = []
    for lnk in links:
        link_details.append(
            f"  {lnk.get('source', '')} → {lnk.get('target', '')} "
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
# System prompts
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
STEP 2 — PYSPARK CODE RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
JOIN TYPES — critical:
  • LeftOuter DataStage join → PySpark how="left"
    - Matched records flow to the main branch
    - Unmatched records MUST be captured with a separate left_anti join
      and written to the exception sink
  • Inner DataStage join → how="inner" (no exception branch needed)
  • Always use the exact join type declared in each stage's join_type field.
    Never assume inner unless explicitly declared.

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
      - Checksum(...)             → F.sha2(F.concat_ws('|', ...), 256)
      - CurrentDate()             → F.current_date()
      - DateFromComponents(9999,12,31) → F.lit('9999-12-31').cast('date')
  • Do NOT invent columns. Do NOT drop columns. Compare to checklist before finishing.

SCD TYPE-2:
  • Use Window.partitionBy(...).orderBy(...) with F.lead() for EFF_END_DATE.
  • IS_CURRENT = 'Y' when EFF_END_DATE is null, 'N' otherwise.
  • Hard-code sentinel open-end date as F.lit('9999-12-31').cast('date') when lead() is null.

SOURCE QUERIES:
  • Preserve the EXACT SQL from each stage's sql field, including WHERE clauses
    and table names (e.g. ERP.ITEM_MASTER, not ERP_INVENTORY).
  • For JDBC sources use .option("query", "<sql>") not dbtable.

GENERAL:
  • DataFrame API only — no RDD.
  • spark.sql.adaptive.enabled=true and coalescePartitions=true in SparkSession.
  • All credentials and paths from os.environ.get() — zero hardcoded values.
  • F.broadcast() for any stage classified as a lookup (PxLookup / small ref table).
  • coalesce() for null-safe arithmetic.
  • orderBy() before the final BQ write to match DataStage sort stages.
  • End with spark.stop().
  • One comment per stage mapping the DataStage stage ID to the PySpark block.

Return ONLY the Python code — no markdown fences, no explanations.\
"""

DATAFUSION_SYSTEM = """\
You are a Google Cloud DataFusion (CDAP) expert specialising in migrating IBM \
DataStage ETL jobs to production DataFusion pipelines.

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
  "artifactType": "...",
  ...
}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 2 — DATAFUSION PIPELINE JSON RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
JOIN TYPES — critical:
  • LeftOuter DataStage join → Joiner plugin with "joinType": "Outer"
    and "requiredInputs" set to only the left/primary input.
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
  • Valid Wrangler directives for allowed operations:
      set-column AVAILABLE_QTY ON_HAND_QTY - RESERVED_QTY
      set-column STOCK_STATUS if(AVAILABLE_QTY == 0, 'OUT_OF_STOCK', if(AVAILABLE_QTY < REORDER_LEVEL, 'LOW_STOCK', 'IN_STOCK'))
      set-column REORDER_REQUIRED if(AVAILABLE_QTY < REORDER_LEVEL, 1, 0)
      set-column DAYS_UNTIL_REORDER if(REORDER_REQUIRED == 1, LEAD_TIME_DAYS, -1)
      set-column RECORD_HASH md5(concat(ITEM_CODE, WAREHOUSE_ID, string(ON_HAND_QTY), string(UNIT_COST)))

SOURCE QUERIES:
  • Preserve the EXACT SQL from the source DSX including table names and WHERE clauses.
  • Use the "importQuery" property on DatabaseQuery plugins.

SORT STAGES:
  • DataStage PxSortStage → "Sorter" plugin (type: "transform", plugin: "Sorter")
    with the same sort keys and order, placed immediately before the sink.

GENERAL:
  • Valid CDAP pipeline JSON: artifactType, config.stages[], config.connections[].
  • Every plugin has: name, type, label, properties.
  • ${VARIABLE} macro syntax for all env-specific values.
  • No hardcoded credentials or project IDs.

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
            f"Generate a complete PySpark migration script for the IBM DataStage job below.\n"
            f"Follow ALL rules in the system prompt exactly.\n\n"
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
            f"Generate a complete Cloud DataFusion pipeline JSON for the IBM DataStage job below.\n"
            f"Follow ALL rules in the system prompt exactly.\n\n"
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
            f'from pyspark.sql import SparkSession\n'
            f'from pyspark.sql import functions as F\n\n'
            f'spark = SparkSession.builder.appName("{job_name}").getOrCreate()\n\n'
            + "\n".join(
                f'# TODO: Implement stage [{s.get("id", "")}] ({s.get("stage_type", "")})'
                for s in stages
            )
            + "\n\nspark.stop()\n"
        )
