"""
Layer 5: Validator Agent — v2.0 with QG-01 through QG-12 Quality Gates
Compares the architecture of the source DataStage job against the generated
output code (PySpark + DataFusion) and produces a structured validation score.
Enforces all 12 quality gates from the v2.0 production specification.
If score < 90% or critical issues found, triggers human review with 3 options.
"""

from __future__ import annotations

import logging
import json

logger = logging.getLogger(__name__)


def _safe_parse_json(raw: str) -> dict:
    """Strip markdown fences and parse JSON."""
    raw = raw.strip()
    if raw.startswith("```"):
        lines = raw.split("\n")
        lines = lines[1:] if lines[0].startswith("```") else lines
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines).strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end > start:
            return json.loads(raw[start:end + 1])
        raise


_VALIDATOR_SYSTEM = """\
You are a senior DataStage migration auditor performing a v2.0 production quality audit.

Your task is to:
1. Compare the architecture of a source IBM DataStage job against the generated PySpark and DataFusion outputs
2. Evaluate all 12 mandatory quality gates
3. Assign a validation score from 0 to 100

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
QUALITY GATES (all 12 must be evaluated)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
QG-01: Every DataStage stage has exactly one entry in DataFusion stages[] array
QG-02: Every DataStage link has exactly one entry in DataFusion connections[] array
QG-03: Every derivation expression appears in metadata column_metadata[*].derivation_expression
QG-04: Every reject path has a corresponding quarantine write in PySpark (with _reject_reason + _reject_date)
QG-05: No connection strings, passwords, or hostnames appear in plain text in any artifact
QG-06: Every column in every schema has datastage_type and spark_type — never empty
QG-07: Every transformation has a confidence score — never absent
QG-08: Every unsupported feature has a blocking flag and estimated_effort_hours
QG-09: The lineage.json graph is acyclic — no circular dependencies
QG-10: Every PySpark stage function has a docstring identifying its DataStage origin
QG-11: Overall migration_completeness_pct is calculated and present in migration_report.json
QG-12: All 8 output artifacts are generated and non-empty

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCORE GUIDE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
100 = perfect match, all stages/columns/joins/expressions present, all QGs pass
 90 = minor cosmetic differences only — fully acceptable
 70 = one or two missing columns or a join type mismatch
 50 = significant gaps: missing stages, wrong join semantics, missing expressions
<50 = critical failures: sources/targets missing, SCD logic absent, wrong schema

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RETURN FORMAT (mandatory)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Return ONLY a valid JSON object:
{
  "score": 0-100,
  "summary": "1-2 sentence overall assessment",
  "quality_gates": {
    "QG-01": { "result": "PASS|FAIL", "detail": "<what was found>" },
    "QG-02": { "result": "PASS|FAIL", "detail": "" },
    "QG-03": { "result": "PASS|FAIL", "detail": "" },
    "QG-04": { "result": "PASS|FAIL", "detail": "" },
    "QG-05": { "result": "PASS|FAIL", "detail": "" },
    "QG-06": { "result": "PASS|FAIL", "detail": "" },
    "QG-07": { "result": "PASS|FAIL", "detail": "" },
    "QG-08": { "result": "PASS|FAIL", "detail": "" },
    "QG-09": { "result": "PASS|FAIL", "detail": "" },
    "QG-10": { "result": "PASS|FAIL", "detail": "" },
    "QG-11": { "result": "PASS|FAIL", "detail": "" },
    "QG-12": { "result": "PASS|FAIL", "detail": "" }
  },
  "gates_passed": 0,
  "gates_failed": 0,
  "issues": ["concise bullet — specific issue found"],
  "strengths": ["concise bullet — what was done correctly"],
  "critical_issues": true|false
}

Return ONLY the JSON. No markdown fences, no explanation.\
"""


class ValidatorAgent:
    """
    LLM-driven validator v2.0. Compares input DataStage job architecture to
    generated PySpark and DataFusion outputs. Enforces QG-01 through QG-12.
    Returns a score, issues list, and per-gate pass/fail results.
    """

    PASS_THRESHOLD = 90  # score must be >= 90 to auto-pass

    def __init__(self, llm=None):
        self.llm = llm

    # -------------------------------------------------------------------------

    def validate(self, job: dict, artifact_files: dict | None = None) -> dict:
        """
        Validate the generated outputs against the source job structure.
        artifact_files: dict mapping artifact type to file path (for QG-12 check).
        Returns:
          {
            "passed": bool,
            "score": 0-100,
            "summary": "...",
            "quality_gates": { "QG-01": { "result": "PASS|FAIL", "detail": "" }, ... },
            "gates_passed": N,
            "gates_failed": N,
            "issues": ["..."],
            "strengths": ["..."],
            "critical_issues": bool,
          }
        """
        job_name = job.get("job_name", "?")
        logger.info(f"[ValidatorAgent] Validating: {job_name}")

        if not self.llm:
            raise RuntimeError("[ValidatorAgent] LLM client is required but not configured.")

        result = self._llm_validate(job, artifact_files or {})

        # Count gate results
        gates = result.get("quality_gates", {})
        gates_passed = sum(1 for g in gates.values() if g.get("result") == "PASS")
        gates_failed  = sum(1 for g in gates.values() if g.get("result") == "FAIL")
        result["gates_passed"] = gates_passed
        result["gates_failed"] = gates_failed

        score  = result.get("score", 0)
        passed = score >= self.PASS_THRESHOLD and not result.get("critical_issues", False)

        result["passed"] = passed
        icon = "✅" if passed else "❌"
        logger.info(
            f"[ValidatorAgent] {job_name}: {icon} score={score}% | "
            f"issues={len(result.get('issues', []))} | "
            f"gates={gates_passed}/{gates_passed + gates_failed} passed | passed={passed}"
        )
        return result

    def propose_fix(self, job: dict, issues: list, user_comment: str = "") -> str:
        """
        Given a list of validation issues (and optional user comment),
        ask the LLM to propose a concrete fix instruction for the TranslatorAgent.
        Returns a plain-text instruction string.
        """
        if not self.llm:
            raise RuntimeError("[ValidatorAgent] LLM client is required for fix proposal.")

        stages = job.get("stages", [])
        stage_ids = [s.get("id", "") for s in stages]

        system = (
            "You are a DataStage migration expert. "
            "Given a list of validation issues found in a migrated PySpark/DataFusion pipeline, "
            "propose a concrete, specific, actionable fix instruction that can be injected "
            "into the TranslatorAgent's context to produce corrected output on the next attempt.\n\n"
            "Rules:\n"
            "  - Be specific — reference exact stage IDs and column names from the job\n"
            "  - Propose concrete code patterns, not vague suggestions\n"
            "  - If a user comment is provided, incorporate it into your proposal\n"
            "  - Return only the instruction text — no preamble, no markdown"
        )

        user_comment_line = f"\nUser's additional guidance: {user_comment}" if user_comment.strip() else ""

        user = (
            f"Job: {job.get('job_name', '?')}\n"
            f"Stages: {stage_ids}\n\n"
            f"Validation issues that must be fixed:\n"
            + "\n".join(f"  - {issue}" for issue in issues)
            + user_comment_line
            + "\n\nPropose a concrete fix instruction for the TranslatorAgent:"
        )

        return self.llm.complete(system, user, agent_name="ValidatorAgent-FixProposer")

    # -------------------------------------------------------------------------

    def _llm_validate(self, job: dict, artifact_files: dict) -> dict:
        stages = job.get("stages", [])
        links = job.get("links", [])
        parameters = job.get("parameters", [])
        connections = job.get("connections", [])
        translation = job.get("translation", {}) if isinstance(job.get("translation"), dict) else {}
        pyspark_code = translation.get("pyspark", "") or ""
        datafusion_raw = translation.get("datafusion", {})
        datafusion_str = (
            json.dumps(datafusion_raw, indent=2)
            if isinstance(datafusion_raw, dict)
            else str(datafusion_raw)
        )

        # Build concise source architecture summary
        source_summary_lines = []
        for s in stages:
            sid = s.get("id", "")
            stype = s.get("stage_type", "")
            cat = s.get("category", "")
            cols = [c.get("name", "") for c in s.get("output_columns", [])]
            transforms = [t.get("output_column", "") for t in s.get("transformations", [])]
            join_type = s.get("join_type", "")
            n_out = s.get("output_pin_count", 1)
            source_summary_lines.append(
                f"  [{sid}] type={stype} cat={cat} pins_out={n_out} "
                f"cols={cols[:8]} transforms={transforms[:8]} join_type={join_type}"
            )

        link_lines = [
            f"  {lnk.get('from_stage', '')} -> {lnk.get('to_stage', '')} "
            f"(pin {lnk.get('from_pin','?')}→{lnk.get('to_pin','?')})"
            for lnk in links[:20]
        ]

        # QG-12: check which artifact files exist
        import os
        from pathlib import Path
        artifact_status = {}
        for artifact_type, fpath in artifact_files.items():
            try:
                exists = Path(fpath).exists() and Path(fpath).stat().st_size > 100
                artifact_status[artifact_type] = "present" if exists else "missing_or_empty"
            except Exception:
                artifact_status[artifact_type] = "unknown"

        expected_artifacts = [
            "pyspark", "datafusion", "migration_report", "validation_report",
            "metadata", "lineage", "ddl", "test_harness"
        ]
        missing_artifacts = [a for a in expected_artifacts if artifact_status.get(a, "missing_or_empty") != "present"]

        user = (
            f"Job: {job.get('job_name', '?')}\n"
            f"Description: {job.get('job_description', '')}\n\n"
            f"SOURCE ARCHITECTURE:\n"
            f"Stages ({len(stages)}):\n" + "\n".join(source_summary_lines) + "\n\n"
            f"Links ({len(links)}):\n" + "\n".join(link_lines) + "\n\n"
            f"Parameters: {[p.get('name', '') for p in parameters]}\n"
            f"Connections: {[(c.get('name', ''), c.get('stage_type', '')) for c in connections]}\n\n"
            f"ARTIFACT STATUS (QG-12):\n"
            f"  Present: {[a for a in expected_artifacts if artifact_status.get(a) == 'present']}\n"
            f"  Missing/empty: {missing_artifacts}\n\n"
            f"GENERATED PYSPARK CODE ({len(pyspark_code.splitlines())} lines):\n"
            f"{pyspark_code[:6000]}\n\n"
            f"GENERATED DATAFUSION CONFIG ({len(datafusion_str.splitlines())} lines):\n"
            f"{datafusion_str[:4000]}"
        )

        raw = self.llm.complete(_VALIDATOR_SYSTEM, user, agent_name="ValidatorAgent")
        return _safe_parse_json(raw)
