"""
Layer 5: Validator Agent — Fully LLM-driven
Replaces the rule-based Validator class.
Compares the architecture of the source DataStage job against the generated
output code (PySpark + DataFusion) and produces a structured validation score.
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


class ValidatorAgent:
    """
    LLM-driven validator. Compares input DataStage job architecture to
    generated PySpark and DataFusion outputs. Returns a score and issues list.
    """

    PASS_THRESHOLD = 90  # score must be >= 90 to auto-pass

    def __init__(self, llm=None):
        self.llm = llm

    # -------------------------------------------------------------------------

    def validate(self, job: dict) -> dict:
        """
        Validate the generated outputs against the source job structure.
        Returns:
          {
            "passed": bool,
            "score": 0-100,
            "summary": "...",
            "issues": ["..."],
            "strengths": ["..."],
            "critical_issues": bool,
          }
        """
        job_name = job.get("job_name", "?")
        logger.info(f"[ValidatorAgent] Validating: {job_name}")

        if not self.llm:
            raise RuntimeError("[ValidatorAgent] LLM client is required but not configured.")

        result = self._llm_validate(job)

        score = result.get("score", 0)
        passed = score >= self.PASS_THRESHOLD and not result.get("critical_issues", False)

        result["passed"] = passed
        icon = "✅" if passed else "❌"
        logger.info(
            f"[ValidatorAgent] {job_name}: {icon} score={score}% | "
            f"issues={len(result.get('issues', []))} | passed={passed}"
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

    def _llm_validate(self, job: dict) -> dict:
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
            source_summary_lines.append(
                f"  [{sid}] type={stype} cat={cat} "
                f"cols={cols[:8]} transforms={transforms[:8]} join_type={join_type}"
            )

        link_lines = [
            f"  {lnk.get('source', '')} -> {lnk.get('target', '')} on={lnk.get('join_key', '')}"
            for lnk in links[:20]
        ]

        system = (
            "You are a senior DataStage migration auditor. "
            "Your task is to compare the architecture of a source IBM DataStage job "
            "against the generated PySpark and Cloud DataFusion output code.\n\n"
            "Evaluate whether the output is a faithful, complete migration of the source job. "
            "Assign a validation score from 0 to 100.\n\n"
            "Score guide:\n"
            "  100 = perfect match, all stages, columns, joins, and expressions present\n"
            "   90 = minor cosmetic differences only — fully acceptable\n"
            "   70 = one or two missing columns or a join type mismatch\n"
            "   50 = significant gaps: missing stages, wrong join semantics, missing expressions\n"
            "  <50 = critical failures: sources/targets missing, SCD logic absent, wrong schema\n\n"
            "Return ONLY a valid JSON object with these exact fields:\n"
            "{\n"
            '  "score": 0-100,\n'
            '  "summary": "1-2 sentence overall assessment",\n'
            '  "issues": ["concise bullet — specific issue found"],\n'
            '  "strengths": ["concise bullet — what was done correctly"],\n'
            '  "critical_issues": true|false\n'
            "}\n"
            "Return ONLY the JSON. No markdown fences, no explanation."
        )

        user = (
            f"Job: {job.get('job_name', '?')}\n"
            f"Description: {job.get('job_description', '')}\n\n"
            f"SOURCE ARCHITECTURE:\n"
            f"Stages ({len(stages)}):\n" + "\n".join(source_summary_lines) + "\n\n"
            f"Links ({len(links)}):\n" + "\n".join(link_lines) + "\n\n"
            f"Parameters: {[p.get('name', '') for p in parameters]}\n"
            f"Connections: {[(c.get('name', ''), c.get('stage_type', '')) for c in connections]}\n\n"
            f"GENERATED PYSPARK CODE ({len(pyspark_code.splitlines())} lines):\n"
            f"{pyspark_code[:6000]}\n\n"
            f"GENERATED DATAFUSION CONFIG ({len(datafusion_str.splitlines())} lines):\n"
            f"{datafusion_str[:4000]}"
        )

        raw = self.llm.complete(system, user, agent_name="ValidatorAgent")
        return _safe_parse_json(raw)
