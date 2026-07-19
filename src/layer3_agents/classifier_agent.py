"""
Agent 2: Classifier Agent — Fully LLM-driven (No Fallbacks)
Sends the complete parsed DataStage job structure to the LLM for complexity
analysis. No rule-based scoring or default fallback objects are permitted.
If the LLM call or JSON parsing fails, it raises an exception to halt the pipeline.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


def safe_json_loads(raw: str) -> dict:
    """Parse JSON strings safely, cleaning markdown fences and finding JSON boundary if needed."""
    raw = raw.strip()
    if raw.startswith("```"):
        # Remove markdown headers
        lines = raw.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines).strip()
    
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Fallback: search for first { and last } to extract JSON
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(raw[start:end+1])
            except json.JSONDecodeError:
                pass
        raise


class ClassifierAgent:
    """
    Classifies a normalized DataStage job by migration complexity using LLM.
    Complexity: simple | medium | complex
    """

    def __init__(self, engine=None, llm=None):
        self.engine = engine
        self.llm = llm

    def run(self, normalized_job: dict) -> dict:
        job_name = normalized_job.get("job_name", "?")
        logger.info(f"[ClassifierAgent] Classifying with LLM: {job_name}")

        if not self.llm:
            raise RuntimeError("[ClassifierAgent] LLM client is required but not configured.")

        classification = self._llm_classify(normalized_job)
        result = dict(normalized_job)
        result["classification"] = classification

        logger.info(
            f"[ClassifierAgent] {job_name}: complexity={classification['complexity']}, "
            f"score={classification['confidence_score']:.2f}"
        )
        return result

    def _llm_classify(self, job: dict) -> dict:
        stages = job.get("stages", [])
        links  = job.get("links", [])
        params = job.get("parameters", [])
        conns  = job.get("connections", [])

        stage_summary = "\n".join(
            f"  - [{s.get('id','')}] type={s.get('stage_type','')} "
            f"category={s.get('category','')} "
            f"transforms={len(s.get('transformations',[]))} "
            f"expressions={[t.get('expression','')[:60] for t in s.get('transformations',[])[:3]]}"
            for s in stages[:30]
        )

        link_summary = "\n".join(
            f"  - {l.get('source','')} → {l.get('target','')} "
            f"(columns={len(l.get('mapped_columns',[]))})"
            for l in links[:20]
        )

        system = (
            "You are a senior IBM DataStage migration architect. "
            "Analyze the provided DataStage job structure and classify it.\n\n"
            "Return ONLY a valid JSON object with these exact fields:\n"
            "{\n"
            '  "complexity": "simple" | "medium" | "complex",\n'
            '  "confidence_score": 0.0-1.0,\n'
            '  "reasoning": "2-4 sentence explanation of why this complexity was chosen",\n'
            '  "ambiguity_flags": ["list of specific things that are unclear or risky"],\n'
            '  "recommendations": ["list of specific things the migration engineer should verify"],\n'
            '  "stage_count": number,\n'
            '  "risk_areas": ["list of high-risk stage IDs or patterns"]\n'
            "}\n\n"
            "Complexity guide:\n"
            "  simple  → linear flow, standard connectors, simple transforms, high automation confidence\n"
            "  medium  → moderate joins/lookups, some custom logic, edge cases present\n"
            "  complex → SCD logic, custom SQL, full outer joins, many-to-many, stored procs, unknown stage types\n\n"
            "Return ONLY the JSON object, no explanation, no markdown fences."
        )

        user = (
            f"Analyze this IBM DataStage job for migration complexity.\n\n"
            f"Job name: {job.get('job_name','?')}\n"
            f"Description: {job.get('job_description','')}\n"
            f"Total stages: {len(stages)} | Links: {len(links)} | Parameters: {len(params)} | Connections: {len(conns)}\n\n"
            f"Stages:\n{stage_summary}\n\n"
            f"Data flow links:\n{link_summary}\n\n"
            f"Parameters: {[p.get('name','') for p in params[:10]]}\n"
            f"Connection types: {list({c.get('stage_type','') for c in conns})}"
        )

        try:
            raw = self.llm.complete(system, user, agent_name="ClassifierAgent")
            data = safe_json_loads(raw)
            return {
                "complexity":       data.get("complexity", "complex"),
                "confidence_score": float(data.get("confidence_score", 0.5)),
                "reasoning":        data.get("reasoning", ""),
                "ambiguity_flags":  data.get("ambiguity_flags", []),
                "recommendations":  data.get("recommendations", []),
                "stage_count":      data.get("stage_count", len(stages)),
                "risk_areas":       data.get("risk_areas", []),
                "automation_action": self._decide_action(data.get("complexity", "complex")),
            }
        except Exception as e:
            logger.error(f"[ClassifierAgent] LLM classification/parsing failed: {e}")
            raise RuntimeError(f"ClassifierAgent LLM classification/parsing failed: {e}. Please ensure the LLM response is valid JSON.")

    def _decide_action(self, complexity: str) -> str:
        return {
            "simple":  "auto_deploy",
            "medium":  "flag_and_review",
            "complex": "full_human_review",
        }.get(complexity, "full_human_review")
