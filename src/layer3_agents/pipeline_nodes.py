"""
Shared Pipeline Node Functions
Reusable node functions for the LangGraph pipeline, used by both
CLI mode (orchestrator.py) and server mode (pipeline_server.py).
Eliminates duplicate logic between the two entry points.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def build_parse_node(parser_class):
    """Create a parse node function using the given DataStageParser class."""
    def parse_node(state: dict) -> dict:
        source = state.get("source_file") or state.get("dsx_file")
        logger.info(f"[Parse] Ingesting file: {Path(source).name}")
        parser = parser_class(source)
        raw = parser.parse()
        logger.info(f"[Parse] XML parsed — {len(raw.get('stages', []))} stages found")
        return {"raw_parsed": raw}
    return parse_node


def build_normalize_node(parser_agent):
    """Create a normalize node function using the given ParserAgent."""
    def normalize_node(state: dict) -> dict:
        logger.info("[Normalize] Normalizing to canonical DataStage schema...")
        normalized = parser_agent.run(state["raw_parsed"])
        # Try hybrid LLM refinement
        source = state.get("source_file") or state.get("dsx_file")
        try:
            raw_xml = Path(source).read_text(encoding="utf-8", errors="ignore")
            normalized = parser_agent.refine_with_llm(normalized, raw_xml)
        except Exception as e:
            logger.debug(f"[Normalize] Hybrid refinement skipped: {e}")
        logger.info("[Normalize] Normalization complete")
        return {"normalized": normalized}
    return normalize_node


def build_classify_node(classifier_agent):
    """Create a classify node function using the given ClassifierAgent."""
    def classify_node(state: dict) -> dict:
        logger.info("[Classify] LLM is analyzing job complexity...")
        classified = classifier_agent.run(state["normalized"])
        cls = classified.get("classification", {})
        complexity = cls.get("complexity", "medium")
        confidence = cls.get("confidence_score", 0.85) * 100
        reasoning = cls.get("reasoning", "")
        logger.info(f"[Classify] Complexity: {complexity} | Confidence: {confidence:.1f}%")
        logger.info(f"[Classify] Reasoning: {reasoning[:120]}...")
        return {"classified": classified}
    return classify_node


def build_translate_node(translator_agent, output_formats=None):
    """Create a translate node function using the given TranslatorAgent."""
    formats = output_formats or ["pyspark", "datafusion"]
    def translate_node(state: dict) -> dict:
        logger.info("[Translate] LLM generating pipeline code...")
        translated = translator_agent.run(state["classified"], formats)
        pyspark_code = translated.get("translation", {}).get("pyspark", "") or ""
        datafusion_cfg = translated.get("translation", {}).get("datafusion", {}) or {}
        datafusion_str = json.dumps(datafusion_cfg, indent=2) if isinstance(datafusion_cfg, dict) else str(datafusion_cfg)
        pyspark_lines = len(pyspark_code.splitlines())
        df_lines = len(datafusion_str.splitlines())
        logger.info(f"[Translate] Done — {pyspark_lines} lines PySpark, {df_lines} lines DataFusion")
        return {"translated": translated}
    return translate_node


def build_review_node(reviewer_agent):
    """Create a review node function using the given ReviewerAgent."""
    def review_node(state: dict) -> dict:
        logger.info("[Review] LLM performing code review...")
        reviewed = reviewer_agent.run(state["translated"])
        rev = reviewed.get("review", {})
        rev_score = rev.get("score", 0.9)
        rev_summary = rev.get("summary", "")
        level = "success" if rev.get("passed") else "warning"
        logger.info(f"[Review] Code review done — score={rev_score:.0%} | {rev_summary[:100]}")
        return {"reviewed": reviewed}
    return review_node


def build_escalation_check(escalation_handler):
    """Create an escalation check function."""
    def check_escalation(classified_job: dict) -> tuple:
        return escalation_handler.check(classified_job)
    return check_escalation


def cleanup_upload(source_path) -> None:
    """Delete source file if it's in the uploads directory."""
    try:
        p = Path(source_path)
        if p.exists() and p.parent.name == "uploads":
            p.unlink()
            logger.info(f"[Cleanup] Removed processed upload: {p.name}")
    except Exception as e:
        logger.warning(f"[Cleanup] Upload cleanup failed: {e}")
