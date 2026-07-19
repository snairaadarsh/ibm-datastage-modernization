"""
Layer 3: Multi-Agent Orchestrator
Coordinates the 6 specialized agents using a LangGraph StateGraph pipeline:
Parser → Classifier → Escalation Gate → Translator → Reviewer → Tester → Documentation → Validation
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, TypedDict, Dict, List

from src.layer1_ingestion.xml_parser import DataStageParser
from src.layer2_reasoning_engine.reasoning_engine import ReasoningEngine
from src.layer3_agents.parser_agent import ParserAgent
from src.layer3_agents.classifier_agent import ClassifierAgent
from src.layer3_agents.translator_agent import TranslatorAgent
from src.layer3_agents.reviewer_agent import ReviewerAgent
from src.layer3_agents.tester_agent import TesterAgent
from src.layer3_agents.documentation_agent import DocumentationAgent
from src.layer5_validation.validator import Validator
from src.layer6_human_review.escalation import EscalationHandler

logger = logging.getLogger(__name__)


class PipelineState(TypedDict):
    job_name: str
    source_file: str
    dry_run: bool
    status: str
    raw_parsed: dict
    normalized: dict
    classified: dict
    translated: dict
    reviewed: dict
    tested: dict
    documented: dict
    validation: dict
    escalation: dict
    output_files: dict
    stage_timings: dict
    started_at: str
    completed_at: str
    elapsed_seconds: float
    error: str | None


class PipelineOrchestrator:
    """
    Orchestrates the full DataStage modernization pipeline.
    Coordinates all agents, validation, human review, and output generation via LangGraph.
    """

    def __init__(self, config: dict | None = None, output_dir: str = "./output"):
        self.config = config or {}
        self.output_dir = Path(output_dir)
        self.output_formats = self.config.get("output", {}).get("formats", ["pyspark", "datafusion"])

        # ── Initialize reasoning engine (the AI brain) ─────────────────────────
        self.engine = ReasoningEngine(self.config.get("complexity", {}))

        # ── Initialize LLM client ──────────────────────────────────────────────
        import os
        from src.llm_client import LLMClient
        provider = self.config.get("llm", {}).get("provider") or os.environ.get("LLM_PROVIDER") or "gemini"
        api_key = self.config.get("llm", {}).get("api_key") or os.environ.get("LLM_API_KEY", "")
        model = self.config.get("llm", {}).get("model") or os.environ.get("LLM_MODEL") or None
        self.llm = LLMClient(provider=provider, api_key=api_key, model=model)

        # ── Initialize all agents ──────────────────────────────────────────────
        self.parser_agent       = ParserAgent(self.engine, self.llm)
        self.classifier_agent   = ClassifierAgent(self.engine, self.llm)
        self.translator_agent   = TranslatorAgent(self.engine, self.llm)
        self.reviewer_agent     = ReviewerAgent(self.engine, self.llm)
        self.tester_agent       = TesterAgent(self.engine, self.llm)
        self.documentation_agent = DocumentationAgent(self.engine, self.llm)

        # ── Layer 5/6 ──────────────────────────────────────────────────────────
        self.validator       = Validator(self.config)
        self.escalation      = EscalationHandler(self.config)

        # ── LangGraph Compilation ──────────────────────────────────────────────
        from langgraph.graph import StateGraph, END

        workflow = StateGraph(PipelineState)

        # 1. Parse node
        def parse_node(state: PipelineState) -> dict:
            t0 = time.time()
            logger.info("[Step 1/8] Parsing DataStage XML...")
            parser = DataStageParser(state["source_file"])
            raw = parser.parse()
            return {
                "raw_parsed": raw,
                "stage_timings": {**state.get("stage_timings", {}), "parse": round(time.time() - t0, 2)}
            }

        # 2. Normalize node
        def normalize_node(state: PipelineState) -> dict:
            t0 = time.time()
            logger.info("[Step 2/8] Parser Agent normalizing structure...")
            normalized = self.parser_agent.run(state["raw_parsed"])
            try:
                raw_xml = Path(state["source_file"]).read_text(encoding="utf-8", errors="ignore")
                normalized = self.parser_agent.refine_with_llm(normalized, raw_xml)
            except Exception as e:
                logger.debug(f"[Orchestrator] Failed to read source file for hybrid refinement: {e}")
            return {
                "normalized": normalized,
                "stage_timings": {**state.get("stage_timings", {}), "parser_agent": round(time.time() - t0, 2)}
            }

        # 3. Classify node
        def classify_node(state: PipelineState) -> dict:
            t0 = time.time()
            logger.info("[Step 3/8] Classifier Agent scoring complexity...")
            classified = self.classifier_agent.run(state["normalized"])
            return {
                "classified": classified,
                "stage_timings": {**state.get("stage_timings", {}), "classifier_agent": round(time.time() - t0, 2)}
            }

        # 4. Escalation gate node
        def escalation_node(state: PipelineState) -> dict:
            should_continue, escalation_result = self.escalation.check(state["classified"])
            status = "in_progress" if should_continue else "escalated"
            if not should_continue:
                logger.warning(f"[Step 4/8] ⛔ Job escalated for human review. Stopping automation.")
            else:
                logger.info(f"[Step 4/8] Escalation check: proceed with automation")
            return {
                "escalation": escalation_result,
                "status": status,
                "completed_at": time.strftime("%Y-%m-%dT%H:%M:%S") if not should_continue else ""
            }

        # 5. Translate node
        def translate_node(state: PipelineState) -> dict:
            t0 = time.time()
            logger.info(f"[Step 5/8] Translator Agent generating outputs...")
            translated = self.translator_agent.run(state["classified"], self.output_formats)
            return {
                "translated": translated,
                "stage_timings": {**state.get("stage_timings", {}), "translator_agent": round(time.time() - t0, 2)}
            }

        # 6. Review node
        def review_node(state: PipelineState) -> dict:
            t0 = time.time()
            logger.info("[Step 6/8] Reviewer Agent reviewing generated code...")
            reviewed = self.reviewer_agent.run(state["translated"])
            return {
                "reviewed": reviewed,
                "stage_timings": {**state.get("stage_timings", {}), "reviewer_agent": round(time.time() - t0, 2)}
            }

        # 7. Tester node
        def tester_node(state: PipelineState) -> dict:
            t0 = time.time()
            logger.info("[Step 7/8] Tester Agent generating test suite...")
            tested = self.tester_agent.run(state["reviewed"])
            return {
                "tested": tested,
                "stage_timings": {**state.get("stage_timings", {}), "tester_agent": round(time.time() - t0, 2)}
            }

        # 8. Documentation node
        def doc_node(state: PipelineState) -> dict:
            t0 = time.time()
            logger.info("[Step 7/8] Documentation Agent writing migration docs...")
            documented = self.documentation_agent.run(state["tested"])
            if not state["dry_run"]:
                self._save_outputs(documented, state["job_name"])
            return {
                "documented": documented,
                "stage_timings": {**state.get("stage_timings", {}), "documentation_agent": round(time.time() - t0, 2)}
            }

        # 9. Validation node
        def validation_node(state: PipelineState) -> dict:
            t0 = time.time()
            logger.info("[Step 8/8] Validator running checks...")
            validation_result = self.validator.validate(state["documented"])
            output_files = self._list_output_files(state["job_name"], state["dry_run"])

            if not state["dry_run"]:
                if hasattr(self, "github") and self.github:
                    self.github.commit_pipeline(state["job_name"], output_files)
                if hasattr(self, "gcs") and self.gcs:
                    self.gcs.upload_report(state["job_name"])

            return {
                "validation": validation_result,
                "output_files": output_files,
                "status": "completed",
                "completed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "stage_timings": {**state.get("stage_timings", {}), "validation": round(time.time() - t0, 2)}
            }

        # Register LangGraph structure
        workflow.add_node("parse", parse_node)
        workflow.add_node("normalize", normalize_node)
        workflow.add_node("classify", classify_node)
        workflow.add_node("escalation", escalation_node)
        workflow.add_node("translate", translate_node)
        workflow.add_node("review", review_node)
        workflow.add_node("tester", tester_node)
        workflow.add_node("documentation", doc_node)
        workflow.add_node("validation", validation_node)

        workflow.set_entry_point("parse")

        workflow.add_edge("parse", "normalize")
        workflow.add_edge("normalize", "classify")
        workflow.add_edge("classify", "escalation")

        def check_escalation(state: PipelineState):
            if state["status"] == "escalated":
                return END
            return "translate"

        workflow.add_conditional_edges(
            "escalation",
            check_escalation,
            {
                END: END,
                "translate": "translate"
            }
        )

        workflow.add_edge("translate", "review")
        workflow.add_edge("review", "tester")
        workflow.add_edge("tester", "documentation")
        workflow.add_edge("documentation", "validation")
        workflow.add_edge("validation", END)

        self.graph = workflow.compile()
        logger.info("[Orchestrator] LangGraph StateGraph pipeline compiled successfully.")

    # ─────────────────────── Public API ───────────────────────────────────────

    def process_file(
        self,
        dsx_file: str | Path,
        dry_run: bool = False,
    ) -> dict:
        dsx_file = Path(dsx_file)
        job_name = dsx_file.stem
        start_time = time.time()

        logger.info(f"\n{'='*60}")
        logger.info(f"[Orchestrator] Processing: {dsx_file.name}")
        logger.info(f"{'='*60}")

        result = {
            "job_name": job_name,
            "source_file": str(dsx_file),
            "dry_run": dry_run,
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "status": "in_progress",
            "stage_timings": {},
        }

        initial_state: PipelineState = {
            "job_name": job_name,
            "source_file": str(dsx_file),
            "dry_run": dry_run,
            "status": "in_progress",
            "raw_parsed": {},
            "normalized": {},
            "classified": {},
            "translated": {},
            "reviewed": {},
            "tested": {},
            "documented": {},
            "validation": {},
            "escalation": {},
            "output_files": {},
            "stage_timings": {},
            "started_at": result["started_at"],
            "completed_at": "",
            "elapsed_seconds": 0.0,
            "error": None
        }

        try:
            final_state = self.graph.invoke(initial_state)

            result.update({
                "stage_timings": final_state["stage_timings"],
                "status": final_state["status"],
                "escalation": final_state["escalation"],
                "completed_at": final_state["completed_at"],
            })

            if final_state["status"] == "completed":
                doc_data = final_state["documented"]
                result.update({
                    "job_name": doc_data.get("job_name", job_name),
                    "job_description": doc_data.get("job_description", ""),
                    "classification": doc_data.get("classification", {}),
                    "review": doc_data.get("review", {}),
                    "validation": final_state["validation"],
                    "output_files": final_state["output_files"],
                })

            result["elapsed_seconds"] = round(time.time() - start_time, 2)

            status_icon = "✅" if result["status"] == "completed" else "⚠️"
            logger.info(f"{status_icon} [{job_name}] Done in {result['elapsed_seconds']}s")

        except Exception as exc:
            logger.error(f"[Orchestrator] Pipeline failed for {dsx_file.name}: {exc}", exc_info=True)
            result.update({
                "status": "failed",
                "error": str(exc),
                "completed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "elapsed_seconds": round(time.time() - start_time, 2),
            })

        return result

    def process_directory(self, input_dir: str | Path, dry_run: bool = False) -> list[dict]:
        """Process all DSX/ISX files in a directory."""
        input_dir = Path(input_dir)
        files = list(input_dir.glob("*.dsx")) + list(input_dir.glob("*.isx"))
        logger.info(f"[Orchestrator] Found {len(files)} file(s) in {input_dir}")

        results = []
        for f in sorted(files):
            result = self.process_file(f, dry_run=dry_run)
            results.append(result)

        return results

    # ─────────────────────── Output helpers ───────────────────────────────────

    def _save_outputs(self, documented: dict, job_name: str) -> None:
        """Write all generated outputs to disk."""
        safe = self._safe_name(job_name)
        translation = documented.get("translation", {})
        tests = documented.get("tests", {})
        documentation = documented.get("documentation", {})

        # PySpark script
        if "pyspark" in self.output_formats:
            pyspark_code = translation.get("pyspark", "")
            if pyspark_code:
                out_path = self.output_dir / "pyspark" / f"{safe}.py"
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(pyspark_code, encoding="utf-8")
                logger.info(f"  → PySpark: {out_path}")

        # DataFusion JSON
        if "datafusion" in self.output_formats:
            df_json = translation.get("datafusion", {})
            if df_json:
                out_path = self.output_dir / "datafusion" / f"{safe}.json"
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(json.dumps(df_json, indent=2, default=str), encoding="utf-8")
                logger.info(f"  → DataFusion: {out_path}")

        # Test suite
        test_code = tests.get("test_code", "")
        if test_code:
            out_path = Path("tests") / f"test_{safe}.py"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(test_code, encoding="utf-8")
            logger.info(f"  → Tests: {out_path}")

        # Migration documentation
        doc_md = documentation.get("markdown", "")
        if doc_md:
            out_path = self.output_dir / "reports" / f"{safe}_migration_doc.md"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(doc_md, encoding="utf-8")
            logger.info(f"  → Docs: {out_path}")

        # Full parsed JSON (for debugging / audit)
        parsed_out = self.output_dir / "reports" / f"{safe}_parsed.json"
        parsed_out.write_text(
            json.dumps({k: v for k, v in documented.items() if k != "translation"}, indent=2, default=str),
            encoding="utf-8",
        )

    def _list_output_files(self, job_name: str, dry_run: bool) -> dict[str, str]:
        safe = self._safe_name(job_name)
        return {
            "pyspark": str(self.output_dir / "pyspark" / f"{safe}.py"),
            "datafusion": str(self.output_dir / "datafusion" / f"{safe}.json"),
            "tests": str(Path("tests") / f"test_{safe}.py"),
            "docs": str(self.output_dir / "reports" / f"{safe}_migration_doc.md"),
        }

    def _safe_name(self, name: str) -> str:
        return name.lower().replace(" ", "_").replace("-", "_").replace(".", "_")
