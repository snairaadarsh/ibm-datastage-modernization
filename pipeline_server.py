#!/usr/bin/env python3
"""
DataStage Modernization Pipeline — Server Mode
Spawned by the Node.js backend. Coordinates the pipeline using a LangGraph StateGraph,
writing SSE event payloads to stdout in real-time.
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional, TypedDict, Any

# Configure log output to stderr (stdout is reserved for Node.js event messages)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

from src.llm_client import emit, LLMClient


# ─────────────────────────────────────────────────────────────────────────────
# Polling helper
# ─────────────────────────────────────────────────────────────────────────────

def poll_human_review_decision(bridge_url: str, job_id: str) -> dict:
    """Poll Node.js REST API for user review decision."""
    import requests as req

    url = f"{bridge_url}/api/jobs/{job_id}/human-review-decision"
    logger.info(f"[Server] Polling for review decision at: {url}")

    max_wait = 1800  # 30 mins
    poll_interval = 2
    elapsed = 0

    while elapsed < max_wait:
        try:
            resp = req.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if data and data.get("decision"):
                    return data
        except Exception as e:
            logger.debug(f"[Server] Poll error: {e}")

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError("Human review response timed out.")


# ─────────────────────────────────────────────────────────────────────────────
# LangGraph State Representation
# ─────────────────────────────────────────────────────────────────────────────

class ServerPipelineState(TypedDict):
    job_id: str
    dsx_file: Path
    bridge_url: str
    llm: LLMClient

    # Executable Agents
    parser_agent: Any
    classifier_agent: Any
    translator_agent: Any
    reviewer_agent: Any
    tester_agent: Any
    doc_agent: Any
    escalation: Any
    validator_agent: Any

    # State tracking
    start_time: float
    raw_parsed: dict
    normalized: dict
    classified: dict
    translated: dict
    reviewed: dict
    tested: dict
    documented: dict
    validation: dict
    status: str
    user_comment: str
    validation_loop_count: int   # tracks validator retry iterations
    validation_decision: str     # 'pending' | 'proceed' | 'ai_fix' | 'comment'
    error: Optional[str]


# ─────────────────────────────────────────────────────────────────────────────
# run_server_mode
# ─────────────────────────────────────────────────────────────────────────────

def run_server_mode(args) -> None:
    from src.layer1_ingestion.xml_parser import DataStageParser
    from src.layer3_agents.parser_agent import ParserAgent
    from src.layer3_agents.classifier_agent import ClassifierAgent
    from src.layer3_agents.translator_agent import TranslatorAgent
    from src.layer3_agents.reviewer_agent import ReviewerAgent
    from src.layer3_agents.tester_agent import TesterAgent
    from src.layer3_agents.documentation_agent import DocumentationAgent
    from src.layer5_validation.validator_agent import ValidatorAgent
    from src.layer6_human_review.escalation import EscalationHandler
    from langgraph.graph import StateGraph, END

    dsx_file   = Path(args.input)
    job_id     = args.job_id
    bridge_url = args.bridge_url.rstrip("/")

    # ── Build LLM client ──────────────────────────────────────────────────────
    llm = LLMClient(
        provider=args.provider,
        api_key=args.api_key or os.environ.get("LLM_API_KEY", ""),
        model=args.model or None,
        bridge_url=bridge_url,
        job_id=job_id,
    )

    def make_agents(instruction: str = ""):
        """Construct all agents with instruction guidance."""
        llm.user_instruction = instruction

        # Lazy load reasoning engine
        try:
            from src.layer2_reasoning_engine.reasoning_engine import ReasoningEngine
            engine = ReasoningEngine()
        except Exception:
            engine = None

        return (
            ParserAgent(engine, llm),
            ClassifierAgent(engine, llm),
            TranslatorAgent(engine, llm),
            ReviewerAgent(engine, llm),
            TesterAgent(engine, llm),
            DocumentationAgent(engine, llm),
            EscalationHandler({}, llm),
            ValidatorAgent(llm),
        )

    def _llm_propose_solution(classified: dict) -> str:
        """Ask the LLM to propose a concrete solution to flagged risks before translation."""
        cls = classified.get("classification", {})
        risk_areas = cls.get("risk_areas", [])
        ambiguity = cls.get("ambiguity_flags", [])
        recommendations = cls.get("recommendations", [])
        stages = classified.get("stages", [])
        stage_ids = [s.get("id", "") for s in stages]

        system = (
            "You are a DataStage migration expert. The user has approved proceeding with a migration "
            "despite flagged risks. Propose a concrete, specific solution to each flagged risk area "
            "that will be injected as an instruction into the TranslatorAgent.\n\n"
            "Rules:\n"
            "  - Reference exact stage IDs and known risk patterns\n"
            "  - Be concrete (e.g. 'Use Window.partitionBy on ITEM_CODE with F.lead()') not vague\n"
            "  - Return only the instruction text — no preamble, no markdown, no explanation"
        )
        user = (
            f"Job: {classified.get('job_name', '?')}\n"
            f"Stages: {stage_ids}\n"
            f"Risk areas: {risk_areas}\n"
            f"Ambiguity flags: {ambiguity}\n"
            f"Recommendations: {recommendations}\n\n"
            "Propose a concrete fix instruction for the TranslatorAgent:"
        )
        return llm.complete(system, user, agent_name="EscalationSolutionProposer")

    def _llm_merge_proposal_with_comment(classified: dict, user_comment: str) -> str:
        """Merge the LLM's own solution proposal with the user's guidance comment."""
        base_proposal = _llm_propose_solution(classified)
        system = (
            "You are a DataStage migration expert. Merge the following AI-generated solution proposal "
            "with the user's custom guidance into one coherent instruction for the TranslatorAgent.\n\n"
            "Rules:\n"
            "  - Prioritize the user's specific requests\n"
            "  - Keep the AI proposal where the user did not address a risk\n"
            "  - Return only the merged instruction text — no preamble, no markdown"
        )
        user = (
            f"AI proposed solution:\n{base_proposal}\n\n"
            f"User's additional guidance:\n{user_comment}\n\n"
            "Produce the merged instruction:"
        )
        return llm.complete(system, user, agent_name="EscalationMerger")

    # Instantiate LangGraph StateGraph
    workflow = StateGraph(ServerPipelineState)

    # 1. Parse node
    def parse_node(state: ServerPipelineState) -> dict:
        emit("stage", {"stageId": "parse", "status": "active"})
        emit("log",   {"level": "info", "message": f"Ingesting file: {state['dsx_file'].name}"})
        dsx_parser = DataStageParser(state["dsx_file"])
        raw = dsx_parser.parse()
        emit("log",  {"level": "success", "message": f"XML parsed — {len(raw.get('stages', []))} stages found"})
        emit("stage", {"stageId": "parse", "status": "done"})
        return {"raw_parsed": raw}

    # 2. Normalize node
    def normalize_node(state: ServerPipelineState) -> dict:
        emit("stage", {"stageId": "normalize", "status": "active"})
        emit("log",   {"level": "info", "message": "Normalizing to canonical DataStage schema..."})
        normalized = state["parser_agent"].run(state["raw_parsed"])
        try:
            raw_xml = state["dsx_file"].read_text(encoding="utf-8", errors="ignore")
            normalized = state["parser_agent"].refine_with_llm(normalized, raw_xml)
        except Exception as e:
            logger.debug(f"[Server] Failed to read source file for hybrid refinement: {e}")
        emit("log",  {"level": "success", "message": "Normalization complete"})
        emit("stage", {"stageId": "normalize", "status": "done"})
        return {"normalized": normalized}

    # 3. Classify node
    def classify_node(state: ServerPipelineState) -> dict:
        emit("stage", {"stageId": "classify", "status": "active"})
        emit("log",   {"level": "info", "message": "LLM is analyzing job complexity..."})
        classified = state["classifier_agent"].run(state["normalized"])
        cls         = classified.get("classification", {})
        complexity  = cls.get("complexity", "medium")
        confidence  = cls.get("confidence_score", 0.85) * 100
        stage_count = cls.get("stage_count", len(state["normalized"].get("stages", [])))
        reasoning   = cls.get("reasoning", "")
        
        emit("classification", {
            "complexity":      complexity,
            "confidenceScore": round(confidence, 1),
            "stageCount":      stage_count,
            "reasoning":       reasoning,
        })
        emit("log",  {"level": "info", "message": f"Complexity: {complexity} | Confidence: {confidence:.1f}%"})
        emit("log",  {"level": "info", "message": f"LLM reasoning: {reasoning[:120]}..."})
        emit("stage", {"stageId": "classify", "status": "done"})
        return {"classified": classified}

    # 4. Escalation gate node
    def escalation_node(state: ServerPipelineState) -> dict:
        should_continue, escalation_result = state["escalation"].check(state["classified"])

        if not should_continue:
            cls = state["classified"].get("classification", {})
            complexity = cls.get("complexity", "medium")
            confidence = cls.get("confidence_score", 0.85) * 100
            llm_explanation = escalation_result.get("llm_explanation", "")
            reason          = escalation_result.get("reason", "")
            flagged         = escalation_result.get("flagged_stages", [])

            emit("log", {"level": "warning", "message": f"{complexity.upper()} job — routing to Human Review"})
            emit("human_review", {
                "reason":          reason,
                "llmExplanation":  llm_explanation,
                "flaggedStages":   flagged if isinstance(flagged, list) else list(flagged),
                "complexity":      complexity,
                "confidence":      round(confidence, 1),
                "ambiguityFlags":  cls.get("ambiguity_flags", []),
                "recommendations": cls.get("recommendations", []),
                "riskAreas":       cls.get("risk_areas", []),
            })

            # Wait for user decision
            decision_data = poll_human_review_decision(state["bridge_url"], state["job_id"])
            decision      = decision_data.get("decision", "stop")
            user_comment  = decision_data.get("comment", "").strip()

            if decision == "stop":
                emit("log",   {"level": "warning", "message": "User stopped the migration."})
                emit("failed", {"error": "Migration stopped by user during human review."})
                raise RuntimeError("Migration stopped by user during human review.")

            elif decision == "proceed":
                # LLM proposes a concrete solution to each flagged risk, then proceeds
                emit("log", {"level": "info", "message": "LLM is proposing solutions to flagged risks..."})
                solution = _llm_propose_solution(state["classified"])
                emit("log", {"level": "success", "message": f"AI solution proposal ready — injecting into pipeline: {solution[:120]}..."})
                p_agent, c_agent, t_agent, r_agent, te_agent, d_agent, esc_agent, val_agent = make_agents(solution)
                return {
                    "parser_agent": p_agent, "classifier_agent": c_agent, "translator_agent": t_agent,
                    "reviewer_agent": r_agent, "tester_agent": te_agent, "doc_agent": d_agent,
                    "escalation": esc_agent, "validator_agent": val_agent,
                    "status": "in_progress", "user_comment": solution,
                }

            elif decision == "comment":
                # LLM merges its own solution proposal with the user's comment
                emit("log", {"level": "info", "message": "LLM merging AI solution with your guidance..."})
                merged_instruction = _llm_merge_proposal_with_comment(state["classified"], user_comment)
                emit("log", {"level": "success", "message": f"Merged instruction ready — injecting into pipeline: {merged_instruction[:120]}..."})
                p_agent, c_agent, t_agent, r_agent, te_agent, d_agent, esc_agent, val_agent = make_agents(merged_instruction)
                return {
                    "parser_agent": p_agent, "classifier_agent": c_agent, "translator_agent": t_agent,
                    "reviewer_agent": r_agent, "tester_agent": te_agent, "doc_agent": d_agent,
                    "escalation": esc_agent, "validator_agent": val_agent,
                    "status": "in_progress", "user_comment": merged_instruction,
                }

        return {"status": "in_progress"}

    # 5. Translate node
    def translate_node(state: ServerPipelineState) -> dict:
        emit("stage", {"stageId": "translate", "status": "active"})
        emit("log",   {"level": "info", "message": "LLM generating PySpark pipeline code..."})
        translated     = state["translator_agent"].run(state["classified"], ["pyspark", "datafusion"])
        pyspark_code   = translated.get("translation", {}).get("pyspark", "") or ""
        datafusion_cfg = translated.get("translation", {}).get("datafusion", {}) or {}
        datafusion_str = json.dumps(datafusion_cfg, indent=2) if isinstance(datafusion_cfg, dict) else str(datafusion_cfg)
        pyspark_lines  = len(pyspark_code.splitlines())
        df_lines       = len(datafusion_str.splitlines())
        emit("log",  {"level": "success", "message": f"Translation complete — {pyspark_lines} lines PySpark, {df_lines} lines DataFusion JSON"})
        emit("stage", {"stageId": "translate", "status": "done"})
        return {"translated": translated}

    # 6. Review node
    def review_node(state: ServerPipelineState) -> dict:
        emit("stage", {"stageId": "review", "status": "active"})
        emit("log",   {"level": "info", "message": "LLM performing code review..."})
        reviewed  = state["reviewer_agent"].run(state["translated"])
        rev       = reviewed.get("review", {})
        rev_score = rev.get("score", 0.9)
        rev_summary = rev.get("summary", "")
        emit("log",  {"level": "success" if rev.get("passed") else "warning", "message": f"Code review done — score={rev_score:.0%} | {rev_summary[:100]}"})
        emit("stage", {"stageId": "review", "status": "done"})
        return {"reviewed": reviewed}

    # Helper: poll validation-decision endpoint
    def poll_validation_decision(bridge_url: str, job_id: str) -> dict:
        """Poll Node.js REST API for user validation review decision."""
        import requests as req
        url = f"{bridge_url}/api/jobs/{job_id}/validation-decision"
        logger.info(f"[Server] Polling for validation decision at: {url}")
        max_wait = 1800
        poll_interval = 2
        elapsed = 0
        while elapsed < max_wait:
            try:
                resp = req.get(url, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    if data and data.get("decision"):
                        return data
            except Exception as e:
                logger.debug(f"[Server] Validation poll error: {e}")
            time.sleep(poll_interval)
            elapsed += poll_interval
        raise TimeoutError("Validation review response timed out.")

    # 7. Validate node — LLM-driven with 3-option retry loop (max 3 iterations)
    def validate_node(state: ServerPipelineState) -> dict:
        emit("stage", {"stageId": "validate", "status": "active"})
        emit("log",   {"level": "info", "message": "LLM generating pytest unit test suite..."})
        tested     = state["tester_agent"].run(state["reviewed"])
        test_code  = tested.get("tests", {}).get("test_code", "")
        test_lines = len(test_code.splitlines())
        emit("log",  {"level": "info", "message": "Generating migration documentation..."})
        documented = state["doc_agent"].run(tested)

        MAX_VALIDATION_LOOPS = 3
        loop_count = state.get("validation_loop_count", 0)
        current_translated = state["translated"]

        while True:
            emit("log", {"level": "info", "message": f"ValidatorAgent comparing source architecture to generated outputs (attempt {loop_count + 1}/{MAX_VALIDATION_LOOPS + 1})..."})

            # Inject translation into documented so ValidatorAgent can access it
            validated_job = dict(documented)
            validated_job["translation"] = current_translated.get("translation", {})

            val_result = state["validator_agent"].validate(validated_job)
            score      = val_result.get("score", 0)
            issues     = val_result.get("issues", [])
            passed     = val_result.get("passed", False)
            summary    = val_result.get("summary", "")

            score_color = "success" if passed else "warning"
            emit("log", {"level": score_color, "message": f"Validation score: {score}% — {summary[:120]}"})

            if passed:
                emit("log", {"level": "success", "message": f"Validation passed ({score}% >= 90%) — no critical issues."})
                emit("stage", {"stageId": "validate", "status": "done"})
                return {
                    "tested": tested, "documented": documented,
                    "translated": current_translated,
                    "validation": val_result,
                    "validation_loop_count": loop_count,
                }

            # Validation failed — decide what to do
            if loop_count >= MAX_VALIDATION_LOOPS:
                emit("log", {"level": "warning", "message": f"Max validation retries ({MAX_VALIDATION_LOOPS}) reached — forcing user decision."})

            emit("log", {"level": "warning", "message": f"Validation score {score}% < 90% — routing to Validation Review."})
            emit("validation_review", {
                "score":          score,
                "summary":        summary,
                "issues":         issues,
                "strengths":      val_result.get("strengths", []),
                "critical_issues": val_result.get("critical_issues", False),
                "loopCount":      loop_count,
                "maxLoops":       MAX_VALIDATION_LOOPS,
                "canRetry":       loop_count < MAX_VALIDATION_LOOPS,
            })

            vd = poll_validation_decision(state["bridge_url"], state["job_id"])
            v_decision  = vd.get("decision", "proceed")
            v_comment   = vd.get("comment", "").strip()

            if v_decision == "proceed" or loop_count >= MAX_VALIDATION_LOOPS:
                emit("log", {"level": "info", "message": "User accepted validation result — proceeding."})
                emit("stage", {"stageId": "validate", "status": "done"})
                return {
                    "tested": tested, "documented": documented,
                    "translated": current_translated,
                    "validation": val_result,
                    "validation_loop_count": loop_count,
                }

            # AI fix or comment — propose a fix and re-translate
            emit("log", {"level": "info", "message": "LLM is proposing a fix for the validation issues..."})
            fix_instruction = state["validator_agent"].propose_fix(
                validated_job, issues, user_comment=v_comment
            )
            emit("log", {"level": "info", "message": f"Fix proposed — re-translating: {fix_instruction[:120]}..."})

            # Re-initialize TranslatorAgent with fix instruction and re-translate only
            llm.user_instruction = fix_instruction
            from src.layer3_agents.translator_agent import TranslatorAgent as TA
            try:
                from src.layer2_reasoning_engine.reasoning_engine import ReasoningEngine
                engine = ReasoningEngine()
            except Exception:
                engine = None
            fix_translator = TA(engine, llm)
            emit("stage", {"stageId": "translate", "status": "active"})
            current_translated = fix_translator.run(state["classified"], ["pyspark", "datafusion"])
            emit("stage", {"stageId": "translate", "status": "done"})

            loop_count += 1

    # 8. Report node
    def report_node(state: ServerPipelineState) -> dict:
        emit("stage", {"stageId": "report", "status": "active"})
        elapsed = round(time.time() - state["start_time"], 1)

        pyspark_code   = state["translated"].get("translation", {}).get("pyspark", "") or ""
        datafusion_cfg = state["translated"].get("translation", {}).get("datafusion", {}) or {}
        datafusion_str = json.dumps(datafusion_cfg, indent=2) if isinstance(datafusion_cfg, dict) else str(datafusion_cfg)
        pyspark_lines  = len(pyspark_code.splitlines())
        df_lines       = len(datafusion_str.splitlines())
        test_code      = state["tested"].get("tests", {}).get("test_code", "")
        test_lines     = len(test_code.splitlines())

        cls = state["classified"].get("classification", {})
        complexity  = cls.get("complexity", "medium")
        confidence  = cls.get("confidence_score", 0.85) * 100
        stage_count = cls.get("stage_count", len(state["normalized"].get("stages", [])))

        user_comment = state.get("user_comment", "")

        report = {
            "summary": {
                "jobName":         state["documented"].get("job_name", state["dsx_file"].stem),
                "sourceFile":      state["dsx_file"].name,
                "generatedAt":     time.strftime("%Y-%m-%dT%H:%M:%S"),
                "status":          "completed",
                "complexity":      complexity,
                "confidenceScore": round(confidence, 1),
                "elapsedSeconds":  elapsed,
                "linesGenerated":  {"pyspark": pyspark_lines, "datafusion": df_lines, "tests": test_lines},
                "stagesCount":     stage_count,
                "userInstruction": user_comment if "user_comment" in locals() else "",
            },
            "classification":  cls,
            "review":          state["reviewed"].get("review", {}),
            "validation":      state["validation"],
            "recommendations": state["documented"].get("documentation", {}).get("recommendations", []),
        }

        emit("log",  {"level": "success", "message": f"Pipeline complete in {elapsed}s"})
        emit("stage", {"stageId": "report", "status": "done"})

        emit("completed", {
            "pyspark":        pyspark_code,
            "datafusion":     datafusion_str,
            "tests":          test_code,
            "report":         json.dumps(report, indent=2, default=str),
            "elapsedSeconds": elapsed,
        })
        return {"status": "completed"}

    # Add nodes to graph
    workflow.add_node("parse", parse_node)
    workflow.add_node("normalize", normalize_node)
    workflow.add_node("classify", classify_node)
    workflow.add_node("escalation", escalation_node)
    workflow.add_node("translate", translate_node)
    workflow.add_node("review", review_node)
    workflow.add_node("validate", validate_node)
    workflow.add_node("report", report_node)

    # Set entry point
    workflow.set_entry_point("parse")

    # Transitions
    workflow.add_edge("parse", "normalize")
    workflow.add_edge("normalize", "classify")
    workflow.add_edge("classify", "escalation")

    def route_after_escalation(state: ServerPipelineState):
        if state["status"] == "escalated":
            return END
        return "translate"

    workflow.add_conditional_edges(
        "escalation",
        route_after_escalation,
        {
            END: END,
            "translate": "translate"
        }
    )

    workflow.add_edge("translate", "review")
    workflow.add_edge("review", "validate")
    workflow.add_edge("validate", "report")
    workflow.add_edge("report", END)

    # Compile the graph
    graph = workflow.compile()

    # Instantiate agents
    p_agent, c_agent, t_agent, r_agent, te_agent, d_agent, esc_agent, val_agent = make_agents()

    initial_state: ServerPipelineState = {
        "job_id": job_id,
        "dsx_file": dsx_file,
        "bridge_url": bridge_url,
        "llm": llm,
        "parser_agent": p_agent,
        "classifier_agent": c_agent,
        "translator_agent": t_agent,
        "reviewer_agent": r_agent,
        "tester_agent": te_agent,
        "doc_agent": d_agent,
        "escalation": esc_agent,
        "validator_agent": val_agent,
        "start_time": time.time(),
        "raw_parsed": {},
        "normalized": {},
        "classified": {},
        "translated": {},
        "reviewed": {},
        "tested": {},
        "documented": {},
        "validation": {},
        "status": "in_progress",
        "user_comment": "",
        "validation_loop_count": 0,
        "validation_decision": "pending",
        "error": None
    }

    try:
        graph.invoke(initial_state)
    except Exception as exc:
        logger.error(f"Pipeline error: {exc}", exc_info=True)
        emit("failed", {"error": str(exc)})
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="DataStage Pipeline — Server Mode")
    parser.add_argument("--server-mode", action="store_true", required=True)
    parser.add_argument("--input",       required=True, help="Path to .dsx/.isx file")
    parser.add_argument("--job-id",      required=True, help="Job UUID from Node server")
    parser.add_argument("--provider",    default="gemini",
                        choices=["openai", "gemini", "anthropic", "ag_chat"])
    parser.add_argument("--api-key",     default="")
    parser.add_argument("--model",       default="")
    parser.add_argument("--bridge-url",  default="http://localhost:3001")
    args = parser.parse_args()
    run_server_mode(args)


if __name__ == "__main__":
    main()
