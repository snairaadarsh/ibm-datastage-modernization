"""
Layer 6: Escalation Handler — LLM-powered (No Fallbacks)
Both 'medium' and 'complex' jobs are escalated for human review.
The LLM generates a specific, detailed explanation. Strictly requires LLM.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class EscalationHandler:
    """
    Routes jobs to human review based on LLM-determined complexity.
    Strictly requires LLM and raises a RuntimeError on failure.
    """

    def __init__(self, config: dict | None = None, llm=None):
        self.config = config or {}
        self.llm    = llm

    def check(self, classified_job: dict) -> tuple[bool, dict]:
        classification = classified_job.get("classification", {})
        complexity     = classification.get("complexity", "complex")
        job_name       = classified_job.get("job_name", "?")
        reasoning      = classification.get("reasoning", "")
        ambiguity      = classification.get("ambiguity_flags", [])
        risk_areas     = classification.get("risk_areas", [])
        recommendations= classification.get("recommendations", [])

        if complexity == "simple":
            logger.info(f"[Escalation] '{job_name}': SIMPLE → auto-proceed")
            return True, {
                "complexity":         "simple",
                "escalated":          False,
                "instructions":       "Fully automated migration. No human review required.",
                "llm_explanation":    "",
                "flagged_stages":     [],
                "suggested_action":   "proceed",
            }

        logger.warning(f"[Escalation] '{job_name}': {complexity.upper()} → requesting human review")

        if not self.llm:
            raise RuntimeError("[EscalationHandler] LLM client is required but not configured.")

        try:
            llm_explanation = self._llm_explain(classified_job)
        except Exception as e:
            logger.error(f"[Escalation] LLM explanation generation failed: {e}")
            raise RuntimeError(f"EscalationHandler LLM explanation failed: {e}")

        return False, {
            "complexity":      complexity,
            "escalated":       True,
            "llm_explanation": llm_explanation,
            "ambiguity_flags": ambiguity,
            "risk_areas":      risk_areas,
            "recommendations": recommendations,
            "flagged_stages":  risk_areas,
            "reason":          reasoning,
            "instructions": (
                "Human review required before code generation can proceed. "
                "Please review the AI's analysis and decide how to proceed."
            ),
        }

    def _llm_explain(self, job: dict) -> str:
        classification = job.get("classification", {})
        complexity     = classification.get("complexity", "?")
        reasoning      = classification.get("reasoning", "")
        ambiguity      = classification.get("ambiguity_flags", [])
        risk_areas     = classification.get("risk_areas", [])
        recommendations= classification.get("recommendations", [])
        stages         = job.get("stages", [])

        stage_list = ", ".join(
            f"{s.get('id','')} ({s.get('stage_type','')})" 
            for s in stages[:15]
        )

        system = (
            "You are an IBM DataStage migration expert explaining a migration review situation "
            "to a data engineering team lead.\n\n"
            "Write a concise bullet-point summary of why this job requires human review. "
            "Return EXACTLY 3-5 bullet points. Each bullet must:\n"
            "  • Start with the bullet character •\n"
            "  • Be one sentence — specific to this job, not generic\n"
            "  • Cover one of: what makes it complex, which stage is risky, "
            "what could go wrong, or what the reviewer must verify\n\n"
            "Do NOT write paragraphs, headers, or any text outside the bullet list. "
            "Return only the bullet points."
        )

        user = (
            f"Explain why this DataStage job requires human review before migration can proceed.\n\n"
            f"Job: {job.get('job_name','?')}\n"
            f"Complexity: {complexity}\n"
            f"LLM initial reasoning: {reasoning}\n"
            f"Ambiguity flags: {ambiguity}\n"
            f"Risk areas identified: {risk_areas}\n"
            f"Recommendations: {recommendations}\n"
            f"Stage list: {stage_list}"
        )

        return self.llm.complete(system, user, agent_name="EscalationHandler")
