"""
Agent 6: Documentation Agent — Fully LLM-driven (No Fallbacks)
Sends the complete migration context to the LLM and asks it to write
a natural-language migration report. Strictly requires LLM completion.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class DocumentationAgent:
    """
    Generates human-readable migration documentation using an LLM.
    Strictly requires LLM and raises a RuntimeError on failure.
    """

    def __init__(self, engine=None, llm=None):
        self.engine = engine
        self.llm = llm

    def run(self, reviewed_job: dict) -> dict:
        job_name = reviewed_job.get("job_name", "?")
        logger.info(f"[DocumentationAgent] Writing docs with LLM: {job_name}")

        if not self.llm:
            raise RuntimeError("[DocumentationAgent] LLM client is required but not configured.")

        try:
            doc = self._llm_generate_docs(reviewed_job)
        except Exception as e:
            logger.error(f"[DocumentationAgent] LLM documentation generation failed: {e}")
            raise RuntimeError(f"DocumentationAgent LLM generation failed: {e}")

        result = dict(reviewed_job)
        result["documentation"] = {
            "markdown":      doc,
            "doc_file_name": f"{self._safe(job_name)}_migration_doc.md",
            "recommendations": reviewed_job.get("classification", {}).get("recommendations", []),
        }
        logger.info(f"[DocumentationAgent] Docs generated for '{job_name}'")
        return result

    def _llm_generate_docs(self, job: dict) -> str:
        job_name  = job.get("job_name", "?")
        job_desc  = job.get("job_description", "")
        stages    = job.get("stages", [])
        sources   = job.get("sources", [])
        targets   = job.get("targets", [])
        params    = job.get("parameters", [])
        conns     = job.get("connections", [])
        cls       = job.get("classification", {})
        review    = job.get("review", {})
        translation = job.get("translation", {})

        stage_list = "\n".join(
            f"  - [{s.get('id','')}] {s.get('stage_type','')} ({s.get('category','')}): "
            f"{', '.join(c.get('name','') for c in s.get('output_columns',[])[:4])}"
            for s in stages[:20]
        )

        review_issues = "\n".join(
            f"  - [{i.get('severity','').upper()}] {i.get('location','')}: {i.get('issue','')}"
            for i in review.get("issues", [])[:10]
        )

        pyspark_lines = len((translation.get("pyspark") or "").splitlines())

        system = (
            "You are a technical writer at a data engineering consultancy. "
            "Write a professional migration documentation report in Markdown for an IBM DataStage "
            "job that has been migrated to Apache PySpark and Google Cloud DataFusion.\n\n"
            "The report must include these sections:\n"
            "  1. **Executive Summary** — What this job does, why it was migrated, and what the outcome is\n"
            "  2. **Migration Overview** — A table with key metrics (complexity, confidence, stage count, etc.)\n"
            "  3. **Source-to-Target Mapping** — Describe the data flow from sources to targets in plain English\n"
            "  4. **Migration Decisions** — Explain each non-trivial translation decision (e.g. why a join was implemented a certain way)\n"
            "  5. **Known Risks & Limitations** — Specific risks based on the review issues and ambiguity flags\n"
            "  6. **Deployment Checklist** — Step-by-step checklist specific to this job's connections and parameters\n"
            "  7. **Testing Guidance** — How to validate the migrated pipeline produces correct results\n\n"
            "Write in a professional, precise tone. Be specific to this job — do not write generic text. "
            "Return ONLY the Markdown text. No code fences around the Markdown."
        )

        user = (
            f"Write migration documentation for this IBM DataStage job.\n\n"
            f"Job name: {job_name}\n"
            f"Description: {job_desc}\n"
            f"Complexity: {cls.get('complexity','?')} | Confidence: {cls.get('confidence_score',0):.0%}\n"
            f"LLM Analysis: {cls.get('reasoning','')}\n"
            f"Ambiguity flags: {cls.get('ambiguity_flags',[])}\n"
            f"Recommendations from classifier: {cls.get('recommendations',[])}\n"
            f"Risk areas: {cls.get('risk_areas',[])}\n\n"
            f"Stages ({len(stages)}):\n{stage_list}\n\n"
            f"Sources: {sources}\n"
            f"Targets: {targets}\n"
            f"Parameters: {[p.get('name','') for p in params[:10]]}\n"
            f"Connections: {[(c.get('name',''), c.get('stage_type','')) for c in conns[:6]]}\n\n"
            f"Code review result: passed={review.get('passed',True)}, score={review.get('score',1.0):.0%}\n"
            f"Review summary: {review.get('summary','')}\n"
            f"Review issues:\n{review_issues}\n\n"
            f"PySpark output: {pyspark_lines} lines generated\n"
            f"DataFusion output: {'Generated' if translation.get('datafusion') else 'Not generated'}"
        )

        return self.llm.complete(system, user, agent_name="DocumentationAgent")

    def _safe(self, name: str) -> str:
        return name.lower().replace(" ", "_").replace("-", "_").replace(".", "_")
