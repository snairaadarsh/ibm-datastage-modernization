"""
Agent 6: Documentation Agent — v2.0 Production Documentation (No Fallbacks)
Sends the complete migration context to the LLM and asks it to write
a natural-language migration report following the v2.0 7-section standard.
Strictly requires LLM completion.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_DOCS_SYSTEM = """\
You are a technical writer at a data engineering consultancy. Write a professional \
migration documentation report in Markdown for an IBM DataStage job that has been \
migrated to Apache PySpark and Google Cloud DataFusion.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MANDATORY SECTIONS (in this exact order)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. **Executive Summary**
   - What this job does (data flow summary in 2–3 sentences)
   - Why it was migrated (business drivers)
   - Outcome summary (migration completeness, overall confidence)

2. **Migration Overview**
   A markdown table with these rows:
   | Metric | Value |
   | Job Name | <name> |
   | Complexity | <complexity> |
   | Confidence | <score %> |
   | Stage Count | <N> |
   | Source Systems | <list> |
   | Target Systems | <list> |
   | Parameters | <list or None> |
   | Review Score | <score %> |
   | Migration Completeness | <pct>% |

3. **Source-to-Target Mapping**
   Plain English description of data flow:
   - Where data originates (source stage names, SQL queries)
   - Each transformation step (what is computed, why)
   - Where data lands (target stage names, write modes)
   - Exception/reject paths and their destinations

4. **Migration Decisions**
   Explain each non-trivial translation decision:
   - Join type choices (LeftOuter → left join + anti-join, why)
   - Lookup implementation (broadcast join, failure mode)
   - SCD implementation (window spec, sentinel dates, surrogate keys)
   - Aggregation choices
   - Partitioning approach
   - Anything marked as a risk area or ambiguity flag

5. **Known Risks & Limitations**
   Specific risks (not generic). For each risk:
   - Risk description tied to a specific stage
   - Severity (HIGH / MEDIUM / LOW)
   - Recommended mitigation action

6. **Deployment Checklist**
   Specific to this job's connections, parameters, and environment:
   - Environment variables that MUST be set (list them)
   - IAM permissions required (BigQuery, Oracle, etc.)
   - Pre-flight checks (source table existence, BQ dataset creation)
   - Run command with correct --run-mode flag

7. **Testing Guidance**
   How to validate the migrated pipeline:
   - Which pytest test classes cover which stages
   - Row count checks to run post-load
   - Data reconciliation queries (specific to this job's source/target tables)
   - Boundary cases to verify manually

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STYLE RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • Professional, precise tone
  • Be SPECIFIC to this job — no generic boilerplate
  • Reference actual stage names, column names, and SQL from the input
  • Numbered sections with ## headings
  • Return ONLY the Markdown. No code fences around the entire document.\
"""


class DocumentationAgent:
    """
    Generates human-readable migration documentation using an LLM.
    v2.0: Enforces the 7-section standard with metric tables,
    stage-specific risk analysis, and deployment checklist.
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

        # Build stage list with full detail
        stage_list = "\n".join(
            f"  - [{s.get('id','')}] {s.get('stage_type','')} ({s.get('category','')}):"
            f" cols={[c.get('name','') for c in s.get('output_columns',[])[:4]]}"
            f" transforms={[(t.get('output_column',''), t.get('expression','')) for t in s.get('transformations',[])[:3]]}"
            f" join_type={s.get('join_type','')} join_key={s.get('join_key','')} write_mode={s.get('write_mode','')}"
            for s in stages[:20]
        )

        review_issues = "\n".join(
            f"  - [{i.get('severity','').upper()}] {i.get('location','')}: {i.get('issue','')}"
            for i in review.get("issues", [])[:10]
        )

        pyspark_lines = len((translation.get("pyspark") or "").splitlines())

        # Compute migration completeness from review score
        completeness_pct = round(review.get("score", 1.0) * 100, 1)

        user = (
            f"Write the v2.0 migration documentation report for this IBM DataStage job.\n\n"
            f"Job name: {job_name}\n"
            f"Description: {job_desc}\n"
            f"Complexity: {cls.get('complexity','?')} | Confidence: {cls.get('confidence_score',0):.0%}\n"
            f"Migration completeness: {completeness_pct}%\n"
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
            f"Review issues:\n{review_issues}\n"
            f"Review strengths: {review.get('strengths',[])[:3]}\n\n"
            f"PySpark output: {pyspark_lines} lines generated\n"
            f"DataFusion output: {'Generated' if translation.get('datafusion') else 'Not generated'}"
        )

        return self.llm.complete(_DOCS_SYSTEM, user, agent_name="DocumentationAgent")

    def _safe(self, name: str) -> str:
        return name.lower().replace(" ", "_").replace("-", "_").replace(".", "_")
