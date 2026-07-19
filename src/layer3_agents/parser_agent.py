"""
Agent 1: Parser Agent
Refines raw parsed DataStage JSON into a canonical, normalized structure
ready for consumption by downstream agents.
"""

from __future__ import annotations
import logging
from typing import Any

logger = logging.getLogger(__name__)


class ParserAgent:
    """
    Normalizes and enriches the raw parsed DataStage JSON.
    Ensures all downstream agents receive a consistent data structure.
    """

    def __init__(self, engine=None, llm=None):
        self.engine = engine
        self.llm = llm

    def run(self, parsed_job: dict) -> dict:
        """
        Normalize and enrich the parsed job structure.
        Returns an enriched canonical form.
        """
        logger.info(f"[ParserAgent] Processing: {parsed_job.get('job_name', '?')}")

        result = dict(parsed_job)  # shallow copy

        # Normalize stage IDs to snake_case variable names
        result["stages"] = [self._normalize_stage(s) for s in parsed_job.get("stages", [])]

        # Build a stage lookup by ID for quick access
        result["stage_map"] = {s["id"]: s for s in result["stages"]}

        # Infer data lineage for each stage
        result["lineage"] = self._build_lineage(result)

        # Detect parameter tokens (e.g., ${DS_RUNDATE})
        result["parameters"] = self._extract_parameters(result)

        # Normalize source connection strings
        result["connections"] = self._extract_connections(result)

        logger.info(
            f"[ParserAgent] Normalized {len(result['stages'])} stages, "
            f"found {len(result['parameters'])} parameters, "
            f"{len(result['connections'])} connections"
        )
        return result

    # ─────────────────────────────────────────────────────────────────────────

    def _normalize_stage(self, stage: dict) -> dict:
        """Add derived fields to a stage: var_name, readable_description."""
        s = dict(stage)
        # Python-safe variable name
        s["var_name"] = self._to_var_name(s["id"])
        # Infer readable description from ID if blank
        if not s.get("description"):
            s["description"] = s["id"].replace("_", " ").title()
        # Flatten properties for easier access
        props = s.get("properties", {})
        s["sql_query"] = (
            props.get("XMLProperties.SelectStatement")
            or props.get("SelectStatement")
            or None
        )
        s["table_name"] = (
            props.get("XMLProperties.TableName")
            or props.get("TableName")
            or None
        )
        s["schema_name"] = props.get("XMLProperties.Schema") or props.get("Schema")
        s["connection_name"] = props.get("Connection.DataSource") or props.get("DataSource")
        s["file_path"] = props.get("File") or props.get("FileName")
        s["write_mode"] = (
            props.get("XMLProperties.WriteMode")
            or props.get("WriteMode")
            or "APPEND"
        )
        s["bq_project"] = props.get("Connection.ProjectId")
        s["bq_dataset"] = props.get("Connection.Dataset")
        s["join_type"] = props.get("JoinType")
        s["join_key"] = props.get("JoinKey")
        s["filter_condition"] = props.get("FilterCondition")
        s["group_by_keys"] = props.get("GroupByKeys")
        s["aggregations"] = props.get("Aggregations")
        s["sort_keys"] = props.get("SortKeys")

        # Lookup-specific properties
        s["lookup_key"]        = props.get("LookupKey")
        s["lookup_type"]       = props.get("LookupType")
        s["no_match_handling"] = props.get("NoMatchHandling")  # "Continue" | "Reject" | "Drop"

        # Canonical pin counts (used by translator and reviewer)
        s["input_pin_count"]  = stage.get("input_pins",  0)
        s["output_pin_count"] = stage.get("output_pins", 0)
        return s

    def _to_var_name(self, identifier: str) -> str:
        """Convert a DataStage stage ID to a Python-safe variable name."""
        name = identifier.lower()
        name = name.replace(" ", "_").replace("-", "_").replace(".", "_")
        # Remove any remaining non-alphanumeric chars
        name = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
        # Prefix with 'df_' to make it a 'DataFrame' variable
        return f"df_{name}"

    def _build_lineage(self, job: dict) -> dict:
        """Build upstream/downstream lineage maps for each stage."""
        upstream: dict[str, list[str]] = {s["id"]: [] for s in job["stages"]}
        downstream: dict[str, list[str]] = {s["id"]: [] for s in job["stages"]}

        for link in job.get("links", []):
            src = link["from_stage"]
            dst = link["to_stage"]
            if src in downstream:
                downstream[src].append(dst)
            if dst in upstream:
                upstream[dst].append(src)

        return {"upstream": upstream, "downstream": downstream}

    def _extract_parameters(self, job: dict) -> list[dict]:
        """Find DataStage parameter tokens like ${DS_RUNDATE} in all properties."""
        import re
        param_pattern = re.compile(r"\$\{([^}]+)\}")
        found: dict[str, set] = {}

        for stage in job.get("stages", []):
            for key, val in stage.get("properties", {}).items():
                if val and isinstance(val, str):
                    for match in param_pattern.finditer(val):
                        token = match.group(1)
                        found.setdefault(token, set()).add(stage["id"])
            fp = stage.get("file_path", "") or ""
            for match in param_pattern.finditer(fp):
                token = match.group(1)
                found.setdefault(token, set()).add(stage["id"])

        return [
            {"name": token, "used_in_stages": list(stages)}
            for token, stages in found.items()
        ]

    def _extract_connections(self, job: dict) -> list[dict]:
        """Extract unique data source connections from all stages."""
        connections: dict[str, dict] = {}
        for stage in job.get("stages", []):
            conn = stage.get("connection_name")
            st = stage.get("stage_type", "")
            if conn and conn not in connections:
                connections[conn] = {
                    "name": conn,
                    "stage_type": st,
                    "used_in": [stage["id"]],
                }
            elif conn:
                connections[conn]["used_in"].append(stage["id"])
        return list(connections.values())

    def refine_with_llm(self, normalized_job: dict, raw_xml: str) -> dict:
        """
        Runs an LLM-based verification step to identify any missing or incorrect properties
        by comparing the rule-based normalized JSON against the raw source XML.
        Retries once on failure. Raises RuntimeError if both attempts fail.
        """
        if not self.llm:
            raise RuntimeError("[ParserAgent] LLM client is required but not configured. Cannot refine parsed output.")

        logger.info(f"[ParserAgent] Refining parsed metadata for '{normalized_job.get('job_name', '?')}' via LLM...")

        system_prompt = (
            "You are an expert IBM DataStage metadata parser. Your job is to compare a rule-based normalized "
            "JSON representation of a DataStage job with its raw source XML/DSX content. Identify any missing "
            "attributes, stage properties (such as join keys, lookup keys, no-match handling, write modes, "
            "or transformer column mapping expressions), links, or parameters that were omitted or incorrectly "
            "parsed by the rules, and correct the JSON object.\n\n"
            "Keep all existing valid structures intact. Return ONLY the complete corrected JSON object, matching the original schema. "
            "Do not include any explanation, markdown formatting, or code fences."
        )

        import json

        def _attempt() -> dict:
            canonical_json_str = json.dumps(normalized_job, indent=2)
            user_prompt = (
                f"Below is the canonical normalized JSON generated by our rule-based parser:\n"
                f"```json\n{canonical_json_str}\n```\n\n"
                f"Compare it with the raw source XML content below and correct any omissions, wrong mappings, "
                f"or missing stage attributes:\n\n"
                f"RAW SOURCE CONTENT:\n{raw_xml}"
            )
            refined_raw = self.llm.complete(system_prompt, user_prompt, agent_name="ParserAgent-Refiner")
            refined_raw = refined_raw.strip()
            if refined_raw.startswith("```"):
                refined_raw = refined_raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            if refined_raw.startswith("json"):
                refined_raw = refined_raw.split("\n", 1)[1].strip()
            return json.loads(refined_raw)

        last_error = None
        for attempt in range(1, 3):  # Try up to 2 times
            try:
                refined_json = _attempt()
                if isinstance(refined_json, dict) and "stages" in refined_json:
                    refined_json["stage_map"] = {s["id"]: s for s in refined_json.get("stages", [])}
                    refined_json["lineage"] = self._build_lineage(refined_json)
                    refined_json["parameters"] = self._extract_parameters(refined_json)
                    refined_json["connections"] = self._extract_connections(refined_json)
                    logger.info(f"[ParserAgent] Metadata successfully refined by LLM (attempt {attempt}).")
                    return refined_json
                else:
                    raise ValueError("Refined JSON did not contain expected 'stages' key.")
            except Exception as e:
                last_error = e
                logger.warning(f"[ParserAgent] LLM refinement attempt {attempt}/2 failed: {e}. {'Retrying...' if attempt < 2 else 'No more retries.'}")

        raise RuntimeError(
            f"[ParserAgent] LLM-based metadata refinement failed after 2 attempts. "
            f"Last error: {last_error}. Pipeline cannot continue without validated LLM parsing."
        )

