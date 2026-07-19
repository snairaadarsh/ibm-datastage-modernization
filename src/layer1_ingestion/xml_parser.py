"""
Layer 1: XML Parser
Parses IBM DataStage DSX/ISX files and converts them to a clean, structured JSON
representation consumed by the downstream agent pipeline.
"""

import json
import logging
from pathlib import Path
from typing import Any

import lxml.etree as ET
import xmltodict

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SQL Type mapping: DataStage SqlType codes → human-readable type + Python type
# ─────────────────────────────────────────────────────────────────────────────
SQL_TYPE_MAP: dict[str, dict] = {
    "4":  {"sql": "INTEGER",   "python": "int",   "spark": "IntegerType"},
    "5":  {"sql": "SMALLINT",  "python": "int",   "spark": "ShortType"},
    "-5": {"sql": "BIGINT",    "python": "int",   "spark": "LongType"},
    "8":  {"sql": "FLOAT",     "python": "float", "spark": "DoubleType"},
    "6":  {"sql": "FLOAT",     "python": "float", "spark": "FloatType"},
    "2":  {"sql": "NUMERIC",   "python": "Decimal","spark": "DecimalType"},
    "3":  {"sql": "DECIMAL",   "python": "Decimal","spark": "DecimalType"},
    "12": {"sql": "VARCHAR",   "python": "str",   "spark": "StringType"},
    "1":  {"sql": "CHAR",      "python": "str",   "spark": "StringType"},
    "-1": {"sql": "LONGVARCHAR","python": "str",  "spark": "StringType"},
    "93": {"sql": "TIMESTAMP", "python": "datetime","spark": "TimestampType"},
    "91": {"sql": "DATE",      "python": "date",  "spark": "DateType"},
    "92": {"sql": "TIME",      "python": "time",  "spark": "StringType"},
    "-2": {"sql": "BINARY",    "python": "bytes", "spark": "BinaryType"},
    "16": {"sql": "BOOLEAN",   "python": "bool",  "spark": "BooleanType"},
}

# Stage type → semantic category mapping
STAGE_TYPE_CATEGORIES: dict[str, str] = {
    # Sources
    "PxOracleConnector":    "source",
    "PxDB2Connector":       "source",
    "PxMSSQLConnector":     "source",
    "PxPostgresConnector":  "source",
    "PxTeradataConnector":  "source",
    "PxSequentialFile":     "source_or_target",
    "PxCsvConnector":       "source_or_target",
    "PxS3Connector":        "source_or_target",
    # Targets
    "PxBigQueryConnector":  "target",
    "PxSnowflakeConnector": "target",
    "PxRedshiftConnector":  "target",
    # Transformations
    "PxTransformer":        "transformer",
    "PxFilter":             "filter",
    "PxJoin":               "join",
    "PxAggregate":          "aggregation",
    "PxSort":               "sort",
    "PxSortStage":          "sort",
    "PxLookup":             "lookup",
    "PxMerge":              "merge",
    "PxPivot":              "pivot",
    "PxUnpivot":            "unpivot",
    "PxRemoveDup":          "dedup",
    "PxChangeCapture":      "cdc",
    "PxCopyStage":          "copy",
    "PxPeekStage":          "passthrough",
    "PxFunnel":             "union",
}


class DataStageParser:
    """
    Parses IBM DataStage DSX/ISX XML files into a canonical structured JSON.
    """

    def __init__(self, file_path: str | Path):
        self.file_path = Path(file_path)
        self._raw_xml: bytes = b""
        self._raw_dict: dict = {}
        self.parsed: dict = {}

    # ─────────────────────── Public API ───────────────────────────────────────

    def parse(self) -> dict:
        """
        Full parse of the DSX/ISX file.
        Returns a canonical dictionary with all extracted metadata.
        """
        logger.info(f"[Parser] Parsing: {self.file_path.name}")
        self._load_xml()
        self._extract_job_metadata()
        self._extract_stages()
        self._extract_links()
        self._build_dag()
        self._classify_io()
        logger.info(
            f"[Parser] Done: {len(self.parsed.get('stages', []))} stages, "
            f"{len(self.parsed.get('links', []))} links"
        )
        return self.parsed

    def to_json(self, indent: int = 2) -> str:
        """Return the parsed result as a formatted JSON string."""
        return json.dumps(self.parsed, indent=indent, default=str)

    def save_json(self, output_path: str | Path) -> Path:
        """Save parsed JSON to disk."""
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(self.to_json(), encoding="utf-8")
        logger.info(f"[Parser] Saved parsed JSON to: {out}")
        return out

    # ─────────────────────── Internal helpers ─────────────────────────────────

    def _strip_namespaces(self, d):
        """Recursively strip XML namespace prefixes from all dict keys."""
        if isinstance(d, dict):
            return {k.split(":")[-1]: self._strip_namespaces(v) for k, v in d.items()}
        elif isinstance(d, list):
            return [self._strip_namespaces(i) for i in d]
        return d

    def _load_xml(self) -> None:
        """Load and validate the XML file."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"DSX file not found: {self.file_path}")
        self._raw_xml = self.file_path.read_bytes()
        try:
            raw = xmltodict.parse(
                self._raw_xml,
                force_list=("Record", "SubRecord", "Column"),
                attr_prefix="@",
            )
            self._raw_dict = self._strip_namespaces(raw)
        except Exception as exc:
            raise ValueError(f"Failed to parse XML: {exc}") from exc

    def _extract_job_metadata(self) -> None:
        """Extract top-level job info."""
        export = self._raw_dict.get("DSExport", {})
        header = export.get("Header", {})
        job_elem = export.get("Job", {})

        # First Record with Type JOB_SEQUENCE holds the job metadata
        records = job_elem.get("Record", [])
        job_record = next(
            (r for r in records if r.get("@Type") in ("JOB_SEQUENCE", "JOB_PARALLEL")),
            {},
        )

        self.parsed = {
            "file_name": self.file_path.name,
            "file_path": str(self.file_path),
            "exporting_tool": header.get("@ExportingTool", "IBM DataStage"),
            "tool_version": header.get("@ExportingToolVersion", "unknown"),
            "server_name": header.get("@ServerName", "unknown"),
            "job_name": job_elem.get("@Identifier", self.file_path.stem),
            "job_description": job_elem.get("@Description", ""),
            "job_category": job_elem.get("@Category", ""),
            "job_type": job_record.get("@Type", "JOB_PARALLEL"),
            "annotation": self._get_property(job_record, "Annotation"),
            "stages": [],
            "links": [],
            "dag": {},
            "sources": [],
            "targets": [],
            "transformations": [],
            "parameters": [],
            "complexity_hints": [],
        }

    def _extract_stages(self) -> None:
        """Extract all stage records and their properties."""
        export = self._raw_dict.get("DSExport", {})
        records = export.get("Job", {}).get("Record", [])

        stages: list[dict] = []
        for rec in records:
            rec_type = rec.get("@Type", "")
            # Skip job-level records and link records
            if rec_type in ("JOB_SEQUENCE", "JOB_PARALLEL", "DSLink"):
                continue

            stage_type = self._get_property(rec, "StageType") or rec_type
            if not stage_type or stage_type == rec_type and "Connector" not in rec_type:
                # Only include records that look like stages
                if rec_type not in STAGE_TYPE_CATEGORIES:
                    continue

            stage = {
                "id": rec.get("@Identifier", ""),
                "name": rec.get("@Identifier", ""),
                "description": rec.get("@Description", ""),
                "stage_type": stage_type,
                "category": STAGE_TYPE_CATEGORIES.get(stage_type, "unknown"),
                "annotation": self._get_property(rec, "Annotation"),
                "input_pins": int(self._get_property(rec, "InputPins") or 0),
                "output_pins": int(self._get_property(rec, "OutputPins") or 0),
                "properties": self._extract_properties(rec),
                "output_columns": self._extract_output_columns(rec),
                "transformations": self._extract_transformations(rec),
            }

            # Detect complexity hints
            self._detect_complexity_hints(stage)
            stages.append(stage)

        self.parsed["stages"] = stages

    def _extract_links(self) -> None:
        """Extract all DSLink records representing data flow between stages."""
        export = self._raw_dict.get("DSExport", {})
        records = export.get("Job", {}).get("Record", [])

        links = []
        for rec in records:
            if rec.get("@Type") != "DSLink":
                continue
            link = {
                "id": rec.get("@Identifier", ""),
                "from_stage": self._get_property(rec, "FromStage"),
                "to_stage": self._get_property(rec, "ToStage"),
                "from_pin": int(self._get_property(rec, "FromPin") or 0),
                "to_pin": int(self._get_property(rec, "ToPin") or 0),
            }
            links.append(link)
        self.parsed["links"] = links

    def _build_dag(self) -> None:
        """
        Build a directed acyclic graph (adjacency list) from stages and links.
        Enables topological ordering for code generation.
        """
        stage_ids = {s["id"] for s in self.parsed["stages"]}
        dag: dict[str, list[str]] = {sid: [] for sid in stage_ids}

        for link in self.parsed["links"]:
            src = link["from_stage"]
            dst = link["to_stage"]
            if src in dag:
                dag[src].append(dst)

        # Compute topological order
        visited: set[str] = set()
        topo_order: list[str] = []

        def dfs(node: str) -> None:
            if node in visited:
                return
            visited.add(node)
            for neighbor in dag.get(node, []):
                dfs(neighbor)
            topo_order.append(node)

        for node in dag:
            dfs(node)

        self.parsed["dag"] = {
            "adjacency": dag,
            "topological_order": list(reversed(topo_order)),
        }

    def _classify_io(self) -> None:
        """Classify stages into sources, targets, and transformations."""
        stage_map = {s["id"]: s for s in self.parsed["stages"]}
        link_sources = {lnk["from_stage"] for lnk in self.parsed["links"]}
        link_targets = {lnk["to_stage"] for lnk in self.parsed["links"]}

        sources, targets, transformations = [], [], []

        for stage in self.parsed["stages"]:
            cat = stage["category"]
            sid = stage["id"]

            if cat == "source" or (cat == "source_or_target" and sid not in link_targets):
                sources.append(sid)
            elif cat == "target" or (cat == "source_or_target" and sid not in link_sources):
                targets.append(sid)
            else:
                transformations.append(sid)

        self.parsed["sources"] = sources
        self.parsed["targets"] = targets
        self.parsed["transformations"] = transformations

        # Summary stats
        self.parsed["summary"] = {
            "total_stages": len(self.parsed["stages"]),
            "total_links": len(self.parsed["links"]),
            "source_count": len(sources),
            "target_count": len(targets),
            "transformation_count": len(transformations),
            "stage_types": list({s["stage_type"] for s in self.parsed["stages"]}),
            "complexity_hints": self.parsed["complexity_hints"],
        }

    def _detect_complexity_hints(self, stage: dict) -> None:
        """Flag complex patterns for the classifier agent."""
        hints = self.parsed.setdefault("complexity_hints", [])
        st = stage["stage_type"]

        existing_hint_stages = {h["stage"] for h in hints if h["hint"] == "lookup_join"}
        if st == "PxLookup" and stage["id"] not in existing_hint_stages:
            hints.append({"stage": stage["id"], "hint": "lookup_join", "weight": 0.2})
        if st in ("PxChangeCapture", "PxMerge"):
            hints.append({"stage": stage["id"], "hint": "cdc_or_merge", "weight": 0.3})
        if len(stage.get("transformations", [])) > 5:
            hints.append({"stage": stage["id"], "hint": "complex_transformer", "weight": 0.2})
        # SCD type 2 keyword detection
        props_str = json.dumps(stage.get("properties", {})).lower()
        if "scd" in props_str or "slowly changing" in props_str or "eff_end_date" in props_str:
            hints.append({"stage": stage["id"], "hint": "scd_type2", "weight": 0.4})
        # Checksum / hash detection
        for txf in stage.get("transformations", []):
            expr = (txf.get("expression") or "").lower()
            if "checksum" in expr or "hash" in expr:
                hints.append({"stage": stage["id"], "hint": "hash_computation", "weight": 0.1})

    # ─────────────────────── XML helpers ──────────────────────────────────────

    def _get_property(self, record: dict, prop_name: str) -> str | None:
        """Extract a named property from a DataStage record's Property list."""
        # Direct Property elements
        props = record.get("Property", [])
        if isinstance(props, dict):
            props = [props]
        if isinstance(props, list):
            for p in props:
                if isinstance(p, dict) and p.get("@name") == prop_name:
                    return p.get("#text") or p.get("@Value")
        return None

    def _extract_properties(self, record: dict) -> dict:
        """Extract all Collection/SubRecord properties as a flat dict."""
        result: dict[str, Any] = {}
        collection = record.get("Collection", {})
        if isinstance(collection, dict):
            collections = [collection]
        elif isinstance(collection, list):
            collections = collection
        else:
            return result

        for coll in collections:
            coll_name = coll.get("@name", "")
            if coll_name == "Properties":
                sub_records = coll.get("SubRecord", [])
                if isinstance(sub_records, dict):
                    sub_records = [sub_records]
                for sr in sub_records:
                    name_props = sr.get("Property", [])
                    if isinstance(name_props, dict):
                        name_props = [name_props]
                    name = None
                    value = None
                    for p in name_props:
                        if isinstance(p, dict):
                            if p.get("@name") == "Name":
                                name = p.get("#text")
                            elif p.get("@name") == "Value":
                                value = p.get("#text")
                    if name:
                        result[name] = value
        return result

    def _extract_output_columns(self, record: dict) -> list[dict]:
        """Extract output column definitions from OutputPin collections."""
        columns: list[dict] = []
        collection = record.get("Collection", {})
        if isinstance(collection, dict):
            collections = [collection]
        elif isinstance(collection, list):
            collections = collection
        else:
            return columns

        for coll in collections:
            if coll.get("@name") == "OutputPin":
                sub_records = coll.get("SubRecord", [])
                if isinstance(sub_records, dict):
                    sub_records = [sub_records]
                for sr in sub_records:
                    col_collection = sr.get("Collection", {})
                    if isinstance(col_collection, dict):
                        col_collections = [col_collection]
                    elif isinstance(col_collection, list):
                        col_collections = col_collection
                    else:
                        continue
                    for cc in col_collections:
                        if cc.get("@name") == "Column":
                            col_subs = cc.get("SubRecord", [])
                            if isinstance(col_subs, dict):
                                col_subs = [col_subs]
                            for col_sr in col_subs:
                                col = self._parse_column(col_sr)
                                if col.get("name"):
                                    columns.append(col)
        return columns

    def _parse_column(self, col_record: dict) -> dict:
        """Parse a single column SubRecord into a structured dict."""
        props = col_record.get("Property", [])
        if isinstance(props, dict):
            props = [props]

        col: dict[str, Any] = {}
        for p in props:
            if not isinstance(p, dict):
                continue
            key = p.get("@name", "")
            val = p.get("#text") or p.get("@Value")
            col[key] = val

        sql_type_code = col.get("SqlType", "12")
        type_info = SQL_TYPE_MAP.get(str(sql_type_code), {"sql": "VARCHAR", "python": "str", "spark": "StringType"})

        return {
            "name": col.get("Name"),
            "sql_type_code": sql_type_code,
            "sql_type": type_info["sql"],
            "python_type": type_info["python"],
            "spark_type": type_info["spark"],
            "precision": col.get("Precision"),
            "scale": col.get("Scale"),
            "nullable": col.get("Nullable", "1") != "0",
        }

    def _extract_transformations(self, record: dict) -> list[dict]:
        """Extract transformer expressions from Transformations collection."""
        txfs: list[dict] = []
        collection = record.get("Collection", {})
        if isinstance(collection, dict):
            collections = [collection]
        elif isinstance(collection, list):
            collections = collection
        else:
            return txfs

        for coll in collections:
            if coll.get("@name") == "Transformations":
                sub_records = coll.get("SubRecord", [])
                if isinstance(sub_records, dict):
                    sub_records = [sub_records]
                for sr in sub_records:
                    txf = self._extract_txf_record(sr)
                    if txf:
                        txfs.append(txf)
        return txfs

    def _extract_txf_record(self, sr: dict) -> dict | None:
        """Extract a single transformation sub-record."""
        props = sr.get("Property", [])
        if isinstance(props, dict):
            props = [props]
        data: dict[str, str] = {}
        for p in props:
            if isinstance(p, dict):
                key = p.get("@name", "")
                val = p.get("#text", "")
                data[key] = val
        if not data.get("Name"):
            return None
        return {
            "name": data.get("Name"),
            "expression": data.get("Expression"),
            "output_column": data.get("OutputCol", data.get("Name")),
        }


# ─────────────────────── Convenience function ─────────────────────────────────

def parse_datastage_file(file_path: str | Path) -> dict:
    """
    Parse a DataStage DSX/ISX file and return the structured JSON dict.
    Convenience wrapper around DataStageParser.
    """
    parser = DataStageParser(file_path)
    return parser.parse()
