"""
Layer 2: Reasoning Engine
The core AI intelligence of the modernization pipeline. This module encodes
the domain expertise of a senior ETL migration engineer, translating IBM
DataStage constructs into PySpark and Google Cloud DataFusion equivalents.
No external API calls — all reasoning is performed in-process.
"""

from __future__ import annotations

import re
import logging
from typing import Any

from .pattern_library import (
    STRING_PATTERNS, DATE_PATTERNS, MATH_PATTERNS,
    NULL_CONDITIONAL_PATTERNS, JOIN_TYPE_MAP, WRITE_MODE_MAP,
    CONNECTOR_SPARK_MAP, AGG_FUNCTION_MAP,
)
from .confidence_scorer import ConfidenceScorer, ConfidenceResult

logger = logging.getLogger(__name__)


class ReasoningEngine:
    """
    Core reasoning engine that drives all agent transformations.
    Translates parsed DataStage structures into cloud-native equivalents.
    """

    def __init__(self, config: dict | None = None):
        self.config = config or {}
        self.scorer = ConfidenceScorer(config)
        # Merge all pattern groups for expression translation
        self._patterns: dict[str, dict[str, str]] = {}
        self.translation_warnings: list[dict] = []  # Track failed pattern translations
        for group in [STRING_PATTERNS, DATE_PATTERNS, MATH_PATTERNS, NULL_CONDITIONAL_PATTERNS]:
            for target, mapping in group.items():
                if target not in self._patterns:
                    self._patterns[target] = {}
                self._patterns[target].update(mapping)

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def score(self, parsed_job: dict) -> ConfidenceResult:
        """Return confidence score and complexity classification."""
        return self.scorer.score(parsed_job)

    def translate_expression(self, expression: str, target: str = "pyspark") -> str:
        """
        Translate a DataStage transformer expression to PySpark or DataFusion.

        Handles:
        - Built-in function calls (Upcase, Year, Checksum, etc.)
        - If/Then/Else conditional chains → when().otherwise() / CASE WHEN
        - String concatenation (:)
        - Basic arithmetic and comparison operators
        """
        if not expression:
            return expression

        expr = expression.strip()

        # 1. Handle If/Then/Else chains (recursive)
        if re.match(r"^If\s+", expr, re.IGNORECASE):
            return self._translate_conditional(expr, target)

        # 2. Apply all pattern substitutions
        patterns = self._patterns.get(target, {})
        for pattern, replacement in patterns.items():
            try:
                expr = re.sub(pattern, replacement, expr)
            except re.error as e:
                logger.warning(
                    f"[ReasoningEngine] Regex pattern failed: '{pattern}' → '{replacement}' | "
                    f"Error: {e} | Expression: '{expression[:80]}'"
                )
                self.translation_warnings.append({
                    "type": "regex_failure",
                    "pattern": pattern,
                    "replacement": replacement,
                    "expression": expression[:120],
                    "error": str(e),
                })

        # 3. DS string concatenation operator (:) → || or concat
        if ":" in expr and not expr.startswith("jdbc"):
            expr = self._translate_concatenation(expr, target)

        # 4. Operator normalization
        expr = self._normalize_operators(expr, target)

        return expr

    def translate_filter(self, condition: str, target: str = "pyspark") -> str:
        """Translate a DataStage filter condition to target syntax."""
        cond = condition.strip()
        cond = self._normalize_operators(cond, target)
        # DataStage IS NOT NULL / IS NULL are standard SQL — keep as-is
        return cond

    def translate_aggregation(self, agg_string: str, target: str = "pyspark") -> list[dict]:
        """
        Parse DataStage aggregation string like:
        "SUM(NET_AMOUNT) AS TOTAL_REVENUE,COUNT(SALE_ID) AS TOTAL_ORDERS"
        → list of {function, column, alias, expression}
        """
        aggregations = []
        parts = [p.strip() for p in agg_string.split(",")]
        agg_fn_map = AGG_FUNCTION_MAP.get(target, AGG_FUNCTION_MAP["pyspark"])

        for part in parts:
            m = re.match(
                r"(\w+)\(([^)]+)\)\s+AS\s+(\w+)",
                part,
                re.IGNORECASE,
            )
            if m:
                fn_name = m.group(1).upper()
                col_name = m.group(2).strip()
                alias = m.group(3).strip()
                mapped_fn = agg_fn_map.get(fn_name, fn_name if target == "datafusion" else f"F.{fn_name.lower()}")
                aggregations.append({
                    "function": fn_name,
                    "column": col_name,
                    "alias": alias,
                    "mapped_function": mapped_fn,
                    "expression": f"{mapped_fn}('{col_name}')" if target == "pyspark" else f"{mapped_fn}({col_name}) AS {alias}",
                })
        return aggregations

    def get_join_type(self, ds_join_type: str, target: str = "pyspark") -> str:
        """Map DataStage join type to target join type string."""
        mapping = JOIN_TYPE_MAP.get(ds_join_type, {})
        return mapping.get(target, "inner")

    def get_write_mode(self, ds_write_mode: str, target: str = "pyspark") -> str:
        """Map DataStage write mode to target write mode."""
        mapping = WRITE_MODE_MAP.get(ds_write_mode.upper(), {})
        return mapping.get(target, "append")

    def get_spark_connector_info(self, stage_type: str) -> dict:
        """Return Spark reader/writer hints for a DataStage connector type."""
        return CONNECTOR_SPARK_MAP.get(stage_type, {"format": "jdbc", "driver": None, "url_prefix": None})

    def infer_schema_from_columns(self, columns: list[dict], target: str = "pyspark") -> str:
        """
        Generate a schema definition string from parsed column definitions.
        For PySpark: StructType([StructField(...), ...])
        For DataFusion: schema specification
        """
        if not columns:
            return ""

        if target == "pyspark":
            fields = []
            for col in columns:
                spark_type = col.get("spark_type", "StringType")
                nullable = col.get("nullable", True)
                # Handle decimal with precision/scale
                if spark_type == "DecimalType":
                    prec = col.get("precision", 18)
                    scale = col.get("scale", 2)
                    type_str = f"DecimalType({prec}, {scale})"
                else:
                    type_str = f"{spark_type}()"
                null_str = "True" if nullable else "False"
                fields.append(f'    StructField("{col["name"]}", {type_str}, {null_str})')
            return "StructType([\n" + ",\n".join(fields) + "\n])"

        elif target == "datafusion":
            fields = []
            for col in columns:
                sql_type = col.get("sql_type", "STRING")
                prec = col.get("precision")
                scale = col.get("scale")
                if sql_type in ("NUMERIC", "DECIMAL") and prec:
                    type_str = f"DECIMAL({prec},{scale or 0})"
                elif sql_type in ("VARCHAR", "CHAR") and prec:
                    type_str = f"STRING"  # DataFusion uses STRING
                else:
                    type_str = sql_type
                fields.append(f'"{col["name"]}": "{type_str}"')
            return "{" + ", ".join(fields) + "}"

        return ""

    # ─────────────────────────────────────────────────────────────────────────
    # Internal translation helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _translate_conditional(self, expr: str, target: str) -> str:
        """
        Recursively translate DataStage If/Then/Else chains.

        DS:      If A > 10 Then "GOLD" Else If A > 5 Then "SILVER" Else "BRONZE"
        PySpark: when(col('A') > 10, 'GOLD').when(col('A') > 5, 'SILVER').otherwise('BRONZE')
        DFusion: CASE WHEN A > 10 THEN 'GOLD' WHEN A > 5 THEN 'SILVER' ELSE 'BRONZE' END
        """
        branches = self._parse_if_then_else(expr)

        if target == "pyspark":
            parts = []
            for i, branch in enumerate(branches):
                condition = branch.get("condition")
                value = branch.get("value", "")
                value = self._quote_value(value)
                if condition:
                    parts.append(f"when({condition}, {value})")
                else:
                    parts.append(f"otherwise({value})")
            return ".".join(parts)

        elif target == "datafusion":
            parts = ["CASE"]
            for branch in branches:
                condition = branch.get("condition")
                value = branch.get("value", "")
                value_sql = self._to_sql_literal(value)
                if condition:
                    parts.append(f"WHEN {condition} THEN {value_sql}")
                else:
                    parts.append(f"ELSE {value_sql}")
            parts.append("END")
            return " ".join(parts)

        return expr

    def _parse_if_then_else(self, expr: str) -> list[dict]:
        """
        Parse DataStage If/Then/Else expression into a list of branches.
        Each branch: {"condition": str | None, "value": str}
        """
        if expr.lower().count("if ") > 1:
            logger.warning(
                f"[ReasoningEngine] Detected nested If/Then/Else chains in expression: '{expr}'. "
                "Rule-based parsing might misparse; falling back to LLM translation for correctness."
            )
        branches = []
        remaining = expr.strip()

        while remaining:
            # Match "If <condition> Then <value>"
            m = re.match(
                r"^If\s+(.+?)\s+Then\s+(.+?)(?:\s+Else\s+|$)",
                remaining,
                re.IGNORECASE | re.DOTALL,
            )
            if m:
                condition = self._translate_condition(m.group(1).strip())
                value = m.group(2).strip()
                branches.append({"condition": condition, "value": value})
                # Advance past "Else "
                else_pos = re.search(r"\s+Else\s+", remaining, re.IGNORECASE)
                if else_pos:
                    remaining = remaining[else_pos.end():]
                else:
                    break
            else:
                # Remainder is the else value
                branches.append({"condition": None, "value": remaining.strip()})
                break

        return branches

    def _translate_condition(self, condition: str) -> str:
        """Translate a DS condition string to Spark/SQL-compatible syntax."""
        cond = condition
        # DataStage uses = for equality (not ==)
        cond = re.sub(r"(?<![<>!])=(?!=)", "==", cond)
        # Handle IS NOT NULL / IS NULL (keep as-is for Spark)
        cond = cond.replace(" Is Not Null", " is not null").replace(" Is Null", " is null")
        return cond

    def _normalize_operators(self, expr: str, target: str) -> str:
        """Normalize DataStage operators to target syntax."""
        # DataStage uses & for string concat in some contexts
        if target == "pyspark":
            # Keep SQL-style comparisons; Spark accepts both
            pass
        return expr

    def _translate_concatenation(self, expr: str, target: str) -> str:
        """
        DataStage uses : for string concatenation.
        PySpark: concat(a, b)   DataFusion: CONCAT(a, b)
        """
        # Split on : being careful not to split inside function calls
        parts = [p.strip() for p in expr.split(":")]
        if len(parts) < 2:
            return expr
        if target == "pyspark":
            return f"concat({', '.join(parts)})"
        elif target == "datafusion":
            return f"CONCAT({', '.join(parts)})"
        return expr

    def _quote_value(self, value: str) -> str:
        """Ensure string literals use proper Python quote style."""
        v = value.strip()
        # If it's already a string literal with double quotes, convert to single
        if v.startswith('"') and v.endswith('"'):
            return "'" + v[1:-1] + "'"
        return v

    def _to_sql_literal(self, value: str) -> str:
        """Convert a value to SQL literal format."""
        v = value.strip()
        if v.startswith('"') and v.endswith('"'):
            return "'" + v[1:-1] + "'"
        return v
