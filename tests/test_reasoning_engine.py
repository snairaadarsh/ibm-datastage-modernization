"""
Unit tests for ReasoningEngine (Layer 2: Reasoning Engine).
Tests expression translation, conditional chains, join type mapping,
write mode mapping, aggregation parsing, and schema generation.
"""

import sys
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.layer2_reasoning_engine.reasoning_engine import ReasoningEngine


@pytest.fixture
def engine():
    """Fresh ReasoningEngine instance."""
    return ReasoningEngine()


# ─────────────────────────────────────────────────────────────────────────────
# String Function Translation
# ─────────────────────────────────────────────────────────────────────────────

class TestStringTranslation:
    """Test DataStage string function → PySpark/DataFusion translation."""

    def test_upcase_pyspark(self, engine):
        result = engine.translate_expression("Upcase(CUST_NAME)", "pyspark")
        assert "upper" in result.lower(), f"Expected 'upper' in: {result}"

    def test_downcase_pyspark(self, engine):
        result = engine.translate_expression("Downcase(EMAIL)", "pyspark")
        assert "lower" in result.lower(), f"Expected 'lower' in: {result}"

    def test_trim_pyspark(self, engine):
        result = engine.translate_expression("Trim(ADDRESS)", "pyspark")
        assert "trim" in result.lower(), f"Expected 'trim' in: {result}"

    def test_length_pyspark(self, engine):
        result = engine.translate_expression("Length(NAME)", "pyspark")
        assert "length" in result.lower(), f"Expected 'length' in: {result}"

    def test_left_pyspark(self, engine):
        result = engine.translate_expression("Left(CODE, 3)", "pyspark")
        assert "substring" in result.lower(), f"Expected 'substring' in: {result}"

    def test_upcase_datafusion(self, engine):
        result = engine.translate_expression("Upcase(CUST_NAME)", "datafusion")
        assert "UPPER" in result, f"Expected 'UPPER' in: {result}"

    def test_trim_datafusion(self, engine):
        result = engine.translate_expression("Trim(ADDRESS)", "datafusion")
        assert "TRIM" in result, f"Expected 'TRIM' in: {result}"


# ─────────────────────────────────────────────────────────────────────────────
# Date Function Translation
# ─────────────────────────────────────────────────────────────────────────────

class TestDateTranslation:
    """Test DataStage date function → PySpark translation."""

    def test_year_pyspark(self, engine):
        result = engine.translate_expression("Year(ORDER_DATE)", "pyspark")
        assert "year" in result.lower(), f"Expected 'year' in: {result}"

    def test_month_pyspark(self, engine):
        result = engine.translate_expression("Month(ORDER_DATE)", "pyspark")
        assert "month" in result.lower(), f"Expected 'month' in: {result}"

    def test_current_date_pyspark(self, engine):
        result = engine.translate_expression("CurrentDate()", "pyspark")
        assert "current_date" in result.lower(), f"Expected 'current_date' in: {result}"

    def test_datediff_pyspark(self, engine):
        result = engine.translate_expression("DateDiff(END_DATE, START_DATE)", "pyspark")
        assert "datediff" in result.lower(), f"Expected 'datediff' in: {result}"


# ─────────────────────────────────────────────────────────────────────────────
# Math Function Translation
# ─────────────────────────────────────────────────────────────────────────────

class TestMathTranslation:
    """Test DataStage math function → PySpark translation."""

    def test_abs_pyspark(self, engine):
        result = engine.translate_expression("Abs(AMOUNT)", "pyspark")
        assert "abs" in result.lower(), f"Expected 'abs' in: {result}"

    def test_round_pyspark(self, engine):
        result = engine.translate_expression("Round(PRICE, 2)", "pyspark")
        assert "round" in result.lower(), f"Expected 'round' in: {result}"

    def test_checksum_pyspark(self, engine):
        result = engine.translate_expression("Checksum(ROW_DATA)", "pyspark")
        assert "hash" in result.lower(), f"Expected 'hash' in: {result}"


# ─────────────────────────────────────────────────────────────────────────────
# Null / Conditional Translation
# ─────────────────────────────────────────────────────────────────────────────

class TestNullConditionalTranslation:
    """Test null-handling and conditional function translation."""

    def test_isnull_pyspark(self, engine):
        result = engine.translate_expression("IsNull(REGION)", "pyspark")
        assert "is null" in result.lower(), f"Expected 'is null' in: {result}"

    def test_nvl_pyspark(self, engine):
        result = engine.translate_expression("Nvl(REGION, 'UNKNOWN')", "pyspark")
        assert "coalesce" in result.lower(), f"Expected 'coalesce' in: {result}"

    def test_nvl_datafusion(self, engine):
        result = engine.translate_expression("Nvl(REGION, 'UNKNOWN')", "datafusion")
        assert "COALESCE" in result, f"Expected 'COALESCE' in: {result}"


# ─────────────────────────────────────────────────────────────────────────────
# If/Then/Else Conditional Chains
# ─────────────────────────────────────────────────────────────────────────────

class TestConditionalChains:
    """Test If/Then/Else translation to when/otherwise and CASE WHEN."""

    def test_simple_if_then_else_pyspark(self, engine):
        expr = 'If AMOUNT > 1000 Then "HIGH" Else "LOW"'
        result = engine.translate_expression(expr, "pyspark")
        assert "when" in result.lower(), f"Expected 'when' in: {result}"
        assert "otherwise" in result.lower(), f"Expected 'otherwise' in: {result}"

    def test_simple_if_then_else_datafusion(self, engine):
        expr = 'If AMOUNT > 1000 Then "HIGH" Else "LOW"'
        result = engine.translate_expression(expr, "datafusion")
        assert "CASE" in result, f"Expected 'CASE' in: {result}"
        assert "WHEN" in result, f"Expected 'WHEN' in: {result}"
        assert "ELSE" in result, f"Expected 'ELSE' in: {result}"
        assert "END" in result, f"Expected 'END' in: {result}"

    def test_empty_expression_passthrough(self, engine):
        """Empty expression should be returned as-is."""
        assert engine.translate_expression("", "pyspark") == ""
        assert engine.translate_expression("  ", "pyspark") == ""


# ─────────────────────────────────────────────────────────────────────────────
# Join Type Mapping
# ─────────────────────────────────────────────────────────────────────────────

class TestJoinTypeMapping:
    """Test DataStage join type → Spark/DataFusion mapping."""

    def test_inner_join(self, engine):
        assert engine.get_join_type("Inner", "pyspark") == "inner"

    def test_left_outer_join(self, engine):
        assert engine.get_join_type("LeftOuter", "pyspark") == "left"

    def test_full_outer_join(self, engine):
        assert engine.get_join_type("FullOuter", "pyspark") == "outer"

    def test_cross_join(self, engine):
        assert engine.get_join_type("Cross", "pyspark") == "cross"

    def test_lookup_maps_to_left(self, engine):
        assert engine.get_join_type("Lookup", "pyspark") == "left"

    def test_unknown_join_defaults_to_inner(self, engine):
        assert engine.get_join_type("UnknownType", "pyspark") == "inner"

    def test_left_outer_datafusion(self, engine):
        assert engine.get_join_type("LeftOuter", "datafusion") == "LEFT OUTER"


# ─────────────────────────────────────────────────────────────────────────────
# Write Mode Mapping
# ─────────────────────────────────────────────────────────────────────────────

class TestWriteModeMapping:
    """Test DataStage write mode → Spark mapping."""

    def test_append_mode(self, engine):
        assert engine.get_write_mode("APPEND", "pyspark") == "append"

    def test_overwrite_mode(self, engine):
        assert engine.get_write_mode("OVERWRITE", "pyspark") == "overwrite"

    def test_merge_mode(self, engine):
        assert engine.get_write_mode("MERGE", "pyspark") == "merge"

    def test_unknown_defaults_to_append(self, engine):
        assert engine.get_write_mode("UNKNOWN_MODE", "pyspark") == "append"


# ─────────────────────────────────────────────────────────────────────────────
# Aggregation Parsing
# ─────────────────────────────────────────────────────────────────────────────

class TestAggregationParsing:
    """Test aggregation string parsing."""

    def test_single_sum(self, engine):
        aggs = engine.translate_aggregation("SUM(NET_AMOUNT) AS TOTAL_REVENUE", "pyspark")
        assert len(aggs) == 1
        assert aggs[0]["function"] == "SUM"
        assert aggs[0]["column"] == "NET_AMOUNT"
        assert aggs[0]["alias"] == "TOTAL_REVENUE"

    def test_multiple_aggregations(self, engine):
        agg_str = "SUM(NET_AMOUNT) AS TOTAL_REVENUE,COUNT(SALE_ID) AS TOTAL_ORDERS"
        aggs = engine.translate_aggregation(agg_str, "pyspark")
        assert len(aggs) == 2
        assert aggs[0]["function"] == "SUM"
        assert aggs[1]["function"] == "COUNT"

    def test_aggregation_datafusion(self, engine):
        aggs = engine.translate_aggregation("MAX(PRICE) AS MAX_PRICE", "datafusion")
        assert len(aggs) == 1
        assert "MAX" in aggs[0]["mapped_function"]


# ─────────────────────────────────────────────────────────────────────────────
# Schema Generation
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemaGeneration:
    """Test schema string generation from column definitions."""

    def test_pyspark_struct_type(self, engine):
        columns = [
            {"name": "ID", "spark_type": "IntegerType", "nullable": False},
            {"name": "NAME", "spark_type": "StringType", "nullable": True},
        ]
        schema = engine.infer_schema_from_columns(columns, "pyspark")
        assert "StructType" in schema
        assert "StructField" in schema
        assert '"ID"' in schema
        assert '"NAME"' in schema

    def test_decimal_precision(self, engine):
        columns = [
            {"name": "AMOUNT", "spark_type": "DecimalType", "nullable": True, "precision": 18, "scale": 2},
        ]
        schema = engine.infer_schema_from_columns(columns, "pyspark")
        assert "DecimalType(18, 2)" in schema

    def test_empty_columns_returns_empty(self, engine):
        assert engine.infer_schema_from_columns([], "pyspark") == ""


# ─────────────────────────────────────────────────────────────────────────────
# Translation Warning Tracking
# ─────────────────────────────────────────────────────────────────────────────

class TestTranslationWarnings:
    """Verify that regex failures are tracked (not silently swallowed)."""

    def test_warnings_list_initialized(self, engine):
        assert hasattr(engine, "translation_warnings")
        assert isinstance(engine.translation_warnings, list)
        assert len(engine.translation_warnings) == 0

    def test_valid_expression_no_warnings(self, engine):
        engine.translate_expression("Upcase(NAME)", "pyspark")
        assert len(engine.translation_warnings) == 0
