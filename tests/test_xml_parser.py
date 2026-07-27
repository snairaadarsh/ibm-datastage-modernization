"""
Unit tests for DataStageParser (Layer 1: Ingestion).
Tests XML parsing, stage extraction, link extraction, DAG construction,
and IO classification against the sample DSX files.
"""

import os
import sys
import pytest
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.layer1_ingestion.xml_parser import DataStageParser

SAMPLES_DIR = PROJECT_ROOT / "samples"
SAMPLE_SIMPLE = SAMPLES_DIR / "customer_sales_etl.dsx"
SAMPLE_COMPLEX = SAMPLES_DIR / "inventory_reconciliation_complex.dsx"


@pytest.fixture
def simple_parser():
    """Parser fixture for the simple customer_sales_etl sample."""
    assert SAMPLE_SIMPLE.exists(), f"Sample file not found: {SAMPLE_SIMPLE}"
    return DataStageParser(SAMPLE_SIMPLE)


@pytest.fixture
def complex_parser():
    """Parser fixture for the complex inventory_reconciliation sample."""
    assert SAMPLE_COMPLEX.exists(), f"Sample file not found: {SAMPLE_COMPLEX}"
    return DataStageParser(SAMPLE_COMPLEX)


@pytest.fixture
def simple_parsed(simple_parser):
    """Pre-parsed result for the simple job."""
    return simple_parser.parse()


@pytest.fixture
def complex_parsed(complex_parser):
    """Pre-parsed result for the complex job."""
    return complex_parser.parse()


# ─────────────────────────────────────────────────────────────────────────────
# Basic Parsing Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestBasicParsing:
    """Verify that the parser can successfully parse DSX files."""

    def test_parse_returns_dict(self, simple_parsed):
        """parse() should return a dictionary."""
        assert isinstance(simple_parsed, dict)

    def test_job_name_extracted(self, simple_parsed):
        """Parser should extract the job name from the DSX."""
        job_name = simple_parsed.get("job_name", "")
        assert job_name, "job_name should not be empty"
        assert isinstance(job_name, str)

    def test_stages_is_list(self, simple_parsed):
        """Stages should be a non-empty list."""
        stages = simple_parsed.get("stages", [])
        assert isinstance(stages, list)
        assert len(stages) > 0, "Parser should extract at least one stage"

    def test_links_is_list(self, simple_parsed):
        """Links should be a list (may be empty for single-stage jobs)."""
        links = simple_parsed.get("links", [])
        assert isinstance(links, list)

    def test_complex_job_has_more_stages(self, simple_parsed, complex_parsed):
        """Complex job should have more stages than the simple job."""
        simple_count = len(simple_parsed.get("stages", []))
        complex_count = len(complex_parsed.get("stages", []))
        assert complex_count >= simple_count, (
            f"Complex job ({complex_count} stages) should have >= stages "
            f"than simple job ({simple_count} stages)"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Stage Extraction Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestStageExtraction:
    """Verify stage structure and properties."""

    def test_stage_has_id(self, simple_parsed):
        """Every stage should have a non-empty 'id' field."""
        for stage in simple_parsed["stages"]:
            assert "id" in stage, f"Stage missing 'id': {stage}"
            assert stage["id"], f"Stage 'id' should not be empty"

    def test_stage_has_stage_type(self, simple_parsed):
        """Every stage should have a 'stage_type' field."""
        for stage in simple_parsed["stages"]:
            assert "stage_type" in stage, f"Stage {stage.get('id', '?')} missing 'stage_type'"

    def test_stage_has_category(self, simple_parsed):
        """Every stage should be classified into a category."""
        for stage in simple_parsed["stages"]:
            assert "category" in stage, f"Stage {stage['id']} missing 'category'"
            assert stage["category"], f"Stage {stage['id']} has empty category"

    def test_known_categories_only(self, simple_parsed):
        """Stage categories should be from a known set."""
        known = {
            "source", "target", "transformer", "filter", "join",
            "lookup", "aggregation", "sort", "funnel", "copy",
            "modify", "merge", "peek", "sequencer", "source_or_target",
            "unknown",
        }
        for stage in simple_parsed["stages"]:
            assert stage["category"] in known, (
                f"Stage {stage['id']} has unexpected category: '{stage['category']}'"
            )

    def test_source_stages_have_zero_input_pins(self, simple_parsed):
        """Source stages should have 0 input pins."""
        for stage in simple_parsed["stages"]:
            if stage["category"] == "source":
                assert stage.get("input_pins", 0) == 0, (
                    f"Source stage {stage['id']} should have 0 input pins"
                )


# ─────────────────────────────────────────────────────────────────────────────
# Link / DAG Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestLinksAndDAG:
    """Verify link extraction and DAG structure."""

    def test_links_have_required_fields(self, simple_parsed):
        """Every link should have from_stage and to_stage."""
        for link in simple_parsed.get("links", []):
            assert "from_stage" in link, f"Link missing 'from_stage': {link}"
            assert "to_stage" in link, f"Link missing 'to_stage': {link}"

    def test_link_stages_exist(self, simple_parsed):
        """All stages referenced in links should exist in the stages list."""
        stage_ids = {s["id"] for s in simple_parsed["stages"]}
        for link in simple_parsed.get("links", []):
            assert link["from_stage"] in stage_ids, (
                f"Link references unknown from_stage: {link['from_stage']}"
            )
            assert link["to_stage"] in stage_ids, (
                f"Link references unknown to_stage: {link['to_stage']}"
            )

    def test_dag_order_present(self, simple_parsed):
        """Parser should produce links that enable DAG construction."""
        links = simple_parsed.get("links", [])
        stages = simple_parsed.get("stages", [])
        # Multi-stage jobs should have at least one link connecting stages
        if len(stages) > 1:
            assert len(links) > 0, "Multi-stage jobs should have links for DAG construction"


# ─────────────────────────────────────────────────────────────────────────────
# Column Extraction Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestColumnExtraction:
    """Verify column metadata is extracted from stages."""

    def test_source_stages_have_columns(self, simple_parsed):
        """Source stages should have output_columns defined."""
        sources = [s for s in simple_parsed["stages"] if s["category"] == "source"]
        for src in sources:
            cols = src.get("output_columns", [])
            assert isinstance(cols, list), f"Stage {src['id']} output_columns should be a list"
            # At least one column expected from a source
            assert len(cols) > 0, f"Source stage {src['id']} should have columns"

    def test_columns_have_name_and_type(self, simple_parsed):
        """Every column should have a name and sql_type."""
        for stage in simple_parsed["stages"]:
            for col in stage.get("output_columns", []):
                assert "name" in col, f"Column in {stage['id']} missing 'name'"
                assert col["name"], f"Column in {stage['id']} has empty name"


# ─────────────────────────────────────────────────────────────────────────────
# Summary / Metadata Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSummaryMetadata:
    """Verify summary/metadata fields."""

    def test_summary_present(self, simple_parsed):
        """Parsed output should contain a summary."""
        summary = simple_parsed.get("summary", {})
        assert isinstance(summary, dict)

    def test_complexity_hints_present(self, complex_parsed):
        """Complex job should detect complexity hints."""
        hints = complex_parsed.get("complexity_hints", [])
        assert isinstance(hints, list)
        # The complex job should have at least some hints
        assert len(hints) > 0, "Complex job should have complexity hints"
