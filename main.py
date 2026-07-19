#!/usr/bin/env python3
"""
IBM DataStage ETL Modernization Pipeline
Main CLI entry point.

Usage:
    python main.py --input samples/customer_sales_etl.dsx
    python main.py --input samples/            (process all DSX files in directory)
    python main.py --input samples/ --dry-run  (no files written to disk)
    python main.py --watch                     (watch input/ directory continuously)
    python main.py --report                    (print current migration report)
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import box
from rich.text import Text

# ── Load environment variables ─────────────────────────────────────────────────
load_dotenv()

console = Console()

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load YAML configuration."""
    path = Path(config_path)
    if path.exists():
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}
    logger.warning(f"Config not found at {config_path}, using defaults")
    return {}


def print_banner() -> None:
    banner = Text()
    banner.append("IBM DataStage ", style="bold cyan")
    banner.append("ETL Modernization Pipeline", style="bold white")
    banner.append("\n  Powered by Antigravity IDE Agent", style="dim")
    console.print(Panel(banner, border_style="cyan", padding=(1, 4)))



def print_result(result: dict) -> None:
    """Pretty-print a single job pipeline result."""
    job_name = result.get("job_name", "?")
    status = result.get("status", "unknown")
    classification = result.get("classification", {})
    complexity = classification.get("complexity", "?")
    confidence = classification.get("confidence_score", 0.0)
    review = result.get("review", {})
    validation = result.get("validation", {})
    elapsed = result.get("elapsed_seconds", 0)
    escalation = result.get("escalation", {})

    status_style = {
        "completed": "[bold green]COMPLETED[/]",
        "escalated": "[bold yellow]ESCALATED[/]",
        "failed": "[bold red]FAILED[/]",
    }.get(status, f"[dim]{status}[/]")

    complexity_style = {
        "simple": "[green]Simple[/]",
        "medium": "[yellow]Medium[/]",
        "complex": "[red]Complex[/]",
    }.get(complexity, complexity)

    table = Table(box=box.ROUNDED, border_style="cyan", show_header=False, padding=(0, 1))
    table.add_column("Field", style="dim", width=24)
    table.add_column("Value")

    table.add_row("Job Name", f"[bold]{job_name}[/]")
    table.add_row("Status", status_style)
    table.add_row("Complexity", complexity_style)
    table.add_row("Confidence", f"{confidence:.1%}")
    table.add_row("Review", f"{'Passed' if review.get('passed') else 'Issues found'} (score={review.get('score', 0):.1%})")
    table.add_row("Validation", f"{validation.get('overall_status', '?').upper()} (score={validation.get('overall_score', 0):.1%})")
    table.add_row("Elapsed", f"{elapsed}s")

    if escalation.get("escalated"):
        table.add_row("Escalation", f"[red]ESCALATION: {escalation.get('instructions', '')}[/]")
    elif escalation.get("flagged_sections"):
        table.add_row("Flags", f"[yellow]{len(escalation['flagged_sections'])} section(s) need review[/]")

    output_files = result.get("output_files", {})
    if output_files.get("pyspark"):
        table.add_row("PySpark Output", f"[cyan]{output_files['pyspark']}[/]")
    if output_files.get("datafusion"):
        table.add_row("DataFusion Output", f"[cyan]{output_files['datafusion']}[/]")
    if output_files.get("tests"):
        table.add_row("Test Suite", f"[cyan]{output_files['tests']}[/]")

    if result.get("error"):
        table.add_row("Error", f"[red]{result['error']}[/]")

    console.print(table)


def print_report(reports_dir: str = "./output/reports") -> None:
    """Print the current migration report summary."""
    report_path = Path(reports_dir) / "migration_report.json"
    if not report_path.exists():
        console.print("[yellow]No migration report found. Run some jobs first.[/]")
        return

    with open(report_path) as f:
        report = json.load(f)

    metrics = report.get("metrics", {})
    jobs = report.get("jobs", [])

    # Metrics panel
    m_table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    m_table.add_column("Metric", style="dim")
    m_table.add_column("Value", style="bold")
    m_table.add_row("Total Jobs", str(metrics.get("total_jobs", 0)))
    m_table.add_row("Successful", f"[green]{metrics.get('successful', 0)}[/]")
    m_table.add_row("Escalated", f"[yellow]{metrics.get('escalated', 0)}[/]")
    m_table.add_row("Failed", f"[red]{metrics.get('failed', 0)}[/]")
    m_table.add_row("Success Rate", f"{metrics.get('success_rate', 0):.1%}")
    m_table.add_row("Avg Confidence", f"{metrics.get('avg_confidence', 0):.1%}")
    m_table.add_row("Total Stages Migrated", str(metrics.get("total_stages_migrated", 0)))

    console.print(Panel(m_table, title="[bold cyan]Migration Metrics[/]", border_style="cyan"))

    # Jobs table
    if jobs:
        j_table = Table(box=box.ROUNDED, border_style="dim")
        j_table.add_column("Job Name", style="bold")
        j_table.add_column("Status")
        j_table.add_column("Complexity")
        j_table.add_column("Confidence")
        j_table.add_column("Review")
        j_table.add_column("Validation")

        for job in jobs:
            status = job.get("status", "?")
            s_icon = {"completed": "OK", "escalated": "!!", "failed": "X"}.get(status, "?")
            complexity = job.get("complexity", "?")
            c_style = {"simple": "green", "medium": "yellow", "complex": "red"}.get(complexity, "white")
            j_table.add_row(
                job.get("job_name", "?"),
                f"{s_icon} {status}",
                f"[{c_style}]{complexity}[/]",
                f"{job.get('confidence_score', 0):.1%}",
                f"{'✅' if job.get('review_passed') else '⚠️'} {job.get('review_score', 0):.1%}",
                f"{job.get('validation_status', '?')} {job.get('validation_score', 0):.1%}",
            )
        console.print(j_table)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="IBM DataStage ETL Modernization Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input", "-i",
        help="Path to a .dsx/.isx file or directory containing multiple files",
        metavar="PATH",
    )
    parser.add_argument(
        "--config", "-c",
        default="config/config.yaml",
        help="Path to config.yaml (default: config/config.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and analyze without writing output files",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Watch the input/ directory for new DSX files continuously",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Display the current migration report and exit",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["pyspark", "datafusion"],
        default=["pyspark", "datafusion"],
        help="Output formats to generate (default: both)",
    )
    parser.add_argument(
        "--automate-complex",
        action="store_true",
        help="Skip human-review gate even for complex jobs (use with caution)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose debug logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    print_banner()

    # ── Show report only ───────────────────────────────────────────────────────
    if args.report:
        print_report()
        return

    if not args.input and not args.watch:
        parser.print_help()
        console.print("\n[yellow]Hint: Use --input samples/customer_sales_etl.dsx to get started![/]\n")
        return

    # ── Load config ────────────────────────────────────────────────────────────
    config = load_config(args.config)
    if args.automate_complex:
        config.setdefault("automate_complex", True)
    config.setdefault("output", {})["formats"] = args.formats

    # ── Initialize orchestrator ────────────────────────────────────────────────
    from src.layer3_agents.orchestrator import PipelineOrchestrator
    orchestrator = PipelineOrchestrator(config=config, output_dir="./output")

    # ── Watch mode ─────────────────────────────────────────────────────────────
    if args.watch:
        from src.layer1_ingestion.file_watcher import FileWatcher
        console.print("[cyan]>> Watching [bold]./input/[/] for new .dsx/.isx files...[/]\n")

        def on_new_file(path):
            console.print(f"\n[cyan]>> New file: {path.name}[/]")
            result = orchestrator.process_file(path, dry_run=args.dry_run)
            print_result(result)

        watcher = FileWatcher("./input", callback=on_new_file)
        # Process existing files first
        existing = watcher.scan_existing()
        for f in existing:
            on_new_file(f)
        watcher.run_forever()
        return

    # ── File or directory mode ─────────────────────────────────────────────────
    input_path = Path(args.input)
    if not input_path.exists():
        console.print(f"[red]X Input not found: {input_path}[/]")
        sys.exit(1)

    if input_path.is_dir():
        console.print(f"[cyan]>> Processing directory: {input_path}[/]\n")
        results = orchestrator.process_directory(input_path, dry_run=args.dry_run)
        for r in results:
            print_result(r)
        print_report()
    else:
        console.print(f"[cyan]>> Processing file: {input_path.name}[/]\n")
        result = orchestrator.process_file(input_path, dry_run=args.dry_run)
        print_result(result)


if __name__ == "__main__":
    main()
