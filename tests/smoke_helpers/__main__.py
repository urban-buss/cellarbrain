"""CLI entry point: ``py -m tests.smoke_helpers``.

Orchestrates: pytest → discover → clean → full load → syncs →
verify → integrity → cross-run → MCP integration → report.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import SmokeConfig
from .discover import discover_raw_folders, get_environment, validate_folder
from .report import generate_report, write_report
from .runner import clean_output, run_etl, run_pytest, rebuild_server
from .verify import (
    check_dossiers,
    check_entity_counts,
    check_etl_runs,
    check_parquet_files,
    check_wine_schema,
    check_cross_run,
    check_fk_integrity,
    check_dossier_integrity,
    run_validation,
)


def main(argv: list[str] | None = None) -> int:
    """Run the full smoke-test pipeline. Returns 0 on success, 1 on failure."""
    parser = argparse.ArgumentParser(
        prog="smoke_helpers",
        description="Run ETL smoke tests and generate a report.",
    )
    parser.add_argument(
        "--raw-dir", type=Path, default=Path("raw"),
        help="Directory containing YYMMDD raw CSV folders (default: raw)",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("output"),
        help="ETL output directory (default: output)",
    )
    parser.add_argument(
        "--report-dir", type=Path, default=Path("smoke-reports"),
        help="Directory for report output (default: smoke-reports)",
    )
    parser.add_argument(
        "--folders", type=str, default=None,
        help="Comma-separated list of YYMMDD folders (default: auto-discover)",
    )
    parser.add_argument(
        "--trigger", type=str, default="py -m tests.smoke_helpers",
        help="Description of what triggered this run",
    )
    parser.add_argument(
        "--settings-file", type=str, default=None,
        help="Path to cellarbrain TOML settings file",
    )

    args = parser.parse_args(argv)

    # --- Phase 0: Pytest ---
    print("--- Phase 0: Python Tests ---")
    pytest_result = run_pytest()
    status = "PASS" if pytest_result.ok else "FAIL"
    print(f"  [{status}] {pytest_result.passed} passed, {pytest_result.failed} failed, {pytest_result.errors} errors")
    if not pytest_result.ok:
        # Show truncated error output
        for line in pytest_result.output.splitlines()[-20:]:
            print(f"    {line}")
    print()

    # --- Settings ---
    settings = None
    if args.settings_file:
        from cellarbrain.settings import load_settings
        settings = load_settings(args.settings_file)

    # --- Discover folders ---
    if args.folders:
        folders = [f.strip() for f in args.folders.split(",")]
    else:
        folders = discover_raw_folders(args.raw_dir)

    if not folders:
        print(f"Error: no YYMMDD folders found in {args.raw_dir}", file=sys.stderr)
        return 1

    # Validate each folder has the expected CSVs
    for folder in folders:
        if not validate_folder(args.raw_dir, folder):
            print(f"Error: folder {folder} is missing expected CSV files", file=sys.stderr)
            return 1

    # --- Environment ---
    env = get_environment()
    config = SmokeConfig(
        raw_dir=args.raw_dir,
        output_dir=args.output_dir,
        folders=folders,
        python_version=env["python_version"],
        cellarbrain_version=env["cellarbrain_version"],
    )

    print(f"Smoke test: {len(folders)} folders: {', '.join(folders)}")
    print(f"Python {config.python_version}, cellarbrain {config.cellarbrain_version}")
    print(f"Output → {args.output_dir}\n")

    # --- Phase 1: Clean + ETL runs ---
    removed = clean_output(args.output_dir)
    if removed:
        print(f"Cleaned {removed} existing Parquet files\n")

    runs = []
    for i, folder in enumerate(folders):
        sync = i > 0  # first folder = full load
        mode = "sync" if sync else "full load"
        print(f"--- Run {i + 1}: {folder} ({mode}) ---")
        result = run_etl(
            args.raw_dir, folder, args.output_dir,
            sync=sync, settings=settings,
        )
        runs.append(result)
        ok_str = "OK" if result.exit_ok else "FAIL"
        print(
            f"  {ok_str}: {result.csv_counts.get('wines', '?')} wines, "
            f"{result.csv_counts.get('bottles', '?')} bottles, "
            f"validation {result.validation_passed}/{result.validation_passed + result.validation_failed}"
        )
        if result.errors:
            print(f"  ERRORS: {result.errors}")
        print()

    # --- Phase 2: Output verification ---
    print("--- Phase 2: Output Verification ---")
    output_checks = [
        check_parquet_files(args.output_dir),
        check_etl_runs(args.output_dir, expected_count=len(runs)),
        check_entity_counts(args.output_dir),
        check_wine_schema(args.output_dir),
        check_dossiers(args.output_dir),
        run_validation(args.output_dir),
    ]
    for check in output_checks:
        status = "PASS" if check.passed else "FAIL"
        print(f"  [{status}] {check.name}: {check.details}")
    print()

    # --- Phase 3: Data integrity ---
    print("--- Phase 3: Data Integrity ---")
    fk_checks = check_fk_integrity(args.output_dir)
    dossier_checks = check_dossier_integrity(args.output_dir)
    integrity_checks = fk_checks + dossier_checks
    for check in integrity_checks:
        status = "PASS" if check.passed else "FAIL"
        print(f"  [{status}] {check.name}: {check.details}")
    print()

    # --- Phase 4: Cross-run consistency ---
    cross_checks = check_cross_run(args.output_dir) if len(runs) > 1 else []
    if cross_checks:
        print("--- Phase 4: Cross-run Consistency ---")
        for check in cross_checks:
            status = "PASS" if check.passed else "FAIL"
            print(f"  [{status}] {check.name}: {check.details}")
        print()

    # --- Phase 5: MCP integration ---
    mcp_checks: list = []
    print("--- Phase 5: MCP Integration ---")
    rebuild_result = rebuild_server()
    if rebuild_result.ok:
        print(f"  Server rebuilt: {rebuild_result.exe_path}")
        from .mcp_checks import run_mcp_checks
        mcp_checks = run_mcp_checks(rebuild_result.exe_path, args.output_dir)
        for check in mcp_checks:
            status = "PASS" if check.passed else "FAIL"
            print(f"  [{status}] {check.name}: {check.details}")
    else:
        print(f"  [FAIL] Server rebuild failed: {rebuild_result.output[:200]}")
        from . import CheckResult
        mcp_checks = [CheckResult(
            name="Server rebuild",
            passed=False,
            details=rebuild_result.output[:200],
        )]
    print()

    # --- Report ---
    report_content = generate_report(
        config, runs, output_checks, cross_checks,
        trigger=args.trigger,
        pytest_result=pytest_result,
        integrity_checks=integrity_checks,
        mcp_checks=mcp_checks,
    )
    report_path = write_report(report_content, args.report_dir)
    print(f"Report written to {report_path}")

    # --- Overall result ---
    all_pass = (
        pytest_result.ok
        and all(c.passed for c in output_checks)
        and all(c.passed for c in integrity_checks)
        and all(c.passed for c in cross_checks)
        and all(c.passed for c in mcp_checks)
        and all(r.exit_ok for r in runs)
    )
    print(f"\nOverall: {'PASS' if all_pass else 'FAIL'}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
