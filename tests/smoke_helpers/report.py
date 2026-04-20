"""Generate Markdown smoke-test reports matching the agent template."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from . import CheckResult, RunResult, SmokeConfig
from .runner import PytestResult


def generate_report(
    config: SmokeConfig,
    runs: list[RunResult],
    output_checks: list[CheckResult],
    cross_checks: list[CheckResult],
    *,
    trigger: str = "",
    findings: list[str] | None = None,
    pytest_result: PytestResult | None = None,
    integrity_checks: list[CheckResult] | None = None,
    mcp_checks: list[CheckResult] | None = None,
) -> str:
    """Return a complete Markdown report string."""
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d %H:%M")

    all_checks = output_checks + cross_checks + (integrity_checks or []) + (mcp_checks or [])
    pytest_ok = pytest_result.ok if pytest_result else True
    all_pass = pytest_ok and all(c.passed for c in all_checks) and all(r.exit_ok for r in runs)
    overall = "PASS" if all_pass else "FAIL"

    lines: list[str] = []
    _a = lines.append

    _a(f"# ETL Smoke Test Report — {date_str} UTC\n")
    _a(f"**Agent:** cellarbrain-smoketest")
    _a(f'**Trigger:** {trigger or "automated"}')
    _a(f"**Overall:** {overall}")
    _a(f"**Raw folders:** {', '.join(config.folders)}\n")

    # --- Environment ---
    _a("## Environment\n")
    _a(f"- Python: {config.python_version}")
    _a(f"- cellarbrain: {config.cellarbrain_version}")
    _a(f"- OS: Windows\n")

    # --- Summary (first for quick triage) ---
    _a("## Summary\n")
    if pytest_result:
        pt_status = "passed" if pytest_result.ok else "FAILED"
        _a(f"- Python tests: {pytest_result.passed} passed, {pytest_result.failed} failed ({pt_status})")
    run_pass = sum(1 for r in runs if r.exit_ok or r.validation_failed == 0)
    _a(f"- ETL runs: {run_pass}/{len(runs)} completed successfully")
    out_pass = sum(1 for c in output_checks if c.passed)
    _a(f"- Output checks: {out_pass}/{len(output_checks)} passed")
    if integrity_checks:
        int_pass = sum(1 for c in integrity_checks if c.passed)
        _a(f"- Integrity checks: {int_pass}/{len(integrity_checks)} passed")
    if cross_checks:
        cross_pass = sum(1 for c in cross_checks if c.passed)
        _a(f"- Cross-run checks: {cross_pass}/{len(cross_checks)} passed")
    if mcp_checks:
        mcp_pass = sum(1 for c in mcp_checks if c.passed)
        _a(f"- MCP integration: {mcp_pass}/{len(mcp_checks)} passed")
    _a(f"- **Overall: {overall}**\n")

    # --- Findings ---
    _a("## Findings\n")
    if findings:
        for finding in findings:
            _a(f"- {finding}")
    else:
        _a("No issues found.")
    _a("")

    # --- Python Tests ---
    if pytest_result:
        _a("## Python Tests\n")
        pt_icon = "PASS" if pytest_result.ok else "FAIL"
        _a(f"**Result:** {pt_icon} — {pytest_result.passed} passed, "
           f"{pytest_result.failed} failed, {pytest_result.errors} errors\n")
        if not pytest_result.ok:
            _a("```")
            # Show last 30 lines of output for failed tests
            for line in pytest_result.output.splitlines()[-30:]:
                _a(line)
            _a("```")
        _a("")

    # --- ETL Runs ---
    _a("## ETL Runs\n")
    for i, run in enumerate(runs, start=1):
        mode = "full load" if not run.sync_mode else "sync"
        _a(f"### Run {i}: {run.folder} ({mode})\n")
        _a("| Metric | Value |")
        _a("|--------|-------|")
        _a(f"| Exit code | {'0 (ok)' if run.exit_ok else '1 (warnings only)'} |")
        _a(f"| Wine CSV rows | {run.csv_counts.get('wines', '?')} |")
        _a(f"| Bottle CSV rows | {run.csv_counts.get('bottles', '?')} |")
        _a(f"| Gone CSV rows | {run.csv_counts.get('bottles_gone', '?')} |")

        sm = run.slug_matching
        if sm:
            slug_str = (
                f"{sm.get('existing', 0)} existing, "
                f"{sm.get('new', 0)} new, "
                f"{sm.get('deleted', 0)} deleted, "
                f"{sm.get('revived', 0)} revived, "
                f"{sm.get('renamed', 0)} renamed"
            )
            _a(f"| Slug matching | {slug_str} |")

        ec = run.entity_counts
        if ec:
            entity_parts = []
            for key, label in [
                ("winery", "wineries"), ("appellation", "appellations"),
                ("grape", "grapes"), ("cellar", "cellars"), ("provider", "providers"),
            ]:
                if key in ec:
                    entity_parts.append(f"{ec[key]} {label}")
            if entity_parts:
                _a(f"| Entities built | {', '.join(entity_parts)} |")

            for key, label in [
                ("wine", "Wines"), ("wine_grape", "Wine-grapes"),
                ("tasting", "Tastings"), ("pro_rating", "Pro ratings"),
                ("tracked_wine", "Tracked wines"),
            ]:
                if key in ec:
                    _a(f"| {label} | {ec[key]} |")

            if "bottle" in ec:
                b_str = f"{ec['bottle']}"
                if "bottle_stored" in ec:
                    b_str += f" ({ec['bottle_stored']} stored, {ec.get('bottle_gone', 0)} gone)"
                _a(f"| Bottles | {b_str} |")

        cs = run.change_summary
        if cs:
            _a(f"| Inserts | {cs.get('inserts', 0)} |")
            _a(f"| Updates | {cs.get('updates', 0)} |")
            _a(f"| Deletes | {cs.get('deletes', 0)} |")
            _a(f"| Renames | {cs.get('renames', 0)} |")

        _a(f"| Validation | {run.validation_passed} passed, {run.validation_failed} failed |")

        dossier_str = str(run.dossier_count)
        if run.companion_count:
            dossier_str += f" wine + {run.companion_count} companion"
        _a(f"| Dossiers generated | {dossier_str} |")

        if run.warnings:
            _a(f"| Warnings | {len(run.warnings)}: {'; '.join(run.warnings[:3])} |")
        else:
            _a(f"| Warnings | None |")
        _a(f"| Errors | {'; '.join(run.errors) if run.errors else 'None'} |")
        _a("")

    # --- Output Verification ---
    _a("## Output Verification\n")
    _a("| # | Check | Result | Details |")
    _a("|---|-------|--------|---------|")
    for i, check in enumerate(output_checks, start=1):
        result = "PASS" if check.passed else "FAIL"
        _a(f"| 3.{i} | {check.name} | {result} | {check.details} |")
    _a("")

    # --- Entity Row Counts (from check_entity_counts data) ---
    entity_check = next((c for c in output_checks if c.data and "wine" in c.data), None)
    if entity_check and entity_check.data:
        _a("### Entity Row Counts\n")
        _a("| Entity | Rows |")
        _a("|--------|------|")
        for name in [
            "winery", "appellation", "grape", "cellar", "provider",
            "tracked_wine", "wine", "wine_grape", "bottle",
            "tasting", "pro_rating", "etl_run", "change_log",
        ]:
            if name in entity_check.data:
                _a(f"| {name} | {entity_check.data[name]} |")
        _a("")

    # --- Cross-run Consistency ---
    if cross_checks:
        _a("## Cross-run Consistency\n")
        _a("| Check | Result | Details |")
        _a("|-------|--------|---------|")
        for check in cross_checks:
            result = "PASS" if check.passed else "FAIL"
            _a(f"| {check.name} | {result} | {check.details} |")
        _a("")

    # --- Data Integrity ---
    if integrity_checks:
        # Split FK vs dossier checks for separate tables
        fk_list = [c for c in integrity_checks if c.name.startswith("FK ")]
        dossier_list = [c for c in integrity_checks if not c.name.startswith("FK ")]

        if fk_list:
            _a("## Data Integrity — FK Constraints\n")
            _a("| Constraint | Result | Details |")
            _a("|-----------|--------|---------|")
            for check in fk_list:
                result = "PASS" if check.passed else "FAIL"
                _a(f"| {check.name} | {result} | {check.details} |")
            _a("")

        if dossier_list:
            _a("## Data Integrity — Dossier Linkage\n")
            _a("| Check | Result | Details |")
            _a("|-------|--------|---------|")
            for check in dossier_list:
                result = "PASS" if check.passed else "FAIL"
                _a(f"| {check.name} | {result} | {check.details} |")
            _a("")

    # --- MCP Integration ---
    if mcp_checks:
        tool_list = [c for c in mcp_checks if "tool:" in c.name]
        res_list = [c for c in mcp_checks if "resource:" in c.name]
        other_list = [c for c in mcp_checks if "tool:" not in c.name and "resource:" not in c.name]

        _a("## MCP Server Integration\n")

        if other_list:
            for check in other_list:
                result = "PASS" if check.passed else "FAIL"
                _a(f"**{check.name}:** {result} — {check.details}\n")

        if tool_list:
            _a("### Tool Checks\n")
            _a("| Tool | Result | Details |")
            _a("|------|--------|---------|")
            for check in tool_list:
                result = "PASS" if check.passed else "FAIL"
                _a(f"| {check.name} | {result} | {check.details} |")
            _a("")

        if res_list:
            _a("### Resource Checks\n")
            _a("| Resource | Result | Details |")
            _a("|----------|--------|---------|")
            for check in res_list:
                result = "PASS" if check.passed else "FAIL"
                _a(f"| {check.name} | {result} | {check.details} |")
            _a("")

    return "\n".join(lines)


def write_report(content: str, report_dir: Path) -> Path:
    """Write *content* to a timestamped file in *report_dir*. Returns the path."""
    report_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    filename = now.strftime("%Y-%m-%d-%H%M%S") + ".md"
    path = report_dir / filename
    path.write_text(content, encoding="utf-8")
    return path
