"""Run the ETL pipeline, pytest suite, and capture structured results."""

from __future__ import annotations

import contextlib
import io
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from . import RunResult
from .discover import csv_paths


def run_etl(
    raw_dir: Path,
    folder: str,
    output_dir: Path,
    *,
    sync: bool = False,
    settings: object | None = None,
) -> RunResult:
    """Execute one ETL run via ``cli.run()`` and return structured metrics.

    Captures stdout to parse the metrics that ``cli.run()`` prints.
    """
    from cellarbrain.cli import run as cli_run

    wines_csv, bottles_csv, gone_csv = csv_paths(raw_dir, folder)

    buf = io.StringIO()
    warnings_buf: list[str] = []
    errors_buf: list[str] = []

    try:
        with contextlib.redirect_stdout(buf):
            ok = cli_run(
                wines_csv,
                bottles_csv,
                str(output_dir),
                sync_mode=sync,
                bottles_gone_csv=gone_csv,
                settings=settings,
            )
    except Exception as exc:
        errors_buf.append(str(exc))
        ok = False

    stdout = buf.getvalue()

    result = RunResult(
        folder=folder,
        sync_mode=sync,
        exit_ok=ok,
    )

    # --- Parse captured stdout ---
    result.csv_counts = _parse_csv_counts(stdout)
    result.slug_matching = _parse_slug_matching(stdout)
    result.entity_counts = _parse_entity_counts(stdout)
    result.change_summary = _parse_change_summary(stdout)
    result.validation_passed, result.validation_failed = _parse_validation(stdout)
    result.dossier_count, result.companion_count = _parse_dossiers(stdout)

    # Duplicate natural key warnings come from stderr (via warnings module)
    # We don't capture stderr here; the agent can note them separately.
    result.warnings = warnings_buf
    result.errors = errors_buf

    return result


def clean_output(output_dir: Path) -> int:
    """Remove Parquet files and dossier directories from *output_dir*.

    Returns count of items removed (files + directories).
    """
    import shutil

    count = 0
    if output_dir.is_dir():
        for pq in output_dir.glob("*.parquet"):
            pq.unlink()
            count += 1
        wines_dir = output_dir / "wines"
        if wines_dir.is_dir():
            shutil.rmtree(wines_dir)
            count += 1
    return count


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Python executable resolution
# ---------------------------------------------------------------------------


def _get_venv_python() -> str:
    """Return the project venv Python if available, else sys.executable.

    When invoked via the Windows ``py`` launcher, ``sys.executable`` points to
    the system Python which may lack optional dependencies.  This helper
    resolves the project ``.venv`` to ensure subprocess calls use the correct
    environment.
    """
    project_root = Path(__file__).resolve().parents[2]
    for sub in ("Scripts", "bin"):
        candidate = project_root / ".venv" / sub / "python.exe"
        if candidate.exists():
            return str(candidate)
        candidate = candidate.with_suffix("")  # Unix: no .exe
        if candidate.exists():
            return str(candidate)
    return sys.executable


# ---------------------------------------------------------------------------
# Pytest runner
# ---------------------------------------------------------------------------


@dataclass
class PytestResult:
    """Structured result from running the full pytest suite."""

    passed: int = 0
    failed: int = 0
    errors: int = 0
    warnings: int = 0
    total: int = 0
    exit_code: int = 0
    output: str = ""

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


def run_pytest() -> PytestResult:
    """Run ``pytest --tb=short -q`` and parse the summary line."""
    python = _get_venv_python()
    proc = subprocess.run(
        [python, "-m", "pytest", "--tb=short", "-q"],
        capture_output=True,
        text=True,
        timeout=600,
    )
    result = PytestResult(exit_code=proc.returncode, output=proc.stdout + proc.stderr)

    # Parse the summary line, e.g. "817 passed, 2 warnings in 12.34s"
    # or "3 failed, 814 passed, 2 warnings in 15.67s"
    summary = re.search(
        r"=+ .* =+\s*\n(.*(?:passed|failed|error).*)\n",
        result.output,
    )
    if not summary:
        # Fallback for -q output without banner: last line with "passed"
        summary = re.search(r"(\d+ passed.*) in [\d.]+s", result.output)
    if summary:
        line = summary.group(1)
        m_fail = re.search(r"(\d+) failed", line)
        m_pass = re.search(r"(\d+) passed", line)
        m_warn = re.search(r"(\d+) warnings?", line)
        m_err = re.search(r"(\d+) errors?", line)
        result.failed = int(m_fail.group(1)) if m_fail else 0
        result.passed = int(m_pass.group(1)) if m_pass else 0
        result.warnings = int(m_warn.group(1)) if m_warn else 0
        result.errors = int(m_err.group(1)) if m_err else 0
    result.total = result.passed + result.failed + result.errors
    return result


# ---------------------------------------------------------------------------
# Server rebuild
# ---------------------------------------------------------------------------


@dataclass
class RebuildResult:
    """Result from rebuilding the server (pip install -e .)."""

    ok: bool
    exe_path: Path | None = None
    output: str = ""


def rebuild_server() -> RebuildResult:
    """Run ``pip install -e .`` and verify the CLI exe exists."""
    python = _get_venv_python()
    proc = subprocess.run(
        [python, "-m", "pip", "install", "-e", ".", "-q"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if proc.returncode != 0:
        return RebuildResult(ok=False, output=proc.stdout + proc.stderr)

    # Find the exe — Scripts/ on Windows, bin/ on Unix.
    # Check both sys.executable's directory and the project .venv,
    # since the Windows `py` launcher may resolve to the system Python.
    search_dirs = [Path(sys.executable).parent]
    project_venv = Path(__file__).resolve().parents[2] / ".venv"
    for sub in ("Scripts", "bin"):
        candidate = project_venv / sub
        if candidate.is_dir() and candidate not in search_dirs:
            search_dirs.append(candidate)

    for d in search_dirs:
        for name in ("cellarbrain.exe", "cellarbrain"):
            exe = d / name
            if exe.exists():
                return RebuildResult(ok=True, exe_path=exe, output=proc.stdout + proc.stderr)

    return RebuildResult(
        ok=False,
        output=f"pip install succeeded but cellarbrain exe not found in {search_dirs}",
    )


# ---------------------------------------------------------------------------
# Stdout parsers — extract metrics from cli.run() printed output
# ---------------------------------------------------------------------------


def _parse_csv_counts(text: str) -> dict[str, int]:
    """Extract CSV row counts from stdout."""
    counts: dict[str, int] = {}
    for m in re.finditer(r"→\s*(\d+)\s+(wine|bottle|gone bottle)\s+rows", text):
        key = {"wine": "wines", "bottle": "bottles", "gone bottle": "bottles_gone"}[m.group(2)]
        counts[key] = int(m.group(1))
    return counts


def _parse_slug_matching(text: str) -> dict[str, int]:
    """Extract slug matching summary."""
    m = re.search(
        r"Slug matching:\s*(\d+)\s+existing,\s*(\d+)\s+new,\s*(\d+)\s+deleted,"
        r"\s*(\d+)\s+revived,\s*(\d+)\s+renamed",
        text,
    )
    if not m:
        return {}
    return {
        "existing": int(m.group(1)),
        "new": int(m.group(2)),
        "deleted": int(m.group(3)),
        "revived": int(m.group(4)),
        "renamed": int(m.group(5)),
    }


def _parse_entity_counts(text: str) -> dict[str, int]:
    """Extract entity counts from the 'Building lookup/core entities' section."""
    counts: dict[str, int] = {}
    patterns = {
        "winery": r"Wineries:\s+(\d+)",
        "appellation": r"Appellations:\s+(\d+)",
        "grape": r"Grapes:\s+(\d+)",
        "cellar": r"Cellars:\s+(\d+)",
        "provider": r"Providers:\s+(\d+)",
        "wine": r"Wines:\s+(\d+)",
        "wine_grape": r"Wine-grapes:\s+(\d+)",
        "tasting": r"Tastings:\s+(\d+)",
        "pro_rating": r"Pro ratings:\s+(\d+)",
        "tracked_wine": r"Tracked wines:\s+(\d+)",
    }
    for key, pat in patterns.items():
        m = re.search(pat, text)
        if m:
            counts[key] = int(m.group(1))

    # Bottles: N (stored: S, gone: G)
    m = re.search(r"Bottles:\s+(\d+)\s+\(stored:\s+(\d+),\s+gone:\s+(\d+)\)", text)
    if m:
        counts["bottle"] = int(m.group(1))
        counts["bottle_stored"] = int(m.group(2))
        counts["bottle_gone"] = int(m.group(3))

    return counts


def _parse_change_summary(text: str) -> dict[str, int]:
    """Extract inserts/updates/deletes/renames."""
    m = re.search(
        r"Inserts:\s+(\d+)\s+Updates:\s+(\d+)\s+Deletes:\s+(\d+)\s+Renames:\s+(\d+)",
        text,
    )
    if not m:
        return {}
    return {
        "inserts": int(m.group(1)),
        "updates": int(m.group(2)),
        "deletes": int(m.group(3)),
        "renames": int(m.group(4)),
    }


def _parse_validation(text: str) -> tuple[int, int]:
    """Extract validation passed/failed counts."""
    m = re.search(r"(\d+)\s+passed,\s+(\d+)\s+failed", text)
    if not m:
        return 0, 0
    return int(m.group(1)), int(m.group(2))


def _parse_dossiers(text: str) -> tuple[int, int]:
    """Extract dossier and companion dossier counts."""
    wine_count = 0
    companion_count = 0

    m = re.search(r"(?:Generated|Regenerated)\s+(\d+)\s+wine\s+dossier", text)
    if m:
        wine_count = int(m.group(1))

    m = re.search(r"Generated\s+(\d+)\s+companion\s+dossier", text)
    if m:
        companion_count = int(m.group(1))

    return wine_count, companion_count
