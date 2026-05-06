"""Subprocess wrapper for invoking ``cellarbrain etl`` after file placement."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def run_etl(
    raw_dir: Path,
    output_dir: Path,
    config_path: Path | None = None,
    *,
    expected_files: tuple[str, ...] = (
        "export-wines.csv",
        "export-bottles-stored.csv",
        "export-bottles-gone.csv",
    ),
    timeout: int = 300,
) -> tuple[int, str]:
    """Run ``cellarbrain etl`` as a subprocess.

    Parameters
    ----------
    raw_dir:
        Directory containing the top-level CSV working set.
    output_dir:
        Target directory for Parquet + dossier output.
    config_path:
        Optional path to ``cellarbrain.toml``.
    expected_files:
        Filenames to pass to the ETL command (in positional order:
        wines, bottles-stored, bottles-gone).
    timeout:
        Seconds before the ETL subprocess is killed.

    Returns
    -------
    (exit_code, combined_output) — stdout + stderr from the subprocess.
    """
    wines, bottles, bottles_gone = expected_files

    cmd = [
        sys.executable,
        "-m",
        "cellarbrain",
        *(["--config", str(config_path)] if config_path else []),
        "etl",
        str(raw_dir / wines),
        str(raw_dir / bottles),
        str(raw_dir / bottles_gone),
        "-o",
        str(output_dir),
    ]

    logger.info("Running ETL: %s", " ".join(cmd))
    try:
        env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        output = result.stdout + result.stderr
        if result.returncode == 0:
            logger.info("ETL completed successfully (exit 0)")
        else:
            logger.error("ETL failed (exit %d): %s", result.returncode, output)
        return result.returncode, output
    except subprocess.TimeoutExpired:
        logger.error("ETL timed out after %d seconds", timeout)
        return -1, f"ETL timed out after {timeout} seconds"
