"""Discover raw CSV folders and gather environment info."""

from __future__ import annotations

import re
import sys
from pathlib import Path

from cellarbrain.settings import PathsConfig

_FOLDER_RE = re.compile(r"^\d{6}$")

_PATHS_DEFAULTS = PathsConfig()

_EXPECTED_CSVS = (
    _PATHS_DEFAULTS.wines_filename,
    _PATHS_DEFAULTS.bottles_filename,
    _PATHS_DEFAULTS.bottles_gone_filename,
)


def discover_raw_folders(raw_dir: Path) -> list[str]:
    """Return YYMMDD folder names under *raw_dir*, sorted chronologically.

    Only directories whose name is exactly 6 digits are included.
    """
    if not raw_dir.is_dir():
        return []
    return sorted(d.name for d in raw_dir.iterdir() if d.is_dir() and _FOLDER_RE.match(d.name))


def validate_folder(raw_dir: Path, folder: str) -> bool:
    """Return True if *folder* contains all 3 expected CSV files."""
    folder_path = raw_dir / folder
    return all((folder_path / csv).is_file() for csv in _EXPECTED_CSVS)


def csv_paths(raw_dir: Path, folder: str) -> tuple[str, str, str]:
    """Return (wines_csv, bottles_csv, bottles_gone_csv) paths as strings."""
    base = raw_dir / folder
    return (
        str(base / _PATHS_DEFAULTS.wines_filename),
        str(base / _PATHS_DEFAULTS.bottles_filename),
        str(base / _PATHS_DEFAULTS.bottles_gone_filename),
    )


def get_environment() -> dict[str, str]:
    """Return Python version and cellarbrain version."""
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

    cb_version = "unknown"
    try:
        from importlib.metadata import version

        cb_version = version("cellarbrain")
    except Exception:
        pass

    return {
        "python_version": python_version,
        "cellarbrain_version": cb_version,
    }
