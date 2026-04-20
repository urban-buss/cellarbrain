"""Reusable helpers for ETL smoke testing.

Provides structured dataclasses and composable modules for discovering
raw CSV folders, running the ETL pipeline, verifying output, and
generating Markdown reports.  No hardcoded data — all paths and
configuration come from caller arguments.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class SmokeConfig:
    """Top-level configuration for a smoke-test run."""

    raw_dir: Path
    output_dir: Path
    folders: list[str]          # ordered; first = full load
    python_version: str
    cellarbrain_version: str


@dataclass
class RunResult:
    """Structured metrics captured from a single ETL run."""

    folder: str
    sync_mode: bool
    exit_ok: bool
    csv_counts: dict[str, int] = field(default_factory=dict)
    slug_matching: dict[str, int] = field(default_factory=dict)
    entity_counts: dict[str, int] = field(default_factory=dict)
    change_summary: dict[str, int] = field(default_factory=dict)
    validation_passed: int = 0
    validation_failed: int = 0
    dossier_count: int = 0
    companion_count: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class CheckResult:
    """Result of a single verification check."""

    name: str
    passed: bool
    details: str
    data: dict | None = None
