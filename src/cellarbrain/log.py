"""Logging configuration for the cellarbrain CLI and MCP server."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .settings import LoggingConfig


def setup_logging(
    config: LoggingConfig,
    *,
    level_override: str | None = None,
    log_file_override: str | None = None,
    quiet_stderr: bool = False,
) -> None:
    """Configure the root logger for the process.

    Args:
        config: LoggingConfig from settings.
        level_override: CLI flag override (e.g. "DEBUG" from -vv).
        log_file_override: CLI --log-file override.
        quiet_stderr: If True, stderr handler stays at WARNING regardless
            of root level.  Used by the MCP server to avoid polluting stdio.
    """
    root = logging.getLogger()
    root.handlers.clear()

    level = getattr(logging, (level_override or config.level).upper(), logging.WARNING)
    root.setLevel(level)

    formatter = logging.Formatter(config.format, datefmt=config.date_format)

    # --- stderr handler ---
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)
    stderr_handler.setLevel(logging.WARNING if quiet_stderr else level)
    root.addHandler(stderr_handler)

    # --- optional rotating file handler ---
    log_file = log_file_override or config.log_file
    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            path,
            maxBytes=config.max_bytes,
            backupCount=config.backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        root.addHandler(file_handler)

    # --- suppress noisy third-party loggers ---
    for name in ("httpx", "httpcore", "duckdb", "mcp", "anyio"):
        logging.getLogger(name).setLevel(logging.WARNING)
