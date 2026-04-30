"""Logging configuration for the cellarbrain CLI and MCP server."""

from __future__ import annotations

import json
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .settings import LoggingConfig


class JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects."""

    def __init__(self, session_id: str = "") -> None:
        super().__init__()
        self._session_id = session_id

    def format(self, record: logging.LogRecord) -> str:
        obj = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "session_id": self._session_id,
        }
        if record.exc_info and record.exc_info[1] is not None:
            obj["exception"] = self.formatException(record.exc_info)
        # Forward extra fields set by callers (e.g. turn_id)
        for key in ("turn_id", "event_type", "tool_name", "duration_ms"):
            val = getattr(record, key, None)
            if val is not None:
                obj[key] = val
        return json.dumps(obj, default=str)


def setup_logging(
    config: LoggingConfig,
    *,
    level_override: str | None = None,
    log_file_override: str | None = None,
    quiet_stderr: bool = False,
    session_id: str = "",
) -> None:
    """Configure the root logger for the process.

    Args:
        config: LoggingConfig from settings.
        level_override: CLI flag override (e.g. "DEBUG" from -vv).
        log_file_override: CLI --log-file override.
        quiet_stderr: If True, stderr handler stays at WARNING regardless
            of root level.  Used by the MCP server to avoid polluting stdio.
        session_id: Observability session ID embedded in JSON log lines.
    """
    root = logging.getLogger()
    root.handlers.clear()

    level = getattr(logging, (level_override or config.level).upper(), logging.WARNING)
    root.setLevel(level)

    json_fmt = config.format == "json"
    # For stderr, always use a text formatter (even in json mode)
    text_format = "%(asctime)s %(levelname)-8s %(name)s — %(message)s" if json_fmt else config.format
    formatter = logging.Formatter(text_format, datefmt=config.date_format)

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
        if json_fmt:
            file_handler.setFormatter(JsonFormatter(session_id=session_id))
        else:
            file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        root.addHandler(file_handler)

    # --- suppress noisy third-party loggers ---
    for name in ("httpx", "httpcore", "duckdb", "mcp", "anyio"):
        logging.getLogger(name).setLevel(logging.WARNING)
