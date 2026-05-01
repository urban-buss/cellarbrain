"""Tests for cellarbrain.log — logging configuration helper."""

from __future__ import annotations

import json
import logging
import sys
from logging.handlers import RotatingFileHandler

import pytest

from cellarbrain.log import JsonFormatter, setup_logging
from cellarbrain.settings import LoggingConfig


@pytest.fixture(autouse=True)
def _clean_root_logger():
    """Restore root logger state after each test."""
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    yield
    root.handlers = original_handlers
    root.setLevel(original_level)


class TestSetupLogging:
    def test_default_config_adds_stderr_handler(self):
        setup_logging(LoggingConfig())
        root = logging.getLogger()
        assert len(root.handlers) == 1
        handler = root.handlers[0]
        assert isinstance(handler, logging.StreamHandler)
        assert handler.stream is sys.stderr

    def test_root_level_from_config(self):
        setup_logging(LoggingConfig(level="INFO"))
        assert logging.getLogger().level == logging.INFO

    def test_level_override(self):
        setup_logging(LoggingConfig(level="WARNING"), level_override="DEBUG")
        assert logging.getLogger().level == logging.DEBUG

    def test_quiet_stderr(self):
        setup_logging(
            LoggingConfig(level="DEBUG"),
            level_override="DEBUG",
            quiet_stderr=True,
        )
        root = logging.getLogger()
        assert root.level == logging.DEBUG
        stderr_handler = root.handlers[0]
        assert stderr_handler.level == logging.WARNING

    def test_file_handler_created(self, tmp_path):
        log_file = str(tmp_path / "test.log")
        setup_logging(LoggingConfig(), log_file_override=log_file)
        root = logging.getLogger()
        assert len(root.handlers) == 2
        file_handler = root.handlers[1]
        assert isinstance(file_handler, RotatingFileHandler)

    def test_file_handler_creates_parent_dirs(self, tmp_path):
        log_file = str(tmp_path / "sub" / "deep" / "test.log")
        setup_logging(LoggingConfig(), log_file_override=log_file)
        assert (tmp_path / "sub" / "deep").is_dir()

    def test_file_handler_from_config(self, tmp_path):
        log_file = str(tmp_path / "cfg.log")
        setup_logging(LoggingConfig(log_file=log_file))
        root = logging.getLogger()
        assert len(root.handlers) == 2
        assert isinstance(root.handlers[1], RotatingFileHandler)

    def test_file_handler_rotation_settings(self, tmp_path):
        log_file = str(tmp_path / "rot.log")
        setup_logging(
            LoggingConfig(
                log_file=log_file,
                max_bytes=1024,
                backup_count=5,
            )
        )
        file_handler = logging.getLogger().handlers[1]
        assert file_handler.maxBytes == 1024
        assert file_handler.backupCount == 5

    def test_cli_log_file_overrides_config(self, tmp_path):
        cfg_file = str(tmp_path / "cfg.log")
        cli_file = str(tmp_path / "cli.log")
        setup_logging(
            LoggingConfig(log_file=cfg_file),
            log_file_override=cli_file,
        )
        file_handler = logging.getLogger().handlers[1]
        assert "cli.log" in file_handler.baseFilename

    def test_third_party_suppressed(self):
        setup_logging(LoggingConfig(level="DEBUG"), level_override="DEBUG")
        for name in ("httpx", "httpcore", "duckdb", "mcp", "anyio"):
            assert logging.getLogger(name).level == logging.WARNING

    def test_clears_existing_handlers(self):
        root = logging.getLogger()
        root.addHandler(logging.StreamHandler())
        root.addHandler(logging.StreamHandler())
        assert len(root.handlers) >= 2
        setup_logging(LoggingConfig())
        assert len(root.handlers) == 1


class TestJsonFormatter:
    def test_produces_valid_json(self):
        fmt = JsonFormatter(session_id="abc123")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="hello %s",
            args=("world",),
            exc_info=None,
        )
        line = fmt.format(record)
        obj = json.loads(line)
        assert obj["message"] == "hello world"
        assert obj["level"] == "INFO"

    def test_includes_session_id(self):
        fmt = JsonFormatter(session_id="sess-42")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        obj = json.loads(fmt.format(record))
        assert obj["session_id"] == "sess-42"

    def test_includes_extra_fields(self):
        fmt = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        record.turn_id = "turn-1"
        record.duration_ms = 42.0
        obj = json.loads(fmt.format(record))
        assert obj["turn_id"] == "turn-1"
        assert obj["duration_ms"] == 42.0

    def test_setup_logging_json_format(self, tmp_path):
        log_file = str(tmp_path / "test.log")
        setup_logging(
            LoggingConfig(format="json", log_file=log_file),
            session_id="sess-json",
        )
        root = logging.getLogger()
        file_handler = root.handlers[1]
        assert isinstance(file_handler.formatter, JsonFormatter)
