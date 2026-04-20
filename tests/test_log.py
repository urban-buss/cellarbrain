"""Tests for cellarbrain.log — logging configuration helper."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler

import pytest

from cellarbrain.log import setup_logging
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
        setup_logging(LoggingConfig(
            log_file=log_file, max_bytes=1024, backup_count=5,
        ))
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
