"""Tests for the service module — macOS launchd service management."""

from __future__ import annotations

import pathlib
from unittest.mock import MagicMock, patch

import pytest

from cellarbrain.service import (
    ALL_SERVICES,
    DEFAULT_SERVICES,
    INGEST_SERVICE,
    ServiceDef,
    _build_path,
    _program_arguments,
    _resolve_entry_point,
    generate_plist,
    get_log_paths,
    get_status,
    install_service,
    plist_to_xml,
    require_macos,
    uninstall_service,
)

# ---------------------------------------------------------------------------
# ServiceDef
# ---------------------------------------------------------------------------


class TestServiceDef:
    def test_ingest_service_label(self):
        assert INGEST_SERVICE.label == "com.cellarbrain.ingest"

    def test_plist_path_under_launch_agents(self):
        path = INGEST_SERVICE.plist_path
        assert path.name == "com.cellarbrain.ingest.plist"
        assert "LaunchAgents" in str(path)

    def test_all_services_contains_ingest(self):
        assert "ingest" in ALL_SERVICES

    def test_default_services_is_tuple(self):
        assert isinstance(DEFAULT_SERVICES, tuple)
        assert "ingest" in DEFAULT_SERVICES


# ---------------------------------------------------------------------------
# require_macos
# ---------------------------------------------------------------------------


class TestRequireMacOS:
    @patch("cellarbrain.service.platform.system", return_value="Windows")
    def test_raises_on_windows(self, _mock):
        with pytest.raises(SystemExit, match="only supported on macOS"):
            require_macos()

    @patch("cellarbrain.service.platform.system", return_value="Linux")
    def test_raises_on_linux(self, _mock):
        with pytest.raises(SystemExit, match="only supported on macOS"):
            require_macos()

    @patch("cellarbrain.service.platform.system", return_value="Darwin")
    def test_passes_on_macos(self, _mock):
        require_macos()  # should not raise


# ---------------------------------------------------------------------------
# _build_path
# ---------------------------------------------------------------------------


class TestBuildPath:
    def test_prepends_venv_bin(self):
        result = _build_path("/Users/me/.venv/bin/cellarbrain")
        assert result.startswith("/Users/me/.venv/bin:")
        assert "/usr/local/bin" in result

    def test_does_not_duplicate_system_dir(self):
        result = _build_path("/usr/local/bin/cellarbrain")
        assert result == "/usr/local/bin:/usr/bin:/bin"

    def test_does_not_duplicate_usr_bin(self):
        result = _build_path("/usr/bin/cellarbrain")
        assert result == "/usr/local/bin:/usr/bin:/bin"


# ---------------------------------------------------------------------------
# _program_arguments
# ---------------------------------------------------------------------------


class TestProgramArguments:
    def test_with_config(self):
        args = _program_arguments("/venv/bin/cellarbrain", "/path/to/config.toml", INGEST_SERVICE)
        assert args[0] == "/venv/bin/cellarbrain"
        assert "--config" in args
        assert "ingest" in args

    def test_without_config(self):
        args = _program_arguments("/venv/bin/cellarbrain", None, INGEST_SERVICE)
        assert "--config" not in args
        assert args == ["/venv/bin/cellarbrain", "ingest"]

    def test_python_fallback(self):
        args = _program_arguments("/usr/bin/python3", None, INGEST_SERVICE)
        assert args[:3] == ["/usr/bin/python3", "-m", "cellarbrain"]
        assert "ingest" in args


# ---------------------------------------------------------------------------
# _resolve_entry_point
# ---------------------------------------------------------------------------


class TestResolveEntryPoint:
    @patch("cellarbrain.service.shutil.which", return_value="/venv/bin/cellarbrain")
    def test_uses_which(self, _mock):
        result = _resolve_entry_point()
        assert "cellarbrain" in result

    @patch("cellarbrain.service.shutil.which", return_value=None)
    def test_falls_back_to_sys_executable(self, _mock):
        result = _resolve_entry_point()
        assert "python" in result.lower() or "cellarbrain" in result.lower()


# ---------------------------------------------------------------------------
# generate_plist
# ---------------------------------------------------------------------------


class TestGeneratePlist:
    def test_has_required_keys(self):
        plist = generate_plist(
            entry_point="/venv/bin/cellarbrain",
            config_path="/path/config.toml",
            working_dir="/data",
            service=INGEST_SERVICE,
        )
        assert plist["Label"] == "com.cellarbrain.ingest"
        assert plist["RunAtLoad"] is True
        assert plist["KeepAlive"] is True
        assert plist["ThrottleInterval"] == 30
        assert "PYTHONUNBUFFERED" in plist["EnvironmentVariables"]

    def test_program_arguments(self):
        plist = generate_plist(
            entry_point="/venv/bin/cellarbrain",
            config_path="/path/config.toml",
            working_dir="/data",
            service=INGEST_SERVICE,
        )
        args = plist["ProgramArguments"]
        assert args[0] == "/venv/bin/cellarbrain"
        assert "--config" in args
        assert "ingest" in args

    def test_working_directory(self):
        plist = generate_plist(
            entry_point="/venv/bin/cellarbrain",
            config_path=None,
            working_dir="/my/data",
            service=INGEST_SERVICE,
        )
        assert plist["WorkingDirectory"] == "/my/data"

    def test_no_config(self):
        plist = generate_plist(
            entry_point="/venv/bin/cellarbrain",
            config_path=None,
            working_dir="/data",
            service=INGEST_SERVICE,
        )
        assert "--config" not in plist["ProgramArguments"]

    def test_log_paths_under_library_logs(self):
        plist = generate_plist(
            entry_point="/venv/bin/cellarbrain",
            config_path=None,
            working_dir="/data",
            service=INGEST_SERVICE,
        )
        assert "Logs" in plist["StandardOutPath"]
        assert "cellarbrain" in plist["StandardOutPath"]
        assert "Logs" in plist["StandardErrorPath"]
        assert "cellarbrain" in plist["StandardErrorPath"]

    def test_path_includes_venv(self):
        plist = generate_plist(
            entry_point="/Users/me/.venv/bin/cellarbrain",
            config_path=None,
            working_dir="/data",
            service=INGEST_SERVICE,
        )
        assert "/Users/me/.venv/bin" in plist["EnvironmentVariables"]["PATH"]


# ---------------------------------------------------------------------------
# plist_to_xml
# ---------------------------------------------------------------------------


class TestPlistToXml:
    def test_returns_bytes(self):
        plist = {"Label": "test", "RunAtLoad": True}
        result = plist_to_xml(plist)
        assert isinstance(result, bytes)
        assert b"<plist" in result
        assert b"test" in result


# ---------------------------------------------------------------------------
# install_service
# ---------------------------------------------------------------------------


class TestInstallService:
    def test_raises_if_exists_without_force(self, tmp_path):
        svc = ServiceDef(
            label="com.test",
            command_args=("ingest",),
            stdout_log="out.log",
            stderr_log="err.log",
        )
        plist_path = tmp_path / "com.test.plist"
        plist_path.write_text("existing")

        plist = {"Label": "com.test", "RunAtLoad": True}

        with patch.object(type(svc), "plist_path", new_callable=lambda: property(lambda self: plist_path)):
            with pytest.raises(FileExistsError, match="already exists"):
                install_service(plist, service=svc, force=False)

    @patch("cellarbrain.service.subprocess.run")
    def test_writes_plist_and_loads(self, mock_run, tmp_path):
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        plist_path = tmp_path / "LaunchAgents" / "com.test.plist"

        svc = ServiceDef(
            label="com.test",
            command_args=("ingest",),
            stdout_log="out.log",
            stderr_log="err.log",
        )
        plist = {"Label": "com.test", "RunAtLoad": True}

        with (
            patch.object(type(svc), "plist_path", new_callable=lambda: property(lambda self: plist_path)),
            patch("cellarbrain.service.LAUNCH_AGENTS_DIR", tmp_path / "LaunchAgents"),
            patch("cellarbrain.service.LOG_DIR", tmp_path / "Logs"),
        ):
            msg = install_service(plist, service=svc, force=False, start=True)

        assert plist_path.exists()
        assert "Plist written" in msg
        assert "loaded" in msg.lower()

    @patch("cellarbrain.service.subprocess.run")
    def test_no_start_skips_load(self, mock_run, tmp_path):
        plist_path = tmp_path / "LaunchAgents" / "com.test.plist"

        svc = ServiceDef(
            label="com.test",
            command_args=("ingest",),
            stdout_log="out.log",
            stderr_log="err.log",
        )
        plist = {"Label": "com.test", "RunAtLoad": True}

        with (
            patch.object(type(svc), "plist_path", new_callable=lambda: property(lambda self: plist_path)),
            patch("cellarbrain.service.LAUNCH_AGENTS_DIR", tmp_path / "LaunchAgents"),
            patch("cellarbrain.service.LOG_DIR", tmp_path / "Logs"),
        ):
            msg = install_service(plist, service=svc, force=False, start=False)

        assert plist_path.exists()
        assert "not loaded" in msg.lower()
        mock_run.assert_not_called()

    @patch("cellarbrain.service.subprocess.run")
    def test_force_overwrites(self, mock_run, tmp_path):
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        plist_path = tmp_path / "LaunchAgents" / "com.test.plist"
        plist_path.parent.mkdir(parents=True)
        plist_path.write_text("old")

        svc = ServiceDef(
            label="com.test",
            command_args=("ingest",),
            stdout_log="out.log",
            stderr_log="err.log",
        )
        plist = {"Label": "com.test", "RunAtLoad": True}

        with (
            patch.object(type(svc), "plist_path", new_callable=lambda: property(lambda self: plist_path)),
            patch("cellarbrain.service.LAUNCH_AGENTS_DIR", tmp_path / "LaunchAgents"),
            patch("cellarbrain.service.LOG_DIR", tmp_path / "Logs"),
        ):
            msg = install_service(plist, service=svc, force=True, start=True)

        assert plist_path.exists()
        assert plist_path.read_text() != "old"
        assert "Plist written" in msg


# ---------------------------------------------------------------------------
# uninstall_service
# ---------------------------------------------------------------------------


class TestUninstallService:
    @patch("cellarbrain.service.subprocess.run")
    def test_unloads_and_removes(self, mock_run, tmp_path):
        mock_run.return_value = MagicMock(returncode=0)
        plist_path = tmp_path / "com.test.plist"
        plist_path.write_text("plist content")

        svc = ServiceDef(
            label="com.test",
            command_args=("ingest",),
            stdout_log="out.log",
            stderr_log="err.log",
        )

        with patch.object(type(svc), "plist_path", new_callable=lambda: property(lambda self: plist_path)):
            msg = uninstall_service(service=svc)

        assert not plist_path.exists()
        assert "unloaded" in msg.lower()
        assert "removed" in msg.lower()

    def test_handles_missing_plist(self, tmp_path):
        plist_path = tmp_path / "com.test.plist"
        svc = ServiceDef(
            label="com.test",
            command_args=("ingest",),
            stdout_log="out.log",
            stderr_log="err.log",
        )

        with patch.object(type(svc), "plist_path", new_callable=lambda: property(lambda self: plist_path)):
            msg = uninstall_service(service=svc)

        assert "nothing to uninstall" in msg.lower()


# ---------------------------------------------------------------------------
# get_status
# ---------------------------------------------------------------------------


class TestGetStatus:
    def test_not_installed(self, tmp_path):
        plist_path = tmp_path / "com.test.plist"
        svc = ServiceDef(
            label="com.test",
            command_args=("ingest",),
            stdout_log="out.log",
            stderr_log="err.log",
        )

        with patch.object(type(svc), "plist_path", new_callable=lambda: property(lambda self: plist_path)):
            result = get_status(service=svc)

        assert "not installed" in result.lower()

    @patch("cellarbrain.service.subprocess.run")
    def test_loaded_and_running(self, mock_run, tmp_path):
        import plistlib

        plist_path = tmp_path / "com.test.plist"
        plist_data = {
            "Label": "com.test",
            "ProgramArguments": ["/venv/bin/cellarbrain", "ingest"],
        }
        plist_path.write_bytes(plistlib.dumps(plist_data, fmt=plistlib.FMT_XML))

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="12345\t0\tcom.test\n",
        )
        svc = ServiceDef(
            label="com.test",
            command_args=("ingest",),
            stdout_log="out.log",
            stderr_log="err.log",
        )

        with (
            patch.object(type(svc), "plist_path", new_callable=lambda: property(lambda self: plist_path)),
            patch("cellarbrain.service._resolve_entry_point", return_value="/venv/bin/cellarbrain"),
        ):
            result = get_status(service=svc)

        assert "12345" in result
        assert "yes" in result.lower()

    @patch("cellarbrain.service.subprocess.run")
    def test_not_loaded(self, mock_run, tmp_path):
        import plistlib

        plist_path = tmp_path / "com.test.plist"
        plist_data = {"Label": "com.test", "ProgramArguments": ["/venv/bin/cellarbrain"]}
        plist_path.write_bytes(plistlib.dumps(plist_data, fmt=plistlib.FMT_XML))

        mock_run.return_value = MagicMock(returncode=0, stdout="")
        svc = ServiceDef(
            label="com.test",
            command_args=("ingest",),
            stdout_log="out.log",
            stderr_log="err.log",
        )

        with patch.object(type(svc), "plist_path", new_callable=lambda: property(lambda self: plist_path)):
            result = get_status(service=svc)

        assert "not loaded" in result.lower()

    @patch("cellarbrain.service.subprocess.run")
    def test_stale_entry_point_warning(self, mock_run, tmp_path):
        import plistlib

        plist_path = tmp_path / "com.test.plist"
        plist_data = {
            "Label": "com.test",
            "ProgramArguments": ["/old/path/cellarbrain", "ingest"],
        }
        plist_path.write_bytes(plistlib.dumps(plist_data, fmt=plistlib.FMT_XML))

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="999\t0\tcom.test\n",
        )
        svc = ServiceDef(
            label="com.test",
            command_args=("ingest",),
            stdout_log="out.log",
            stderr_log="err.log",
        )

        with (
            patch.object(type(svc), "plist_path", new_callable=lambda: property(lambda self: plist_path)),
            patch("cellarbrain.service._resolve_entry_point", return_value="/new/path/cellarbrain"),
        ):
            result = get_status(service=svc)

        assert "warning" in result.lower()
        assert "entry point has changed" in result.lower()


# ---------------------------------------------------------------------------
# get_log_paths
# ---------------------------------------------------------------------------


class TestGetLogPaths:
    def test_returns_two_paths(self):
        stdout, stderr = get_log_paths(service=INGEST_SERVICE)
        assert isinstance(stdout, pathlib.Path)
        assert isinstance(stderr, pathlib.Path)
        assert "stdout" in stdout.name
        assert "stderr" in stderr.name

    def test_log_dir_is_library_logs(self):
        stdout, stderr = get_log_paths(service=INGEST_SERVICE)
        assert "Logs" in str(stdout)
        assert "cellarbrain" in str(stdout)
        assert "Logs" in str(stderr)
        assert "cellarbrain" in str(stderr)
