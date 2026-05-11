"""macOS launchd service management for cellarbrain daemons.

Generates launchd plist files from the running environment and manages
service lifecycle via ``launchctl``.  Only supported on macOS (Darwin).
"""

from __future__ import annotations

import pathlib
import platform
import plistlib
import shutil
import subprocess
import sys
from dataclasses import dataclass

LAUNCH_AGENTS_DIR = pathlib.Path.home() / "Library" / "LaunchAgents"
LOG_DIR = pathlib.Path.home() / "Library" / "Logs" / "cellarbrain"


# ---------------------------------------------------------------------------
# Service definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ServiceDef:
    """Describes a cellarbrain daemon that can be managed as a launchd service."""

    label: str
    command_args: tuple[str, ...]
    stdout_log: str
    stderr_log: str

    @property
    def plist_path(self) -> pathlib.Path:
        return LAUNCH_AGENTS_DIR / f"{self.label}.plist"


INGEST_SERVICE = ServiceDef(
    label="com.cellarbrain.ingest",
    command_args=("ingest",),
    stdout_log="ingest-stdout.log",
    stderr_log="ingest-stderr.log",
)

DASHBOARD_SERVICE = ServiceDef(
    label="com.cellarbrain.dashboard",
    command_args=("dashboard",),
    stdout_log="dashboard-stdout.log",
    stderr_log="dashboard-stderr.log",
)

ALL_SERVICES: dict[str, ServiceDef] = {
    "ingest": INGEST_SERVICE,
    "dashboard": DASHBOARD_SERVICE,
}

DEFAULT_SERVICES = ("ingest",)


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def require_macos() -> None:
    """Raise ``SystemExit`` if the current platform is not macOS."""
    if platform.system() != "Darwin":
        raise SystemExit("Service management is only supported on macOS.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_entry_point() -> str:
    """Return the absolute path to the ``cellarbrain`` entry-point script."""
    ep = shutil.which("cellarbrain")
    if ep:
        return str(pathlib.Path(ep).resolve())
    return str(pathlib.Path(sys.executable).resolve())


def _build_path(entry_point: str) -> str:
    """Build a PATH string that includes the entry-point's directory."""
    # Use PurePosixPath since this code targets macOS only, but may be
    # tested on Windows where pathlib.Path would use backslashes.
    from pathlib import PurePosixPath

    ep_dir = str(PurePosixPath(entry_point).parent)
    base = "/usr/local/bin:/usr/bin:/bin"
    if ep_dir not in base.split(":"):
        return f"{ep_dir}:{base}"
    return base


def _program_arguments(
    entry_point: str,
    config_path: str | None,
    service: ServiceDef,
) -> list[str]:
    """Build the ``ProgramArguments`` list for a plist."""
    # When the entry point is a Python interpreter (no ``cellarbrain`` script
    # found), invoke as ``python -m cellarbrain``.
    if pathlib.Path(entry_point).name in ("python", "python3", "python.exe", "python3.exe"):
        args: list[str] = [entry_point, "-m", "cellarbrain"]
    else:
        args = [entry_point]

    if config_path:
        args.extend(["--config", str(pathlib.Path(config_path).resolve())])
    args.extend(service.command_args)
    return args


# ---------------------------------------------------------------------------
# Plist generation
# ---------------------------------------------------------------------------


def generate_plist(
    *,
    entry_point: str,
    config_path: str | None,
    working_dir: str,
    service: ServiceDef,
) -> dict:
    """Build a launchd plist dict from resolved runtime values.

    Returns a plain ``dict`` ready for ``plistlib.dumps()``.
    """
    return {
        "Label": service.label,
        "ProgramArguments": _program_arguments(entry_point, config_path, service),
        "RunAtLoad": True,
        "KeepAlive": True,
        "StandardOutPath": str(LOG_DIR / service.stdout_log),
        "StandardErrorPath": str(LOG_DIR / service.stderr_log),
        "WorkingDirectory": working_dir,
        "EnvironmentVariables": {
            "PATH": _build_path(entry_point),
            "PYTHONUNBUFFERED": "1",
        },
        "ThrottleInterval": 30,
    }


def plist_to_xml(plist: dict) -> bytes:
    """Serialise a plist dict to XML bytes."""
    return plistlib.dumps(plist, fmt=plistlib.FMT_XML)


# ---------------------------------------------------------------------------
# Service lifecycle
# ---------------------------------------------------------------------------


def install_service(
    plist: dict,
    *,
    service: ServiceDef,
    force: bool = False,
    start: bool = True,
) -> str:
    """Write the plist file and optionally load via ``launchctl``.

    Returns a human-readable status message.
    """
    plist_path = service.plist_path

    if plist_path.exists() and not force:
        raise FileExistsError(f"Plist already exists: {plist_path}\nUse --force to overwrite.")

    # Ensure directories exist
    LAUNCH_AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Unload first if replacing an existing service
    if plist_path.exists():
        subprocess.run(
            ["launchctl", "unload", str(plist_path)],
            capture_output=True,
        )

    plist_path.write_bytes(plist_to_xml(plist))

    lines = [f"Plist written: {plist_path}"]

    if start:
        result = subprocess.run(
            ["launchctl", "load", str(plist_path)],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            lines.append("Service loaded — will start at login and restart on exit.")
        else:
            stderr = result.stderr.strip()
            lines.append(f"Warning: launchctl load returned exit {result.returncode}")
            if stderr:
                lines.append(f"  {stderr}")
    else:
        lines.append("Plist written but not loaded (use `launchctl load` manually).")

    return "\n".join(lines)


def uninstall_service(*, service: ServiceDef) -> str:
    """Unload the service and remove the plist file.

    Returns a human-readable status message.
    """
    plist_path = service.plist_path
    lines: list[str] = []

    if plist_path.exists():
        result = subprocess.run(
            ["launchctl", "unload", str(plist_path)],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            lines.append("Service unloaded.")
        else:
            lines.append(f"launchctl unload exited {result.returncode} (may not have been loaded).")

        plist_path.unlink()
        lines.append(f"Plist removed: {plist_path}")
    else:
        lines.append(f"No plist found at {plist_path} — nothing to uninstall.")

    return "\n".join(lines)


def get_status(*, service: ServiceDef) -> str:
    """Query ``launchctl`` for service state.

    Returns a formatted multi-line string.
    """
    plist_path = service.plist_path
    lines: list[str] = [f"Service: {service.label}"]

    if not plist_path.exists():
        lines.append("Plist:   not installed")
        lines.append(f"         (expected at {plist_path})")
        return "\n".join(lines)

    lines.append(f"Plist:   {plist_path}")

    # Parse launchctl list output for our label
    result = subprocess.run(
        ["launchctl", "list"],
        capture_output=True,
        text=True,
    )
    pid: str | None = None
    exit_code: str | None = None
    loaded = False

    if result.returncode == 0:
        for line in result.stdout.splitlines():
            parts = line.split("\t")
            if len(parts) >= 3 and parts[2] == service.label:
                loaded = True
                pid = parts[0] if parts[0] != "-" else None
                exit_code = parts[1] if parts[1] != "-" else None
                break

    if loaded:
        lines.append("Loaded:  yes")
        if pid:
            lines.append(f"PID:     {pid}")
            lines.append("Running: yes")
        else:
            lines.append("Running: no")
        if exit_code and exit_code != "0":
            lines.append(f"Last exit code: {exit_code}")
    else:
        lines.append("Loaded:  no (plist exists but service not loaded)")
        lines.append("         Run `cellarbrain service install` to load.")

    # Stale entry-point check
    _check_stale_entry_point(plist_path, lines)

    return "\n".join(lines)


def _check_stale_entry_point(plist_path: pathlib.Path, lines: list[str]) -> None:
    """Warn if the plist's entry point differs from the current one."""
    try:
        with open(plist_path, "rb") as f:
            plist_data = plistlib.load(f)
        plist_ep = plist_data.get("ProgramArguments", [None])[0]
        current_ep = _resolve_entry_point()
        if plist_ep and plist_ep != current_ep:
            lines.append("")
            lines.append("Warning: entry point has changed since install!")
            lines.append(f"  Plist:   {plist_ep}")
            lines.append(f"  Current: {current_ep}")
            lines.append("  Run `cellarbrain service install --force` to update.")
    except Exception:
        pass


def get_log_paths(*, service: ServiceDef) -> tuple[pathlib.Path, pathlib.Path]:
    """Return ``(stdout_path, stderr_path)`` for the service."""
    return (
        LOG_DIR / service.stdout_log,
        LOG_DIR / service.stderr_log,
    )
