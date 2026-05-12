"""Installation and configuration diagnostics for cellarbrain."""

from __future__ import annotations

import importlib.metadata
import importlib.util
import json
import os
import pathlib
import platform
import shutil
import sys
from dataclasses import asdict, dataclass

from .settings import Settings
from .skills import SKILL_NAMES

# MCP surface counts — kept as constants to avoid importing the heavy
# mcp_server module just for diagnostics.
MCP_TOOL_COUNT = 28
MCP_RESOURCE_COUNT = 10
MCP_DEFAULT_PORT = 8080

# Extras and the importable module names used to probe them.
_EXTRAS: dict[str, list[str]] = {
    "ml": ["sentence_transformers", "faiss"],
}

# Display-friendly package names used for version lookups via
# importlib.metadata (distribution names, not module names).
_EXTRA_DIST_NAMES: dict[str, list[str]] = {
    "ml": ["sentence-transformers", "faiss-cpu"],
}

_CORE_PACKAGES = [
    "pyarrow",
    "duckdb",
    "pandas",
    "tabulate",
    "mcp",
    "httpx",
    "starlette",
    "uvicorn",
    "jinja2",
    "imapclient",
    "keyring",
    "beautifulsoup4",
    "lxml",
    "jellyfish",
]


@dataclass(frozen=True)
class ExtraStatus:
    installed: bool
    packages: dict[str, str]


@dataclass(frozen=True)
class DataSummary:
    last_etl: str | None
    wines: int
    bottles_stored: int
    bottles_consumed: int


@dataclass(frozen=True)
class InfoReport:
    version: str
    python_version: str
    python_impl: str
    python_executable: str
    platform_system: str
    platform_machine: str
    install_type: str
    install_location: str
    entry_point: str | None
    config_file: str | None
    config_resolution: str
    env_config: str | None
    env_data_dir: str | None
    data_dir: str
    raw_dir: str
    wines_dir: str
    backup_dir: str
    log_db: str | None
    sommelier_model_dir: str
    parquet_file_count: int
    core_packages: dict[str, str]
    extras: dict[str, ExtraStatus]
    mcp_tools: int
    mcp_resources: int
    mcp_default_port: int
    dashboard_port: int
    mcp_config_snippet: dict
    skills_bundled: int
    skills_installed_count: int
    skills_target: str
    data_summary: DataSummary | None


# ---------------------------------------------------------------------------
# Collection helpers
# ---------------------------------------------------------------------------


def _get_version(name: str) -> str:
    """Return the installed version of a distribution, or '?' if missing."""
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "?"


def _detect_install_type() -> tuple[str, str]:
    """Detect whether the package is installed in editable mode.

    Returns:
        (install_type, install_location) tuple.
    """
    try:
        dist = importlib.metadata.distribution("cellarbrain")
    except importlib.metadata.PackageNotFoundError:
        return ("unknown", "unknown")

    location = str(dist._path.parent) if hasattr(dist, "_path") else "unknown"

    raw = dist.read_text("direct_url.json")
    if raw:
        try:
            data = json.loads(raw)
            if data.get("dir_info", {}).get("editable", False):
                url = data.get("url", "")
                if url.startswith("file://"):
                    location = url[len("file:///") :]
                return ("editable", location)
        except (json.JSONDecodeError, AttributeError):
            pass

    return ("regular", location)


def _check_extras() -> dict[str, ExtraStatus]:
    """Probe each optional-dependency group and return install status."""
    result: dict[str, ExtraStatus] = {}
    for group, modules in _EXTRAS.items():
        installed = all(importlib.util.find_spec(m) is not None for m in modules)
        packages: dict[str, str] = {}
        if installed:
            for dist_name in _EXTRA_DIST_NAMES.get(group, []):
                packages[dist_name] = _get_version(dist_name)
        result[group] = ExtraStatus(installed=installed, packages=packages)
    return result


def _count_parquet_files(data_dir: pathlib.Path) -> int:
    """Count .parquet files in the data directory."""
    if not data_dir.is_dir():
        return 0
    return sum(1 for _ in data_dir.glob("*.parquet"))


def _get_data_summary(data_dir: pathlib.Path) -> DataSummary | None:
    """Read a quick data summary from Parquet files.

    Returns None if the data directory has no Parquet data.
    """
    etl_path = data_dir / "etl_run.parquet"
    wine_path = data_dir / "wine.parquet"
    bottle_path = data_dir / "bottle.parquet"

    if not wine_path.is_file():
        return None

    try:
        import pyarrow.parquet as pq

        last_etl: str | None = None
        if etl_path.is_file():
            etl_table = pq.read_table(etl_path, columns=["started_at"])
            starts = etl_table.column("started_at").to_pylist()
            if starts:
                last_etl = str(starts[-1])

        wine_table = pq.read_table(wine_path, columns=["wine_id"])
        wines = len(wine_table)

        bottles_stored = 0
        bottles_consumed = 0
        if bottle_path.is_file():
            bottle_table = pq.read_table(bottle_path, columns=["status"])
            statuses = bottle_table.column("status").to_pylist()
            bottles_stored = sum(1 for s in statuses if s == "stored")
            bottles_consumed = sum(1 for s in statuses if s == "consumed")

        return DataSummary(
            last_etl=last_etl,
            wines=wines,
            bottles_stored=bottles_stored,
            bottles_consumed=bottles_consumed,
        )
    except Exception:
        return None


def _get_skills_status(target_dir: pathlib.Path) -> int:
    """Count how many bundled skills are installed at the target."""
    if not target_dir.is_dir():
        return 0
    count = 0
    for name in SKILL_NAMES:
        if (target_dir / name / "SKILL.md").is_file():
            count += 1
    return count


def _build_mcp_config_snippet(
    settings: Settings,
    entry_point: str | None,
) -> dict:
    """Build a Claude Desktop / OpenClaw MCP config JSON snippet."""
    command = entry_point or "cellarbrain"
    args: list[str] = []
    # Always include -d with absolute data_dir so the config works
    # regardless of the MCP client's working directory.
    data_dir = str(pathlib.Path(settings.paths.data_dir).resolve())
    args.extend(["-d", data_dir])
    if settings.config_source:
        config_abs = str(pathlib.Path(settings.config_source).resolve())
        args.extend(["-c", config_abs])
    args.append("mcp")

    return {
        "mcpServers": {
            "cellarbrain": {
                "command": command,
                "args": args,
            },
        },
    }


def _config_resolution_label(settings: Settings) -> str:
    """Describe how the config file was resolved."""
    source = settings.config_source
    if source is None:
        return "built-in defaults (no config file found)"

    env = os.environ.get("CELLARBRAIN_CONFIG")
    if env and pathlib.Path(env).resolve() == pathlib.Path(source).resolve():
        return "CELLARBRAIN_CONFIG env var"

    name = pathlib.Path(source).name
    if name == "cellarbrain.local.toml":
        return "cellarbrain.local.toml (local override)"
    if name == "cellarbrain.toml":
        return "cellarbrain.toml (cwd fallback)"

    return f"--config {source}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def collect_info(settings: Settings) -> InfoReport:
    """Collect all installation and configuration diagnostics."""
    install_type, install_location = _detect_install_type()
    entry_point = shutil.which("cellarbrain")

    data_dir = pathlib.Path(settings.paths.data_dir).resolve()
    skills_target = pathlib.Path.home() / ".openclaw" / "skills" / "cellarbrain"

    log_db = settings.logging.log_db
    if log_db is None:
        candidate = data_dir / "logs" / "cellarbrain-logs.duckdb"
        log_db = str(candidate) if candidate.exists() else None

    return InfoReport(
        version=_get_version("cellarbrain"),
        python_version=platform.python_version(),
        python_impl=platform.python_implementation(),
        python_executable=sys.executable,
        platform_system=platform.system(),
        platform_machine=platform.machine(),
        install_type=install_type,
        install_location=install_location,
        entry_point=entry_point,
        config_file=settings.config_source,
        config_resolution=_config_resolution_label(settings),
        env_config=os.environ.get("CELLARBRAIN_CONFIG"),
        env_data_dir=os.environ.get("CELLARBRAIN_DATA_DIR"),
        data_dir=str(data_dir),
        raw_dir=str(pathlib.Path(settings.paths.raw_dir).resolve()),
        wines_dir=str(data_dir / settings.paths.wines_subdir),
        backup_dir=str(pathlib.Path(settings.backup.backup_dir).resolve()),
        log_db=log_db,
        sommelier_model_dir=settings.sommelier.model_dir,
        parquet_file_count=_count_parquet_files(data_dir),
        core_packages={p: _get_version(p) for p in _CORE_PACKAGES},
        extras=_check_extras(),
        mcp_tools=MCP_TOOL_COUNT,
        mcp_resources=MCP_RESOURCE_COUNT,
        mcp_default_port=MCP_DEFAULT_PORT,
        dashboard_port=settings.dashboard.port,
        mcp_config_snippet=_build_mcp_config_snippet(settings, entry_point),
        skills_bundled=len(SKILL_NAMES),
        skills_installed_count=_get_skills_status(skills_target),
        skills_target=str(skills_target),
        data_summary=_get_data_summary(data_dir),
    )


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------


def format_text(report: InfoReport) -> str:
    """Format the report as human-readable text with section headers."""
    lines: list[str] = []

    def _kv(key: str, value: str, indent: int = 0) -> None:
        prefix = " " * indent
        lines.append(f"{prefix}{key + ':':<18}{value}")

    def _section(title: str) -> None:
        lines.append("")
        lines.append(title)
        lines.append("-" * len(title))

    lines.append("Cellarbrain Info")
    lines.append("=" * 16)
    lines.append("")
    _kv("Version", report.version)
    _kv("Python", f"{report.python_version} ({report.python_impl})")
    _kv("Python path", report.python_executable)
    _kv("Platform", f"{report.platform_system} {report.platform_machine}")
    _kv("Install type", report.install_type)
    _kv("Install location", report.install_location)
    _kv("Entry point", report.entry_point or "(not found)")

    _section("Config")
    _kv("Config file", report.config_file or "(none)")
    _kv("Resolution", report.config_resolution, indent=2)
    _kv("CELLARBRAIN_CONFIG", report.env_config or "(not set)")
    _kv("CELLARBRAIN_DATA_DIR", report.env_data_dir or "(not set)")

    _section("Paths")
    _kv("Data directory", report.data_dir)
    _kv("Parquet files", f"{report.parquet_file_count} files", indent=2)
    _kv("Raw directory", report.raw_dir)
    _kv("Dossier directory", report.wines_dir)
    _kv("Backup directory", report.backup_dir)
    _kv("Log database", report.log_db or "(not found)")
    _kv("Sommelier model", report.sommelier_model_dir)

    _section("Modules")
    core_parts = [f"{k} {v}" for k, v in report.core_packages.items()]
    lines.append(f"{'Core:':<18}\u2713 {', '.join(core_parts)}")
    for group, status in report.extras.items():
        if status.installed:
            pkg_str = ", ".join(f"{k} {v}" for k, v in status.packages.items())
            lines.append(f"[{group}]:{' ' * max(1, 17 - len(group) - 3)}\u2713 {pkg_str}")
        else:
            lines.append(
                f"[{group}]:{' ' * max(1, 17 - len(group) - 3)}"
                f"\u2717 not installed  \u2192  pip install cellarbrain[{group}]"
            )

    _section("MCP Server")
    _kv("Command", "cellarbrain mcp")
    _kv("Transport", f"stdio (default), sse (--transport sse --port {report.mcp_default_port})")
    _kv("Tools", str(report.mcp_tools))
    _kv("Resources", str(report.mcp_resources))
    _kv("Dashboard port", str(report.dashboard_port))
    lines.append("")
    lines.append("Claude Desktop / OpenClaw config:")
    lines.append(json.dumps(report.mcp_config_snippet, indent=2))

    _section("Skills")
    skill_names_str = ", ".join(SKILL_NAMES[:3]) + ", ..."
    _kv("Bundled", f"{report.skills_bundled} skills ({skill_names_str})")
    _kv("Install target", report.skills_target)
    if report.skills_installed_count == report.skills_bundled:
        _kv("Installed", f"\u2713 ({report.skills_installed_count}/{report.skills_bundled} up to date)")
    elif report.skills_installed_count > 0:
        _kv("Installed", f"partial ({report.skills_installed_count}/{report.skills_bundled})")
    else:
        _kv("Installed", "\u2717 not installed  \u2192  cellarbrain install-skills")

    if report.data_summary:
        _section("Data Summary")
        ds = report.data_summary
        _kv("Last ETL run", ds.last_etl or "unknown")
        _kv("Wines", f"{ds.wines:,}")
        _kv("Bottles stored", f"{ds.bottles_stored:,}")
        _kv("Bottles consumed", f"{ds.bottles_consumed:,}")

    lines.append("")
    return "\n".join(lines)


def format_json(report: InfoReport) -> str:
    """Serialize the report as JSON."""
    data = asdict(report)
    # Convert ExtraStatus objects (already dicts via asdict)
    return json.dumps(data, indent=2, default=str)


def format_mcp_config(report: InfoReport) -> str:
    """Return just the MCP config snippet as JSON."""
    return json.dumps(report.mcp_config_snippet, indent=2)
