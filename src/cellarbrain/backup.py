"""Backup and restore for the cellarbrain data directory.

Creates timestamped zip archives of Parquet files, dossiers, and custom
config before ETL runs. Provides retention pruning and restore from archive.
"""

from __future__ import annotations

import logging
import pathlib
import zipfile
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

# Directories excluded from backup by default (rebuildable / diagnostic).
_DEFAULT_EXCLUDE_DIRS = frozenset({"sommelier", "logs"})


def create_backup(
    data_dir: str | pathlib.Path,
    backup_dir: str | pathlib.Path,
    *,
    include_sommelier: bool = False,
    include_logs: bool = False,
    max_backups: int = 5,
) -> pathlib.Path:
    """Create a zip backup of the data directory.

    Returns the path to the created archive.
    Raises FileNotFoundError if data_dir does not exist.
    """
    data = pathlib.Path(data_dir)
    if not data.exists():
        raise FileNotFoundError(f"Data directory not found: {data}")

    bkp = pathlib.Path(backup_dir)
    bkp.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(UTC).strftime("%Y-%m-%dT%H%M%S")
    archive_name = f"cellarbrain-{ts}.zip"
    archive_path = bkp / archive_name

    exclude_dirs: set[str] = set()
    if not include_sommelier:
        exclude_dirs.add("sommelier")
    if not include_logs:
        exclude_dirs.add("logs")

    file_count = 0
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in sorted(data.rglob("*")):
            if file.is_dir():
                continue
            rel = file.relative_to(data)
            # Skip excluded directories
            if any(part in exclude_dirs for part in rel.parts):
                continue
            zf.write(file, arcname=str(rel))
            file_count += 1

    size_mb = archive_path.stat().st_size / (1024 * 1024)
    logger.info(
        "Backup created: %s (%d files, %.1f MB)",
        archive_path,
        file_count,
        size_mb,
    )

    # Prune old backups
    _prune_backups(bkp, max_backups)

    return archive_path


def _prune_backups(backup_dir: pathlib.Path, max_backups: int) -> list[pathlib.Path]:
    """Remove oldest backups exceeding max_backups. Returns removed paths."""
    archives = sorted(
        backup_dir.glob("cellarbrain-*.zip"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    removed: list[pathlib.Path] = []
    for old in archives[max_backups:]:
        old.unlink()
        removed.append(old)
        logger.info("Pruned old backup: %s", old.name)
    return removed


def list_backups(backup_dir: str | pathlib.Path) -> list[dict]:
    """List available backups with metadata.

    Returns list of dicts with keys: path, name, size_mb, file_count.
    Sorted newest-first.
    """
    bkp = pathlib.Path(backup_dir)
    if not bkp.exists():
        return []

    result: list[dict] = []
    for archive in sorted(bkp.glob("cellarbrain-*.zip"), reverse=True):
        with zipfile.ZipFile(archive, "r") as zf:
            file_count = len(zf.namelist())
        result.append(
            {
                "path": archive,
                "name": archive.name,
                "size_mb": round(archive.stat().st_size / (1024 * 1024), 1),
                "file_count": file_count,
            }
        )
    return result


def restore_backup(
    archive_path: str | pathlib.Path,
    data_dir: str | pathlib.Path,
    *,
    dry_run: bool = False,
) -> int:
    """Restore a backup archive to the data directory.

    Overwrites existing files. Does NOT delete files in data_dir that
    aren't in the archive (preserves sommelier indexes, logs, etc.).

    Returns the number of files restored.
    """
    archive = pathlib.Path(archive_path)
    data = pathlib.Path(data_dir)

    if not archive.exists():
        raise FileNotFoundError(f"Backup not found: {archive}")

    with zipfile.ZipFile(archive, "r") as zf:
        members = zf.namelist()
        if dry_run:
            return len(members)
        data.mkdir(parents=True, exist_ok=True)
        zf.extractall(data)

    logger.info("Restored %d files from %s", len(members), archive.name)
    return len(members)
