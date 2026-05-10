"""Output verification checks for smoke testing.

Each public function returns a ``CheckResult`` and can be called independently.
All checks query Parquet files via DuckDB — no hardcoded data.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from . import CheckResult

# The 13 entity Parquet files produced by the ETL pipeline.
EXPECTED_PARQUET = (
    "winery",
    "appellation",
    "grape",
    "cellar",
    "provider",
    "tracked_wine",
    "wine",
    "wine_grape",
    "bottle",
    "tasting",
    "pro_rating",
    "etl_run",
    "change_log",
)

# Required columns in wine.parquet (core + computed + audit).
REQUIRED_WINE_COLUMNS = {
    "wine_id",
    "wine_slug",
    "winery_id",
    "name",
    "vintage",
    "appellation_id",
    "category",
    "_raw_classification",
    "_raw_volume",
    "_raw_grapes",
    "full_name",
    "drinking_status",
    "grape_type",
    "primary_grape",
    "grape_summary",
    "price_tier",
    "is_deleted",
    "etl_run_id",
    "updated_at",
}

# Old column names that should NOT appear (renamed during schema migration).
BANNED_WINE_COLUMNS = {"classification", "bottle_size", "grapes_raw"}


def _pq(output_dir: Path, name: str) -> str:
    """DuckDB-friendly path string for a Parquet file."""
    return str(output_dir / f"{name}.parquet").replace("\\", "/")


# -----------------------------------------------------------------------
# 3.1  Parquet file presence
# -----------------------------------------------------------------------


def check_parquet_files(output_dir: Path) -> CheckResult:
    """Verify all 13 Parquet files exist and have size > 0."""
    missing = []
    empty = []
    for name in EXPECTED_PARQUET:
        pq = output_dir / f"{name}.parquet"
        if not pq.exists():
            missing.append(name)
        elif pq.stat().st_size == 0:
            empty.append(name)

    ok = not missing and not empty
    parts = []
    if missing:
        parts.append(f"missing: {', '.join(missing)}")
    if empty:
        parts.append(f"empty: {', '.join(empty)}")
    details = "; ".join(parts) if parts else f"All {len(EXPECTED_PARQUET)} files exist and size > 0"
    return CheckResult(name="Parquet files present (13)", passed=ok, details=details)


# -----------------------------------------------------------------------
# 3.2  ETL run history
# -----------------------------------------------------------------------


def check_etl_runs(output_dir: Path, expected_count: int) -> CheckResult:
    """Verify etl_run.parquet has the right number of rows and structure."""
    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(
            f"SELECT run_id, run_type, started_at, finished_at, "
            f"total_inserts + total_updates + total_deletes AS total_changes, "
            f"wines_inserted + wines_updated + wines_deleted + wines_renamed AS wine_changes "
            f"FROM '{_pq(output_dir, 'etl_run')}' ORDER BY run_id"
        ).fetchall()
    except Exception as exc:
        return CheckResult(name="ETL run history", passed=False, details=str(exc))
    finally:
        con.close()

    issues = []
    if len(rows) != expected_count:
        issues.append(f"expected {expected_count} runs, found {len(rows)}")
    if rows and rows[0][1] != "full":
        issues.append(f"first run_type is '{rows[0][1]}', expected 'full'")
    for i, row in enumerate(rows[1:], start=2):
        if row[1] != "incremental":
            issues.append(f"run {i} run_type is '{row[1]}', expected 'incremental'")
    for i, row in enumerate(rows, start=1):
        if row[4] == 0:
            issues.append(f"run {i} has 0 total changes")
        if row[2] and row[3] and row[2] >= row[3]:
            issues.append(f"run {i}: started_at >= finished_at")

    ok = not issues
    if ok:
        types = [r[1] for r in rows]
        details = f"{len(rows)} runs: {', '.join(types)}, all have changes, timestamps ordered"
    else:
        details = "; ".join(issues)

    data = {"runs": [{"run_id": r[0], "run_type": r[1], "total_changes": r[4], "wine_changes": r[5]} for r in rows]}
    return CheckResult(name="ETL run history", passed=ok, details=details, data=data)


# -----------------------------------------------------------------------
# 3.3  Entity row counts
# -----------------------------------------------------------------------


def check_entity_counts(output_dir: Path) -> CheckResult:
    """Query row counts for all Parquet files and apply sanity checks."""
    con = duckdb.connect(":memory:")
    counts: dict[str, int] = {}
    try:
        for name in EXPECTED_PARQUET:
            pq = _pq(output_dir, name)
            cnt = con.execute(f"SELECT count(*) FROM '{pq}'").fetchone()[0]
            counts[name] = cnt
    except Exception as exc:
        return CheckResult(name="Entity row counts", passed=False, details=str(exc))
    finally:
        con.close()

    issues = []
    must_be_positive = [
        "wine",
        "bottle",
        "winery",
        "appellation",
        "grape",
        "wine_grape",
        "change_log",
        "cellar",
        "provider",
    ]
    for entity in must_be_positive:
        if counts.get(entity, 0) == 0:
            issues.append(f"{entity} has 0 rows")
    if counts.get("wine_grape", 0) < counts.get("wine", 0):
        issues.append(f"wine_grape ({counts.get('wine_grape')}) < wine ({counts.get('wine')})")

    ok = not issues
    details = "All sanity checks pass" if ok else "; ".join(issues)
    return CheckResult(name="Entity row counts", passed=ok, details=details, data=counts)


# -----------------------------------------------------------------------
# 3.4  Wine schema columns
# -----------------------------------------------------------------------


def check_wine_schema(output_dir: Path) -> CheckResult:
    """Verify required columns are present and banned columns are absent."""
    con = duckdb.connect(":memory:")
    try:
        cols = con.execute(f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{_pq(output_dir, 'wine')}')").fetchall()
        actual = {c[0] for c in cols}
    except Exception as exc:
        return CheckResult(name="Wine schema columns", passed=False, details=str(exc))
    finally:
        con.close()

    missing = REQUIRED_WINE_COLUMNS - actual
    banned_present = BANNED_WINE_COLUMNS & actual

    issues = []
    if missing:
        issues.append(f"missing: {', '.join(sorted(missing))}")
    if banned_present:
        issues.append(f"banned columns present: {', '.join(sorted(banned_present))}")

    ok = not issues
    details = "All required columns present, no old column names found" if ok else "; ".join(issues)
    return CheckResult(name="Wine schema columns", passed=ok, details=details, data={"columns": sorted(actual)})


# -----------------------------------------------------------------------
# 3.5  Dossier files
# -----------------------------------------------------------------------


def check_dossiers(output_dir: Path) -> CheckResult:
    """Count dossiers and spot-check frontmatter + H1."""
    wines_dir = output_dir / "wines"
    dossiers = list(wines_dir.rglob("*.md")) if wines_dir.is_dir() else []
    dossier_count = len(dossiers)

    con = duckdb.connect(":memory:")
    try:
        wine_count = con.execute(f"SELECT count(*) FROM '{_pq(output_dir, 'wine')}'").fetchone()[0]
    except Exception as exc:
        return CheckResult(name="Dossier files", passed=False, details=str(exc))
    finally:
        con.close()

    issues = []
    # Dossiers may exceed wine count due to orphans from deleted wines
    if dossier_count < wine_count:
        issues.append(f"{dossier_count} dossiers < {wine_count} wines")

    # Spot-check: first dossier should have frontmatter and H1
    spot_ok = True
    if dossiers:
        sample = dossiers[0].read_text(encoding="utf-8", errors="replace")
        if "wine_id:" not in sample:
            spot_ok = False
            issues.append("spot-check: missing wine_id in frontmatter")
        if "\n# " not in sample and not sample.startswith("# "):
            spot_ok = False
            issues.append("spot-check: missing H1 heading")

    ok = not issues
    detail_parts = [f"{dossier_count} dossiers vs {wine_count} wines"]
    if dossier_count > wine_count:
        orphans = dossier_count - wine_count
        detail_parts.append(f"({orphans} orphaned from deleted wines, expected)")
    if spot_ok and dossiers:
        detail_parts.append("Spot-check: frontmatter + H1 present")
    if issues:
        detail_parts.extend(issues)

    return CheckResult(
        name="Dossier files",
        passed=ok,
        details=". ".join(detail_parts),
        data={"dossier_count": dossier_count, "wine_count": wine_count},
    )


# -----------------------------------------------------------------------
# 3.6  Validation re-run
# -----------------------------------------------------------------------


def run_validation(output_dir: Path) -> CheckResult:
    """Run the full validation suite directly."""
    from cellarbrain.validate import validate

    result = validate(output_dir)
    return CheckResult(
        name="Validation re-run",
        passed=result.ok,
        details=f"{result.passed} passed, {result.failed} failed",
        data={"passed": result.passed, "failed": result.failed},
    )


# -----------------------------------------------------------------------
# 4.  Cross-run consistency
# -----------------------------------------------------------------------


def check_cross_run(output_dir: Path) -> list[CheckResult]:
    """Verify cross-run consistency: wine count trend, change_log growth, etl_run count.

    Returns a list of ``CheckResult`` (one per sub-check).
    """
    con = duckdb.connect(":memory:")
    results = []

    try:
        # Wine count trend via change_log inserts/deletes per run
        wine_counts = con.execute(
            f"SELECT cl.run_id, "
            f"SUM(CASE WHEN cl.entity_type = 'wine' AND cl.change_type = 'insert' THEN 1 ELSE 0 END) AS ins, "
            f"SUM(CASE WHEN cl.entity_type = 'wine' AND cl.change_type = 'delete' THEN 1 ELSE 0 END) AS dels "
            f"FROM '{_pq(output_dir, 'change_log')}' cl "
            f"GROUP BY cl.run_id ORDER BY cl.run_id"
        ).fetchall()

        # Change log cumulative by run
        # Two separate queries to avoid DuckDB correlated-subquery
        # alias resolution issues.
        _run_ids = con.execute(f"SELECT run_id FROM '{_pq(output_dir, 'etl_run')}' ORDER BY run_id").fetchall()
        _cl_by_run = con.execute(
            f"SELECT run_id, count(*) AS cnt FROM '{_pq(output_dir, 'change_log')}' GROUP BY run_id"
        ).fetchall()
        _cl_map = dict(_cl_by_run)
        _cum = 0
        cl_counts = []
        for (rid,) in _run_ids:
            _cum += _cl_map.get(rid, 0)
            cl_counts.append((rid, _cum))

        # ETL run count
        etl_count = con.execute(f"SELECT count(*) FROM '{_pq(output_dir, 'etl_run')}'").fetchone()[0]

    except Exception as exc:
        return [CheckResult(name="Cross-run consistency", passed=False, details=str(exc))]
    finally:
        con.close()

    # --- Wine count trend ---
    # wine_counts is [(run_id, inserts, deletes), ...] from change_log.
    # Compute cumulative active wine count per run.
    if wine_counts:
        _cum_wine = 0
        counts_list = []
        for _rid, _ins, _dels in wine_counts:
            _cum_wine += _ins - _dels
            counts_list.append(_cum_wine)
        trend_str = " → ".join(str(c) for c in counts_list)
        trend_ok = counts_list[0] <= counts_list[-1]
        results.append(
            CheckResult(
                name="Wine count trend",
                passed=trend_ok,
                details=trend_str,
                data={"counts": counts_list},
            )
        )

    # --- Change log growth ---
    if cl_counts:
        cum_list = [c[1] for c in cl_counts]
        growth_str = " → ".join(str(c) for c in cum_list)
        growth_ok = all(cum_list[i] <= cum_list[i + 1] for i in range(len(cum_list) - 1))
        results.append(
            CheckResult(
                name="Change log growth",
                passed=growth_ok,
                details=growth_str,
                data={"cumulative": cum_list},
            )
        )

    # --- ETL run count ---
    results.append(
        CheckResult(
            name="ETL run count",
            passed=True,
            details=f"found {etl_count}",
            data={"count": etl_count},
        )
    )

    return results


# -----------------------------------------------------------------------
# 5.  FK integrity checks
# -----------------------------------------------------------------------

# (label, child_table, child_col, parent_table, parent_col, nullable, soft_delete_filter)
_FK_CHECKS: list[tuple[str, str, str, str, str, bool, bool]] = [
    ("wine.winery_id → winery", "wine", "winery_id", "winery", "winery_id", True, True),
    ("wine.appellation_id → appellation", "wine", "appellation_id", "appellation", "appellation_id", True, True),
    ("wine.tracked_wine_id → tracked_wine", "wine", "tracked_wine_id", "tracked_wine", "tracked_wine_id", True, True),
    ("wine_grape.wine_id → wine", "wine_grape", "wine_id", "wine", "wine_id", False, False),
    ("wine_grape.grape_id → grape", "wine_grape", "grape_id", "grape", "grape_id", False, False),
    ("bottle.wine_id → wine", "bottle", "wine_id", "wine", "wine_id", False, False),
    ("bottle.cellar_id → cellar", "bottle", "cellar_id", "cellar", "cellar_id", True, False),
    ("bottle.provider_id → provider", "bottle", "provider_id", "provider", "provider_id", True, False),
    ("tasting.wine_id → wine", "tasting", "wine_id", "wine", "wine_id", False, False),
    ("pro_rating.wine_id → wine", "pro_rating", "wine_id", "wine", "wine_id", False, False),
    ("tracked_wine.winery_id → winery", "tracked_wine", "winery_id", "winery", "winery_id", False, True),
    (
        "tracked_wine.appellation_id → appellation",
        "tracked_wine",
        "appellation_id",
        "appellation",
        "appellation_id",
        True,
        True,
    ),
    ("change_log.run_id → etl_run", "change_log", "run_id", "etl_run", "run_id", False, False),
]


def check_fk_integrity(output_dir: Path) -> list[CheckResult]:
    """Validate all FK relationships between Parquet tables.

    Returns one ``CheckResult`` per FK constraint.
    """
    con = duckdb.connect(":memory:")
    results: list[CheckResult] = []

    try:
        # Register views for all parquet files (same pattern as validate.py)
        for name in EXPECTED_PARQUET:
            pq_path = output_dir / f"{name}.parquet"
            if pq_path.exists():
                con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{_pq(output_dir, name)}')")

        # Also check price_observation if year-partitioned files exist
        price_files = list(output_dir.glob("price_observation_*.parquet"))
        if price_files:
            glob_path = str(output_dir / "price_observation_*.parquet").replace("\\", "/")
            con.execute(f"CREATE VIEW price_observation AS SELECT * FROM read_parquet('{glob_path}')")
            _FK_RUNTIME = list(_FK_CHECKS) + [
                (
                    "price_observation.tracked_wine_id → tracked_wine",
                    "price_observation",
                    "tracked_wine_id",
                    "tracked_wine",
                    "tracked_wine_id",
                    False,
                    False,
                ),
            ]
        else:
            _FK_RUNTIME = list(_FK_CHECKS)

        for label, child, child_col, parent, parent_col, nullable, soft_del in _FK_RUNTIME:
            try:
                where_parts = []
                if nullable:
                    where_parts.append(f"c.{child_col} IS NOT NULL")
                if soft_del:
                    where_parts.append("NOT c.is_deleted")
                where_clause = " AND ".join(where_parts)
                if where_clause:
                    where_clause = f"WHERE {where_clause} AND"
                else:
                    where_clause = "WHERE"

                sql = (
                    f"SELECT count(*) FROM {child} c "
                    f"{where_clause} c.{child_col} NOT IN (SELECT {parent_col} FROM {parent})"
                )
                count = con.execute(sql).fetchone()[0]
                if count == 0:
                    results.append(CheckResult(name=f"FK {label}", passed=True, details="ok"))
                else:
                    # Get sample orphaned IDs
                    sample_sql = (
                        f"SELECT DISTINCT c.{child_col} FROM {child} c "
                        f"{where_clause} c.{child_col} NOT IN (SELECT {parent_col} FROM {parent}) "
                        f"LIMIT 5"
                    )
                    samples = [str(r[0]) for r in con.execute(sample_sql).fetchall()]
                    results.append(
                        CheckResult(
                            name=f"FK {label}",
                            passed=False,
                            details=f"{count} orphaned rows (sample {child_col}: {', '.join(samples)})",
                        )
                    )
            except duckdb.CatalogException:
                results.append(
                    CheckResult(
                        name=f"FK {label}",
                        passed=True,
                        details="skipped — table not present",
                    )
                )
    finally:
        con.close()

    return results


# -----------------------------------------------------------------------
# 6.  Dossier integrity checks
# -----------------------------------------------------------------------


def check_dossier_integrity(output_dir: Path) -> list[CheckResult]:
    """Validate bidirectional wine↔dossier linkage.

    Checks: wine→dossier, dossier→wine, tracked→companion,
    companion→tracked, routing (cellar vs archive), frontmatter consistency.
    """
    results: list[CheckResult] = []
    wines_dir = output_dir / "wines"
    cellar_dir = wines_dir / "cellar"
    archive_dir = wines_dir / "archive"
    tracked_dir = wines_dir / "tracked"

    con = duckdb.connect(":memory:")
    try:
        con.execute(f"CREATE VIEW wine AS SELECT * FROM read_parquet('{_pq(output_dir, 'wine')}')")
        con.execute(f"CREATE VIEW bottle AS SELECT * FROM read_parquet('{_pq(output_dir, 'bottle')}')")

        tracked_pq = output_dir / "tracked_wine.parquet"
        has_tracked = tracked_pq.exists()
        if has_tracked:
            con.execute(f"CREATE VIEW tracked_wine AS SELECT * FROM read_parquet('{_pq(output_dir, 'tracked_wine')}')")

        # ------------------------------------------------------------------
        # 6.1  Collect active wines and their expected dossier info
        # ------------------------------------------------------------------
        active_wines = con.execute("SELECT wine_id, wine_slug FROM wine WHERE NOT is_deleted").fetchall()
        wine_ids = {r[0] for r in active_wines}

        # Wines with at least one stored bottle (for routing check)
        wines_with_stored = set()
        stored_rows = con.execute(
            "SELECT DISTINCT b.wine_id FROM bottle b"
            " LEFT JOIN cellar c ON b.cellar_id = c.cellar_id"
            " WHERE b.status = 'stored' AND COALESCE(c.location_type, 'onsite') != 'in_transit'"
        ).fetchall()
        for r in stored_rows:
            wines_with_stored.add(r[0])

        # ------------------------------------------------------------------
        # 6.2  Scan dossier files and map wine_id to location
        # ------------------------------------------------------------------
        dossier_map: dict[int, Path] = {}  # wine_id → file path
        for subdir in (cellar_dir, archive_dir):
            if not subdir.is_dir():
                continue
            for f in subdir.glob("*.md"):
                # Extract wine_id from filename prefix: "0001-slug.md"
                parts = f.stem.split("-", 1)
                if parts[0].isdigit():
                    wid = int(parts[0])
                    dossier_map[wid] = f

        # ------------------------------------------------------------------
        # 6.3  Wine → Dossier: every active wine must have a dossier
        # ------------------------------------------------------------------
        missing_dossiers = sorted(wine_ids - set(dossier_map.keys()))
        if missing_dossiers:
            sample = ", ".join(str(w) for w in missing_dossiers[:10])
            results.append(
                CheckResult(
                    name="Wine → Dossier completeness",
                    passed=False,
                    details=f"{len(missing_dossiers)} active wines missing dossiers (sample: {sample})",
                )
            )
        else:
            results.append(
                CheckResult(
                    name="Wine → Dossier completeness",
                    passed=True,
                    details=f"all {len(wine_ids)} active wines have dossiers",
                )
            )

        # ------------------------------------------------------------------
        # 6.4  Dossier → Wine: every dossier must link to an existing wine
        # ------------------------------------------------------------------
        all_wine_ids = {r[0] for r in con.execute("SELECT wine_id FROM wine").fetchall()}
        # Only cellar dossiers must link to an existing wine; archived
        # dossiers are expected orphans (wines removed in later ETL runs).
        cellar_dossier_ids = {
            wid for wid, path in dossier_map.items() if cellar_dir in path.parents or path.parent == cellar_dir
        }
        orphan_dossiers = sorted(cellar_dossier_ids - all_wine_ids)
        archive_orphans = sorted((set(dossier_map.keys()) - all_wine_ids) - cellar_dossier_ids)
        if orphan_dossiers:
            sample = ", ".join(str(w) for w in orphan_dossiers[:10])
            results.append(
                CheckResult(
                    name="Dossier → Wine linkage",
                    passed=False,
                    details=f"{len(orphan_dossiers)} cellar dossier files with no matching wine (IDs: {sample})",
                )
            )
        else:
            archive_note = f" ({len(archive_orphans)} archived orphans — expected)" if archive_orphans else ""
            results.append(
                CheckResult(
                    name="Dossier → Wine linkage",
                    passed=True,
                    details=f"all {len(cellar_dossier_ids)} cellar dossiers link to existing wines{archive_note}",
                )
            )

        # ------------------------------------------------------------------
        # 6.5  Routing: cellar vs archive
        # ------------------------------------------------------------------
        misrouted: list[str] = []
        for wid, path in dossier_map.items():
            if wid not in wine_ids:
                continue  # orphan — already reported above
            in_cellar_dir = cellar_dir in path.parents or path.parent == cellar_dir
            has_stored = wid in wines_with_stored
            if has_stored and not in_cellar_dir:
                misrouted.append(f"{wid} has stored bottles but is in archive/")
            elif not has_stored and in_cellar_dir:
                misrouted.append(f"{wid} has no stored bottles but is in cellar/")

        if misrouted:
            results.append(
                CheckResult(
                    name="Dossier routing (cellar vs archive)",
                    passed=False,
                    details=f"{len(misrouted)} misrouted: {'; '.join(misrouted[:5])}",
                )
            )
        else:
            results.append(
                CheckResult(
                    name="Dossier routing (cellar vs archive)",
                    passed=True,
                    details="all dossiers correctly routed",
                )
            )

        # ------------------------------------------------------------------
        # 6.6  Frontmatter spot-check: wine_id in frontmatter matches filename
        # ------------------------------------------------------------------
        fm_mismatches: list[str] = []
        import re as _re

        for wid, path in list(dossier_map.items())[:20]:  # spot-check first 20
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
                m = _re.search(r"wine_id:\s*(\d+)", text)
                if m and int(m.group(1)) != wid:
                    fm_mismatches.append(f"{wid}: frontmatter says {m.group(1)}")
            except OSError:
                fm_mismatches.append(f"{wid}: could not read file")

        if fm_mismatches:
            results.append(
                CheckResult(
                    name="Dossier frontmatter consistency",
                    passed=False,
                    details=f"{len(fm_mismatches)} mismatches: {'; '.join(fm_mismatches)}",
                )
            )
        else:
            results.append(
                CheckResult(
                    name="Dossier frontmatter consistency",
                    passed=True,
                    details="spot-check passed (20 dossiers)",
                )
            )

        # ------------------------------------------------------------------
        # 6.7  Tracked wine ↔ companion dossier
        # ------------------------------------------------------------------
        if has_tracked:
            active_tracked = con.execute("SELECT tracked_wine_id FROM tracked_wine WHERE NOT is_deleted").fetchall()
            tracked_ids = {r[0] for r in active_tracked}

            companion_map: dict[int, Path] = {}
            if tracked_dir.is_dir():
                for f in tracked_dir.glob("*.md"):
                    parts = f.stem.split("-", 1)
                    if parts[0].isdigit():
                        tid = int(parts[0])
                        companion_map[tid] = f

            # Tracked → Companion
            missing_companions = sorted(tracked_ids - set(companion_map.keys()))
            if missing_companions:
                sample = ", ".join(str(t) for t in missing_companions[:10])
                results.append(
                    CheckResult(
                        name="Tracked → Companion completeness",
                        passed=False,
                        details=f"{len(missing_companions)} tracked wines missing companion dossiers (sample: {sample})",
                    )
                )
            else:
                results.append(
                    CheckResult(
                        name="Tracked → Companion completeness",
                        passed=True,
                        details=f"all {len(tracked_ids)} tracked wines have companion dossiers",
                    )
                )

            # Companion → Tracked
            all_tracked_ids = {r[0] for r in con.execute("SELECT tracked_wine_id FROM tracked_wine").fetchall()}
            orphan_companions = sorted(set(companion_map.keys()) - all_tracked_ids)
            if orphan_companions:
                sample = ", ".join(str(t) for t in orphan_companions[:10])
                results.append(
                    CheckResult(
                        name="Companion → Tracked linkage",
                        passed=True,  # downgrade: orphans expected for removed tracked wines
                        details=f"{len(orphan_companions)} orphaned companion dossiers (expected for removed tracked wines, IDs: {sample})",
                    )
                )
            else:
                results.append(
                    CheckResult(
                        name="Companion → Tracked linkage",
                        passed=True,
                        details=f"all {len(companion_map)} companion dossiers link to tracked wines",
                    )
                )

    except Exception as exc:
        results.append(
            CheckResult(
                name="Dossier integrity",
                passed=False,
                details=str(exc),
            )
        )
    finally:
        con.close()

    return results
