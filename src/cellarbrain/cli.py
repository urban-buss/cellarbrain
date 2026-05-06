"""CLI entry point for the Cellarbrain ETL pipeline and agent tools."""

from __future__ import annotations

import argparse
import logging
import pathlib
import re
import sys
import warnings
from datetime import UTC, datetime
from importlib.metadata import version as _pkg_version

from . import companion_markdown, incremental, markdown, transform, vinocell_reader, writer
from . import validate as val
from .computed import classify_cellar, convert_to_default_currency, enrich_wines
from .settings import CurrencyConfig, Settings, load_settings

logger = logging.getLogger(__name__)


def _do_transforms(
    wines_csv: str,
    bottles_csv: str,
    bottles_gone_csv: str,
    *,
    settings: Settings | None = None,
    current_year: int | None = None,
    wine_id_assignments: list | None = None,
    wines_rows: list[dict] | None = None,
) -> dict[str, list[dict]]:
    """Read CSVs and build all normalised entities."""
    if wines_rows is None:
        print(f"Reading wines CSV:   {wines_csv}")
        wines_rows = vinocell_reader.read_wines_csv(wines_csv)
        print(f"  → {len(wines_rows)} wine rows")
        if not wines_rows:
            logger.warning("Wines CSV contains 0 data rows \u2014 is this intentional?")

    print(f"Reading bottles CSV: {bottles_csv}")
    bottles_rows = vinocell_reader.read_bottles_csv(bottles_csv)
    print(f"  → {len(bottles_rows)} bottle rows")
    if not bottles_rows:
        logger.warning("Bottles CSV contains 0 data rows \u2014 is this intentional?")

    print(f"Reading bottles-gone CSV: {bottles_gone_csv}")
    bottles_gone_rows = vinocell_reader.read_bottles_gone_csv(bottles_gone_csv)
    print(f"  → {len(bottles_gone_rows)} gone bottle rows")
    if not bottles_gone_rows:
        logger.warning("Bottles-gone CSV contains 0 data rows \u2014 is this intentional?")

    # --- Lookup entities ---
    print("\nBuilding lookup entities...")
    wineries, winery_lk = transform.build_wineries(wines_rows)
    appellations, appellation_lk = transform.build_appellations(wines_rows)
    grapes, grape_lk = transform.build_grapes(wines_rows)
    cellars, cellar_lk = transform.build_cellars(bottles_rows)
    providers, provider_lk = transform.build_providers(bottles_rows, bottles_gone_rows)

    print(f"  Wineries:     {len(wineries)}")
    print(f"  Appellations: {len(appellations)}")
    print(f"  Grapes:       {len(grapes)}")
    print(f"  Cellars:      {len(cellars)}")
    print(f"  Providers:    {len(providers)}")

    # --- Core entities ---
    print("\nBuilding core entities...")
    wines, wine_lk = transform.build_wines(
        wines_rows,
        winery_lk,
        appellation_lk,
        id_assignments=wine_id_assignments,
    )
    wine_vol_lk = transform.build_wine_volume_lookup(wines_rows, wines)
    wine_grapes = transform.build_wine_grapes(
        wines_rows,
        wine_lk,
        grape_lk,
        wine_volume_lookup=wine_vol_lk,
    )
    bottles = transform.build_bottles(
        bottles_rows,
        wine_lk,
        cellar_lk,
        provider_lk,
        wine_volume_lookup=wine_vol_lk,
    )
    bottles_gone = transform.build_bottles_gone(
        bottles_gone_rows,
        wine_lk,
        provider_lk,
        start_id=len(bottles) + 1,
        wine_volume_lookup=wine_vol_lk,
    )
    all_bottles = bottles + bottles_gone
    tastings = transform.build_tastings(
        wines_rows,
        wine_lk,
        wine_volume_lookup=wine_vol_lk,
    )
    pro_ratings = transform.build_pro_ratings(
        wines_rows,
        bottles_rows,
        wine_lk,
        bottles_gone_rows,
        wine_volume_lookup=wine_vol_lk,
    )

    print(f"  Wines:       {len(wines)}")
    print(f"  Wine-grapes: {len(wine_grapes)}")
    print(f"  Bottles:     {len(all_bottles)} (stored: {len(bottles)}, gone: {len(bottles_gone)})")
    print(f"  Tastings:    {len(tastings)}")
    print(f"  Pro ratings: {len(pro_ratings)}")

    # --- Computed wine properties ---
    grape_id_to_name = {g["grape_id"]: g["name"] for g in grapes}
    winery_id_to_name = {w["winery_id"]: w["name"] for w in wineries}
    appellation_id_to_dict = {a["appellation_id"]: a for a in appellations}
    enrich_wines(
        wines,
        wine_grapes,
        grape_id_to_name,
        winery_id_to_name,
        appellation_id_to_dict,
        settings=settings,
        current_year=current_year,
    )

    # --- Bottle-level: currency normalisation ---
    currency = settings.currency if settings else CurrencyConfig()
    for b in all_bottles:
        b["purchase_price"] = convert_to_default_currency(
            b.get("original_purchase_price"),
            b.get("original_purchase_currency"),
            currency.default,
            currency.rates,
        )
        b["purchase_currency"] = currency.default

    # --- Bottle-level computed property: is_onsite ---
    rules = settings.cellar_rules if settings else ()
    cellar_id_to_name = {c["cellar_id"]: c["name"] for c in cellars}
    for b in all_bottles:
        cellar_name = cellar_id_to_name.get(b.get("cellar_id"))
        cls = classify_cellar(cellar_name, rules)
        b["is_onsite"] = cls == "onsite"
        b["is_in_transit"] = cls == "in_transit"

    # --- Tracked wines (wishlist / favorites) ---
    appellation_by_wine = {w["wine_id"]: w.get("appellation_id") for w in wines}
    tracked_wines, tracked_lk = transform.build_tracked_wines(
        wines,
        appellation_by_wine,
    )
    transform.assign_tracked_wine_ids(wines, tracked_lk)
    if tracked_wines:
        print(f"\n  Tracked wines: {len(tracked_wines)}")

    return {
        "winery": wineries,
        "appellation": appellations,
        "grape": grapes,
        "cellar": cellars,
        "provider": providers,
        "wine": wines,
        "tracked_wine": tracked_wines,
        "wine_grape": wine_grapes,
        "bottle": all_bottles,
        "tasting": tastings,
        "pro_rating": pro_ratings,
    }


def run(
    wines_csv: str,
    bottles_csv: str,
    output_dir: str,
    *,
    sync_mode: bool = False,
    bottles_gone_csv: str,
    settings: Settings | None = None,
) -> bool:
    """Execute the ETL pipeline. Returns True if validation passes."""
    if settings is None:
        settings = load_settings()
    out = pathlib.Path(output_dir)

    # --- Pre-ETL backup (if data already exists) ---
    if out.exists() and any(out.glob("*.parquet")):
        from .backup import create_backup

        try:
            bkp_path = create_backup(
                out,
                settings.backup.backup_dir,
                include_sommelier=settings.backup.include_sommelier,
                include_logs=settings.backup.include_logs,
                max_backups=settings.backup.max_backups,
            )
            print(f"Pre-ETL backup: {bkp_path.name}")
        except Exception as exc:
            logger.warning("Backup failed (continuing ETL): %s", exc)

    now = datetime.now(UTC).replace(tzinfo=None)
    run_id = incremental.next_run_id(out)

    # --- Slug-based wine classification (pre-transform) ---
    print(f"Reading wines CSV:   {wines_csv}")
    wines_rows = vinocell_reader.read_wines_csv(wines_csv)
    print(f"  → {len(wines_rows)} wine rows")
    wine_parquet = out / "wine.parquet"
    existing_wines = incremental._table_to_dicts(wine_parquet)
    matches, deletions = incremental.classify_wines(wines_rows, existing_wines)

    from collections import Counter

    _slug_counts = Counter(m.status for m in matches)
    _del_new = sum(1 for d in deletions if not d.was_already_deleted)
    print(
        f"  Slug matching: "
        f"{_slug_counts.get('existing', 0)} existing, "
        f"{_slug_counts.get('new', 0)} new, "
        f"{_del_new} deleted, "
        f"{_slug_counts.get('revived', 0)} revived, "
        f"{_slug_counts.get('renamed', 0)} renamed"
    )

    entities = _do_transforms(
        wines_csv,
        bottles_csv,
        bottles_gone_csv,
        settings=settings,
        current_year=now.year,
        wine_id_assignments=matches,
        wines_rows=wines_rows,
    )

    skip = frozenset({"wine"})

    if sync_mode:
        print("\nRunning incremental sync...")
        entities, change_log, fk_remappings = incremental.sync(
            entities,
            out,
            run_id,
            now,
            identity_config=settings.identity,
            skip_entities=skip,
        )
        run_type = "incremental"
    else:
        print("\nAnnotating full load...")
        entities, change_log = incremental.annotate_full_load(
            entities,
            out,
            run_id,
            now,
            skip_entities=skip,
        )
        fk_remappings = {}
        run_type = "full"

    # --- Annotate wines via slug-based classification ---
    entities["wine"], wine_changes = incremental.annotate_classified_wines(
        entities["wine"],
        existing_wines,
        matches,
        deletions,
        run_id,
        now,
        fk_remappings=fk_remappings,
    )
    if change_log:
        next_cid = max(c["change_id"] for c in change_log) + 1
    else:
        next_cid = incremental._next_change_id(out)
    for i, ch in enumerate(wine_changes, start=next_cid):
        ch["change_id"] = i
    change_log.extend(wine_changes)

    inserts = sum(1 for c in change_log if c["change_type"] == "insert")
    updates = sum(1 for c in change_log if c["change_type"] == "update")
    deletes = sum(1 for c in change_log if c["change_type"] == "delete")
    renames = sum(1 for c in change_log if c["change_type"] == "rename")
    print(f"  Inserts: {inserts}  Updates: {updates}  Deletes: {deletes}  Renames: {renames}")

    def _is_wine(c: dict) -> bool:
        return c["entity_type"] == "wine"

    wines_inserted = sum(1 for c in change_log if _is_wine(c) and c["change_type"] == "insert")
    wines_updated = sum(1 for c in change_log if _is_wine(c) and c["change_type"] == "update")
    wines_deleted = sum(1 for c in change_log if _is_wine(c) and c["change_type"] == "delete")
    wines_renamed = sum(1 for c in change_log if _is_wine(c) and c["change_type"] == "rename")
    print(
        f"  Wine-level: {wines_inserted} new, {wines_updated} updated, {wines_deleted} deleted, {wines_renamed} renamed"
    )

    # --- Assign format groups & dossier paths (after ID stabilisation) ---
    transform.assign_format_groups(entities["wine"])
    transform.update_format_slugs(entities["wine"])
    transform.assign_dossier_paths(entities)
    transform.assign_tracked_dossier_paths(entities, settings)

    # --- Write entity Parquet files ---
    print(f"\nWriting Parquet files to {out}/")
    paths = writer.write_all(entities, out)
    for name, path in paths.items():
        print(f"  {name}: {path}")

    # --- Write ETL tracking tables ---
    finished_at = datetime.now(UTC).replace(tzinfo=None)
    etl_run = {
        "run_id": run_id,
        "started_at": now,
        "finished_at": finished_at,
        "run_type": run_type,
        "wines_source_hash": incremental.compute_file_hash(wines_csv),
        "bottles_source_hash": incremental.compute_file_hash(bottles_csv),
        "bottles_gone_source_hash": incremental.compute_file_hash(bottles_gone_csv),
        "total_inserts": inserts,
        "total_updates": updates,
        "total_deletes": deletes,
        "wines_inserted": wines_inserted,
        "wines_updated": wines_updated,
        "wines_deleted": wines_deleted,
        "wines_renamed": wines_renamed,
    }
    writer.append_parquet("etl_run", [etl_run], out)
    writer.append_parquet("change_log", change_log, out)
    print(f"  etl_run: {out / 'etl_run.parquet'}")
    print(f"  change_log: {out / 'change_log.parquet'}")

    # --- Generate wine markdown dossiers ---
    current_year = now.year
    if sync_mode:
        wine_ids_to_regen = markdown.affected_wine_ids(change_log, entities)
        deleted_ids = {
            c["entity_id"] for c in change_log if c["entity_type"] == "wine" and c["change_type"] == "delete"
        }
        if wine_ids_to_regen:
            md_paths = markdown.generate_dossiers(
                entities,
                out,
                current_year,
                wine_ids=wine_ids_to_regen,
            )
            print(f"\nRegenerated {len(md_paths)} wine dossier(s)")
        else:
            print("\nNo wine dossiers to regenerate")
        if deleted_ids:
            del_paths = markdown.mark_deleted_dossiers(
                out,
                deleted_ids,
                run_id,
                now.isoformat(),
            )
            print(f"  Marked {len(del_paths)} dossier(s) as deleted")
    else:
        md_paths = markdown.generate_dossiers(entities, out, current_year)
        print(f"\nGenerated {len(md_paths)} wine dossier(s)")

    # --- Generate companion dossiers for tracked wines ---
    if entities.get("tracked_wine"):
        comp_paths = companion_markdown.generate_companion_dossiers(
            entities,
            out,
            settings,
        )
        if comp_paths:
            print(f"Generated {len(comp_paths)} companion dossier(s)")

    # --- Rebuild sommelier wine index (if model exists) ---
    if settings.sommelier.enabled:
        model_dir = pathlib.Path(settings.sommelier.model_dir)
        if (model_dir / "config.json").exists():
            print("\nRebuilding sommelier wine index...")
            try:
                from .sommelier.model import load_model

                model = load_model(str(model_dir))
                wine_dir = out / settings.sommelier.wine_index_dir
                wine_dir.mkdir(parents=True, exist_ok=True)
                _rebuild_wine_index(model, out, wine_dir, settings)
            except ImportError as exc:
                print(f"  Skipped — {exc}")
        else:
            print("\nSommelier model not found — skipping wine index rebuild.")

    # --- Validate ---
    print("\nRunning validation...")
    result = val.validate(out)
    print(result.summary())

    return result.ok


def main(argv: list[str] | None = None) -> None:
    """CLI entry point. Routes to legacy or subcommand interface."""
    args = argv if argv is not None else sys.argv[1:]

    # Backward compatibility: if the first arg looks like a CSV file,
    # delegate to the legacy flat parser.
    if args and args[0].endswith(".csv"):
        warnings.warn(
            "Direct CSV arguments are deprecated. Use: cellarbrain etl <wines.csv> <bottles.csv> [-o output] [--sync]",
            DeprecationWarning,
            stacklevel=2,
        )
        return _legacy_main(args)

    return _subcommand_main(args)


def _run_handler(fn):
    """Execute *fn* with user-facing error reporting.

    Known exception types are printed as ``Error: <message>`` on stderr and
    cause exit code 1.  Unknown exceptions re-raise with the full traceback.
    """
    try:
        return fn()
    except Exception as exc:
        # Domain exceptions are lazily imported to avoid pulling in DuckDB
        # and heavy modules when they are not needed.
        from .dossier_ops import ProtectedSectionError, WineNotFoundError
        from .query import DataStaleError, QueryError

        if isinstance(
            exc,
            (
                ValueError,
                FileNotFoundError,
                UnicodeDecodeError,
                DataStaleError,
                QueryError,
                WineNotFoundError,
                ProtectedSectionError,
            ),
        ):
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(1)
        raise


def _legacy_main(argv: list[str]) -> None:
    """Original flat argument parser (deprecated)."""
    parser = argparse.ArgumentParser(
        prog="cellarbrain",
        description="Transform wine cellar CSV exports into normalized Parquet files.",
    )
    parser.add_argument("wines_csv")
    parser.add_argument("bottles_csv")
    parser.add_argument("bottles_gone_csv")
    parser.add_argument("-o", "--output", default="output")
    parser.add_argument("--sync", action="store_true", default=False)
    args = parser.parse_args(argv)

    ok = _run_handler(
        lambda: run(
            args.wines_csv,
            args.bottles_csv,
            args.output,
            sync_mode=args.sync,
            bottles_gone_csv=args.bottles_gone_csv,
        )
    )
    sys.exit(0 if ok else 1)


def _subcommand_main(argv: list[str]) -> None:
    """Subcommand-based interface."""
    parser = argparse.ArgumentParser(
        prog="cellarbrain",
        description="Cellarbrain wine cellar toolkit — ETL, query, and agent interface.",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {_pkg_version('cellarbrain')}",
    )
    parser.add_argument(
        "-c",
        "--config",
        default=None,
        help="Path to cellarbrain.toml configuration file.",
    )
    parser.add_argument(
        "-d",
        "--data-dir",
        default=None,
        help="Path to the output directory containing Parquet files.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase log verbosity (-v for INFO, -vv for DEBUG).",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress warnings (set log level to ERROR).",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Write logs to a rotating file at this path.",
    )
    sub = parser.add_subparsers(dest="command")

    # --- etl ---
    etl = sub.add_parser("etl", help="Run the ETL pipeline")
    etl.add_argument("wines_csv")
    etl.add_argument("bottles_csv")
    etl.add_argument("bottles_gone_csv")
    etl.add_argument("-o", "--output", default=None)
    etl.add_argument("--sync", action="store_true", default=False)

    # --- validate ---
    sub.add_parser("validate", help="Validate Parquet output")

    # --- query ---
    qry = sub.add_parser("query", help="Run SQL against the cellar")
    qry.add_argument("sql", nargs="?")
    qry.add_argument("-f", "--file", help="Read SQL from file")
    qry.add_argument(
        "--format",
        choices=["table", "csv", "json"],
        default="table",
        dest="output_format",
    )

    # --- stats ---
    sts = sub.add_parser("stats", help="Cellar statistics")
    sts.add_argument(
        "--by",
        choices=[
            "country",
            "region",
            "category",
            "vintage",
            "winery",
            "grape",
            "cellar",
            "provider",
            "status",
        ],
        default=None,
    )

    # --- dossier ---
    dos = sub.add_parser("dossier", help="Wine dossier management")
    dos.add_argument("wine_id", nargs="?", type=int)
    dos.add_argument("--search")
    dos.add_argument("--pending", action="store_true")
    dos.add_argument("--limit", type=int, default=20)
    dos.add_argument(
        "--sections",
        nargs="+",
        metavar="SECTION",
        help="Section keys to include (default: all). E.g. --sections identity producer_profile",
    )

    # --- mcp ---
    mcp_parser = sub.add_parser("mcp", help="Start MCP server")
    mcp_parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
    )
    mcp_parser.add_argument("--port", type=int, default=8080)

    # --- recalc ---
    recalc = sub.add_parser("recalc", help="Recompute calculated fields")
    recalc.add_argument("-o", "--output", default=None)

    # --- rederive-food-tags ---
    rederive = sub.add_parser(
        "rederive-food-tags",
        help="Re-derive food_tags and food_groups from existing prose",
    )
    rederive.add_argument("-o", "--output", default=None)
    rederive.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing",
    )

    # --- wishlist ---
    wish = sub.add_parser("wishlist", help="Wishlist & price tracking")
    wish_sub = wish.add_subparsers(dest="wishlist_command")

    wish_alerts = wish_sub.add_parser("alerts", help="Show wishlist alerts")
    wish_alerts.add_argument("--days", type=int, default=None, help="Alert window in days (default from settings)")

    wish_sub.add_parser("stats", help="Tracked wine statistics")
    wish_sub.add_parser("scan", help="Price scanning (agent-driven)")

    # --- train-model ---
    train = sub.add_parser("train-model", help="Train the sommelier embedding model")
    train.add_argument("--epochs", type=int, default=10, help="Training epochs (default: 10)")
    train.add_argument("--batch-size", type=int, default=32, help="Batch size (default: 32)")
    train.add_argument("--output", default=None, help="Output directory for model weights")

    # --- retrain-model ---
    retrain = sub.add_parser("retrain-model", help="Incrementally retrain the sommelier model")
    retrain.add_argument("--epochs", type=int, default=None, help="Training epochs (default: 5 for retrain)")
    retrain.add_argument("--batch-size", type=int, default=None, help="Batch size")

    # --- rebuild-indexes ---
    rebuild = sub.add_parser("rebuild-indexes", help="Rebuild sommelier FAISS indexes")
    rebuild.add_argument("--wine-only", action="store_true", help="Only rebuild the wine index")
    rebuild.add_argument("--food-only", action="store_true", help="Only rebuild the food index")

    # --- logs ---
    logs_parser = sub.add_parser("logs", help="Query the MCP observability log store")
    logs_parser.add_argument("--errors", action="store_true", help="Show recent errors")
    logs_parser.add_argument("--usage", action="store_true", help="Show tool usage summary")
    logs_parser.add_argument("--latency", action="store_true", help="Show latency statistics")
    logs_parser.add_argument("--sessions", action="store_true", help="Show session summary")
    logs_parser.add_argument("--tail", type=int, default=20, help="Number of recent events (default 20)")
    logs_parser.add_argument("--since", type=int, default=24, help="Hours to look back (default 24)")
    logs_parser.add_argument("--prune", action="store_true", help="Prune old events per retention_days setting")

    # --- dashboard ---
    dash = sub.add_parser("dashboard", help="Start the web explorer dashboard")
    dash.add_argument("--port", type=int, default=8017, help="Port to listen on (default: 8017)")
    dash.add_argument("--open", action="store_true", help="Open browser on startup")
    dash.add_argument("--dev", action="store_true", help="Enable uvicorn auto-reload")

    # --- ingest ---
    ingest = sub.add_parser("ingest", help="Start the email ingestion daemon")
    ingest.add_argument("--once", action="store_true", help="Single poll cycle, then exit")
    ingest.add_argument("--dry-run", action="store_true", help="Detect batches but don't write files or run ETL")
    ingest.add_argument("--setup", action="store_true", help="Interactive credential storage")

    # --- backup ---
    bkp_parser = sub.add_parser("backup", help="Create a backup of the data directory")
    bkp_parser.add_argument("--backup-dir", default=None, help="Backup destination (default from config)")
    bkp_parser.add_argument("--include-sommelier", action="store_true", help="Include sommelier FAISS indexes")
    bkp_parser.add_argument("--include-logs", action="store_true", help="Include observability log database")
    bkp_parser.add_argument("--list", action="store_true", dest="list_backups", help="List available backups")

    # --- restore ---
    rst_parser = sub.add_parser("restore", help="Restore from a backup archive")
    rst_parser.add_argument("archive", nargs="?", help="Path to .zip archive (default: most recent)")
    rst_parser.add_argument("--dry-run", action="store_true", help="Show what would be restored without writing")

    # --- doctor ---
    doc_parser = sub.add_parser("doctor", help="Run diagnostic health checks on the data directory")
    doc_parser.add_argument("--json", action="store_true", help="Output results as JSON (for scripting)")
    doc_parser.add_argument("--strict", action="store_true", help="Treat warnings as errors (exit 1)")
    doc_parser.add_argument(
        "--check",
        type=str,
        nargs="*",
        help="Run only specific checks (parquet, schema, dossier, sommelier, currency, etl, backup, disk, integrity)",
    )

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    settings = load_settings(args.config)

    # --- Configure logging ---
    from .log import setup_logging

    level_override = None
    if args.quiet:
        level_override = "ERROR"
    elif args.verbose >= 2:
        level_override = "DEBUG"
    elif args.verbose >= 1:
        level_override = "INFO"

    quiet_stderr = args.command == "mcp"
    setup_logging(
        settings.logging,
        level_override=level_override,
        log_file_override=args.log_file,
        quiet_stderr=quiet_stderr,
    )
    logger.debug("CLI args: %s", vars(args))

    # CLI --data-dir overrides config file
    if args.data_dir is not None:
        from dataclasses import replace

        from .settings import PathsConfig

        settings = replace(
            settings,
            paths=PathsConfig(
                data_dir=args.data_dir,
                raw_dir=settings.paths.raw_dir,
                wines_subdir=settings.paths.wines_subdir,
                cellar_subdir=settings.paths.cellar_subdir,
                archive_subdir=settings.paths.archive_subdir,
            ),
        )

    handlers = {
        "etl": _cmd_etl,
        "validate": _cmd_validate,
        "query": _cmd_query,
        "stats": _cmd_stats,
        "dossier": _cmd_dossier,
        "mcp": _cmd_mcp,
        "recalc": _cmd_recalc,
        "rederive-food-tags": _cmd_rederive_food_tags,
        "wishlist": _cmd_wishlist,
        "train-model": _cmd_train_model,
        "retrain-model": _cmd_retrain_model,
        "rebuild-indexes": _cmd_rebuild_indexes,
        "logs": _cmd_logs,
        "dashboard": _cmd_dashboard,
        "ingest": _cmd_ingest,
        "backup": _cmd_backup,
        "restore": _cmd_restore,
        "doctor": _cmd_doctor,
    }
    handler = handlers[args.command]
    _run_handler(lambda: handler(args, settings))


def _cmd_etl(args: argparse.Namespace, settings: Settings) -> None:
    output = args.output or settings.paths.data_dir
    ok = run(
        args.wines_csv,
        args.bottles_csv,
        output,
        sync_mode=args.sync,
        bottles_gone_csv=args.bottles_gone_csv,
        settings=settings,
    )
    sys.exit(0 if ok else 1)


def _cmd_validate(args: argparse.Namespace, settings: Settings) -> None:
    result = val.validate(settings.paths.data_dir)
    print(result.summary())
    sys.exit(0 if result.ok else 1)


def _cmd_query(args: argparse.Namespace, settings: Settings) -> None:
    from . import query as q

    sql = args.sql
    if not sql and args.file:
        sql = pathlib.Path(args.file).read_text(encoding="utf-8")
    if not sql:
        print("Error: provide SQL as argument or via -f file", file=sys.stderr)
        sys.exit(1)

    con = q.get_connection(settings.paths.data_dir)

    if args.output_format == "table":
        print(q.execute_query(con, sql, row_limit=settings.query.row_limit))
    else:
        q.validate_sql(sql)
        try:
            df = con.execute(sql).fetchdf()
        except Exception as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(1)
        if args.output_format == "csv":
            print(df.to_csv(index=False))
        elif args.output_format == "json":
            print(df.to_json(orient="records", indent=2))


def _cmd_stats(args: argparse.Namespace, settings: Settings) -> None:
    from . import query as q

    con = q.get_connection(settings.paths.data_dir)
    print(q.cellar_stats(con, group_by=args.by))


def _cmd_dossier(args: argparse.Namespace, settings: Settings) -> None:
    from . import dossier_ops
    from . import query as q

    data_dir = settings.paths.data_dir
    if args.pending:
        print(dossier_ops.pending_research(data_dir, limit=args.limit))
    elif args.search:
        con = q.get_connection(data_dir)
        print(q.find_wine(con, args.search, limit=args.limit))
    elif args.wine_id is not None:
        print(dossier_ops.read_dossier_sections(args.wine_id, data_dir, sections=args.sections))
    else:
        print("Error: provide wine_id, --search, or --pending", file=sys.stderr)
        sys.exit(1)


def _cmd_mcp(args: argparse.Namespace, settings: Settings) -> None:
    import os

    os.environ["CELLARBRAIN_DATA_DIR"] = settings.paths.data_dir
    if args.config:
        os.environ["CELLARBRAIN_CONFIG"] = str(args.config)
    from .mcp_server import mcp, warm_sommelier

    warm_sommelier()
    mcp.settings.port = args.port
    mcp.run(transport=args.transport)


def _cmd_wishlist(args: argparse.Namespace, settings: Settings) -> None:
    from . import query as q

    sub = getattr(args, "wishlist_command", None)
    if sub is None:
        print("Error: provide a wishlist subcommand (alerts, stats, scan)", file=sys.stderr)
        sys.exit(1)

    if sub == "alerts":
        print(q.wishlist_alerts(settings.paths.data_dir, settings=settings, days=args.days))
    elif sub == "stats":
        con = q.get_connection(settings.paths.data_dir)
        print(
            q.execute_query(
                con,
                """
            SELECT
                count(*) AS tracked_wines,
                list_sort(list(DISTINCT category)) AS categories,
                list_sort(list(DISTINCT country)) AS countries
            FROM tracked_wines
        """,
                row_limit=settings.query.row_limit,
            )
        )
        try:
            result = q.execute_query(
                con,
                """
                SELECT
                    count(*) AS total_observations,
                    count(DISTINCT tracked_wine_id) AS wines_with_prices,
                    count(DISTINCT retailer_name) AS retailers,
                    min(observed_at)::DATE AS earliest,
                    max(observed_at)::DATE AS latest
                FROM price_observations
            """,
                row_limit=settings.query.row_limit,
            )
            print()
            print(result)
        except Exception:
            pass
    elif sub == "scan":
        print("Price scanning is agent-driven. Use the cellarbrain-price-tracker agent.")


def _cmd_recalc(args: argparse.Namespace, settings: Settings) -> None:
    """Recompute calculated fields from existing Parquet files."""
    from .computed import (
        classify_cellar,
        compute_age_years,
        compute_drinking_status,
        compute_price_tier,
        convert_to_default_currency,
    )

    out = pathlib.Path(args.output or settings.paths.data_dir)
    current_year = datetime.now(UTC).year
    tiers = settings.price_tiers
    rules = settings.cellar_rules
    currency = settings.currency

    # Read existing Parquet
    wines = writer.read_parquet_rows("wine", out)
    bottles = writer.read_parquet_rows("bottle", out)
    cellars = writer.read_parquet_rows("cellar", out)

    if not wines:
        print("No wine data found — run etl first.", file=sys.stderr)
        sys.exit(1)

    # Build cellar lookup for is_onsite
    cellar_id_to_name = {c["cellar_id"]: c["name"] for c in cellars}

    # Recompute wine-level fields
    wine_changes = 0
    for w in wines:
        old = (w.get("drinking_status"), w.get("age_years"), w.get("list_price"), w.get("price_tier"))
        # Currency normalisation — must happen before price_tier
        w["list_price"] = convert_to_default_currency(
            w.get("original_list_price"),
            w.get("original_list_currency"),
            currency.default,
            currency.rates,
        )
        w["list_currency"] = currency.default if w.get("original_list_price") is not None else None
        w["drinking_status"] = compute_drinking_status(
            w.get("drink_from"),
            w.get("drink_until"),
            w.get("optimal_from"),
            w.get("optimal_until"),
            current_year,
        )
        w["age_years"] = compute_age_years(w.get("vintage"), current_year)
        w["price_tier"] = compute_price_tier(w.get("list_price"), tiers)
        new = (w["drinking_status"], w["age_years"], w["list_price"], w["price_tier"])
        if old != new:
            wine_changes += 1

    # Recompute bottle-level fields
    bottle_changes = 0
    for b in bottles:
        old = (b.get("is_onsite"), b.get("is_in_transit"), b.get("purchase_price"))
        # Currency normalisation
        b["purchase_price"] = convert_to_default_currency(
            b.get("original_purchase_price"),
            b.get("original_purchase_currency"),
            currency.default,
            currency.rates,
        )
        b["purchase_currency"] = currency.default
        cellar_name = cellar_id_to_name.get(b.get("cellar_id"))
        cls = classify_cellar(cellar_name, rules)
        b["is_onsite"] = cls == "onsite"
        b["is_in_transit"] = cls == "in_transit"
        new = (b["is_onsite"], b["is_in_transit"], b["purchase_price"])
        if old != new:
            bottle_changes += 1

    # Write updated Parquet
    writer.write_parquet("wine", wines, out)
    writer.write_parquet("bottle", bottles, out)

    # Regenerate dossiers for changed wines
    if wine_changes:
        # Build minimal entities dict for dossier generation
        entities = {
            "wine": wines,
            "winery": writer.read_parquet_rows("winery", out),
            "appellation": writer.read_parquet_rows("appellation", out),
            "grape": writer.read_parquet_rows("grape", out),
            "wine_grape": writer.read_parquet_rows("wine_grape", out),
            "bottle": bottles,
            "cellar": cellars,
            "provider": writer.read_parquet_rows("provider", out),
            "tasting": writer.read_parquet_rows("tasting", out),
            "pro_rating": writer.read_parquet_rows("pro_rating", out),
            "tracked_wine": writer.read_parquet_rows("tracked_wine", out),
        }
        md_paths = markdown.generate_dossiers(entities, out, current_year)
        print(f"Regenerated {len(md_paths)} dossier(s)")
        if entities["tracked_wine"]:
            comp_paths = companion_markdown.generate_companion_dossiers(
                entities,
                out,
                settings,
            )
            if comp_paths:
                print(f"Regenerated {len(comp_paths)} companion dossier(s)")

    print(f"Recalc complete: {wine_changes} wine(s), {bottle_changes} bottle(s) updated")


# ---------------------------------------------------------------------------
# Rederive food tags subcommand
# ---------------------------------------------------------------------------


def _cmd_rederive_food_tags(args: argparse.Namespace, settings: Settings) -> None:
    """Re-derive food_tags and food_groups from existing food_pairings prose."""
    from .dossier_ops import (
        _auto_derive_food_data,
        _merge_food_groups,
        _merge_food_tags,
        _should_auto_tag,
    )

    out = pathlib.Path(args.output or settings.paths.data_dir)
    wines_dir = out / "wines"

    if not wines_dir.exists():
        print("No dossier directory found — run etl first.", file=sys.stderr)
        sys.exit(1)

    if not _should_auto_tag(settings, str(out)):
        print(
            "Food tag derivation disabled or catalogue missing.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Regex to extract food_pairings section prose (between agent fences)
    # The food_pairings section uses heading "Recommended Pairings" with
    # tag "agent:research" — fences are: <!-- source: agent:research -->
    _section_re = re.compile(
        r"### Recommended Pairings\n"
        r".*?"
        r"<!-- source: agent:research -->\n"
        r"(.*?)"
        r"<!-- source: agent:research — end -->",
        re.DOTALL,
    )
    _category_re = re.compile(r"^category:\s*(\S+)", re.MULTILINE)
    _food_tags_re = re.compile(
        r"^food_tags:\s*(?:\[]\s*\n|(\n(?:\s+-\s+.+\n)*))",
        re.MULTILINE,
    )
    _food_groups_re = re.compile(
        r"^food_groups:\s*(?:\[]\s*\n|(\n(?:\s+-\s+.+\n)*))",
        re.MULTILINE,
    )

    count = 0
    skipped = 0

    for dossier_path in sorted(wines_dir.rglob("*.md")):
        text = dossier_path.read_text(encoding="utf-8")

        # Extract food_pairings prose
        m = _section_re.search(text)
        if not m:
            skipped += 1
            continue

        prose = m.group(1).strip()
        if not prose or "Pending agent action" in prose:
            skipped += 1
            continue

        # Extract wine category
        cat_m = _category_re.search(text)
        wine_category = cat_m.group(1) if cat_m else None

        # Clear existing food_tags and food_groups
        text = _food_tags_re.sub("food_tags: []\n", text)
        text = _food_groups_re.sub("food_groups: []\n", text)

        # Re-derive
        new_tags, new_groups = _auto_derive_food_data(
            prose,
            str(out),
            settings,
            wine_category=wine_category,
        )

        if new_tags:
            text = _merge_food_tags(text, new_tags)
        if new_groups:
            text = _merge_food_groups(text, new_groups)

        if args.dry_run:
            if new_tags or new_groups:
                print(
                    f"  {dossier_path.name}: {len(new_tags)} tags, {len(new_groups)} groups",
                )
                count += 1
        else:
            dossier_path.write_text(text, encoding="utf-8")
            count += 1

    verb = "Would update" if args.dry_run else "Updated"
    print(f"{verb} {count} dossier(s), skipped {skipped}.")


# ---------------------------------------------------------------------------
# Logs subcommand
# ---------------------------------------------------------------------------


def _cmd_logs(args: argparse.Namespace, settings: Settings) -> None:
    """Query the MCP observability log store."""
    import duckdb

    db_path = settings.logging.log_db
    if db_path is None:
        db_path = str(pathlib.Path(settings.paths.data_dir) / "logs" / "cellarbrain-logs.duckdb")

    if not pathlib.Path(db_path).exists():
        print(f"Log store not found: {db_path}", file=sys.stderr)
        print("The MCP server creates this automatically on first run.", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect(db_path, read_only=True)
    hours = args.since
    cutoff = f"now() - INTERVAL '{hours} HOUR'"

    if args.prune:
        con.close()
        con = duckdb.connect(db_path, read_only=False)
        from .observability import EventCollector

        collector = EventCollector.__new__(EventCollector)
        collector._db = con
        collector._config = settings.logging
        collector._buffer = __import__("collections").deque()
        deleted = collector.prune()
        print(f"Pruned {deleted} events older than {settings.logging.retention_days} days.")
        con.close()
        return

    if args.errors:
        rows = con.execute(f"""
            SELECT started_at, name, error_type, error_message, duration_ms
            FROM tool_events
            WHERE status = 'error' AND started_at >= {cutoff}
            ORDER BY started_at DESC
            LIMIT {args.tail}
        """).fetchall()
        if not rows:
            print("No errors found.")
            return
        print(f"{'Timestamp':<22} {'Tool':<25} {'Error':<20} {'Message':<40} {'ms':>8}")
        print("-" * 115)
        for ts, name, etype, emsg, ms in rows:
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if ts else ""
            print(f"{ts_str:<22} {name:<25} {etype or '':<20} {(emsg or '')[:40]:<40} {ms:>8.0f}")
        return

    if args.usage:
        rows = con.execute(f"""
            SELECT name, COUNT(*) AS calls,
                   ROUND(AVG(duration_ms), 1) AS avg_ms,
                   ROUND(MAX(duration_ms), 1) AS max_ms,
                   SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errors
            FROM tool_events
            WHERE started_at >= {cutoff}
            GROUP BY name
            ORDER BY calls DESC
        """).fetchall()
        if not rows:
            print("No events found.")
            return
        print(f"{'Tool':<30} {'Calls':>8} {'Avg ms':>10} {'Max ms':>10} {'Errors':>8}")
        print("-" * 66)
        for name, calls, avg, mx, errs in rows:
            print(f"{name:<30} {calls:>8} {avg:>10.1f} {mx:>10.1f} {errs:>8}")
        return

    if args.latency:
        rows = con.execute(f"""
            SELECT name,
                   ROUND(AVG(duration_ms), 1) AS avg_ms,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ms), 1) AS p50,
                   ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms), 1) AS p95,
                   ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms), 1) AS p99,
                   ROUND(MAX(duration_ms), 1) AS max_ms,
                   COUNT(*) AS calls
            FROM tool_events
            WHERE started_at >= {cutoff}
            GROUP BY name
            ORDER BY avg_ms DESC
        """).fetchall()
        if not rows:
            print("No events found.")
            return
        print(f"{'Tool':<30} {'Avg':>8} {'P50':>8} {'P95':>8} {'P99':>8} {'Max':>8} {'Calls':>8}")
        print("-" * 78)
        for name, avg, p50, p95, p99, mx, calls in rows:
            print(f"{name:<30} {avg:>8.1f} {p50:>8.1f} {p95:>8.1f} {p99:>8.1f} {mx:>8.1f} {calls:>8}")
        return

    if args.sessions:
        rows = con.execute(f"""
            SELECT session_id,
                   MIN(started_at) AS first_event,
                   MAX(ended_at) AS last_event,
                   COUNT(*) AS events,
                   COUNT(DISTINCT turn_id) AS turns,
                   SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errors
            FROM tool_events
            WHERE started_at >= {cutoff}
            GROUP BY session_id
            ORDER BY first_event DESC
        """).fetchall()
        if not rows:
            print("No sessions found.")
            return
        print(f"{'Session':<34} {'First Event':<22} {'Events':>8} {'Turns':>8} {'Errors':>8}")
        print("-" * 80)
        for sid, first, _last, evts, turns, errs in rows:
            ts_str = first.strftime("%Y-%m-%d %H:%M:%S") if first else ""
            print(f"{sid[:32]:<34} {ts_str:<22} {evts:>8} {turns:>8} {errs:>8}")
        return

    # Default: tail recent events
    rows = con.execute(f"""
        SELECT started_at, event_type, name, status, duration_ms, agent_name
        FROM tool_events
        WHERE started_at >= {cutoff}
        ORDER BY started_at DESC
        LIMIT {args.tail}
    """).fetchall()
    if not rows:
        print("No events found.")
        return
    print(f"{'Timestamp':<22} {'Type':<10} {'Name':<25} {'Status':<8} {'ms':>8} {'Agent':<15}")
    print("-" * 88)
    for ts, etype, name, status, ms, agent in rows:
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if ts else ""
        print(f"{ts_str:<22} {etype:<10} {name:<25} {status:<8} {ms:>8.0f} {agent or '':<15}")


def _cmd_dashboard(args: argparse.Namespace, settings: Settings) -> None:
    """Start the web explorer dashboard."""
    try:
        from .dashboard import create_app
    except ImportError:
        print("Dashboard dependencies not installed.", file=sys.stderr)
        print("Run: pip install cellarbrain[dashboard]", file=sys.stderr)
        sys.exit(1)

    import uvicorn

    db_path = settings.logging.log_db
    if db_path is None:
        db_path = str(pathlib.Path(settings.paths.data_dir) / "logs" / "cellarbrain-logs.duckdb")

    if not pathlib.Path(db_path).exists():
        # Create an empty log store so the dashboard can start without prior MCP usage
        pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        import duckdb

        _con = duckdb.connect(db_path)
        from .observability import _CREATE_TABLE_SQL

        _con.execute(_CREATE_TABLE_SQL)
        _con.close()

    data_dir = settings.paths.data_dir
    app = create_app(
        log_db_path=db_path,
        data_dir=data_dir,
        dashboard_config=settings.dashboard,
    )
    host = "127.0.0.1"
    port = args.port

    if args.open:
        import webbrowser

        webbrowser.open(f"http://{host}:{port}")

    uvicorn.run(app, host=host, port=port, log_level="info", reload=getattr(args, "dev", False))


def _cmd_doctor(args: argparse.Namespace, settings: Settings) -> None:
    """Run diagnostic health checks."""

    from .doctor import run_doctor

    checks = args.check if args.check else None
    report = run_doctor(settings, checks=checks)

    if args.json:
        import json

        data = [
            {"name": c.name, "severity": c.severity.value, "message": c.message, "remedy": c.remedy}
            for c in report.checks
        ]
        print(json.dumps(data, indent=2))
    else:
        print(report.summary())

    from .doctor import Severity

    if args.strict:
        is_ok = report.worst_severity in (Severity.OK, Severity.INFO)
    else:
        is_ok = report.ok
    sys.exit(0 if is_ok else 1)


def _cmd_backup(args: argparse.Namespace, settings: Settings) -> None:
    """Create a backup or list existing backups."""
    from .backup import create_backup, list_backups

    backup_dir = args.backup_dir or settings.backup.backup_dir

    if args.list_backups:
        backups = list_backups(backup_dir)
        if not backups:
            print("No backups found.")
            return
        print(f"{'Name':<40} {'Size':>8} {'Files':>6}")
        print("-" * 56)
        for b in backups:
            print(f"{b['name']:<40} {b['size_mb']:>6.1f}MB {b['file_count']:>6}")
        return

    path = create_backup(
        settings.paths.data_dir,
        backup_dir,
        include_sommelier=args.include_sommelier,
        include_logs=args.include_logs,
        max_backups=settings.backup.max_backups,
    )
    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"Backup created: {path}")
    print(f"  Size: {size_mb:.1f} MB")


def _cmd_restore(args: argparse.Namespace, settings: Settings) -> None:
    """Restore from a backup archive."""
    from .backup import list_backups, restore_backup

    backup_dir = settings.backup.backup_dir

    if args.archive:
        archive_path = pathlib.Path(args.archive)
    else:
        backups = list_backups(backup_dir)
        if not backups:
            print("Error: no backups found", file=sys.stderr)
            sys.exit(1)
        archive_path = backups[0]["path"]
        print(f"Using most recent backup: {archive_path.name}")

    count = restore_backup(
        archive_path,
        settings.paths.data_dir,
        dry_run=args.dry_run,
    )
    if args.dry_run:
        print(f"Would restore {count} files (dry-run)")
    else:
        print(f"Restored {count} files from {archive_path.name}")


def _cmd_ingest(args: argparse.Namespace, settings: Settings) -> None:
    """Start the email ingestion daemon or run a single poll cycle."""
    try:
        from .email_poll import IngestDaemon, poll_once
    except ImportError:
        print("Ingest dependencies not installed.", file=sys.stderr)
        print("Run: pip install cellarbrain[ingest]", file=sys.stderr)
        sys.exit(1)

    config = settings.ingest

    if args.setup:
        _ingest_setup()
        return

    if args.once:
        count = poll_once(config, settings, dry_run=args.dry_run)
        if count < 0:
            print(f"Failed {-count} batch(es) (ETL error — messages left unprocessed).")
            sys.exit(1)
        print(f"Processed {count} batch(es).")
        sys.exit(0)

    daemon = IngestDaemon(config, settings)
    daemon.run(dry_run=args.dry_run)


def _ingest_setup() -> None:
    """Interactive credential storage for the ingest daemon."""
    from .email_poll.credentials import store_credentials

    print("Cellarbrain Ingest — Credential Setup")
    print("=" * 40)
    user = input("IMAP username (email address): ").strip()
    if not user:
        print("Error: username is required.", file=sys.stderr)
        sys.exit(1)
    password = input("IMAP password (app-specific password): ").strip()
    if not password:
        print("Error: password is required.", file=sys.stderr)
        sys.exit(1)

    try:
        store_credentials(user, password)
        print(f"\nCredentials stored for '{user}'.")
        print("You can now run: cellarbrain ingest")
    except ImportError:
        print("Error: 'keyring' package not installed.", file=sys.stderr)
        print("Run: pip install cellarbrain[ingest]", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Sommelier helpers
# ---------------------------------------------------------------------------


def _rebuild_wine_index(model, data_dir: pathlib.Path, wine_dir: pathlib.Path, settings: Settings) -> None:
    """Build the wine FAISS index from cellar Parquet data."""
    from . import query as q
    from .sommelier.index import build_index
    from .sommelier.text_builder import (
        build_wine_text,
        extract_tasting_summary,
        normalise_category,
    )

    con = q.get_connection(str(data_dir))
    rows = con.execute("""
        SELECT wine_id, wine_name, country, region, grapes, category,
               food_tags, food_groups
        FROM wines_full
        WHERE bottles_stored > 0
    """).fetchall()

    if not rows:
        print("  No wines with stored bottles — skipping wine index.")
        return

    # Pre-load dossier tasting summaries for embedding enrichment
    wines_dir = data_dir / settings.paths.wines_subdir
    tasting_cache: dict[int, str | None] = {}
    if wines_dir.is_dir():
        for dossier_path in wines_dir.rglob("*.md"):
            try:
                wid = int(dossier_path.stem.split("-", 1)[0])
            except (ValueError, IndexError):
                continue
            text = dossier_path.read_text(encoding="utf-8")
            tasting_cache[wid] = extract_tasting_summary(text)

    texts: list[str] = []
    ids: list[str] = []
    for row in rows:
        wine_id, wine_name, country, region, grapes, category, food_tags, food_groups = row
        category = normalise_category(category)
        food_pairings = None
        if food_tags:
            food_pairings = ", ".join(food_tags[:8])
        groups_str = None
        if food_groups:
            groups_str = ", ".join(food_groups[:8])
        texts.append(
            build_wine_text(
                full_name=wine_name,
                country=country,
                region=region,
                grape_summary=grapes,
                category=category,
                tasting_notes=tasting_cache.get(wine_id),
                food_pairings=food_pairings,
                food_groups=groups_str,
            )
        )
        ids.append(str(wine_id))

    idx_path = wine_dir / "wine.index"
    ids_path = wine_dir / "wine_ids.json"
    n = build_index(texts, ids, model, idx_path, ids_path)
    enriched = sum(1 for wid in ids if tasting_cache.get(int(wid)))
    print(f"  Wine index: {n} wines indexed ({enriched} with tasting notes)")


def _cmd_train_model(args: argparse.Namespace, settings: Settings) -> None:
    from .sommelier.training import train_model

    cfg = settings.sommelier
    output = args.output or cfg.model_dir
    epochs = args.epochs or cfg.training_epochs
    batch_size = args.batch_size or cfg.training_batch_size

    print(f"Training sommelier model ({epochs} epochs, batch {batch_size})...")
    print(f"  Base model: {cfg.base_model}")
    print(f"  Training data: {cfg.pairing_dataset}")
    metrics = train_model(
        pairing_parquet=cfg.pairing_dataset,
        output_dir=output,
        base_model=cfg.base_model,
        epochs=epochs,
        batch_size=batch_size,
        warmup_ratio=cfg.warmup_ratio,
        eval_split=cfg.eval_split,
    )
    print(f"Model saved to {output}")
    for key, value in metrics.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.4f}")
        else:
            print(f"  {key}: {value}")


def _cmd_retrain_model(args: argparse.Namespace, settings: Settings) -> None:
    """Incrementally retrain the sommelier model on accumulated data.

    Loads the existing fine-tuned model (not the base model) and continues
    training for a few epochs on the full pairing dataset.  Then rebuilds
    both FAISS indexes.
    """
    from .sommelier.training import train_model

    cfg = settings.sommelier

    model_dir = pathlib.Path(cfg.model_dir)
    if not model_dir.exists():
        print(f"Error: no trained model found at {model_dir}")
        print("Run `cellarbrain train-model` first for initial training.")
        sys.exit(1)

    dataset_path = pathlib.Path(cfg.pairing_dataset)
    if not dataset_path.exists():
        print(f"Error: pairing dataset not found at {dataset_path}")
        sys.exit(1)

    epochs = args.epochs or 5
    batch_size = args.batch_size or cfg.training_batch_size

    import pyarrow.parquet as pq

    total_pairs = pq.read_metadata(dataset_path).num_rows
    print(f"Retraining sommelier model ({epochs} epochs, batch {batch_size})...")
    print(f"  Existing model: {cfg.model_dir}")
    print(f"  Training data: {cfg.pairing_dataset} ({total_pairs} pairs)")

    metrics = train_model(
        pairing_parquet=cfg.pairing_dataset,
        output_dir=cfg.model_dir,
        base_model=cfg.model_dir,
        epochs=epochs,
        batch_size=batch_size,
        warmup_ratio=cfg.warmup_ratio,
        eval_split=cfg.eval_split,
    )

    print(f"Model retrained and saved to {cfg.model_dir}")
    for key, value in metrics.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.4f}")
        else:
            print(f"  {key}: {value}")

    print("Rebuilding FAISS indexes...")
    _cmd_rebuild_indexes(
        argparse.Namespace(wine_only=False, food_only=False),
        settings,
    )


def _cmd_rebuild_indexes(args: argparse.Namespace, settings: Settings) -> None:
    from .sommelier.index import build_index
    from .sommelier.model import load_model

    cfg = settings.sommelier
    model = load_model(cfg.model_dir)

    if not args.wine_only:
        import pyarrow.parquet as pq

        from .sommelier.text_builder import build_food_text

        table = pq.read_table(cfg.food_catalogue)
        texts: list[str] = []
        ids: list[str] = []
        for i in range(table.num_rows):
            row = {col: table.column(col)[i].as_py() for col in table.column_names}
            texts.append(
                build_food_text(
                    dish_name=row["dish_name"],
                    description=row["description"],
                    ingredients=row["ingredients"],
                    cuisine=row["cuisine"],
                    weight_class=row["weight_class"],
                    protein=row["protein"],
                    flavour_profile=row["flavour_profile"],
                )
            )
            ids.append(row["dish_id"])
        n = build_index(texts, ids, model, cfg.food_index, cfg.food_ids)
        print(f"Food index: {n} dishes indexed")

    if not args.food_only:
        data_dir = pathlib.Path(settings.paths.data_dir)
        wine_dir = data_dir / cfg.wine_index_dir
        wine_dir.mkdir(parents=True, exist_ok=True)
        _rebuild_wine_index(model, data_dir, wine_dir, settings)

    print("Index rebuild complete.")


if __name__ == "__main__":
    main()
