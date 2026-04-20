"""CLI entry point for the Cellarbrain ETL pipeline and agent tools."""

from __future__ import annotations

import argparse
import logging
import pathlib
import sys
import warnings
from datetime import datetime, timezone

from . import companion_markdown, incremental, markdown, vinocell_reader, transform, validate as val, writer
from .computed import compute_is_in_transit, compute_is_onsite, convert_to_default_currency, enrich_wines
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
        wines_rows, winery_lk, appellation_lk,
        id_assignments=wine_id_assignments,
    )
    wine_grapes = transform.build_wine_grapes(wines_rows, wine_lk, grape_lk)
    bottles = transform.build_bottles(bottles_rows, wine_lk, cellar_lk, provider_lk)
    bottles_gone = transform.build_bottles_gone(
        bottles_gone_rows, wine_lk, provider_lk, start_id=len(bottles) + 1,
    )
    all_bottles = bottles + bottles_gone
    tastings = transform.build_tastings(wines_rows, wine_lk)
    pro_ratings = transform.build_pro_ratings(
        wines_rows, bottles_rows, wine_lk, bottles_gone_rows,
    )

    print(f"  Wines:       {len(wines)}")
    print(f"  Wine-grapes: {len(wine_grapes)}")
    print(f"  Bottles:     {len(all_bottles)} (stored: {len(bottles)}, gone: {len(bottles_gone)})")
    print(f"  Tastings:    {len(tastings)}")
    print(f"  Pro ratings: {len(pro_ratings)}")

    # --- Computed wine properties ---
    grape_id_to_name = {g["grape_id"]: g["name"] for g in grapes}
    winery_id_to_name = {w["winery_id"]: w["name"] for w in wineries}
    appellation_id_to_dict = {
        a["appellation_id"]: a for a in appellations
    }
    enrich_wines(wines, wine_grapes, grape_id_to_name,
                 winery_id_to_name, appellation_id_to_dict,
                 settings=settings, current_year=current_year)

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
    offsite = settings.offsite_cellars if settings else ()
    in_transit = settings.in_transit_cellars if settings else ()
    cellar_id_to_name = {c["cellar_id"]: c["name"] for c in cellars}
    for b in all_bottles:
        cellar_name = cellar_id_to_name.get(b.get("cellar_id"))
        b["is_onsite"] = compute_is_onsite(cellar_name, offsite, in_transit)
        b["is_in_transit"] = compute_is_in_transit(cellar_name, in_transit)

    # --- Tracked wines (wishlist / favorites) ---
    appellation_by_wine = {
        w["wine_id"]: w.get("appellation_id") for w in wines
    }
    tracked_wines, tracked_lk = transform.build_tracked_wines(
        wines, appellation_by_wine,
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
        settings = Settings()
    out = pathlib.Path(output_dir)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
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
        wines_csv, bottles_csv, bottles_gone_csv,
        settings=settings, current_year=now.year,
        wine_id_assignments=matches,
        wines_rows=wines_rows,
    )

    skip = frozenset({"wine"})

    if sync_mode:
        print("\nRunning incremental sync...")
        entities, change_log, fk_remappings = incremental.sync(
            entities, out, run_id, now,
            identity_config=settings.identity,
            skip_entities=skip,
        )
        run_type = "incremental"
    else:
        print("\nAnnotating full load...")
        entities, change_log = incremental.annotate_full_load(
            entities, out, run_id, now,
            skip_entities=skip,
        )
        fk_remappings = {}
        run_type = "full"

    # --- Annotate wines via slug-based classification ---
    entities["wine"], wine_changes = incremental.annotate_classified_wines(
        entities["wine"], existing_wines, matches, deletions, run_id, now,
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

    _is_wine = lambda c: c["entity_type"] == "wine"
    wines_inserted = sum(1 for c in change_log if _is_wine(c) and c["change_type"] == "insert")
    wines_updated = sum(1 for c in change_log if _is_wine(c) and c["change_type"] == "update")
    wines_deleted = sum(1 for c in change_log if _is_wine(c) and c["change_type"] == "delete")
    wines_renamed = sum(1 for c in change_log if _is_wine(c) and c["change_type"] == "rename")
    print(f"  Wine-level: {wines_inserted} new, {wines_updated} updated, {wines_deleted} deleted, {wines_renamed} renamed")

    # --- Assign dossier paths (after ID stabilisation) ---
    transform.assign_dossier_paths(entities)
    transform.assign_tracked_dossier_paths(entities, settings)

    # --- Write entity Parquet files ---
    print(f"\nWriting Parquet files to {out}/")
    paths = writer.write_all(entities, out)
    for name, path in paths.items():
        print(f"  {name}: {path}")

    # --- Write ETL tracking tables ---
    finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
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
            c["entity_id"]
            for c in change_log
            if c["entity_type"] == "wine" and c["change_type"] == "delete"
        }
        if wine_ids_to_regen:
            md_paths = markdown.generate_dossiers(
                entities, out, current_year, wine_ids=wine_ids_to_regen,
            )
            print(f"\nRegenerated {len(md_paths)} wine dossier(s)")
        else:
            print("\nNo wine dossiers to regenerate")
        if deleted_ids:
            del_paths = markdown.mark_deleted_dossiers(
                out, deleted_ids, run_id, now.isoformat(),
            )
            print(f"  Marked {len(del_paths)} dossier(s) as deleted")
    else:
        md_paths = markdown.generate_dossiers(entities, out, current_year)
        print(f"\nGenerated {len(md_paths)} wine dossier(s)")

    # --- Generate companion dossiers for tracked wines ---
    if entities.get("tracked_wine"):
        comp_paths = companion_markdown.generate_companion_dossiers(
            entities, out, settings,
        )
        if comp_paths:
            print(f"Generated {len(comp_paths)} companion dossier(s)")

    # --- Rebuild sommelier wine index (if model exists) ---
    if settings.sommelier.enabled:
        model_dir = pathlib.Path(settings.sommelier.model_dir)
        if (model_dir / "config.json").exists():
            print("\nRebuilding sommelier wine index...")
            from .sommelier.model import load_model

            model = load_model(str(model_dir))
            wine_dir = out / settings.sommelier.wine_index_dir
            wine_dir.mkdir(parents=True, exist_ok=True)
            _rebuild_wine_index(model, out, wine_dir, settings)
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
            "Direct CSV arguments are deprecated. "
            "Use: cellarbrain etl <wines.csv> <bottles.csv> [-o output] [--sync]",
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
        from .query import DataStaleError, QueryError
        from .dossier_ops import ProtectedSectionError, WineNotFoundError

        if isinstance(exc, (
            ValueError, FileNotFoundError, UnicodeDecodeError,
            DataStaleError, QueryError, WineNotFoundError, ProtectedSectionError,
        )):
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

    ok = _run_handler(lambda: run(
        args.wines_csv, args.bottles_csv, args.output,
        sync_mode=args.sync, bottles_gone_csv=args.bottles_gone_csv,
    ))
    sys.exit(0 if ok else 1)


def _subcommand_main(argv: list[str]) -> None:
    """Subcommand-based interface."""
    parser = argparse.ArgumentParser(
        prog="cellarbrain",
        description="Cellarbrain wine cellar toolkit — ETL, query, and agent interface.",
    )
    parser.add_argument(
        "-c", "--config", default=None,
        help="Path to cellarbrain.toml configuration file.",
    )
    parser.add_argument(
        "-d", "--data-dir", default=None,
        help="Path to the output directory containing Parquet files.",
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0,
        help="Increase log verbosity (-v for INFO, -vv for DEBUG).",
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true",
        help="Suppress warnings (set log level to ERROR).",
    )
    parser.add_argument(
        "--log-file", default=None,
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
        "--format", choices=["table", "csv", "json"], default="table",
        dest="output_format",
    )

    # --- stats ---
    sts = sub.add_parser("stats", help="Cellar statistics")
    sts.add_argument(
        "--by",
        choices=[
            "country", "region", "category", "vintage", "winery",
            "grape", "cellar", "provider", "status",
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
        "--sections", nargs="+", metavar="SECTION",
        help="Section keys to include (default: all). E.g. --sections identity producer_profile",
    )

    # --- mcp ---
    mcp_parser = sub.add_parser("mcp", help="Start MCP server")
    mcp_parser.add_argument(
        "--transport", choices=["stdio", "sse"], default="stdio",
    )
    mcp_parser.add_argument("--port", type=int, default=8080)

    # --- recalc ---
    recalc = sub.add_parser("recalc", help="Recompute calculated fields")
    recalc.add_argument("-o", "--output", default=None)

    # --- wishlist ---
    wish = sub.add_parser("wishlist", help="Wishlist & price tracking")
    wish_sub = wish.add_subparsers(dest="wishlist_command")

    wish_alerts = wish_sub.add_parser("alerts", help="Show wishlist alerts")
    wish_alerts.add_argument("--days", type=int, default=None,
                             help="Alert window in days (default from settings)")

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
        "wishlist": _cmd_wishlist,
        "train-model": _cmd_train_model,
        "retrain-model": _cmd_retrain_model,
        "rebuild-indexes": _cmd_rebuild_indexes,
    }
    handler = handlers[args.command]
    _run_handler(lambda: handler(args, settings))


def _cmd_etl(args: argparse.Namespace, settings: Settings) -> None:
    output = args.output or settings.paths.data_dir
    ok = run(
        args.wines_csv, args.bottles_csv, output,
        sync_mode=args.sync, bottles_gone_csv=args.bottles_gone_csv,
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
    from . import dossier_ops, query as q

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
        print(q.execute_query(con, """
            SELECT
                count(*) AS tracked_wines,
                list_sort(list(DISTINCT category)) AS categories,
                list_sort(list(DISTINCT country)) AS countries
            FROM tracked_wines
        """, row_limit=settings.query.row_limit))
        try:
            result = q.execute_query(con, """
                SELECT
                    count(*) AS total_observations,
                    count(DISTINCT tracked_wine_id) AS wines_with_prices,
                    count(DISTINCT retailer_name) AS retailers,
                    min(observed_at)::DATE AS earliest,
                    max(observed_at)::DATE AS latest
                FROM price_observations
            """, row_limit=settings.query.row_limit)
            print()
            print(result)
        except Exception:
            pass
    elif sub == "scan":
        print(
            "Price scanning is agent-driven. "
            "Use the cellarbrain-price-tracker agent."
        )


def _cmd_recalc(args: argparse.Namespace, settings: Settings) -> None:
    """Recompute calculated fields from existing Parquet files."""
    from .computed import (
        compute_age_years,
        compute_drinking_status,
        compute_is_in_transit,
        compute_is_onsite,
        compute_price_tier,
        convert_to_default_currency,
    )

    out = pathlib.Path(args.output or settings.paths.data_dir)
    current_year = datetime.now(timezone.utc).year
    tiers = settings.price_tiers
    offsite = settings.offsite_cellars
    in_transit = settings.in_transit_cellars
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
        old = (w.get("drinking_status"), w.get("age_years"),
               w.get("list_price"), w.get("price_tier"))
        # Currency normalisation — must happen before price_tier
        w["list_price"] = convert_to_default_currency(
            w.get("original_list_price"),
            w.get("original_list_currency"),
            currency.default,
            currency.rates,
        )
        w["list_currency"] = (
            currency.default
            if w.get("original_list_price") is not None
            else None
        )
        w["drinking_status"] = compute_drinking_status(
            w.get("drink_from"), w.get("drink_until"),
            w.get("optimal_from"), w.get("optimal_until"),
            current_year,
        )
        w["age_years"] = compute_age_years(w.get("vintage"), current_year)
        w["price_tier"] = compute_price_tier(w.get("list_price"), tiers)
        new = (w["drinking_status"], w["age_years"],
               w["list_price"], w["price_tier"])
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
        b["is_onsite"] = compute_is_onsite(cellar_name, offsite, in_transit)
        b["is_in_transit"] = compute_is_in_transit(cellar_name, in_transit)
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
                entities, out, settings,
            )
            if comp_paths:
                print(f"Regenerated {len(comp_paths)} companion dossier(s)")

    print(f"Recalc complete: {wine_changes} wine(s), {bottle_changes} bottle(s) updated")


# ---------------------------------------------------------------------------
# Sommelier helpers
# ---------------------------------------------------------------------------


def _rebuild_wine_index(model, data_dir: pathlib.Path, wine_dir: pathlib.Path,
                        settings: Settings) -> None:
    """Build the wine FAISS index from cellar Parquet data."""
    from . import query as q
    from .sommelier.index import build_index
    from .sommelier.text_builder import build_wine_text

    con = q.get_connection(str(data_dir))
    rows = con.execute("""
        SELECT wine_id, wine_name, country, region, grapes, category
        FROM wines_full
        WHERE bottles_stored > 0
    """).fetchall()

    if not rows:
        print("  No wines with stored bottles — skipping wine index.")
        return

    texts: list[str] = []
    ids: list[str] = []
    for row in rows:
        wine_id, wine_name, country, region, grapes, category = row
        texts.append(build_wine_text(
            full_name=wine_name,
            country=country,
            region=region,
            grape_summary=grapes,
            category=category,
        ))
        ids.append(str(wine_id))

    idx_path = wine_dir / "wine.index"
    ids_path = wine_dir / "wine_ids.json"
    n = build_index(texts, ids, model, idx_path, ids_path)
    print(f"  Wine index: {n} wines indexed")


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
    for key, val in metrics.items():
        if isinstance(val, float):
            print(f"  {key}: {val:.4f}")
        else:
            print(f"  {key}: {val}")


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
    for key, val in metrics.items():
        if isinstance(val, float):
            print(f"  {key}: {val:.4f}")
        else:
            print(f"  {key}: {val}")

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
            texts.append(build_food_text(
                dish_name=row["dish_name"],
                description=row["description"],
                ingredients=row["ingredients"],
                cuisine=row["cuisine"],
                weight_class=row["weight_class"],
                protein=row["protein"],
                flavour_profile=row["flavour_profile"],
            ))
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
