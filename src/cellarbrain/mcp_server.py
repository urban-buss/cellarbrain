"""MCP server exposing cellarbrain data tools, resources, and prompts.

Provides 16 thin data tools â€” no LLM reasoning. Uses FastMCP from the
Python MCP SDK with stdio transport.
"""

from __future__ import annotations

import functools
import importlib.metadata
import inspect
import json
import logging
import os
import pathlib
import re as _re
import sys
import time
from datetime import UTC
from typing import TYPE_CHECKING

import anyio
from mcp.server.fastmcp import FastMCP

from . import query as q
from .dossier_ops import (
    ProtectedSectionError,
    TrackedWineNotFoundError,
    WineNotFoundError,
)
from .dossier_ops import (
    pending_companion_research as _pending_companion,
)
from .dossier_ops import (
    pending_research as _pending_research,
)
from .dossier_ops import (
    read_companion_dossier as _read_companion,
)
from .dossier_ops import (
    read_dossier as _read_dossier,
)
from .dossier_ops import (
    read_dossier_sections as _read_dossier_sections,
)
from .dossier_ops import (
    stale_research as _stale_research,
)
from .dossier_ops import (
    update_companion_dossier as _update_companion,
)
from .dossier_ops import (
    update_dossier as _update_dossier,
)
from .query import DataStaleError, QueryError
from .settings import Settings, load_settings

if TYPE_CHECKING:
    import duckdb

# ---------------------------------------------------------------------------
# Server identity
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "cellarbrain",
    instructions=(
        "Wine cellar data access â€” query, browse, and update a personal "
        "wine collection stored as Parquet + Markdown. "
        "Data I/O only; reasoning stays in the agent."
    ),
)


def _data_dir() -> str:
    return _load_mcp_settings().paths.data_dir


def _effective_fmt(format: str | None) -> str:
    """Return effective output format: explicit param or settings default."""
    if format is not None:
        return format
    return _load_mcp_settings().output.default_format


def _load_mcp_settings() -> Settings:
    """Load settings once for the MCP server process."""
    global _mcp_settings
    if _mcp_settings is None:
        _mcp_settings = load_settings()
        logger.info(
            "MCP server starting â€” data_dir=%s",
            _mcp_settings.paths.data_dir,
        )
        from .migrate import CURRENT_VERSION, read_schema_version

        _schema_ver = read_schema_version(pathlib.Path(_mcp_settings.paths.data_dir))
        if _schema_ver < CURRENT_VERSION:
            logger.warning(
                "Schema version %d < %d - some queries may fail. Run `cellarbrain migrate` to update.",
                _schema_ver,
                CURRENT_VERSION,
            )

        from .observability import init_observability

        collector = init_observability(
            _mcp_settings.logging,
            _mcp_settings.paths.data_dir,
            subsystem="mcp",
        )
        if collector.lock_conflict_pid is not None:
            import sys

            print(
                f"WARNING: Observability disabled — log store locked by PID {collector.lock_conflict_pid}. "
                f"Set a separate [logging] log_db in cellarbrain.toml to fix.",
                file=sys.stderr,
            )
        # Warn about missing sommelier paths for early diagnosis
        if _mcp_settings.sommelier.enabled:
            import pathlib as _pl

            _som = _mcp_settings.sommelier
            for _label, _p in [
                ("pairing_dataset", _som.pairing_dataset),
                ("food_catalogue", _som.food_catalogue),
            ]:
                if not _pl.Path(_p).exists():
                    logger.warning(
                        "Sommelier %s not found: %s",
                        _label,
                        _p,
                    )
    return _mcp_settings


_mcp_settings: Settings | None = None


# ---------------------------------------------------------------------------
# Query-result cache — memoises expensive read-only tool results.
# Invalidated on ETL reload via invalidate_connections().
# ---------------------------------------------------------------------------

from .query_cache import QueryCache  # noqa: E402

_query_cache = QueryCache(max_size=128)


def _init_query_cache() -> None:
    """Re-initialise cache with current settings (call after settings load)."""
    global _query_cache
    settings = _load_mcp_settings()
    if not settings.cache.enabled:
        _query_cache = QueryCache(max_size=0)
    else:
        _query_cache = QueryCache(
            max_size=settings.cache.max_size,
            ttl_seconds=settings.cache.ttl_seconds,
        )


# ---------------------------------------------------------------------------
# DuckDB connection caching — avoids per-call view creation overhead.
# Call invalidate_connections() after data changes (ETL reload).
# ---------------------------------------------------------------------------

_cached_connection: duckdb.DuckDBPyConnection | None = None
_cached_agent_connection: duckdb.DuckDBPyConnection | None = None
_parquet_fingerprint: float | None = None  # max mtime of parquet files


def _compute_parquet_fingerprint() -> float:
    """Return the max mtime of parquet files in data_dir (0.0 if none)."""
    data_dir = pathlib.Path(_data_dir())
    try:
        mtimes = [f.stat().st_mtime for f in data_dir.glob("*.parquet")]
        return max(mtimes) if mtimes else 0.0
    except OSError:
        return 0.0


def _get_connection():
    global _cached_connection, _parquet_fingerprint
    current_fp = _compute_parquet_fingerprint()
    if _cached_connection is not None and _parquet_fingerprint != current_fp:
        # Parquet files changed — invalidate everything
        invalidate_connections()
    if _cached_connection is None:
        _cached_connection = q.get_connection(_data_dir())
        _parquet_fingerprint = current_fp
    return _cached_connection


def _get_agent_connection():
    """Agent-restricted connection — 6 views only."""
    global _cached_agent_connection
    if _cached_agent_connection is None:
        _cached_agent_connection = q.get_agent_connection(_data_dir())
    return _cached_agent_connection


def invalidate_connections() -> None:
    """Close and discard cached DuckDB connections and query cache.

    Called after ETL reload so the next query picks up fresh Parquet data.
    """
    global _cached_connection, _cached_agent_connection, _parquet_fingerprint
    if _cached_connection is not None:
        _cached_connection.close()
        _cached_connection = None
    if _cached_agent_connection is not None:
        _cached_agent_connection.close()
        _cached_agent_connection = None
    _parquet_fingerprint = None
    _query_cache.invalidate()


# ---------------------------------------------------------------------------
# Custom search synonyms (MCP-writable layer)
# ---------------------------------------------------------------------------

_CUSTOM_SYNONYMS_FILE = "search-synonyms.json"
_custom_synonyms_cache: tuple[float, dict[str, str]] | None = None


def _custom_synonyms_path() -> pathlib.Path:
    return pathlib.Path(_data_dir()) / _CUSTOM_SYNONYMS_FILE


def _load_custom_synonyms() -> dict[str, str]:
    """Load agent-defined synonyms from data_dir, with mtime caching."""
    global _custom_synonyms_cache
    path = _custom_synonyms_path()
    if not path.exists():
        return {}
    mtime = path.stat().st_mtime
    if _custom_synonyms_cache is not None and _custom_synonyms_cache[0] == mtime:
        return _custom_synonyms_cache[1]
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    _custom_synonyms_cache = (mtime, data)
    return data


def _save_custom_synonyms(synonyms: dict[str, str]) -> None:
    """Write agent-defined synonyms to data_dir, invalidating cache."""
    global _custom_synonyms_cache
    path = _custom_synonyms_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(synonyms, f, ensure_ascii=False, indent=2, sort_keys=True)
    _custom_synonyms_cache = None


def _effective_synonyms() -> dict[str, str]:
    """Merge built-in + TOML synonyms with custom JSON overlay."""
    base = dict(_load_mcp_settings().search.synonyms)
    custom = _load_custom_synonyms()
    if custom:
        base.update(custom)
    return base


# ---------------------------------------------------------------------------
# Custom currency rates (MCP-writable layer)
# ---------------------------------------------------------------------------

_CUSTOM_RATES_FILE = "currency-rates.json"
_custom_rates_cache: tuple[float, dict[str, float]] | None = None
_CURRENCY_CODE_RE = _re.compile(r"^[A-Z]{3}$")


def _custom_rates_path() -> pathlib.Path:
    return pathlib.Path(_data_dir()) / _CUSTOM_RATES_FILE


def _load_custom_rates() -> dict[str, float]:
    """Load agent-defined currency rates from data_dir, with mtime caching."""
    global _custom_rates_cache
    path = _custom_rates_path()
    if not path.exists():
        return {}
    mtime = path.stat().st_mtime
    if _custom_rates_cache is not None and _custom_rates_cache[0] == mtime:
        return _custom_rates_cache[1]
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    _custom_rates_cache = (mtime, data)
    return data


def _save_custom_rates(rates: dict[str, float]) -> None:
    """Write agent-defined currency rates to data_dir, invalidating cache."""
    global _custom_rates_cache
    path = _custom_rates_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rates, f, ensure_ascii=False, indent=2, sort_keys=True)
    _custom_rates_cache = None


def _effective_rates() -> dict[str, float]:
    """Merge TOML rates with custom JSON overlay."""
    base = dict(_load_mcp_settings().currency.rates)
    custom = _load_custom_rates()
    if custom:
        base.update(custom)
    return base


def _record_search_event(query: str, telemetry: object) -> None:
    """Record a search event to the observability log store."""
    from .observability import SearchEvent, get_collector

    collector = get_collector()
    if collector is None:
        return

    import uuid
    from datetime import datetime

    now = datetime.now(UTC)
    event = SearchEvent(
        event_id=uuid.uuid4().hex,
        session_id=collector.session_id,
        turn_id=collector.turn_id,
        query=query,
        normalized_query=query.lower().strip(),
        result_count=getattr(telemetry, "result_count", 0),
        intent_matched=getattr(telemetry, "intent_matched", False),
        used_soft_and=getattr(telemetry, "used_soft_and", False),
        used_fuzzy=getattr(telemetry, "used_fuzzy", False),
        used_phonetic=getattr(telemetry, "used_phonetic", False),
        used_suggestions=getattr(telemetry, "used_suggestions", False),
        started_at=now,
        duration_ms=0.0,  # timing captured at tool level by _log_tool
        client_id=None,
    )
    collector.record_search(event)


logger = logging.getLogger(__name__)


def _extract_meta(kwargs: dict) -> dict:
    """Pop meta from kwargs and return it (or empty dict)."""
    meta = kwargs.pop("meta", None)
    return meta if isinstance(meta, dict) else {}


def _build_event(
    fn_name: str,
    event_type: str,
    kwargs: dict,
    meta: dict,
    t0: float,
    result: str | None,
    exc: BaseException | None,
) -> None:
    """Construct a ToolEvent and emit it to the collector."""
    from .mcp_responses import ToolResponse, data_size
    from .observability import ToolEvent, get_collector

    collector = get_collector()
    if collector is None:
        return

    import uuid
    from datetime import datetime

    now = datetime.now(UTC)
    elapsed = (time.perf_counter() - t0) * 1000
    started = datetime.fromtimestamp(
        now.timestamp() - elapsed / 1000,
        tz=UTC,
    )

    # Filter out internal kwargs for parameter logging
    logged_params = {k: v for k, v in kwargs.items() if k != "meta" and not k.startswith("_")}
    params_str = ", ".join(f"{k}={v!r}" for k, v in logged_params.items())

    # Detect soft errors: tool caught exception internally, returned "Error: ..."
    is_soft_error = exc is None and isinstance(result, str) and result.startswith("Error:")
    soft_msg = result[len("Error:") :].strip() if is_soft_error else None

    if exc is not None:
        status = "error"
        error_type = type(exc).__name__
        error_message = str(exc)
    elif is_soft_error:
        status = "error"
        error_type = "HandledError"
        error_message = soft_msg
    else:
        status = "ok"
        error_type = None
        error_message = None

    # Extract structured payload metadata from ToolResponse
    resp_data_size = data_size(result) if isinstance(result, str) else None
    resp_metadata_keys: str | None = None
    resp_cache_hit: bool | None = None
    if isinstance(result, ToolResponse) and result.metadata:
        resp_metadata_keys = ",".join(sorted(result.metadata.keys()))
        resp_cache_hit = result.metadata.get("cache_hit")

    event = ToolEvent(
        event_id=uuid.uuid4().hex,
        session_id=collector.session_id,
        turn_id=meta.get("turn_id", collector.turn_id),
        event_type=event_type,
        name=fn_name,
        started_at=started,
        ended_at=now,
        duration_ms=elapsed,
        status=status,
        request_id=meta.get("request_id"),
        parameters=params_str if params_str else None,
        error_type=error_type,
        error_message=error_message,
        result_size=len(result) if isinstance(result, str) else None,
        agent_name=meta.get("agent_name"),
        trace_id=meta.get("trace_id"),
        client_id=meta.get("client_id"),
        data_size=resp_data_size,
        metadata_keys=resp_metadata_keys,
        cache_hit=resp_cache_hit,
    )
    collector.emit(event)


def _log_tool(fn):
    """Log MCP tool invocations and timing via the observability layer.

    Supports both sync and async tool handlers.  Async handlers are
    awaited transparently so the decorator works on sommelier tools
    that offload blocking work to a thread.
    """
    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def async_wrapper(*args, **kwargs):
            meta = _extract_meta(kwargs)
            params = ", ".join(f"{k}={v!r}" for k, v in kwargs.items() if k != "meta")
            logger.info("tool=%s %s", fn.__name__, params)
            t0 = time.perf_counter()
            exc_caught: BaseException | None = None
            try:
                result = await fn(*args, **kwargs)
            except Exception as e:
                exc_caught = e
                raise
            else:
                elapsed = (time.perf_counter() - t0) * 1000
                if isinstance(result, str) and result.startswith("Error:"):
                    logger.warning("tool=%s error=%r", fn.__name__, result)
                else:
                    logger.info("tool=%s completed elapsed_ms=%.0f", fn.__name__, elapsed)
                return result
            finally:
                _build_event(fn.__name__, "tool", kwargs, meta, t0, locals().get("result"), exc_caught)

        return async_wrapper

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        meta = _extract_meta(kwargs)
        params = ", ".join(f"{k}={v!r}" for k, v in kwargs.items() if k != "meta")
        logger.info("tool=%s %s", fn.__name__, params)
        t0 = time.perf_counter()
        exc_caught: BaseException | None = None
        try:
            result = fn(*args, **kwargs)
        except Exception as e:
            exc_caught = e
            raise
        else:
            elapsed = (time.perf_counter() - t0) * 1000
            if isinstance(result, str) and result.startswith("Error:"):
                logger.warning("tool=%s error=%r", fn.__name__, result)
            else:
                logger.info("tool=%s completed elapsed_ms=%.0f", fn.__name__, elapsed)
            return result
        finally:
            _build_event(fn.__name__, "tool", kwargs, meta, t0, locals().get("result"), exc_caught)

    return wrapper


def _log_resource(fn):
    """Log MCP resource reads via the observability layer."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        logger.info("resource=%s", fn.__name__)
        t0 = time.perf_counter()
        exc_caught: BaseException | None = None
        try:
            result = fn(*args, **kwargs)
        except Exception as e:
            exc_caught = e
            raise
        else:
            elapsed = (time.perf_counter() - t0) * 1000
            logger.info("resource=%s completed elapsed_ms=%.0f", fn.__name__, elapsed)
            return result
        finally:
            _build_event(fn.__name__, "resource", kwargs, {}, t0, locals().get("result"), exc_caught)

    return wrapper


def _log_prompt(fn):
    """Log MCP prompt renders via the observability layer."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        logger.info("prompt=%s", fn.__name__)
        t0 = time.perf_counter()
        exc_caught: BaseException | None = None
        try:
            result = fn(*args, **kwargs)
        except Exception as e:
            exc_caught = e
            raise
        else:
            elapsed = (time.perf_counter() - t0) * 1000
            logger.info("prompt=%s completed elapsed_ms=%.0f", fn.__name__, elapsed)
            return result
        finally:
            _build_event(fn.__name__, "prompt", kwargs, {}, t0, locals().get("result"), exc_caught)

    return wrapper


# ---------------------------------------------------------------------------
# Unified registration decorators — combine logging + wire conversion + FastMCP
# ---------------------------------------------------------------------------


def _tool(name: str | None = None, **tool_kwargs):
    """Register an MCP tool with logging, ToolResponse wire conversion, and FastMCP.

    Returns the *logged* function so that direct calls in tests get a
    ToolResponse/str.  FastMCP receives a wrapper that converts
    ToolResponse → CallToolResult on the wire.

    Structured output validation is disabled (structured_output=False) because
    our tools return CallToolResult with custom structuredContent that doesn't
    match the auto-generated output model the MCP library would derive from
    the ``-> str`` return annotation.
    """

    def decorator(fn):
        from .mcp_responses import to_call_tool_result

        logged_fn = _log_tool(fn)

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def wire_wrapper(*args, **kwargs):
                result = await logged_fn(*args, **kwargs)
                return to_call_tool_result(result)
        else:

            @functools.wraps(fn)
            def wire_wrapper(*args, **kwargs):
                result = logged_fn(*args, **kwargs)
                return to_call_tool_result(result)

        mcp.add_tool(wire_wrapper, name=name, structured_output=False, **tool_kwargs)
        return logged_fn

    return decorator


def _resource(uri: str, **resource_kwargs):
    """Register an MCP resource with logging and FastMCP."""

    def decorator(fn):
        logged_fn = _log_resource(fn)
        mcp.resource(uri, **resource_kwargs)(logged_fn)
        return logged_fn

    return decorator


def _prompt(**prompt_kwargs):
    """Register an MCP prompt with logging and FastMCP."""

    def decorator(fn):
        logged_fn = _log_prompt(fn)
        mcp.prompt(**prompt_kwargs)(logged_fn)
        return logged_fn

    return decorator


# ---------------------------------------------------------------------------
# Tools â€” 16 thin data primitives
# ---------------------------------------------------------------------------


@_tool()
def query_cellar(sql: str, format: str | None = None, meta: dict | None = None) -> str:
    """Run a read-only SQL query against the wine cellar database.

    All views are pre-joined â€” no JOINs needed.

    Slim views (use for most queries):

    - **wines** (20 cols): wine_id, wine_name, vintage, winery_name,
      category, country, region, subregion, primary_grape, blend_type,
      drinking_status, price_tier, price, style_tags, bottles_stored,
      bottles_on_order, bottles_consumed, is_favorite, is_wishlist,
      tracked_wine_id.
    - **bottles** (17 cols): bottle_id, wine_id, wine_name, vintage,
      winery_name, category, country, region, primary_grape,
      drinking_status, price_tier, price, status, cellar_name, shelf,
      output_date, output_type.

    Full views (scores, value, technical detail):

    - **wines_full** (~61 cols): all wines columns plus classification,
      alcohol_pct, cellar_value, best_pro_score, avg_tasting_score, etc.
    - **bottles_full** (~37 cols): all bottles columns plus purchase_price,
      provider_name, is_onsite, is_in_transit, etc.

    Convenience views (all return slim columns):

    - **wines_stored** (20 cols): wines WHERE bottles_stored > 0.
    - **wines_on_order** (20 cols): wines WHERE bottles_on_order > 0.
    - **wines_drinking_now** (20 cols): wines WHERE optimal/drinkable + in stock.
    - **bottles_stored** (17 cols): stored bottles (excludes in-transit).
    - **bottles_consumed** (17 cols): consumed/gone bottles.
    - **bottles_on_order** (17 cols): on order / in-transit bottles.

    View selection guide:

    - Default to **wines_stored** / **bottles_stored** for cellar questions
      (what's in stock, most expensive stored wine, cellar value, etc.).
    - Use **wines** / **bottles** when you need the full catalogue including
      consumed and on-order wines (e.g. purchase history, past tastings).
    - Use **wines_full** / **bottles_full** only when you need:
      - Wine details: alcohol_pct, grapes, volume_ml, classification,
        ageing, serving_temp, winemaking_notes.
      - Bottle details: provider_name, purchase_date, purchase_comment,
        volume_ml, is_in_transit.
      - Aggregates: cellar_value, on_order_value, tasting/rating scores.

    Read the schema://views resource for the complete column reference.
    Only SELECT statements are allowed.

    Args:
        sql: A DuckDB-compatible SQL SELECT statement.
    """
    from .mcp_responses import ToolResponse

    try:
        con = _get_agent_connection()
        text, data = q.execute_query_structured(con, sql, row_limit=_load_mcp_settings().query.row_limit)
        return ToolResponse(text, data=data, metadata={"row_count": data["row_count"]})
    except (QueryError, DataStaleError) as exc:
        return ToolResponse.error(str(exc))


@_tool()
def cellar_stats(
    group_by: str | None = None,
    limit: int = 20,
    sort_by: str | None = None,
    format: str | None = None,
    meta: dict | None = None,
) -> str:
    """Get summary statistics about the wine cellar.

    Returns bottle counts, value totals, category breakdown, and drinking window status.

    Args:
        group_by: Optional dimension to group by. One of:
                  "country", "region", "category", "vintage", "winery",
                  "grape", "cellar", "provider", "status", "on_order".
                  If omitted, returns the overall cellar summary.
        limit:    Maximum groups for grouped output (default 20).
                  A rollup '(other)' row is appended when results exceed limit.
                  Set to 0 for unlimited.
        sort_by:  Sort grouped results by column. One of: "bottles" (default),
                  "value", "wines", "volume". Default for vintage is chronological.
                  Ignored when group_by is omitted.
        format:   Output format: "markdown" (default) or "plain" (no tables, iMessage-friendly).
    """
    from .mcp_responses import ToolResponse

    try:
        con = _get_connection()
        currency = _load_mcp_settings().currency.default
        cache_params = {"group_by": group_by, "limit": limit, "sort_by": sort_by, "currency": currency}

        def _compute():
            return q.cellar_stats(
                con, group_by=group_by, currency=currency, limit=limit, sort_by=sort_by, fmt=_effective_fmt(format)
            )

        text, cache_hit = _query_cache.get_or_compute("cellar_stats", cache_params, _compute)
        data = {"group_by": group_by, "currency": currency}
        return ToolResponse(text, data=data, metadata={"cache_hit": cache_hit})
    except (ValueError, DataStaleError) as exc:
        return ToolResponse.error(str(exc))


@_tool()
def cellar_churn(
    period: str | None = None,
    year: int | None = None,
    month: int | None = None,
    format: str | None = None,
    meta: dict | None = None,
) -> str:
    """Get cellar churn (roll-forward) analysis showing inventory movement.

    Shows beginning balance, purchases, consumption, and ending balance
    for bottles, value (CHF), and volume (L).

    Args:
        period: Optional granularity for multi-period output.
                "month" â€” month-by-month for the given year (or current year).
                "year"  â€” year-by-year since first purchase.
                If omitted, returns a single-period summary.
        year:   Year to report on (default: current year).
        month:  Month 1â€“12. Used for single-period monthly churn.
                Ignored when period is "month" or "year".

    Examples:
        cellar_churn()                          â†’ current month
        cellar_churn(year=2025, month=3)        â†’ March 2025
        cellar_churn(year=2025)                 â†’ full year 2025
        cellar_churn(period="month")            â†’ month-by-month, current year
        cellar_churn(period="month", year=2025) â†’ month-by-month, 2025
        cellar_churn(period="year")             â†’ year-by-year, all years
    """
    try:
        con = _get_connection()
        return q.cellar_churn(
            con,
            period=period,
            year=year,
            month=month,
            currency=_load_mcp_settings().currency.default,
            fmt=_effective_fmt(format),
        )
    except (ValueError, DataStaleError) as exc:
        return f"Error: {exc}"


@_tool()
def consumption_velocity(
    months: int = 6,
    meta: dict | None = None,
) -> str:
    """Get consumption velocity analysis — acquisition vs consumption rates.

    Shows month-by-month bottles acquired and consumed, average rates,
    net cellar growth per month, and a 12-month projection of cellar size.

    Use this to understand whether the cellar is growing or shrinking,
    and at what pace.

    Args:
        months: Number of past months to analyse (default 6, max 60).
                The current (incomplete) month is excluded.
    """
    from .mcp_responses import ToolResponse

    if months < 1 or months > 60:
        return ToolResponse.error("months must be between 1 and 60.")

    try:
        con = _get_connection()
        text, data = q.consumption_velocity(con, months=months, currency=_load_mcp_settings().currency.default)
        return ToolResponse(text, data=data, metadata={"months": months})
    except (ValueError, DataStaleError) as exc:
        return ToolResponse.error(str(exc))


@_tool()
def cellar_gaps(
    dimension: str | None = None,
    months: int = 12,
    meta: dict | None = None,
) -> str:
    """Identify underrepresented categories (gaps) in the cellar.

    Analyses the cellar across four dimensions to find areas that may
    need attention or restocking:

    - **region**: Regions consumed frequently but with 0-1 bottles remaining.
    - **grape**: Grape varieties with bottles stored but none in a
      drinkable window for the next 12 months.
    - **price_tier**: Price tiers with stored bottles but no ready-to-drink
      options tonight.
    - **vintage**: Aging-worthy categories (red, fortified) with empty
      vintage decades.

    Args:
        dimension: Analyse a single dimension — one of "region", "grape",
                   "price_tier", "vintage". If omitted, returns all four.
        months: Lookback months for consumption frequency inference
                (default 12, max 60). Used by the region dimension.
    """
    from .mcp_responses import ToolResponse

    try:
        con = _get_connection()
        text, data = q.cellar_gaps(con, dimension=dimension, months=months)
        return ToolResponse(text, data=data, metadata={"dimension": dimension, "months": months})
    except (ValueError, DataStaleError) as exc:
        return ToolResponse.error(str(exc))


@_tool()
def cellar_info(verbose: bool = False, meta: dict | None = None) -> str:
    """Return general information about this cellar installation.

    Returns version, default currency, data directory, ETL freshness,
    inventory counts, and configuration metadata. Set verbose=True for
    extended diagnostics including Python/MCP versions, currency rates,
    and additional counts.

    Args:
        verbose: Include extended diagnostics (Python/MCP versions,
                 currency rates, companion dossier counts, etc.).
    """
    s = _load_mcp_settings()

    # -- Version -----------------------------------------------------------
    try:
        version = importlib.metadata.version("cellarbrain")
    except importlib.metadata.PackageNotFoundError:
        version = "unknown"

    # -- Section 1: Cellar Info (always) -----------------------------------
    lines = [
        "## Cellar Info\n",
        "| Setting | Value |",
        "|:--------|:------|",
        f"| Version | {version} |",
        f"| Default currency | {s.currency.default} |",
        f"| Data directory | {s.paths.data_dir} |",
        f"| Config file | {s.config_source or '(built-in defaults)'} |",
        f"| Row limit | {s.query.row_limit} |",
        f"| Search limit | {s.query.search_limit} |",
    ]

    if verbose:
        lines.append(f"| Python | {sys.version.split()[0]} |")
        try:
            mcp_version = importlib.metadata.version("mcp")
        except importlib.metadata.PackageNotFoundError:
            mcp_version = "unknown"
        lines.append(f"| MCP SDK | {mcp_version} |")
        if s.offsite_cellars:
            lines.append(f"| Offsite cellars | {', '.join(s.offsite_cellars)} |")
        if s.in_transit_cellars:
            lines.append(f"| In-transit cellars | {', '.join(s.in_transit_cellars)} |")

    # -- Sections 2 & 3 need a DB connection -------------------------------
    try:
        con = _get_connection()
    except (DataStaleError, Exception):
        lines.append("\n*No ETL data available â€” run `cellarbrain etl` first.*")
        return "\n".join(lines)

    # -- Section 2: Data Freshness -----------------------------------------
    try:
        etl_row = con.execute(
            "SELECT started_at, run_type, "
            "total_inserts, total_updates, total_deletes "
            "FROM etl_run ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        if etl_row:
            started, run_type, inserts, updates, deletes = etl_row
            ts = started.strftime("%Y-%m-%d %H:%M UTC")
            lines.append("\n## Data Freshness\n")
            lines.append("| Metric | Value |")
            lines.append("|:-------|:------|")
            lines.append(f"| Last ETL run | {ts} ({run_type}) |")
            lines.append(f"| Last changeset | +{inserts} inserts, ~{updates} updates, -{deletes} deletes |")
            if verbose:
                total_runs = con.execute("SELECT COUNT(*) FROM etl_run").fetchone()[0]
                lines.append(f"| Total ETL runs | {total_runs} |")
        else:
            lines.append("\n*No ETL runs recorded.*")
    except Exception:
        lines.append("\n*ETL data not available.*")

    # -- Section 3: Inventory ----------------------------------------------
    try:
        data_dir = pathlib.Path(s.paths.data_dir)
        wine_pq = (data_dir / "wine.parquet").as_posix()
        bottle_pq = (data_dir / "bottle.parquet").as_posix()
        tracked_pq = (data_dir / "tracked_wine.parquet").as_posix()

        inv = con.execute(
            f"SELECT "
            f"(SELECT COUNT(*) FROM read_parquet('{wine_pq}') WHERE NOT is_deleted), "
            f"(SELECT COUNT(*) FROM read_parquet('{bottle_pq}') WHERE status = 'stored'), "
            f"(SELECT COUNT(*) FROM read_parquet('{tracked_pq}') WHERE NOT is_deleted)"
        ).fetchone()
        wines, bottles, tracked = inv

        wines_dir = data_dir / s.paths.wines_subdir
        dossier_count = 0
        for subdir in (s.paths.cellar_subdir, s.paths.archive_subdir):
            d = wines_dir / subdir
            if d.is_dir():
                dossier_count += len(list(d.glob("*.md")))

        price_obs = 0
        try:
            po_pq = (data_dir / "price_observation.parquet").as_posix()
            price_obs = con.execute(f"SELECT COUNT(*) FROM read_parquet('{po_pq}')").fetchone()[0]
        except Exception:
            pass

        lines.append("\n## Inventory\n")
        lines.append("| Entity | Count |")
        lines.append("|:-------|------:|")
        lines.append(f"| Wines | {wines:,} |")
        lines.append(f"| Bottles in cellar | {bottles:,} |")
        lines.append(f"| Tracked wines | {tracked:,} |")
        lines.append(f"| Dossiers | {dossier_count:,} |")
        if price_obs:
            lines.append(f"| Price observations | {price_obs:,} |")

        if verbose:
            tracked_dir = data_dir / s.wishlist.wishlist_subdir
            companion_count = len(list(tracked_dir.glob("*.md"))) if tracked_dir.is_dir() else 0
            lines.append(f"| Companion dossiers | {companion_count:,} |")
    except Exception:
        lines.append("\n*Inventory data not available.*")

    # -- Verbose: Currency Rates -------------------------------------------
    if verbose and s.currency.rates:
        lines.append("\n## Currency Rates\n")
        lines.append("| Currency | Rate (per CHF) |")
        lines.append("|:---------|---------------:|")
        for code, rate in sorted(s.currency.rates.items()):
            lines.append(f"| {code} | {rate} |")

    from .mcp_responses import ToolResponse

    text = "\n".join(lines)
    data = {
        "version": version,
        "data_dir": s.paths.data_dir,
        "currency": s.currency.default,
    }
    return ToolResponse(text, data=data)


@_tool()
def find_wine(
    query: str, limit: int | None = None, fuzzy: bool = False, format: str | None = None, meta: dict | None = None
) -> str:
    """Search for wines in the cellar by name, winery, region, grape, style, or any attribute.

    Tokenises multi-word queries: each word must match at least one of 12
    searchable columns â€” wine name, winery, country, region, subregion,
    classification, category, primary grape, subcategory, sweetness,
    effervescence, specialty â€” plus vintage exact match (AND across words,
    OR across columns). Uses accent-insensitive matching so "Chateau" finds
    "ChÃ¢teau". Set fuzzy=True for tolerant matching that also handles typos
    via Jaro-Winkler similarity.

    Automatically translates German search terms (e.g. "Rotwein" â†’ "red",
    "Schweiz" â†’ "Switzerland") and strips common noise words ("Weingut",
    "Jahrgang") using a configurable synonym dictionary.

    Supports attribute-based intent queries that filter or sort by structured
    fields. Recognised patterns include:
    - Drinking status: "ready to drink", "too young", "past optimal"
    - Price: "under 30", "budget"
    - Ratings: "top rated", "best rated", "highest rated"
    - Stock: "low stock", "last bottle", "running low"
    Intents can be combined with free-text terms (e.g. "Barolo ready to drink").

    Recognises wine-style concept keywords and expands them to match
    concrete wine names. Supported concepts:
    - "sparkling" â†’ Prosecco, Champagne, CrÃ©mant, Cava, etc.
    - "dessert" â†’ Sauternes, Tokaji, Moscato, Eiswein, etc.
    - "fortified" â†’ Port, Sherry, Madeira, Marsala, etc.
    - "sweet" â†’ Sauternes, Tokaji, late harvest, etc.
    - "natural" â†’ natural wine, vin nature
    System concepts: "tracked", "favorite"/"favourite", "wishlist" filter
    by the corresponding boolean/FK attribute.
    German terms (Schaumwein, SÃ¼sswein, Dessertwein, LikÃ¶rwein, Sekt) are
    mapped to concepts via the synonym layer.

    When strict AND returns no results and â‰¥2 text tokens remain, a
    soft-AND fallback fires: requires at least one token to match and
    ranks by match count. Intent/system filters stay mandatory. Results
    are prefixed with "Partial match" to signal relaxed matching.

    Args:
        query: Search terms (e.g., "Spanish Tempranillo", "Bordeaux 2018",
               "ready to drink", "under 30", "top rated Barolo").
        limit: Maximum results to return.
        fuzzy: Enable fuzzy matching for typo tolerance (slower, off by default).
    """
    from .mcp_responses import ToolResponse

    try:
        con = _get_agent_connection()
        effective_limit = limit if limit is not None else _load_mcp_settings().query.search_limit
        if effective_limit < 1:
            return ToolResponse.error("limit must be at least 1.")

        cache_params = {"query": query, "limit": effective_limit, "fuzzy": fuzzy}
        cache_key = _query_cache.make_key("find_wine", cache_params)
        found, cached = _query_cache.get(cache_key)
        if found:
            result_text, telemetry_data = cached
            _record_search_event(query, telemetry_data)
            data = {
                "query": query,
                "result_count": telemetry_data.result_count,
                "used_fuzzy": telemetry_data.used_fuzzy,
                "used_phonetic": telemetry_data.used_phonetic,
                "used_soft_and": telemetry_data.used_soft_and,
            }
            return ToolResponse(
                result_text, data=data, metadata={"row_count": telemetry_data.result_count, "cache_hit": True}
            )

        result, telemetry = q.find_wine_with_telemetry(
            con,
            query,
            limit=effective_limit,
            fuzzy=fuzzy,
            synonyms=_effective_synonyms(),
            fmt=_effective_fmt(format),
        )
        _query_cache.put(cache_key, (result, telemetry))
        _record_search_event(query, telemetry)
        data = {
            "query": query,
            "result_count": telemetry.result_count,
            "used_fuzzy": telemetry.used_fuzzy,
            "used_phonetic": telemetry.used_phonetic,
            "used_soft_and": telemetry.used_soft_and,
        }
        return ToolResponse(result, data=data, metadata={"row_count": telemetry.result_count, "cache_hit": False})
    except (QueryError, DataStaleError) as exc:
        return ToolResponse.error(str(exc))


@_tool()
def wine_suggestions(query: str, limit: int = 5, meta: dict | None = None) -> str:
    """Suggest wine names similar to the query (autocomplete / "did you mean").

    Uses Jaro-Winkler similarity to find wine names that are close to the
    search term. Useful when a search returns no results and you want to
    offer alternatives to the user.

    Args:
        query: The search term to find suggestions for.
        limit: Maximum number of suggestions to return (default 5).
    """
    try:
        con = _get_agent_connection()
        settings = _load_mcp_settings()
        threshold = settings.search.suggest_threshold
        return q.suggest_wines(con, query, limit=limit, threshold=threshold)
    except (QueryError, DataStaleError) as exc:
        return f"Error: {exc}"


@_tool()
def search_stats(window_days: int = 7, limit: int = 20, meta: dict | None = None) -> str:
    """Show search query statistics from the observability log.

    Returns tables of: top queries, zero-result queries, fuzzy/phonetic
    rescued queries, and average search latency over the specified window.

    Args:
        window_days: Number of days to look back (default 7).
        limit: Maximum rows per table (default 20).
    """
    from .observability import get_collector

    collector = get_collector()
    if collector is None:
        return "Error: observability not initialised."
    if collector._db is None:
        return "Error: log store not available."
    collector.flush()
    db = collector._db
    try:
        # Top queries
        top_df = db.execute(
            f"SELECT normalized_query AS query, COUNT(*) AS count, "
            f"ROUND(AVG(result_count), 1) AS avg_results "
            f"FROM search_events "
            f"WHERE started_at >= now() - INTERVAL '{window_days} days' "
            f"GROUP BY normalized_query ORDER BY count DESC LIMIT {limit}"
        ).fetchdf()
        # Zero-result queries
        zero_df = db.execute(
            f"SELECT normalized_query AS query, COUNT(*) AS count "
            f"FROM search_events "
            f"WHERE started_at >= now() - INTERVAL '{window_days} days' "
            f"AND result_count = 0 "
            f"GROUP BY normalized_query ORDER BY count DESC LIMIT {limit}"
        ).fetchdf()
        # Fuzzy/phonetic rescued
        rescued_df = db.execute(
            f"SELECT normalized_query AS query, "
            f"CASE WHEN used_phonetic THEN 'phonetic' "
            f"WHEN used_fuzzy THEN 'fuzzy' "
            f"WHEN used_soft_and THEN 'soft-and' END AS rescue_type, "
            f"result_count "
            f"FROM search_events "
            f"WHERE started_at >= now() - INTERVAL '{window_days} days' "
            f"AND (used_fuzzy OR used_phonetic OR used_soft_and) "
            f"ORDER BY started_at DESC LIMIT {limit}"
        ).fetchdf()
        # Summary stats
        summary = db.execute(
            f"SELECT COUNT(*) AS total_searches, "
            f"ROUND(AVG(duration_ms), 1) AS avg_latency_ms, "
            f"SUM(CASE WHEN result_count = 0 THEN 1 ELSE 0 END) AS zero_result_count, "
            f"SUM(CASE WHEN used_fuzzy THEN 1 ELSE 0 END) AS fuzzy_rescues, "
            f"SUM(CASE WHEN used_phonetic THEN 1 ELSE 0 END) AS phonetic_rescues "
            f"FROM search_events "
            f"WHERE started_at >= now() - INTERVAL '{window_days} days'"
        ).fetchone()
    except Exception as exc:
        return f"Error: {exc}"

    parts = [f"## Search Statistics (last {window_days} days)\n"]
    if summary:
        parts.append(
            f"**Total searches:** {summary[0]} | "
            f"**Avg latency:** {summary[1]} ms | "
            f"**Zero-result:** {summary[2]} | "
            f"**Fuzzy rescues:** {summary[3]} | "
            f"**Phonetic rescues:** {summary[4]}\n"
        )
    if not top_df.empty:
        parts.append("\n### Top Queries\n")
        parts.append(_to_md_table(top_df))
    if not zero_df.empty:
        parts.append("\n### Zero-Result Queries\n")
        parts.append(_to_md_table(zero_df))
    if not rescued_df.empty:
        parts.append("\n### Rescued Queries (fuzzy/phonetic/soft-and)\n")
        parts.append(_to_md_table(rescued_df))
    return "\n".join(parts)


def _to_md_table(df) -> str:
    """Convert a pandas DataFrame to a Markdown table."""
    from tabulate import tabulate

    return tabulate(df, headers="keys", tablefmt="pipe", showindex=False)


@_tool()
def read_dossier(
    wine_id: int,
    sections: list[str] | None = None,
    meta: dict | None = None,
) -> str:
    """Read a wine dossier, optionally filtered to specific sections.

    When *sections* is omitted, returns the full dossier. When provided,
    returns frontmatter + H1 + subtitle + only the requested H2 sections.
    Raises an error for unrecognised section keys.

    Args:
        wine_id: The numeric wine ID (from query results).
        sections: Optional list of section keys to include. Valid keys:
            ETL: "identity", "origin", "grapes", "characteristics",
                 "drinking_window", "cellar_inventory", "purchase_history",
                 "consumption_history", "owner_notes"
            Mixed: "ratings_reviews", "tasting_notes", "food_pairings"
            Agent: "producer_profile", "vintage_report", "wine_description",
                   "market_availability", "similar_wines", "agent_log"
    """
    try:
        return _read_dossier_sections(wine_id, _data_dir(), sections=sections)
    except (WineNotFoundError, ValueError) as exc:
        return f"Error: {exc}"


@_tool()
def update_dossier(
    wine_id: int,
    section: str,
    content: str,
    agent_name: str = "research",
    sources: list[str] | None = None,
    confidence: str | None = None,
    meta: dict | None = None,
) -> str:
    """Update an agent-owned section in a wine dossier.

    Only agent-owned sections can be updated. ETL-owned sections are protected.
    The content must be valid Markdown. A log entry is automatically appended
    to the Agent Log section.

    Allowed sections:
    - producer_profile
    - vintage_report
    - wine_description
    - market_availability
    - similar_wines
    - ratings_reviews (agent subsection only)
    - tasting_notes (community subsection only)
    - food_pairings (agent subsection only)

    Args:
        wine_id: The numeric wine ID.
        section: Section key to update (see allowed list above).
        content: Markdown content to write into the section.
        agent_name: Name of the agent making the update (for audit trail).
        sources: Optional list of source URLs/names used for the research.
        confidence: Optional confidence level (e.g. "high", "medium", "low").
    """
    try:
        return _update_dossier(
            wine_id,
            section,
            content,
            _data_dir(),
            agent_name,
            sources=sources,
            confidence=confidence,
        )
    except (WineNotFoundError, ProtectedSectionError, ValueError) as exc:
        return f"Error: {exc}"


@_tool()
def get_format_siblings(wine_id: int, meta: dict | None = None) -> str:
    """Get format siblings for a wine (e.g. Standard + Magnum of the same wine).

    Returns a Markdown table listing all format variants that share the same
    wine identity (winery, name, vintage) but differ in bottle volume.

    Args:
        wine_id: The numeric wine ID to look up siblings for.
    """
    try:
        from . import query as _q

        con = _get_agent_connection()
        result = _q.format_siblings(con, wine_id)
        return result if result else f"Wine {wine_id} has no format siblings."
    except (QueryError, DataStaleError) as exc:
        return f"Error: {exc}"


@_tool()
def similar_wines(
    wine_id: int,
    limit: int = 5,
    include_gone: bool = False,
    meta: dict | None = None,
) -> str:
    """Find wines similar to a given wine based on structural attributes.

    Uses 6-signal weighted scoring: winery affinity, region/appellation,
    grape composition overlap, category match, price tier adjacency, and
    food group overlap. Returns a ranked Markdown table with scores and
    human-readable match signals explaining why each wine is similar.

    Results are capped at 2 per winery for diversity. Format siblings are
    excluded (use get_format_siblings for those). Minimum score threshold: 0.15.

    Args:
        wine_id: The target wine to find similar wines for.
        limit: Maximum similar wines to return (default 5, max 20).
        include_gone: Include wines with no bottles stored (default False).
    """
    try:
        from . import similarity as _sim

        con = _get_agent_connection()
        effective_limit = min(max(limit, 1), 20)
        return _sim.similar_wines(
            con,
            wine_id,
            _data_dir(),
            limit=effective_limit,
            include_gone=include_gone,
        )
    except (QueryError, DataStaleError) as exc:
        return f"Error: {exc}"


@_tool()
def batch_update_dossier(
    wine_ids: list[int],
    section: str,
    content: str,
    agent_name: str = "research",
    sources: list[str] | None = None,
    confidence: str | None = None,
    meta: dict | None = None,
) -> str:
    """Update an agent-owned dossier section for multiple wines at once.

    Useful for applying the same content (e.g. producer_profile) to all format
    variants of a wine. Each wine is updated independently; failures do not
    stop processing of remaining wines.

    Args:
        wine_ids: List of numeric wine IDs to update.
        section: Section key to update (same as update_dossier).
        content: Markdown content to write into the section.
        agent_name: Name of the agent making the update.
        sources: Optional list of source URLs/names used for the research.
        confidence: Optional confidence level (e.g. "high", "medium", "low").
    """
    data_dir = _data_dir()
    results: list[str] = []
    ok_count = 0
    for wid in wine_ids:
        try:
            _update_dossier(
                wid,
                section,
                content,
                data_dir,
                agent_name,
                sources=sources,
                confidence=confidence,
            )
            ok_count += 1
            results.append(f"  ✓ Wine {wid}")
        except (WineNotFoundError, ProtectedSectionError, ValueError) as exc:
            results.append(f"  ✗ Wine {wid}: {exc}")
    summary = f"Updated {ok_count}/{len(wine_ids)} dossiers for section '{section}'."
    return summary + "\n" + "\n".join(results)


@_tool()
def reload_data(mode: str = "sync", meta: dict | None = None) -> str:
    """Trigger a reload of the wine cellar data from CSV exports.

    Runs the cellarbrain ETL pipeline to re-process CSV files and update
    Parquet files and wine dossiers. Agent-owned sections in dossiers
    are preserved across reloads.

    Args:
        mode: "sync" for incremental (detects changes, preserves IDs) or
              "full" for complete reload. Default: "sync".
    """
    from . import cli as _cli

    settings = _load_mcp_settings()
    data_dir = settings.paths.data_dir
    raw_dir = os.path.join(os.path.dirname(data_dir), "raw")

    wines_csv = os.path.join(raw_dir, settings.paths.wines_filename)
    bottles_csv = os.path.join(raw_dir, settings.paths.bottles_filename)
    bottles_gone_csv = os.path.join(raw_dir, settings.paths.bottles_gone_filename)

    if not os.path.exists(wines_csv):
        from .mcp_responses import ToolResponse

        return ToolResponse.error(f"CSV file not found: {wines_csv}. Export from your cellar app first.")

    if not os.path.exists(bottles_csv):
        from .mcp_responses import ToolResponse

        return ToolResponse.error(f"CSV file not found: {bottles_csv}. Export from your cellar app first.")

    if not os.path.exists(bottles_gone_csv):
        from .mcp_responses import ToolResponse

        return ToolResponse.error(f"CSV file not found: {bottles_gone_csv}. Export from your cellar app first.")

    try:
        # Reconfigure stdout to handle Unicode on Windows (MCP stdio transport
        # may use the system codepage which cannot encode arrows/em-dashes).
        import sys

        if hasattr(sys.stdout, "reconfigure"):
            try:
                sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass
        ok = _cli.run(
            wines_csv,
            bottles_csv,
            data_dir,
            sync_mode=(mode == "sync"),
            bottles_gone_csv=bottles_gone_csv,
            settings=_load_mcp_settings(),
        )
        invalidate_connections()
        from .mcp_responses import ToolResponse

        text = f"ETL completed ({'passed' if ok else 'failed validation'}). Mode: {mode}."
        return ToolResponse(text, data={"mode": mode, "ok": ok})
    except (ValueError, FileNotFoundError, UnicodeDecodeError, UnicodeEncodeError, OSError) as exc:
        from .mcp_responses import ToolResponse

        return ToolResponse.error(f"running ETL: {exc}")


@_tool()
def pending_research(
    limit: int | None = None,
    section: str | None = None,
    include_stale: bool = False,
    stale_months: int = 6,
    meta: dict | None = None,
) -> str:
    """List wines that have pending agent sections to research.

    Scans per-vintage wine dossier frontmatter for agent_sections_pending
    entries.  Returns wines sorted by priority: favorites first, then by
    bottle count.  Does **not** include companion dossiers â€” use
    ``pending_companion_research`` for those.

    Args:
        limit: Maximum number of wines to return.
        section: Optional filter â€” only return wines pending this specific
            section (e.g. "vintage_report", "producer_profile",
            "wine_description", "ratings_reviews", "tasting_notes",
            "food_pairings", "similar_wines", "market_availability").
    """
    effective_limit = limit if limit is not None else _load_mcp_settings().query.pending_limit
    return _pending_research(
        _data_dir(),
        limit=effective_limit,
        section=section,
        include_stale=include_stale,
        stale_months=stale_months,
    )


@_tool()
def stale_research(
    months: int = 6,
    limit: int = 20,
    section: str | None = None,
    meta: dict | None = None,
) -> str:
    """Find dossier sections where research is older than a threshold.

    Scans populated agent sections for the ``<!-- research-meta: ... -->``
    comment and reports those with a date older than *months* months.
    Sections without research metadata are ignored (use ``pending_research``
    for sections that have never been researched).

    Args:
        months: Age threshold in months (default 6).
        limit: Maximum results to return.
        section: Optional filter — only check this specific section key.
    """
    return _stale_research(_data_dir(), months=months, limit=limit, section=section)


@_tool()
def pending_companion_research(limit: int | None = None, meta: dict | None = None) -> str:
    """List tracked wines that have pending companion dossier sections.

    Scans companion dossier frontmatter for agent_sections_pending entries.
    Returns tracked wines sorted by tracked_wine_id.

    Args:
        limit: Maximum number of tracked wines to return.
    """
    effective_limit = limit if limit is not None else _load_mcp_settings().query.pending_limit
    return _pending_companion(_data_dir(), limit=effective_limit)


@_tool()
def research_completeness(
    wine_id: int | None = None,
    limit: int = 50,
    sort: str = "score_asc",
    min_score: int | None = None,
    max_score: int | None = None,
    meta: dict | None = None,
) -> str:
    """Get research completeness scores for wines in the cellar.

    Scores range from 0 (no research) to 100 (fully researched, fresh,
    with food tags and pro ratings). Components:
    - Section population (50 pts): 8 research sections
    - Research freshness (20 pts): populated sections with recent metadata
    - Food data (15 pts): food_tags (10) + food_groups (5)
    - Pro ratings (15 pts): at least one professional rating

    Queryable via SQL: ``SELECT * FROM wines_completeness ORDER BY completeness_score``.

    Args:
        wine_id: Optional — return score for a single wine only.
        limit: Maximum wines to return (default 50).
        sort: Sort order. "score_asc" (least complete first, default),
              "score_desc" (most complete first).
        min_score: Optional minimum score filter (inclusive).
        max_score: Optional maximum score filter (inclusive).
    """
    from .mcp_responses import ToolResponse

    try:
        con = _get_agent_connection()
        # Check if view exists
        views = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        ]
        if "wines_completeness" not in views:
            return ToolResponse.error(
                "Research completeness data not available. Run 'cellarbrain etl' or 'cellarbrain recalc' first."
            )

        # Build query
        conditions: list[str] = []
        if wine_id is not None:
            conditions.append(f"wine_id = {int(wine_id)}")
        if min_score is not None:
            conditions.append(f"completeness_score >= {int(min_score)}")
        if max_score is not None:
            conditions.append(f"completeness_score <= {int(max_score)}")

        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        order = "completeness_score ASC" if sort == "score_asc" else "completeness_score DESC"
        sql = f"SELECT * FROM wines_completeness{where} ORDER BY {order} LIMIT {int(limit)}"

        result = con.execute(sql).fetchdf()
        if result.empty:
            return ToolResponse("*No wines match the given criteria.*")

        # Format as Markdown table
        lines = [
            "| wine_id | Wine | Vintage | Score | Pop | Pend | Fresh | Stale | Tags | Groups | Pro |",
            "|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for _, row in result.iterrows():
            lines.append(
                f"| {row['wine_id']} "
                f"| {row['wine_name'] or '—'} "
                f"| {row['vintage'] or 'NV'} "
                f"| {row['completeness_score']}% "
                f"| {row['populated_count']}/8 "
                f"| {row['pending_count']} "
                f"| {row['fresh_count']} "
                f"| {row['stale_count']} "
                f"| {'✓' if row['has_food_tags'] else '✗'} "
                f"| {'✓' if row['has_food_groups'] else '✗'} "
                f"| {'✓' if row['has_pro_ratings'] else '✗'} |"
            )

        # Add summary
        avg_score = result["completeness_score"].mean()
        total = len(result)
        lines.append("")
        lines.append(f"**Summary:** {total} wines shown, average score {avg_score:.0f}%")

        text = "\n".join(lines)
        data = {"wine_count": total, "avg_score": round(float(avg_score), 1)}
        return ToolResponse(text, data=data)
    except (QueryError, DataStaleError) as exc:
        from .mcp_responses import ToolResponse as TR

        return TR.error(str(exc))


@_tool()
def read_companion_dossier(
    tracked_wine_id: int,
    sections: list[str] | None = None,
    meta: dict | None = None,
) -> str:
    """Read a companion dossier for a tracked wine.

    Companion dossiers contain cross-vintage research: producer deep dives,
    vintage trackers, buying guides, and price trackers.

    Args:
        tracked_wine_id: The numeric tracked wine ID.
        sections: Optional list of section keys to include. Valid keys:
            "producer_deep_dive", "vintage_tracker", "buying_guide",
            "price_tracker", "vintages_in_cellar".
    """
    try:
        return _read_companion(tracked_wine_id, _data_dir(), sections=sections)
    except TrackedWineNotFoundError as exc:
        return f"Error: {exc}"


@_tool()
def update_companion_dossier(
    tracked_wine_id: int,
    section: str,
    content: str,
    agent_name: str = "research",
    sources: list[str] | None = None,
    confidence: str | None = None,
    meta: dict | None = None,
) -> str:
    """Update an agent-owned section in a companion dossier.

    Allowed sections:
    - producer_deep_dive
    - vintage_tracker
    - buying_guide
    - price_tracker

    Args:
        tracked_wine_id: The numeric tracked wine ID.
        section: Section key to update (see allowed list above).
        content: Markdown content to write into the section.
        agent_name: Name of the agent making the update.
        sources: Optional list of source URLs/names used for the research.
        confidence: Optional confidence level (e.g. "high", "medium", "low").
    """
    try:
        return _update_companion(
            tracked_wine_id,
            section,
            content,
            _data_dir(),
            agent_name=agent_name,
            sources=sources,
            confidence=confidence,
        )
    except (TrackedWineNotFoundError, ProtectedSectionError, ValueError) as exc:
        return f"Error: {exc}"


@_tool()
def list_companion_dossiers(pending_only: bool = False, meta: dict | None = None) -> str:
    """List companion dossiers for tracked wines.

    When *pending_only* is False, returns all tracked wines from the database.
    When True, returns only those with pending research sections.

    Args:
        pending_only: If True, only show tracked wines with pending sections.
    """
    if pending_only:
        return _pending_companion(
            _data_dir(),
            limit=_load_mcp_settings().query.pending_limit,
        )
    try:
        con = _get_agent_connection()
        return q.execute_query(
            con,
            """
            SELECT tracked_wine_id, winery_name, wine_name,
                   category, country, region, vintages
            FROM tracked_wines
            ORDER BY tracked_wine_id
        """,
            row_limit=_load_mcp_settings().query.row_limit,
        )
    except (QueryError, DataStaleError) as exc:
        return f"Error: {exc}"


@_tool()
def log_price(
    tracked_wine_id: int,
    bottle_size_ml: int,
    retailer_name: str,
    price: float,
    currency: str,
    in_stock: bool,
    vintage: int | None = None,
    retailer_url: str | None = None,
    notes: str | None = None,
    observation_source: str = "agent",
    meta: dict | None = None,
) -> str:
    """Record a price observation for a tracked wine.

    Automatically converts to CHF using configured exchange rates.
    Deduplicates by (tracked_wine_id, vintage, bottle_size_ml, retailer_name, date).

    Args:
        tracked_wine_id: The numeric tracked wine ID.
        bottle_size_ml: Bottle size in millilitres (e.g. 750).
        retailer_name: Name of the retailer or shop.
        price: Price amount in the specified currency.
        currency: ISO currency code (e.g. "CHF", "EUR", "USD").
        in_stock: Whether the wine is currently in stock.
        vintage: Optional vintage year (None for NV wines).
        retailer_url: Optional URL to the product page.
        notes: Optional notes about the observation.
        observation_source: Source of the observation (default "agent").
    """
    from datetime import datetime
    from decimal import Decimal

    observation = {
        "tracked_wine_id": tracked_wine_id,
        "vintage": vintage,
        "bottle_size_ml": bottle_size_ml,
        "retailer_name": retailer_name,
        "retailer_url": retailer_url,
        "price": Decimal(str(price)),
        "currency": currency,
        "in_stock": in_stock,
        "observed_at": datetime.now(),
        "observation_source": observation_source,
        "notes": notes,
    }
    try:
        return q.log_price(_data_dir(), observation, settings=_load_mcp_settings())
    except (TrackedWineNotFoundError, ValueError) as exc:
        return f"Error: {exc}"


@_tool()
def tracked_wine_prices(
    tracked_wine_id: int,
    vintage: int | None = None,
    format: str | None = None,
    meta: dict | None = None,
) -> str:
    """Get the latest prices for a tracked wine from all retailers.

    Returns the most recent in-stock prices, sorted by price (cheapest first).

    Args:
        tracked_wine_id: The numeric tracked wine ID.
        vintage: Optional vintage to filter by.
        format: Output format: "markdown" (default) or "plain" (iMessage-friendly).
    """
    try:
        return q.get_tracked_wine_prices(
            _data_dir(),
            tracked_wine_id,
            vintage=vintage,
            fmt=_effective_fmt(format),
        )
    except (TrackedWineNotFoundError, DataStaleError, QueryError) as exc:
        return f"Error: {exc}"


@_tool()
def price_history(
    tracked_wine_id: int,
    vintage: int | None = None,
    months: int = 12,
    format: str | None = None,
    meta: dict | None = None,
) -> str:
    """Get the monthly price history for a tracked wine.

    Returns monthly price statistics (min, max, avg) over the specified
    number of months.

    Args:
        tracked_wine_id: The numeric tracked wine ID.
        vintage: Optional vintage to filter by.
        months: Number of months of history to include (default 12).
        format: Output format: "markdown" (default) or "plain" (iMessage-friendly).
    """
    try:
        return q.get_price_history(
            _data_dir(),
            tracked_wine_id,
            vintage=vintage,
            months=months,
            fmt=_effective_fmt(format),
        )
    except (TrackedWineNotFoundError, DataStaleError, QueryError) as exc:
        return f"Error: {exc}"


@_tool()
def wishlist_alerts(days: int | None = None, format: str | None = None, meta: dict | None = None) -> str:
    """Get current wishlist alerts â€” price drops, new listings, back in stock, etc.

    Scans recent price observations and returns prioritised alerts
    grouped by severity (High / Medium).

    Args:
        days: Alert window in days (default from settings, typically 30).
    """
    try:
        return q.wishlist_alerts(
            _data_dir(),
            settings=_load_mcp_settings(),
            days=days,
            fmt=_effective_fmt(format),
        )
    except (DataStaleError, QueryError) as exc:
        return f"Error: {exc}"


@_tool()
def search_synonyms(
    action: str = "list",
    key: str = "",
    value: str = "",
    meta: dict | None = None,
) -> str:
    """View or manage search synonym mappings used by find_wine.

    Synonyms translate query tokens before searching (e.g. "Rotwein" â†’ "red",
    "Schweiz" â†’ "Switzerland"). An empty value marks the key as a stopword
    (dropped from the query).

    Args:
        action: "list" to view all synonyms, "add" to add/update a custom
                synonym, "remove" to delete a custom synonym.
        key: The query token to map (required for add/remove). Stored lowercase.
        value: Replacement text (required for add). Empty string = stopword.
    """
    if action == "list":
        effective = _effective_synonyms()
        custom = _load_custom_synonyms()
        lines = [
            f"**{len(effective)} search synonyms** ({len(effective) - len(custom)} built-in, {len(custom)} custom)\n",
            "| Token | Replacement | Source |",
            "|:------|:------------|:-------|",
        ]
        for k in sorted(effective):
            src = "custom" if k in custom else "built-in"
            v = effective[k] if effective[k] else "*(stopword)*"
            lines.append(f"| {k} | {v} | {src} |")
        return "\n".join(lines)

    if action == "add":
        if not key:
            return "Error: key is required for action='add'."
        norm_key = key.strip().lower()
        if not norm_key:
            return "Error: key must be a non-empty string."
        custom = _load_custom_synonyms()
        custom[norm_key] = value
        _save_custom_synonyms(custom)
        label = f"'{value}'" if value else "*(stopword)*"
        return f"Added custom synonym: '{norm_key}' â†’ {label}."

    if action == "remove":
        if not key:
            return "Error: key is required for action='remove'."
        norm_key = key.strip().lower()
        custom = _load_custom_synonyms()
        if norm_key not in custom:
            return f"Error: '{norm_key}' is not a custom synonym."
        del custom[norm_key]
        _save_custom_synonyms(custom)
        return f"Removed custom synonym: '{norm_key}'."

    return f"Error: unknown action '{action}'. Use 'list', 'add', or 'remove'."


# ---------------------------------------------------------------------------


@_tool()
def currency_rates(
    action: str = "list",
    currency: str = "",
    rate: float | None = None,
    meta: dict | None = None,
) -> str:
    """View or manage currency exchange rates used for price normalisation.

    All wine and bottle prices are converted to the default currency (CHF)
    using fixed exchange rates. If the ETL encounters a currency without a
    configured rate, it fails. Use this tool to add the missing rate, then
    retry the ETL.

    Rates express: 1 unit of foreign currency = X units of CHF.
    Example: EUR rate of 0.93 means 1 EUR = 0.93 CHF.

    Args:
        action: "list" to view all rates, "set" to add/update a rate,
                "remove" to delete a custom rate.
        currency: ISO 4217 currency code (required for set/remove).
                  Example: "EUR", "USD", "GBP", "RON".
        rate: Exchange rate to CHF (required for set). Must be > 0.
    """
    global _mcp_settings
    default_currency = _load_mcp_settings().currency.default

    if action == "list":
        effective = _effective_rates()
        custom = _load_custom_rates()
        lines = [
            f"**{len(effective)} currency rates** (default: {default_currency})\n",
            f"| Currency | Rate (\u2192 {default_currency}) | Source |",
            "|:---------|:-------------|:-------|",
        ]
        for code in sorted(effective):
            src = "custom" if code in custom else "toml"
            lines.append(f"| {code} | {effective[code]} | {src} |")
        return "\n".join(lines)

    if action == "set":
        if not currency:
            return "Error: currency is required for action='set'."
        norm = currency.strip().upper()
        if not _CURRENCY_CODE_RE.match(norm):
            return f"Error: '{currency}' is not a valid ISO 4217 currency code (3 uppercase letters)."
        if norm == default_currency:
            return f"Error: cannot set a rate for the default currency ({default_currency})."
        if rate is None:
            return "Error: rate is required for action='set'."
        if rate <= 0:
            return f"Error: rate must be positive, got {rate}."
        custom = _load_custom_rates()
        custom[norm] = rate
        _save_custom_rates(custom)
        # Invalidate cached settings so next MCP operation sees updated rates
        _mcp_settings = None
        return f"Set exchange rate: 1 {norm} = {rate} {default_currency} (custom)."

    if action == "remove":
        if not currency:
            return "Error: currency is required for action='remove'."
        norm = currency.strip().upper()
        custom = _load_custom_rates()
        if norm not in custom:
            return f"Error: '{norm}' is not a custom rate."
        del custom[norm]
        _save_custom_rates(custom)
        _mcp_settings = None
        return (
            f"Removed custom rate for {norm}. Note: if this currency appears in wine data, the next ETL run will fail."
        )

    return f"Error: unknown action '{action}'. Use 'list', 'set', or 'remove'."


# ---------------------------------------------------------------------------
# Tools — Sommelier (food-wine pairing retrieval)
# ---------------------------------------------------------------------------

_sommelier_engine = None
_food_catalogue_meta: dict[str, dict] | None = None


def _get_food_catalogue_meta() -> dict[str, dict]:
    """Return cached dish_id â†’ metadata mapping from the food catalogue."""
    global _food_catalogue_meta
    if _food_catalogue_meta is None:
        import pyarrow.parquet as pq

        path = _load_mcp_settings().sommelier.food_catalogue
        table = pq.read_table(
            path,
            columns=[
                "dish_id",
                "cuisine",
                "weight_class",
                "protein",
                "flavour_profile",
            ],
        )
        _food_catalogue_meta = {}
        for i in range(table.num_rows):
            fp = table.column("flavour_profile")[i].as_py()
            _food_catalogue_meta[table.column("dish_id")[i].as_py()] = {
                "cuisine": table.column("cuisine")[i].as_py() or "",
                "weight": table.column("weight_class")[i].as_py() or "",
                "protein": table.column("protein")[i].as_py() or "",
                "flavour": ", ".join(fp) if fp else "",
            }
    return _food_catalogue_meta


def _get_sommelier_engine():
    """Return a cached SommelierEngine instance."""
    global _sommelier_engine
    if _sommelier_engine is None:
        from .sommelier.engine import SommelierEngine

        settings = _load_mcp_settings()
        _sommelier_engine = SommelierEngine(
            settings.sommelier,
            settings.paths.data_dir,
        )
    return _sommelier_engine


_hybrid_engine = None


def _get_hybrid_engine():
    """Return a cached HybridPairingEngine bound to the current settings.

    The engine is constructed lazily so that the optional sommelier
    dependencies are not imported until they are actually needed.  When
    the sommelier model is not available the engine simply degrades to
    pure RAG retrieval — see :class:`HybridPairingEngine`.
    """
    global _hybrid_engine
    if _hybrid_engine is None:
        from .hybrid_pairing import HybridPairingEngine
        from .sommelier.engine import check_availability

        settings = _load_mcp_settings()
        engine = None
        if check_availability(settings.sommelier.model_dir) is None:
            try:
                engine = _get_sommelier_engine()
            except Exception as exc:  # never fail to construct the hybrid engine
                logger.warning("Sommelier engine init failed: %s", exc)
                engine = None
        _hybrid_engine = HybridPairingEngine(engine, settings.sommelier)
    return _hybrid_engine


def warm_sommelier() -> None:
    """Eagerly load the sommelier model, indexes, and food catalogue.

    Call this **before** ``mcp.run()`` so the heavy C-extension imports
    (torch, sentence-transformers, faiss) happen while the GIL is free.
    Once the event loop starts, the GIL contention from these imports
    would block stdio message processing and cause client timeouts.
    """
    from .sommelier.engine import check_availability
    from .sommelier.index import IndexNotFoundError

    settings = _load_mcp_settings()
    if check_availability(settings.sommelier.model_dir) is not None:
        return  # model not trained — nothing to warm up

    try:
        engine = _get_sommelier_engine()
        engine._ensure_model()
        engine._ensure_food_index()
        engine._ensure_wine_index()
        _get_food_catalogue_meta()
        logger.info("Sommelier warmed up")
    except (IndexNotFoundError, ImportError) as exc:
        logger.warning("Sommelier warm-up skipped: %s", exc)


@_tool()
async def suggest_wines(food_query: str, limit: int = 10, meta: dict | None = None) -> str:
    """Suggest wines from the cellar that pair with a food description.

    Uses embedding-based semantic similarity to find wines whose flavour
    profile, body, and character complement the given dish.  Returns a
    ranked table of wines with similarity scores and metadata.

    Only wines with bottles currently in stock are included. The calling
    agent is expected to read dossiers for the top results, apply
    food-pairing rules, and explain the recommendations.

    Args:
        food_query: Free-text food description (e.g. "grilled lamb chops
                    with rosemary and roasted vegetables").
        limit: Maximum number of wines to return (default 10).
    """
    return await anyio.to_thread.run_sync(
        lambda: _suggest_wines_sync(food_query, limit),
    )


def _suggest_wines_sync(food_query: str, limit: int) -> str:
    """Blocking helper for suggest_wines â€” runs in a thread."""
    try:
        engine = _get_sommelier_engine()
        err = engine.check_availability()
        if err:
            return f"Error: {err}"
        results = engine.suggest_wines(food_query, limit=limit)
    except Exception as exc:
        return f"Error: {exc}"

    if not results:
        return "No matching wines found."

    # Enrich with DuckDB metadata
    try:
        con = _get_connection()
        wine_ids = [r.wine_id for r in results]
        placeholders = ", ".join(str(wid) for wid in wine_ids)
        rows = con.execute(f"""
            SELECT wine_id, wine_name, vintage, category, country, region,
                   primary_grape, bottles_stored, price,
                   volume_ml, bottle_format, price_per_750ml
            FROM wines
            WHERE wine_id IN ({placeholders})
        """).fetchdf()
        meta = {int(r["wine_id"]): r for _, r in rows.iterrows()}
    except Exception:
        meta = {}

    lines = ["| Rank | Score | Wine | Vintage | Category | Region | Grape | Bottles | Price | Size | Price/750 mL |"]
    lines.append(
        "|------|-------|------|---------|----------|--------|-------|---------|-------|------|--------------|"
    )
    for i, r in enumerate(results, 1):
        m = meta.get(r.wine_id, {})
        name = m.get("wine_name", f"Wine #{r.wine_id}")
        vint = m.get("vintage", "")
        cat = m.get("category", "")
        region = m.get("region", "")
        grape = m.get("primary_grape", "")
        bottles = m.get("bottles_stored", "")
        price = m.get("price", "")
        size = m.get("bottle_format", "")
        price_750 = m.get("price_per_750ml", "")
        lines.append(
            f"| {i} | {r.score:.3f} | {name} | {vint} | {cat} | {region} "
            f"| {grape} | {bottles} | {price} | {size} | {price_750} |"
        )
    return "\n".join(lines)


@_tool()
async def suggest_foods(wine_id: int, limit: int = 10, meta: dict | None = None) -> str:
    """Suggest dishes that pair well with a wine from the cellar.

    Uses embedding-based semantic similarity to find dishes from the food
    catalogue whose flavour profile complements the given wine. Returns a
    ranked table of dishes with similarity scores.

    The calling agent is expected to filter, rerank, and explain the
    suggestions using its food-pairing skill and LLM.

    Args:
        wine_id: The numeric wine ID (from find_wine or query_cellar).
        limit: Maximum number of dishes to return (default 10).
    """
    return await anyio.to_thread.run_sync(
        lambda: _suggest_foods_sync(wine_id, limit),
    )


def _suggest_foods_sync(wine_id: int, limit: int) -> str:
    """Blocking helper for suggest_foods â€” runs in a thread."""
    try:
        engine = _get_sommelier_engine()
        err = engine.check_availability()
        if err:
            return f"Error: {err}"
        results = engine.suggest_foods(wine_id, limit=limit)
    except Exception as exc:
        return f"Error: {exc}"

    if not results:
        return "No matching dishes found."

    # Enrich with food catalogue metadata
    try:
        food_meta = _get_food_catalogue_meta()
    except Exception:
        food_meta = {}

    lines = ["| Rank | Score | Dish | Cuisine | Weight | Protein | Flavour Profile |"]
    lines.append("|------|-------|------|---------|--------|---------|-----------------|")
    for i, r in enumerate(results, 1):
        fm = food_meta.get(r.dish_id, {})
        cuisine = fm.get("cuisine", "")
        weight = fm.get("weight", "")
        protein = fm.get("protein", "")
        flavour = fm.get("flavour", "")
        lines.append(f"| {i} | {r.score:.3f} | {r.dish_name} | {cuisine} | {weight} | {protein} | {flavour} |")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# pairing_candidates — RAG multi-strategy retrieval
# ---------------------------------------------------------------------------


@_tool()
async def pairing_candidates(
    dish_description: str,
    category: str | None = None,
    weight: str | None = None,
    protein: str | None = None,
    cuisine: str | None = None,
    grapes: str | None = None,
    limit: int = 15,
    format: str | None = None,
    meta: dict | None = None,
) -> str:
    """Find pairing-candidate wines using multi-strategy structured retrieval.

    Combines category, grape, food-tag, food-group, and region-affinity
    strategies to find wines that pair with the described dish. Does NOT
    require the sommelier model — works with base DuckDB data.

    If only *dish_description* is provided (no category, protein, or
    grapes), the tool auto-classifies the dish server-side using keyword
    matching.  This makes the tool usable by small LLMs that cannot
    reliably classify dishes themselves.

    Args:
        dish_description: Free-text dish description (e.g. "grilled lamb
                          chops with rosemary"). Required.
        category: Target wine category (red/white/rose/sparkling/sweet).
                  Auto-inferred from protein if omitted.
        weight: Dish weight class (light/medium/heavy). Used for grape
                selection and food-group matching. Default: medium.
        protein: Dominant protein (red_meat/poultry/fish/seafood/pork/
                 game/vegetarian/cheese). Auto-inferred from
                 dish_description if omitted.
        cuisine: Cuisine origin (French/Italian/Swiss/Japanese/etc.).
                 Auto-inferred from dish_description if omitted.
        grapes: Comma-separated target grape varieties. Overrides
                automatic grape inference when provided.
        limit: Maximum candidates to return (default 15).
    """
    return await anyio.to_thread.run_sync(
        lambda: _pairing_candidates_sync(
            dish_description, category, weight, protein, cuisine, grapes, limit, _effective_fmt(format)
        ),
    )


def _pairing_candidates_sync(
    dish_description: str,
    category: str | None,
    weight: str | None,
    protein: str | None,
    cuisine: str | None,
    grapes: str | None,
    limit: int,
    fmt: str = "markdown",
) -> str:
    """Blocking helper for pairing_candidates."""
    from . import pairing

    try:
        con = _get_agent_connection()
        grape_list = [g.strip() for g in grapes.split(",")] if grapes else None
        hybrid = _get_hybrid_engine()
        outcome = hybrid.retrieve(
            con,
            dish_description=dish_description,
            category=category,
            weight=weight,
            protein=protein,
            cuisine=cuisine,
            grapes=grape_list,
            limit=limit,
        )
        results = outcome.candidates
    except Exception as exc:
        return f"Error: {exc}"

    if not results:
        return "No pairing candidates found for this dish profile."

    table = pairing.format_table(results, fmt=fmt)
    return f"{table}\n\n{_format_mode_trailer(outcome)}"


# ---------------------------------------------------------------------------
# pair_wine — simplified single-shot pairing for small LLMs
# ---------------------------------------------------------------------------


@_tool()
async def pair_wine(
    dish: str,
    occasion: str | None = None,
    limit: int = 5,
    format: str | None = None,
    meta: dict | None = None,
) -> str:
    """Find wines from your cellar that pair well with a dish.

    Just describe the food — the tool classifies the dish, searches the
    cellar, and returns ready-to-present recommendations with reasons.
    No need to specify protein, weight, or category yourself.

    Args:
        dish: What you're eating (e.g. "grilled lamb with rosemary",
              "raclette", "sushi platter", "chocolate cake").
        occasion: Optional context like "casual dinner", "date night",
                  or "large group" (currently informational).
        limit: Number of recommendations to return (default 5).
    """
    return await anyio.to_thread.run_sync(
        lambda: _pair_wine_sync(dish, occasion, limit, _effective_fmt(format)),
    )


def _pair_wine_sync(dish: str, occasion: str | None, limit: int, fmt: str = "markdown") -> str:
    """Blocking helper for pair_wine."""
    from . import pairing

    try:
        con = _get_agent_connection()
        classification = pairing.classify_dish(dish)
        hybrid = _get_hybrid_engine()
        outcome = hybrid.retrieve(
            con,
            dish_description=dish,
            category=classification.category,
            weight=classification.weight,
            protein=classification.protein,
            cuisine=classification.cuisine,
            limit=max(limit, 5) * 3,  # fetch extra for quality ranking
        )
        results = outcome.candidates
    except Exception as exc:
        return f"Error: {exc}"

    if not results:
        return f'No wines found for "{dish}". Your cellar may not have suitable wines in stock.'

    explained = pairing.format_explained(results, dish, classification, limit=limit, fmt=fmt)
    return f"{explained}\n{_format_mode_trailer(outcome)}"


def _format_mode_trailer(outcome) -> str:
    """Render a one-line mode trailer for hybrid pairing tool output."""
    if outcome.mode == "hybrid":
        return (
            f"_Mode: hybrid (RAG → embedding re-rank, "
            f"blend={outcome.blend:.2f}, "
            f"pool={outcome.rag_count}, reranked={outcome.rerank_count})_"
        )
    reason = outcome.fallback_reason or "rag_only"
    return f"_Mode: rag (reason: {reason})_"


# ---------------------------------------------------------------------------
# add_pairing â€” append training pair
# ---------------------------------------------------------------------------


def _append_pairing(
    dataset_path: pathlib.Path,
    *,
    food_text: str,
    wine_text: str,
    pairing_score: float,
    pairing_reason: str,
) -> None:
    """Append a single pairing row to the dataset Parquet file."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    new_row = pa.table(
        {
            "food_text": [food_text],
            "ingredients": [[]],
            "wine_text": [wine_text],
            "grape": [""],
            "region": [""],
            "style": [""],
            "pairing_score": [pairing_score],
            "pairing_reason": [pairing_reason],
        }
    )

    if dataset_path.exists():
        existing = pq.read_table(dataset_path)
        combined = pa.concat_tables([existing, new_row], promote_options="default")
    else:
        dataset_path.parent.mkdir(parents=True, exist_ok=True)
        combined = new_row

    pq.write_table(combined, dataset_path)


@_tool()
def add_pairing(
    food_text: str,
    wine_text: str,
    pairing_score: float,
    pairing_reason: str = "",
    meta: dict | None = None,
) -> str:
    """Add a food-wine pairing example to the training dataset.

    Called by the agent after explaining a food-wine pairing to generate
    training data for the sommelier model.  The pair is appended to the
    pairing dataset Parquet file.

    Args:
        food_text: Embedding-format food description (e.g. "duck confit,
                   slow cooked, crispy skin | ingredients: duck legs, duck
                   fat, garlic, thyme | French | heavy | poultry | confit
                   | rich, savory, herbal").
        wine_text: Embedding-format wine description (e.g. "ChÃ¢teau Musar
                   Rouge | Cabernet Sauvignon, Cinsault, Carignan | Bekaa
                   Valley, Lebanon | red").
        pairing_score: Quality score from 0.0 (terrible pairing) to 1.0
                       (perfect pairing).
        pairing_reason: Brief explanation of why this pairing works or
                        doesn't (e.g. "Tannin structure complements rich
                        duck fat; herbal notes bridge thyme in the dish").
    """
    if not food_text or not food_text.strip():
        return "Error: food_text must be non-empty."
    if not wine_text or not wine_text.strip():
        return "Error: wine_text must be non-empty."
    if not 0.0 <= pairing_score <= 1.0:
        return "Error: pairing_score must be between 0.0 and 1.0."

    try:
        settings = _load_mcp_settings()
        dataset_path = pathlib.Path(settings.sommelier.pairing_dataset)
        dataset_path.parent.mkdir(parents=True, exist_ok=True)
        _append_pairing(
            dataset_path,
            food_text=food_text.strip(),
            wine_text=wine_text.strip(),
            pairing_score=pairing_score,
            pairing_reason=pairing_reason.strip(),
        )
        import pyarrow.parquet as pq

        total = pq.read_metadata(dataset_path).num_rows
        return f"Pairing added. Dataset now has {total} pairs."
    except Exception as exc:
        return f"Error: {exc}"


@_tool()
def server_stats(period: str = "24h", meta: dict | None = None) -> str:
    """Return usage, latency, and error statistics from the MCP log store.

    Queries the observability DuckDB log store for recent tool invocations.

    Args:
        period: Time window â€” e.g. "1h", "24h", "7d", "30d". Default "24h".
    """
    from .observability import get_collector

    collector = get_collector()
    if collector is None or collector._db is None:
        return "Error: Observability log store not available."

    # Parse period into interval string
    unit_map = {"h": "HOUR", "d": "DAY"}
    raw = period.strip().lower()
    if len(raw) < 2 or raw[-1] not in unit_map or not raw[:-1].isdigit():
        return "Error: period must be like '1h', '24h', or '7d'."
    num = int(raw[:-1])
    interval = f"{num} {unit_map[raw[-1]]}"

    db = collector._db
    try:
        cutoff_sql = f"now() - INTERVAL '{interval}'"

        # Usage summary
        usage = db.execute(f"""
            SELECT
                event_type,
                name,
                COUNT(*) AS calls,
                ROUND(AVG(duration_ms), 1) AS avg_ms,
                ROUND(MAX(duration_ms), 1) AS max_ms,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errors
            FROM tool_events
            WHERE started_at >= {cutoff_sql}
            GROUP BY event_type, name
            ORDER BY calls DESC
        """).fetchall()

        if not usage:
            return f"No events recorded in the last {period}."

        # Session summary
        sessions = db.execute(f"""
            SELECT
                COUNT(DISTINCT session_id) AS sessions,
                COUNT(DISTINCT turn_id) AS turns,
                COUNT(*) AS total_events,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS total_errors,
                ROUND(AVG(duration_ms), 1) AS overall_avg_ms
            FROM tool_events
            WHERE started_at >= {cutoff_sql}
        """).fetchone()

        lines = [
            f"## Server Stats (last {period})\n",
            "### Overview\n",
            "| Metric | Value |",
            "|:-------|------:|",
            f"| Sessions | {sessions[0]} |",
            f"| Turns | {sessions[1]} |",
            f"| Total events | {sessions[2]} |",
            f"| Errors | {sessions[3]} |",
            f"| Avg latency | {sessions[4]} ms |",
            "",
            "### Per-Tool Breakdown\n",
            "| Type | Name | Calls | Avg ms | Max ms | Errors |",
            "|:-----|:-----|------:|-------:|-------:|-------:|",
        ]
        for row in usage:
            lines.append(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} |")

        return "\n".join(lines)
    except Exception as exc:
        return f"Error: {exc}"


@_tool()
def cache_stats(meta: dict | None = None) -> str:
    """Return query cache hit/miss statistics.

    Shows current cache utilisation, hit ratio, and eviction counts.
    The query cache memoises results of cellar_stats and find_wine to
    avoid redundant computation for repeated identical queries.
    """
    from .mcp_responses import ToolResponse

    s = _query_cache.stats()
    lines = [
        "## Query Cache Stats\n",
        "| Metric | Value |",
        "|:-------|------:|",
        f"| Enabled | {_query_cache.enabled} |",
        f"| Entries | {s.size} / {s.max_size} |",
        f"| Hits | {s.hits} |",
        f"| Misses | {s.misses} |",
        f"| Hit ratio | {s.hit_ratio:.1%} |",
        f"| Evictions | {s.evictions} |",
        f"| Invalidations | {s.invalidations} |",
    ]
    text = "\n".join(lines)
    data = {
        "enabled": _query_cache.enabled,
        "size": s.size,
        "max_size": s.max_size,
        "hits": s.hits,
        "misses": s.misses,
        "hit_ratio": round(s.hit_ratio, 4),
        "evictions": s.evictions,
        "invalidations": s.invalidations,
    }
    return ToolResponse(text, data=data)


# ---------------------------------------------------------------------------
# cellar_anomalies — anomaly detection
# ---------------------------------------------------------------------------


@_tool()
def cellar_anomalies(
    severity: str | None = None,
    kinds: str | None = None,
    meta: dict | None = None,
) -> str:
    """Detect unusual patterns in tool usage, latency, errors, and ETL output.

    Runs anomaly detectors against the observability log store and
    etl_run.parquet, returning any detected anomalies.

    Args:
        severity: Filter by severity — "info", "warn", or "critical". Default: all.
        kinds: Comma-separated list of anomaly kinds to include. Default: all.
            Kinds: call_volume_spike, latency_spike, error_cluster,
            result_size_drift, etl_mass_delete, etl_partial_export.
    """
    from .anomaly import detect_all
    from .observability import get_collector

    settings = _load_mcp_settings()
    cfg = settings.anomaly

    if not cfg.enabled:
        return "Anomaly detection is disabled in configuration."

    # Get log store connection
    collector = get_collector()
    con = collector._db if collector is not None else None

    data_dir = settings.paths.data_dir

    try:
        anomalies = detect_all(
            con,
            data_dir,
            enabled=cfg.enabled,
            baseline_days=cfg.baseline_days,
            volume_window_hours=cfg.volume_window_hours,
            volume_factor=cfg.volume_factor,
            volume_min_calls=cfg.volume_min_calls,
            latency_factor=cfg.latency_factor,
            latency_min_samples=cfg.latency_min_samples,
            error_window_hours=cfg.error_window_hours,
            error_cluster_min=cfg.error_cluster_min,
            drift_pct=cfg.drift_pct,
            drift_min_samples=cfg.drift_min_samples,
            etl_baseline_runs=cfg.etl_baseline_runs,
            etl_delete_min_abs=cfg.etl_delete_min_abs,
            etl_delete_min_pct=cfg.etl_delete_min_pct,
        )
    except Exception as exc:
        return f"Error: {exc}"

    # Filter by severity
    if severity:
        sev = severity.strip().lower()
        anomalies = [a for a in anomalies if a.severity == sev]

    # Filter by kinds
    if kinds:
        kind_set = {k.strip() for k in kinds.split(",")}
        anomalies = [a for a in anomalies if a.kind in kind_set]

    if not anomalies:
        return "No anomalies detected."

    # Format as Markdown
    lines = [f"## Anomalies Detected ({len(anomalies)})\n"]
    current_sev = None
    for a in anomalies:
        if a.severity != current_sev:
            current_sev = a.severity
            badge = {"critical": "🔴", "warn": "🟡", "info": "🔵"}.get(a.severity, "")
            lines.append(f"\n### {badge} {a.severity.upper()}\n")
            lines.append("| Subject | Message | Observed | Baseline |")
            lines.append("|:--------|:--------|:---------|:---------|")
        lines.append(f"| {a.subject} | {a.message} | {a.observed:.1f} | {a.baseline:.1f} |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# train_sommelier — trigger model training via MCP
# ---------------------------------------------------------------------------


@_tool()
def train_sommelier(
    epochs: int = 10,
    batch_size: int = 32,
    incremental: bool = False,
    meta: dict | None = None,
) -> str:
    """Train or retrain the sommelier food-wine pairing model.

    Triggers fine-tuning of the sentence-transformer model on the pairing
    dataset, then rebuilds FAISS indexes.  Can do a full training from the
    base model or an incremental retrain from the existing fine-tuned model.

    Args:
        epochs: Number of training epochs (default: from config, typically 10
                for full, 5 for incremental).
        batch_size: Training batch size (default: from config).
        incremental: If True, retrain from existing fine-tuned model instead
                     of the base model.  Requires a previously trained model.
    """
    try:
        from .sommelier.training import train_model
    except ImportError:
        return "Error: ML dependencies not installed. Run: pip install cellarbrain[ml]"

    settings = _load_mcp_settings()
    cfg = settings.sommelier

    model_dir = pathlib.Path(cfg.model_dir)
    dataset_path = pathlib.Path(cfg.pairing_dataset)

    if not dataset_path.exists():
        return f"Error: pairing dataset not found at {dataset_path}"

    if incremental:
        if not model_dir.exists():
            return "Error: no trained model found. Run train_sommelier without incremental=True first."
        base = str(model_dir)
    else:
        base = cfg.base_model

    try:
        import pyarrow.parquet as pq

        total_pairs = pq.read_metadata(dataset_path).num_rows

        metrics = train_model(
            pairing_parquet=cfg.pairing_dataset,
            output_dir=cfg.model_dir,
            base_model=base,
            epochs=epochs,
            batch_size=batch_size,
            warmup_ratio=cfg.warmup_ratio,
            eval_split=cfg.eval_split,
        )

        # Rebuild FAISS indexes after training
        from .sommelier.index import rebuild_indexes

        rebuild_indexes(settings)

        lines = [
            "## Training Complete\n",
            f"- Mode: {'incremental' if incremental else 'full'}",
            f"- Epochs: {epochs}",
            f"- Batch size: {batch_size}",
            f"- Training pairs: {total_pairs}",
            f"- Model saved to: {cfg.model_dir}",
            "",
            "### Metrics\n",
            "| Metric | Value |",
            "|:-------|------:|",
        ]
        for key, val in metrics.items():
            if isinstance(val, float):
                lines.append(f"| {key} | {val:.4f} |")
            else:
                lines.append(f"| {key} | {val} |")

        lines.append("\nFAISS indexes rebuilt successfully.")
        return "\n".join(lines)
    except Exception as exc:
        return f"Error during training: {exc}"


# ---------------------------------------------------------------------------
# Drink-tonight bridge (dashboard sidecar)
# ---------------------------------------------------------------------------


@_tool()
def get_drink_tonight(meta: dict | None = None) -> str:
    """Return the wines the user has staged on the dashboard's "Drink Tonight" shortlist.

    The list lives in ``<data_dir>/.drink-tonight.json`` and is mirrored by
    the dashboard whenever the user adds or removes a wine. Each row joins
    the wine record with the user-supplied note and timestamp.

    Returns a Markdown table or an empty-state message if the list is empty.
    """
    from .dashboard.sidecars import read_drink_tonight

    items = read_drink_tonight(_data_dir())
    if not items:
        return "Drink-tonight shortlist is empty."

    con = _get_agent_connection()
    placeholders = ",".join("?" for _ in items)
    rows = con.execute(
        f"""
        SELECT wine_id, wine_name, vintage, winery_name, category,
               bottles_stored, drinking_status
        FROM wines_full WHERE wine_id IN ({placeholders})
        """,
        [int(it["wine_id"]) for it in items],
    ).fetchall()
    by_id = {r[0]: r for r in rows}
    meta_by_id = {int(it["wine_id"]): it for it in items}

    lines = [
        f"# Drink Tonight ({len(items)})",
        "",
        "| Wine | Vintage | Producer | Status | Stock | Added | Note |",
        "|:-----|:-------:|:---------|:-------|------:|:------|:-----|",
    ]
    for it in items:
        wid = int(it["wine_id"])
        r = by_id.get(wid)
        m = meta_by_id.get(wid, {})
        if r is None:
            lines.append(f"| #{wid} (missing) | — | — | — | — | {m.get('added_at', '')} | {m.get('note') or ''} |")
            continue
        lines.append(
            f"| {r[1]} | {r[2] or '—'} | {r[3] or '—'} | {r[6] or '—'} | "
            f"{r[5]} | {m.get('added_at', '')} | {m.get('note') or ''} |"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Smart recommendations
# ---------------------------------------------------------------------------


@_tool()
def recommend_tonight(
    occasion: str | None = None,
    cuisine: str | None = None,
    guests: int | None = None,
    budget: str | None = None,
    limit: int = 5,
    meta: dict | None = None,
) -> str:
    """Score and rank cellar wines for tonight's drinking occasion.

    Combines urgency (drinking window proximity), occasion fit (price tier,
    format, category), cuisine pairing (if specified), freshness (avoid
    recently-consumed), and diversity (varied wineries/grapes) into a
    single ranked recommendation.

    Args:
        occasion: Context -- casual, dinner_party, celebration, romantic,
                  solo, weeknight, tasting. Affects price/format fit.
        cuisine: Free-text cuisine or dish (e.g. "Italian", "grilled lamb").
                 Triggers pairing engine integration.
        guests: Number of people (affects format preference -- magnum for 5+).
        budget: Price constraint -- any, under_15, under_30, under_50, special.
        limit: Number of recommendations (default 5, max 15).
    """
    from .recommend import (
        RecommendParams,
        RecommendWeights,
        format_recommendations,
        recommend,
    )

    try:
        cfg = _load_mcp_settings().recommend
        w = RecommendWeights.from_config(cfg)
        effective_limit = min(limit, cfg.max_limit)

        params = RecommendParams(
            occasion=occasion,
            cuisine=cuisine,
            guests=guests,
            budget=budget,
            limit=effective_limit,
        )
        results = recommend(
            _get_agent_connection(),
            params,
            weights=w,
            data_dir=_data_dir(),
        )
        text = format_recommendations(results, params)
        from .mcp_responses import ToolResponse

        data = {
            "params": {"occasion": occasion, "cuisine": cuisine, "guests": guests, "budget": budget},
            "results": [
                {"wine_id": r.wine_id, "score": round(r.total_score, 3), "wine_name": r.wine_name} for r in results
            ],
        }
        return ToolResponse(text, data=data, metadata={"row_count": len(results)})
    except Exception as exc:
        from .mcp_responses import ToolResponse

        return ToolResponse.error(str(exc))


@_tool()
def plan_trip(
    destination: str,
    include_producers: bool = True,
    include_gaps: bool = True,
    limit: int = 20,
    meta: dict | None = None,
) -> str:
    """Generate a wine travel brief for a destination region or country.

    Combines cellar inventory, producer visit suggestions (from dossier
    research), and regional gap analysis into a single structured document
    for pre-trip planning.

    Matches the destination against country, region, and subregion fields
    using accent-insensitive search.  Returns inventory table, highlights,
    producer profiles, and gaps (subregions/grapes you once owned but no
    longer have in stock).

    Args:
        destination: Region, country, or subregion name (e.g. "Piedmont",
                     "Burgundy", "Italy", "Barolo").
        include_producers: Extract producer visit suggestions from dossiers.
        include_gaps: Show regional gaps (subregions/grapes not in stock).
        limit: Maximum wines to include in the inventory table.
    """
    from .mcp_responses import ToolResponse
    from .travel import build_travel_brief, format_travel_brief

    if not destination or not destination.strip():
        return ToolResponse.error("destination is required.")

    try:
        con = _get_agent_connection()
        brief = build_travel_brief(
            con,
            destination.strip(),
            _data_dir(),
            include_producers=include_producers,
            include_gaps=include_gaps,
            limit=min(limit, 50),
        )
        if brief is None:
            return ToolResponse(
                f'No wines found matching destination "{destination}". '
                f"Try a different region, country, or subregion name.",
                data={"destination": destination, "match": False},
            )
        text = format_travel_brief(brief)
        data = {
            "destination": brief.destination,
            "match_level": brief.match_level,
            "wine_count": len(brief.wines),
            "total_bottles": brief.total_bottles,
        }
        return ToolResponse(text, data=data, metadata={"row_count": len(brief.wines)})
    except Exception as exc:
        from .mcp_responses import ToolResponse

        return ToolResponse.error(str(exc))


@_tool()
def learning_path(
    topic: str,
    lesson_size: int = 4,
    include_purchases: bool = True,
    include_dossier_insights: bool = True,
    meta: dict | None = None,
) -> str:
    """Generate a guided wine education path for a topic.

    Maps the user's cellar onto pedagogical frameworks — curated for
    major regions (Burgundy, Piedmont, Bordeaux, Germany/VDP) and
    data-driven for others.  Returns a structured learning path with
    progress tracking, a tasting lesson ordering wines from entry-level
    to pinnacle, and purchase suggestions to fill educational gaps.

    Args:
        topic: Region, country, subregion, or grape name
               (e.g. "Burgundy", "Piedmont", "Nebbiolo").
        lesson_size: Number of wines in the tasting lesson (2-6).
        include_purchases: Show purchase gap suggestions.
        include_dossier_insights: Include producer profile excerpts.
    """
    from .education import build_learning_path, format_learning_path
    from .mcp_responses import ToolResponse

    if not topic or not topic.strip():
        return ToolResponse.error("topic is required.")

    try:
        con = _get_agent_connection()
        path = build_learning_path(
            con,
            topic.strip(),
            _data_dir(),
            lesson_size=lesson_size,
            include_purchases=include_purchases,
            include_dossier_insights=include_dossier_insights,
        )
        if path is None:
            return ToolResponse(
                f'No wines found matching topic "{topic}". Try a different region, country, or grape name.',
                data={"topic": topic, "match": False},
            )
        text = format_learning_path(path)
        data = {
            "topic": path.topic,
            "framework": path.framework_name,
            "match_type": path.match_type,
            "total_wines": path.total_wines,
            "curriculum_size": len(path.curriculum),
            "purchase_gaps": len(path.purchase_suggestions),
        }
        return ToolResponse(text, data=data, metadata={"row_count": path.total_wines})
    except Exception as exc:
        from .mcp_responses import ToolResponse

        return ToolResponse.error(str(exc))


@_tool()
def cellar_digest(
    period: str = "daily",
    meta: dict | None = None,
) -> str:
    """Proactive cellar intelligence brief with urgency warnings and recommendations.

    Returns a formatted digest with: wines past optimal (drink soon), wines
    newly entering their optimal window, today's top pick, inventory summary,
    and recent ETL changes.

    Args:
        period: "daily" (1-day lookback) or "weekly" (7-day lookback).
    """
    from .digest import build_digest, format_digest

    if period not in ("daily", "weekly"):
        return "Error: period must be 'daily' or 'weekly'."

    try:
        con = _get_connection()
        result = build_digest(con, _data_dir(), period=period)
        return format_digest(result)
    except Exception as exc:
        return f"Error: {exc}"


@_tool()
def wine_of_the_day(meta: dict | None = None) -> str:
    """Today's deterministic wine pick \u2014 urgency-weighted daily rotation.

    Returns a single wine recommendation that stays the same all day and
    changes at midnight. The pick is weighted toward wines with drinking-
    window urgency (past optimal, approaching end of window) while still
    rotating through regions and grapes over time.

    Use this for a zero-effort daily suggestion answering "what should I
    open tonight?" before the user even asks.
    """
    from .mcp_responses import ToolResponse
    from .wotd import format_wine_of_the_day, pick_wine_of_the_day

    try:
        con = _get_connection()
        pick = pick_wine_of_the_day(con, data_dir=_data_dir())
        text = format_wine_of_the_day(pick)
        if pick is None:
            return ToolResponse(text, metadata={"empty": True})
        data = {
            "wine_id": pick.wine_id,
            "wine_name": pick.wine_name,
            "vintage": pick.vintage,
            "winery_name": pick.winery_name,
            "category": pick.category,
            "region": pick.region,
            "primary_grape": pick.primary_grape,
            "drinking_status": pick.drinking_status,
            "bottles_stored": pick.bottles_stored,
            "score": round(pick.score, 3),
            "reason": pick.reason,
        }
        return ToolResponse(text, data=data, metadata={"wine_id": pick.wine_id})
    except Exception as exc:
        from .mcp_responses import ToolResponse

        return ToolResponse.error(str(exc))


@_tool()
def plan_dinner(
    courses: str,
    guests: int = 4,
    budget: str = "any",
    style: str = "classic",
    dinner_time: str | None = None,
    meta: dict | None = None,
) -> str:
    """Plan a complete wine flight for a multi-course dinner party.

    Given a pipe-separated list of courses (in serving order), select one
    wine per course from the cellar that creates a harmonious progression
    from light to heavy. Includes preparation timeline with chilling and
    decanting guidance.

    Args:
        courses: Pipe-separated course descriptions, e.g.
            "oysters | risotto ai funghi | lamb rack | cheese plate"
        guests: Number of diners (default 4).
        budget: Total budget tier - "any", "under_50", "under_100",
            "under_150", "under_200", "under_300".
        style: Pairing style (currently only "classic").
        dinner_time: Optional time as HH:MM (24h) for timeline.
    """
    from .dinner import format_flight_plan, plan_flight
    from .mcp_responses import ToolResponse

    try:
        course_list = [c.strip() for c in courses.split("|") if c.strip()]
        settings = _load_mcp_settings()

        if not course_list:
            return ToolResponse.error("No courses provided. Separate courses with |")

        if len(course_list) > settings.dinner.max_courses:
            return ToolResponse.error(
                f"Too many courses ({len(course_list)}). Maximum is {settings.dinner.max_courses}."
            )

        # Parse dinner_time to minutes-from-midnight
        time_minutes: int | None = None
        if dinner_time:
            parts = dinner_time.split(":")
            if len(parts) == 2:
                time_minutes = int(parts[0]) * 60 + int(parts[1])

        con = _get_connection()
        engine = _get_hybrid_engine()

        plan = plan_flight(
            con,
            engine,
            course_list,
            guests=guests,
            budget=budget,
            style=style,
            dinner_time_minutes=time_minutes,
            pool_size=settings.dinner.pool_size,
            glasses_per_bottle=settings.dinner.glasses_per_bottle,
        )

        text = format_flight_plan(plan)
        data = {
            "courses": [
                {
                    "course_number": p.course_number,
                    "course_description": p.course_description,
                    "wine_id": p.wine_id,
                    "wine_name": p.wine_name,
                    "vintage": p.vintage,
                    "bottles_needed": p.bottles_needed,
                    "serving_temp_c": p.serving_temp_c,
                    "decant_minutes": p.decant_minutes,
                }
                for p in plan.courses
            ],
            "total_bottles": plan.total_bottles,
            "total_cost": plan.total_cost,
        }
        return ToolResponse(text, data=data, metadata={"guests": guests})
    except Exception as exc:
        from .mcp_responses import ToolResponse

        return ToolResponse.error(str(exc))


@_tool()
def gift_advisor(
    profile: str,
    budget: str = "any",
    occasion: str | None = None,
    protect_last_bottle: bool = True,
    limit: int = 3,
    meta: dict | None = None,
) -> str:
    """Recommend wines from the cellar as gifts for a specific recipient.

    Scores wines on six signals: prestige, storytelling, drinkability,
    recognition, recipient fit, and presentation.  Returns ranked
    suggestions with gift notes.

    Args:
        profile: Free-text recipient description,
                 e.g. "loves bold Italian reds, collector level".
        budget: Named tier (modest/nice/generous/lavish/extraordinary)
                or explicit range "80-150".  Default "any".
        occasion: Optional context: birthday, milestone, thank_you,
                  host_gift, corporate, romantic, holiday.
        protect_last_bottle: If True (default), require >= 2 bottles in stock.
        limit: Number of suggestions (1-10, default 3).
        meta: Reserved for MCP metadata (ignored).
    """
    from .gifting import format_gift_suggestions, suggest_gifts
    from .mcp_responses import ToolResponse

    try:
        con = _get_connection()
        settings = _load_mcp_settings()

        effective_limit = max(1, min(limit, settings.gifting.max_limit))
        min_bottles = settings.gifting.min_bottles if protect_last_bottle else 1

        plan = suggest_gifts(
            con=con,
            profile_text=profile,
            budget=budget,
            occasion=occasion,
            data_dir=settings.data_dir,
            limit=effective_limit,
            min_bottles=min_bottles,
            markup_factor=settings.gifting.markup_factor,
            famous_regions=frozenset(settings.gifting.famous_regions),
        )

        text = format_gift_suggestions(plan)

        data = {
            "profile_summary": plan.profile_summary,
            "budget_tier": plan.budget_tier,
            "occasion": plan.occasion,
            "suggestions": [
                {
                    "wine_id": s.wine_id,
                    "wine_name": s.wine_name,
                    "vintage": s.vintage,
                    "winery_name": s.winery_name,
                    "category": s.category,
                    "region": s.region,
                    "classification": s.classification,
                    "primary_grape": s.primary_grape,
                    "drinking_status": s.drinking_status,
                    "bottles_available": s.bottles_available,
                    "retail_value": s.retail_value,
                    "value_source": s.value_source,
                    "gift_score": {
                        "prestige": s.gift_score.prestige,
                        "storytelling": s.gift_score.storytelling,
                        "drinkability": s.gift_score.drinkability,
                        "recognition": s.gift_score.recognition,
                        "recipient_fit": s.gift_score.recipient_fit,
                        "presentation": s.gift_score.presentation,
                        "total": s.gift_score.total,
                    },
                    "gift_note": s.gift_note,
                }
                for s in plan.suggestions
            ],
            "warnings": plan.warnings,
        }
        return ToolResponse(text, data=data, metadata={"profile": profile})
    except Exception as exc:
        from .mcp_responses import ToolResponse

        return ToolResponse.error(str(exc))


# ---------------------------------------------------------------------------
# Resources â€” browsable data
# ---------------------------------------------------------------------------


@mcp.resource("wine://list")
@_log_resource
def list_wines() -> str:
    """List all wines with basic metadata."""
    try:
        con = _get_connection()
        return q.execute_query(
            con,
            """
            SELECT wine_id, winery_name, wine_name, vintage,
                   category, country, region, is_favorite
            FROM wines
            ORDER BY wine_id
        """,
        )
    except (QueryError, DataStaleError) as exc:
        return f"Error: {exc}"


@mcp.resource("wine://cellar")
@_log_resource
def cellar_wines() -> str:
    """List wines currently in the cellar (at least 1 stored bottle, excludes on-order)."""
    try:
        con = _get_connection()
        return q.execute_query(
            con,
            """
            SELECT wine_id, winery_name, wine_name, vintage,
                   category, bottles_stored
            FROM wines_stored
            ORDER BY bottles_stored DESC
        """,
        )
    except (QueryError, DataStaleError) as exc:
        return f"Error: {exc}"


@mcp.resource("wine://on-order")
@_log_resource
def on_order_wines() -> str:
    """List wines with bottles on order or in transit."""
    try:
        con = _get_connection()
        return q.execute_query(
            con,
            """
            SELECT wine_id, winery_name, wine_name, vintage,
                   category, bottles_on_order, on_order_value
            FROM wines_on_order
            ORDER BY bottles_on_order DESC
        """,
        )
    except (QueryError, DataStaleError) as exc:
        return f"Error: {exc}"


@mcp.resource("wine://favorites")
@_log_resource
def favorite_wines() -> str:
    """List favorite wines."""
    try:
        con = _get_connection()
        return q.execute_query(
            con,
            """
            SELECT wine_id, winery_name, wine_name, vintage,
                   category, country, region
            FROM wines
            WHERE is_favorite
            ORDER BY wine_id
        """,
        )
    except (QueryError, DataStaleError) as exc:
        return f"Error: {exc}"


@mcp.resource("wine://{wine_id}")
@_log_resource
def wine_detail(wine_id: int) -> str:
    """Full dossier for a specific wine."""
    try:
        return _read_dossier(wine_id, _data_dir())
    except WineNotFoundError as exc:
        return f"Error: {exc}"


@mcp.resource("cellar://stats")
@_log_resource
def cellar_overview() -> str:
    """Current cellar statistics snapshot."""
    try:
        con = _get_connection()
        return q.cellar_stats(con)
    except DataStaleError as exc:
        return f"Error: {exc}"


@mcp.resource("cellar://drinking-now")
@_log_resource
def drinking_now() -> str:
    """Wines in their optimal drinking window this year."""
    try:
        con = _get_connection()
        return q.execute_query(
            con,
            """
            SELECT wine_id, winery_name, wine_name, vintage,
                   category, optimal_from, optimal_until, bottles_stored
            FROM wines_drinking_now
            ORDER BY optimal_until ASC
        """,
        )
    except (QueryError, DataStaleError) as exc:
        return f"Error: {exc}"


@mcp.resource("cellar://recommendations")
@_log_resource
def tonight_recommendations() -> str:
    """Smart recommendations based on urgency and cellar state."""
    return recommend_tonight()


@mcp.resource("cellar://digest")
@_log_resource
def cellar_digest_resource() -> str:
    """Proactive cellar intelligence brief — urgency, newly optimal, top pick, changes."""
    return cellar_digest()


@mcp.resource("drink-tonight://list")
@_log_resource
def drink_tonight_list() -> str:
    """Wines the user has staged on the dashboard's "Drink Tonight" shortlist."""
    return get_drink_tonight()


@mcp.resource("etl://last-run")
@_log_resource
def last_etl_run() -> str:
    """Metadata about the most recent ETL run."""
    try:
        con = _get_connection()
        return q.execute_query(
            con,
            """
            SELECT * FROM etl_run ORDER BY run_id DESC LIMIT 1
        """,
        )
    except (QueryError, DataStaleError) as exc:
        return f"Error: {exc}"


@mcp.resource("etl://changes")
@_log_resource
def recent_changes() -> str:
    """Change log entries from the last ETL run."""
    try:
        con = _get_connection()
        return q.execute_query(
            con,
            """
            SELECT cl.* FROM change_log cl
            WHERE cl.run_id = (SELECT max(run_id) FROM etl_run)
            ORDER BY cl.change_id
        """,
        )
    except (QueryError, DataStaleError) as exc:
        return f"Error: {exc}"


# ---------------------------------------------------------------------------
# Schema introspection resource
# ---------------------------------------------------------------------------

_VIEW_DESCRIPTIONS: dict[str, str] = {
    "wines": "One row per wine (slim: 20 columns). Includes ALL non-deleted wines (stored, consumed, on order). Use **wines_stored** for in-cellar queries.",
    "wines_full": "One row per wine (all ~61 columns). Includes ALL non-deleted wines. Use when you need scores, cellar_value, alcohol_pct, or technical detail.",
    "wines_stored": "Same as wines, filtered to bottles_stored > 0. **Default for cellar questions.**",
    "wines_drinking_now": "Same as wines, filtered to optimal/drinkable + in stock.",
    "wines_on_order": "Same as wines, filtered to bottles_on_order > 0.",
    "bottles": "One row per bottle (slim: 17 columns).",
    "bottles_full": "One row per bottle (all ~37 columns). Has purchase_price, cellar_name, shelf.",
    "bottles_stored": "Same as bottles_full, filtered to stored + not in transit.",
    "bottles_consumed": "Same as bottles_full, filtered to consumed/gone.",
    "bottles_on_order": "Same as bottles_full, filtered to on order / in transit.",
    "tracked_wines": "Cross-vintage tracked wines for price monitoring.",
}

_COLUMN_HINTS: dict[tuple[str, str], str] = {
    ("wines", "subregion"): "Sub-appellation or commune (agents often guess 'appellation')",
    ("wines", "primary_grape"): "Dominant grape variety (agents often guess 'grape_variety' or 'grape')",
    (
        "wines",
        "price",
    ): "List/catalogue price per bottle in CHF — see bottles view for actual purchase prices (agents often guess 'price_chf')",
    ("wines", "category"): "Wine colour/type: red, white, rose, sparkling (agents often guess 'color' or 'type')",
    ("wines", "winery_name"): "Producer name (agents often guess 'producer' or 'domaine')",
    ("wines", "blend_type"): "varietal / blend / unknown (agents often guess 'grape_type')",
    ("wines", "style_tags"): "Merged tags: subcategory + sweetness + effervescence + specialty",
    ("wines_full", "best_pro_score"): "Highest professional rating (agents often guess 'score' or 'rating')",
    ("wines_full", "cellar_value"): "Total CHF value of stored bottles",
    (
        "bottles",
        "price",
    ): "Actual purchase price per bottle in CHF (different from wines.price which is the list/catalogue price)",
    ("bottles_full", "price"): "Actual purchase price per bottle in CHF",
}


@mcp.resource("schema://views")
@_log_resource
def view_schemas() -> str:
    """Column reference for all queryable views.

    Returns the complete schema (view name, column, type) for every view
    available in query_cellar(). Read this before writing SQL queries.
    """
    try:
        con = _get_agent_connection()
        rows = con.execute(
            "SELECT table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = 'main' "
            "ORDER BY table_name, ordinal_position"
        ).fetchall()
    except (QueryError, DataStaleError) as exc:
        return f"Error: {exc}"

    current_view: str | None = None
    parts: list[str] = []
    for view_name, col_name, data_type in rows:
        if view_name.startswith("_"):
            continue
        if view_name != current_view:
            if current_view is not None:
                parts.append("")
            desc = _VIEW_DESCRIPTIONS.get(view_name, "")
            parts.append(f"## {view_name}")
            if desc:
                parts.append(desc)
            parts.append("| Column | Type | Hint |")
            parts.append("|--------|------|------|")
            current_view = view_name
        hint = _COLUMN_HINTS.get((view_name, col_name), "")
        parts.append(f"| {col_name} | {data_type} | {hint} |")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Tools — Newsletter Promotions
# ---------------------------------------------------------------------------


@_tool()
def scan_promotions(
    retailer: str = "",
    dry_run: bool = False,
    meta: dict | None = None,
) -> str:
    """Scan newsletter emails for wine promotions and extract deals.

    Connects to the configured IMAP server, fetches newsletters from
    known retailers, and extracts structured promotion data (wine,
    price, discount, rating). Enhanced matching scores each promotion
    by cellar relevance: re-buy opportunities, similar wines, and
    cellar gap fills.

    Args:
        retailer: Only scan a specific retailer (e.g. "kapweine"). Empty = all.
        dry_run: If true, scan but do not archive emails or update state.
    """
    from .promotions import scan_once
    from .promotions.report import format_scored_report

    try:
        settings = _load_mcp_settings()
        result = scan_once(
            settings,
            dry_run=dry_run,
            retailer_filter=retailer or None,
        )
        return format_scored_report(result)
    except Exception as exc:
        return f"Error: {exc}"


@_tool()
def promotion_matches(
    months: int = 3,
    category: str = "",
    retailer: str = "",
    min_score: float = 0.0,
    meta: dict | None = None,
) -> str:
    """Query historical promotion matches scored by cellar relevance.

    Returns promotions that matched your cellar in previous scans,
    filtered by category and recency. Use for month-over-month analysis
    of which promotions are most relevant to your collection.

    Match categories:
    - "rebuy": promotions for wines you own, priced below purchase price
    - "similar": promotions for wines structurally similar to your collection
    - "gap_fill": promotions covering underrepresented areas in your cellar

    Args:
        months: How far back to look (default 3 months).
        category: Filter by match category ("rebuy", "similar", "gap_fill"). Empty = all.
        retailer: Filter by retailer ID. Empty = all.
        min_score: Minimum value_score threshold (0.0–1.0). Default 0.0 = all.
    """
    from .promotions.persistence import load_matches

    try:
        data_dir = _data_dir()
        matches = load_matches(data_dir, months=max(months, 1))

        # Apply filters
        if category:
            matches = [m for m in matches if m.get("match_category") == category]
        if retailer:
            matches = [m for m in matches if m.get("retailer_id") == retailer]
        if min_score > 0:
            matches = [m for m in matches if (m.get("value_score") or 0) >= min_score]

        if not matches:
            return f"No promotion matches found in the last {months} month(s)."

        # Format as markdown table
        lines = [
            f"## Promotion Matches (last {months} month(s))",
            "",
            f"**Total:** {len(matches)} matches",
            "",
            "| Date | Score | Category | Retailer | Wine | Price | Detail |",
            "|------|-------|----------|----------|------|-------|--------|",
        ]
        for m in matches[:50]:  # cap output
            scan_time = m.get("scan_time")
            date_str = scan_time.strftime("%Y-%m-%d") if hasattr(scan_time, "strftime") else "—"
            score = f"{m.get('value_score', 0):.2f}"
            cat = m.get("match_category", "—")
            ret = m.get("retailer_id", "—")
            wine = m.get("wine_name", "—")
            price = f"{m.get('currency', 'CHF')} {m.get('sale_price', '—')}"
            detail = m.get("gap_detail") or m.get("matched_wine_name") or "—"
            lines.append(f"| {date_str} | {score} | {cat} | {ret} | {wine} | {price} | {detail} |")

        return "\n".join(lines)
    except Exception as exc:
        return f"Error: {exc}"


@_tool()
def promotion_history(
    months: int = 6,
    meta: dict | None = None,
) -> str:
    """Month-by-month summary of promotion match trends.

    Aggregates historical promotion matches by month, showing counts
    per category, average value scores, and top retailers. Use for
    tracking how newsletter relevance evolves over time.

    Args:
        months: How many months of history to include (default 6).
    """
    from .promotions.persistence import load_matches

    try:
        data_dir = _data_dir()
        matches = load_matches(data_dir, months=max(months, 1))

        if not matches:
            return f"No promotion matches found in the last {months} month(s)."

        # Group by month
        monthly: dict[str, dict] = {}
        for m in matches:
            scan_time = m.get("scan_time")
            if not hasattr(scan_time, "strftime"):
                continue
            month_key = scan_time.strftime("%Y-%m")
            bucket = monthly.setdefault(
                month_key,
                {
                    "total": 0,
                    "rebuy": 0,
                    "similar": 0,
                    "gap_fill": 0,
                    "scores": [],
                    "retailers": {},
                },
            )
            bucket["total"] += 1
            cat = m.get("match_category", "")
            if cat in ("rebuy", "similar", "gap_fill"):
                bucket[cat] += 1
            score = m.get("value_score", 0)
            if score:
                bucket["scores"].append(score)
            ret = m.get("retailer_id", "")
            if ret:
                bucket["retailers"][ret] = bucket["retailers"].get(ret, 0) + 1

        # Format
        lines = [
            f"## Promotion History (last {months} months)",
            "",
            "| Month | Total | Re-buy | Similar | Gap Fill | Avg Score | Top Retailer |",
            "|-------|-------|--------|---------|----------|-----------|--------------|",
        ]
        for month_key in sorted(monthly.keys()):
            b = monthly[month_key]
            avg = sum(b["scores"]) / len(b["scores"]) if b["scores"] else 0
            top_ret = max(b["retailers"], key=b["retailers"].get) if b["retailers"] else "—"
            lines.append(
                f"| {month_key} | {b['total']} | {b['rebuy']} | "
                f"{b['similar']} | {b['gap_fill']} | {avg:.2f} | {top_ret} |"
            )

        return "\n".join(lines)
    except Exception as exc:
        return f"Error: {exc}"


# ---------------------------------------------------------------------------
# Prompts â€” workflow templates for agent reasoning
# ---------------------------------------------------------------------------


@mcp.prompt()
@_log_prompt
def cellar_qa() -> str:
    """System prompt for answering questions about the wine cellar."""
    try:
        con = _get_connection()
        stats = q.cellar_stats(con)
    except (DataStaleError, QueryError):
        stats = "(Cellar statistics unavailable â€” run reload_data first.)"

    return f"""You are a wine cellar assistant. You have access to a personal wine cellar
with the following profile:

{stats}

Available data tools: query_cellar, cellar_stats, find_wine, read_dossier,
update_dossier, reload_data, pending_research.

These tools provide DATA only. You provide the REASONING.
Always mention the data freshness (last ETL run date) when sharing statistics.
When suggesting wines, prefer wines in their optimal drinking window.
The cellar owner is based in Switzerland â€” prices are typically in CHF.
Comments in the dossiers may be in German (Swiss retailer descriptions)."""


@mcp.prompt()
@_log_prompt
def food_pairing(dish: str) -> str:
    """Workflow template for food pairing â€” agent does the reasoning."""
    return f"""The user wants wine pairing suggestions for: {dish}

You are the sommelier. Use your wine + food knowledge to reason about pairings.
The MCP tools give you the cellar data; you provide the expertise.

Workflow:
1. Use query_cellar() to find wines in cellar that match pairing criteria
   (category, grapes, region, body, sweetness â€” based on YOUR knowledge of
   what pairs well with {dish})
2. For the top candidates, read_dossier() to check tasting notes, alcohol,
   and any owner comments
3. Apply your reasoning: grape affinity, weight matching, flavour bridges,
   regional tradition, etc.
4. Present 3-5 options with brief reasoning for each pairing
5. Note wines in their optimal window â€” they should be prioritised
6. If owner comments (often German) mention food pairings, include them"""


@mcp.prompt()
@_log_prompt
def wine_research(wine_id: int) -> str:
    """Workflow template for deep wine research â€” agent synthesises knowledge."""
    return f"""Research wine #{wine_id} thoroughly.

You are the researcher. Use your wine knowledge + web search to build a
complete profile. MCP tools handle data I/O; you provide the synthesis.

Workflow:
1. read_dossier({wine_id}) â€” review current state
2. Check agent_sections_pending in frontmatter for what's missing
3. For each pending section, use your knowledge (and web search if available)
   to draft the content:
   - producer_profile: winery history, philosophy, key wines
   - vintage_report: weather, harvest conditions, regional consensus
   - wine_description: style, aromatics, structure, ageing potential
   - market_availability: typical price range, where to buy
   - ratings_reviews: professional scores (Parker, Suckling, Decanter, etc.)
   - food_pairings: classic + creative pairing suggestions
4. update_dossier({wine_id}, section, content) for each completed section
5. read_dossier({wine_id}) â€” verify updates were applied
6. Summarise what was found and updated"""


@mcp.prompt()
@_log_prompt
def batch_research(limit: int = 10) -> str:
    """Workflow template for researching multiple wines in a batch."""
    return f"""Research the top {limit} wines that have pending sections.

Workflow:
1. pending_research(limit={limit}) â€” get the priority queue
2. For each wine, follow the wine-research workflow above
3. Pace yourself: complete one wine fully before starting the next
4. After each wine, briefly report what was filled in
5. At the end, summarise: wines researched, sections filled, any gaps"""
