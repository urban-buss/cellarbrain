"""Dashboard application assembly — routes, lifespan, templates."""

from __future__ import annotations

import asyncio
import logging
import pathlib
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import duckdb
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from . import cellar_queries as cellar_q
from . import ingest_queries as ingest_q
from . import queries as obs_q
from . import workbench as wb

if TYPE_CHECKING:
    from cellarbrain.settings import DashboardConfig

_HERE = pathlib.Path(__file__).resolve().parent
_TEMPLATES = Jinja2Templates(directory=str(_HERE / "templates"))
_log = logging.getLogger(__name__)

# ---- Period parsing -------------------------------------------------------

_PERIOD_HOURS = {"1h": 1, "24h": 24, "7d": 168, "30d": 720}


def _period(request: Request) -> tuple[str, int]:
    """Extract period label and hours from query param."""
    p = request.query_params.get("period", "24h")
    return p, _PERIOD_HOURS.get(p, 24)


def _include_wb(request: Request) -> bool:
    """Check if workbench calls should be included in observability views."""
    return request.query_params.get("include_workbench") == "1"


def _wants_partial(request: Request) -> bool:
    """True when HTMX requests a fragment (not a full-page body swap).

    The period-selector links use ``hx-target="body"``; since ``<body>``
    has no ``id``, HTMX omits the ``HX-Target`` header.  Fragment requests
    (e.g. ``hx-target="#wine-tbody"``) always include it.
    """
    return request.headers.get("hx-request") == "true" and request.headers.get("hx-target") is not None


# ---- Lifespan -------------------------------------------------------------


@asynccontextmanager
async def _lifespan(app: Starlette):
    # Observability log store — use open_log_reader for multi-file support
    from cellarbrain.observability import open_log_reader

    try:
        app.state.log_db = open_log_reader(
            app.state.data_dir or ".",
            app.state.log_db_path,
        )
    except FileNotFoundError:
        # No log files yet — open an empty in-memory DB so pages don't crash
        app.state.log_db = duckdb.connect(":memory:")
        app.state.log_db.execute(
            "CREATE TABLE tool_events (event_id VARCHAR, session_id VARCHAR, "
            "turn_id VARCHAR, event_type VARCHAR, name VARCHAR, "
            "started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ, duration_ms DOUBLE, "
            "status VARCHAR, request_id VARCHAR, parameters VARCHAR, "
            "error_type VARCHAR, error_message VARCHAR, result_size INTEGER, "
            "agent_name VARCHAR, trace_id VARCHAR, client_id VARCHAR)"
        )

    # Cellar data (Parquet-backed agent connection)
    cellar_con = None
    if app.state.data_dir:
        try:
            from cellarbrain import query as q

            cellar_con = q.get_agent_connection(app.state.data_dir)
        except Exception as exc:
            _log.warning("Failed to open cellar connection: %s", exc)
    app.state.cellar_con = cellar_con
    app.state.db_lock = asyncio.Lock()

    yield

    app.state.log_db.close()
    if cellar_con is not None:
        cellar_con.close()


# ---- Route handlers (observability) ---------------------------------------


async def index(request: Request) -> HTMLResponse:
    period, hours = _period(request)
    con = request.app.state.log_db
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        overview = obs_q.get_overview(con, hours, exclude_workbench=excl)
        recent_errors = obs_q.get_errors(con, hours, limit=5, exclude_workbench=excl)
        top_tools = obs_q.get_tool_usage(con, hours, limit=10, exclude_workbench=excl)
        ingest_overview = ingest_q.get_ingest_overview(con, hours)
    return _TEMPLATES.TemplateResponse(
        request,
        "index.html",
        context={
            "period": period,
            "overview": overview,
            "errors": recent_errors,
            "top_tools": top_tools,
            "include_workbench": _include_wb(request),
            "ingest": ingest_overview,
        },
    )


async def tools(request: Request) -> HTMLResponse:
    period, hours = _period(request)
    con = request.app.state.log_db
    sort = request.query_params.get("sort", "calls")
    desc = request.query_params.get("dir", "desc") == "desc"
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_tool_usage(con, hours, sort=sort, desc=desc, exclude_workbench=excl)
    template = "partials/tool_rows.html" if _wants_partial(request) else "tools.html"
    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "period": period,
            "tools": data,
        },
    )


async def errors(request: Request) -> HTMLResponse:
    period, hours = _period(request)
    con = request.app.state.log_db
    tool = request.query_params.get("tool")
    error_type = request.query_params.get("error_type")
    offset = int(request.query_params.get("offset", "0"))
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_errors(
            con, hours, tool=tool, error_type=error_type, limit=50, offset=offset, exclude_workbench=excl
        )
        type_summary = obs_q.get_error_type_summary(con, hours, exclude_workbench=excl)
    template = "partials/error_rows.html" if _wants_partial(request) else "errors.html"
    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "period": period,
            "errors": data,
            "type_summary": type_summary,
        },
    )


async def errors_export(request: Request) -> PlainTextResponse:
    """Return an LLM-optimised plain-text error report for clipboard copy."""
    period, hours = _period(request)
    con = request.app.state.log_db
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_errors(con, hours, limit=200, offset=0, exclude_workbench=excl)
        type_summary = obs_q.get_error_type_summary(con, hours, exclude_workbench=excl)
    total = sum(ts["count"] for ts in type_summary)
    lines: list[str] = []
    lines.append(f"# Cellarbrain Error Report — last {period}")
    lines.append(f"Total errors: {total}")
    if type_summary:
        lines.append("")
        lines.append("## Error type breakdown")
        for ts in type_summary:
            lines.append(f"- {ts['error_type'] or 'Unknown'}: {ts['count']}")
    if data:
        lines.append("")
        lines.append(f"## Error details ({len(data)} most recent)")
        for i, e in enumerate(data, 1):
            ts = e["started_at"].strftime("%Y-%m-%d %H:%M:%S") if e["started_at"] else "?"
            lines.append("")
            lines.append(f"### {i}. [{ts}] {e['name']} — {e['error_type'] or 'Unknown'}")
            lines.append(f"Message: {e['error_message'] or '—'}")
            if e.get("duration_ms") is not None:
                lines.append(f"Duration: {e['duration_ms']}ms")
            if e.get("parameters"):
                lines.append(f"Parameters: {e['parameters']}")
    else:
        lines.append("")
        lines.append("No errors recorded in this period.")
    return PlainTextResponse("\n".join(lines))


async def error_detail(request: Request) -> HTMLResponse:
    event_id = request.path_params["event_id"]
    con = request.app.state.log_db
    async with request.app.state.db_lock:
        event = obs_q.get_event_detail(con, event_id)
    return _TEMPLATES.TemplateResponse(
        request,
        "partials/error_detail.html",
        context={
            "event": event,
        },
    )


async def sessions(request: Request) -> HTMLResponse:
    period, hours = _period(request)
    con = request.app.state.log_db
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_sessions(con, hours, exclude_workbench=excl)
    return _TEMPLATES.TemplateResponse(
        request,
        "sessions.html",
        context={
            "period": period,
            "sessions": data,
        },
    )


async def session_detail(request: Request) -> HTMLResponse:
    session_id = request.path_params["session_id"]
    con = request.app.state.log_db
    async with request.app.state.db_lock:
        turns = obs_q.get_session_turns(con, session_id)
    return _TEMPLATES.TemplateResponse(
        request,
        "partials/session_detail.html",
        context={
            "session_id": session_id,
            "turns": turns,
        },
    )


async def turn_events(request: Request) -> HTMLResponse:
    session_id = request.path_params["session_id"]
    turn_id = request.path_params["turn_id"]
    con = request.app.state.log_db
    async with request.app.state.db_lock:
        events = obs_q.get_turn_events(con, session_id, turn_id)
    return _TEMPLATES.TemplateResponse(
        request,
        "partials/turn_events.html",
        context={
            "events": events,
        },
    )


async def latency(request: Request) -> HTMLResponse:
    period, hours = _period(request)
    con = request.app.state.log_db
    tool = request.query_params.get("tool")
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        percentiles = obs_q.get_latency_percentiles(con, hours, tool=tool, exclude_workbench=excl)
        slow = obs_q.get_slow_calls(con, hours, exclude_workbench=excl)
    return _TEMPLATES.TemplateResponse(
        request,
        "latency.html",
        context={
            "period": period,
            "tool_filter": tool,
            "percentiles": percentiles,
            "slow_calls": slow,
        },
    )


async def live(request: Request) -> HTMLResponse:
    return _TEMPLATES.TemplateResponse(request, "live.html")


# ---- Anomaly detection routes ---------------------------------------------


async def anomalies_page(request: Request) -> HTMLResponse:
    """Full anomalies page showing all detected anomalies."""
    from cellarbrain.anomaly import detect_all

    cfg = request.app.state.anomaly_config
    con = request.app.state.log_db
    data_dir = request.app.state.data_dir or ""

    async with request.app.state.db_lock:
        detected = detect_all(
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
    return _TEMPLATES.TemplateResponse(
        request,
        "anomalies.html",
        context={"anomalies": detected},
    )


async def anomalies_banner(request: Request) -> HTMLResponse:
    """Return a small banner partial if critical/warn anomalies exist."""
    from cellarbrain.anomaly import detect_all

    cfg = request.app.state.anomaly_config
    con = request.app.state.log_db
    data_dir = request.app.state.data_dir or ""

    async with request.app.state.db_lock:
        detected = detect_all(
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

    critical = [a for a in detected if a.severity == "critical"]
    warnings = [a for a in detected if a.severity == "warn"]
    return _TEMPLATES.TemplateResponse(
        request,
        "partials/anomaly_banner.html",
        context={"critical": critical, "warnings": warnings},
    )


# ---- Ingest routes --------------------------------------------------------


async def ingest_page(request: Request) -> HTMLResponse:
    period, hours = _period(request)
    con = request.app.state.log_db
    event_type = request.query_params.get("event_type")
    severity = request.query_params.get("severity")
    async with request.app.state.db_lock:
        has_table = ingest_q.has_ingest_table(con)
        overview = ingest_q.get_ingest_overview(con, hours) if has_table else None
        events = ingest_q.get_ingest_events(con, hours, event_type=event_type, severity=severity) if has_table else []
    template = "partials/ingest_rows.html" if _wants_partial(request) else "ingest.html"
    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "period": period,
            "has_table": has_table,
            "overview": overview,
            "events": events,
            "event_type": event_type or "",
            "severity": severity or "",
        },
    )


# ---- Helpers --------------------------------------------------------------


def _int_or_none(val: str | None) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(val)
    except ValueError:
        return None


# ---- Cellar routes --------------------------------------------------------


async def cellar(request: Request) -> HTMLResponse:
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available. Start the dashboard with a valid data directory."},
            status_code=503,
        )

    q = request.query_params.get("q")
    category = request.query_params.get("category")
    region = request.query_params.get("region")
    status = request.query_params.get("status")
    vintage_min = _int_or_none(request.query_params.get("vintage_min"))
    vintage_max = _int_or_none(request.query_params.get("vintage_max"))
    hide_empty = "1" in request.query_params.getlist("hide_empty") if "hide_empty" in request.query_params else True
    sort = request.query_params.get("sort", "wine_name")
    desc = request.query_params.get("dir", "asc") == "desc"
    page = max(1, _int_or_none(request.query_params.get("page")) or 1)
    per_page = 50

    wines, total = cellar_q.get_wines(
        con,
        q=q,
        category=category,
        region=region,
        status=status,
        vintage_min=vintage_min,
        vintage_max=vintage_max,
        hide_empty=hide_empty,
        sort=sort,
        desc=desc,
        limit=per_page,
        offset=(page - 1) * per_page,
    )
    filters = cellar_q.get_filter_options(con)
    quick_stats = cellar_q.get_quick_stats(con)
    wotd = cellar_q.get_wine_of_the_day(con) if not _wants_partial(request) else None
    velocity = cellar_q.get_consumption_velocity(con) if not _wants_partial(request) else None
    total_pages = max(1, (total + per_page - 1) // per_page)

    template = "partials/wine_rows.html" if _wants_partial(request) else "cellar.html"

    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "wines": wines,
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "filters": filters,
            "quick_stats": quick_stats,
            "wotd": wotd,
            "velocity": velocity,
            "q": q or "",
            "category": category or "",
            "region": region or "",
            "status": status or "",
            "sort": sort,
            "desc": desc,
            "hide_empty": hide_empty,
        },
    )


async def wine_detail(request: Request) -> HTMLResponse:
    wine_id = int(request.path_params["wine_id"])
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    wine = cellar_q.get_wine_detail(con, wine_id)
    if wine is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": f"Wine #{wine_id} not found."},
            status_code=404,
        )

    bottles = cellar_q.get_wine_bottles(con, wine_id)
    format_siblings = cellar_q.get_format_siblings(
        con,
        wine_id,
        wine.get("format_group_id"),
    )

    # Resolve and render dossier
    dossier_data = None
    dashboard_notes = ""
    data_dir = request.app.state.data_dir
    if data_dir:
        try:
            from cellarbrain.dossier_ops import (
                read_agent_section_content,
                resolve_dossier_path,
            )

            from .dossier_render import render_dossier

            dossier_path = resolve_dossier_path(wine_id, data_dir)
            dossier_data = render_dossier(dossier_path)
            try:
                dashboard_notes = read_agent_section_content(wine_id, "dashboard_notes", data_dir)
            except Exception:
                dashboard_notes = ""
        except Exception:
            pass  # Dossier rendering is best-effort

    pending_consumed_ids: set[int] = set()
    if data_dir:
        try:
            from .sidecars import read_consumed_pending

            pending_consumed_ids = {int(it["bottle_id"]) for it in read_consumed_pending(data_dir)}
        except Exception:
            pass

    tab = request.query_params.get("tab", "dossier")

    return _TEMPLATES.TemplateResponse(
        request,
        "wine_detail.html",
        context={
            "wine": wine,
            "bottles": bottles,
            "format_siblings": format_siblings,
            "dossier": dossier_data,
            "dashboard_notes": dashboard_notes,
            "pending_consumed_ids": pending_consumed_ids,
            "tab": tab,
        },
    )


# ---- Phase B/C/D: Interactive cellar actions ------------------------------


async def _read_form(request: Request) -> dict:
    """Read a form payload as a plain dict (single-value)."""
    form = await request.form()
    return {k: (v if isinstance(v, str) else "") for k, v in form.items()}


async def wine_notes_save(request: Request) -> HTMLResponse:
    """POST /cellar/{wine_id:int}/notes — save the dashboard_notes section."""
    wine_id = int(request.path_params["wine_id"])
    data_dir = request.app.state.data_dir
    if not data_dir:
        return PlainTextResponse("Cellar data not available.", status_code=503)

    payload = await _read_form(request)
    note = (payload.get("note") or "").strip()

    from cellarbrain.dossier_ops import (
        ProtectedSectionError,
        WineNotFoundError,
        read_agent_section_content,
        update_dossier,
    )

    try:
        if note:
            update_dossier(
                wine_id,
                "dashboard_notes",
                note + "\n",
                data_dir,
                agent_name="dashboard",
            )
            saved_value = note
        else:
            # Empty submission — reset section to placeholder by writing the sentinel.
            update_dossier(
                wine_id,
                "dashboard_notes",
                "*Not yet researched. Pending agent action.*\n",
                data_dir,
                agent_name="dashboard",
            )
            saved_value = ""
    except (WineNotFoundError, ProtectedSectionError) as exc:
        return PlainTextResponse(f"Error: {exc}", status_code=400)
    except Exception as exc:  # pragma: no cover — defensive
        _log.warning("notes save failed for wine %d: %s", wine_id, exc)
        return PlainTextResponse(f"Error: {exc}", status_code=500)

    # Re-read so we render whatever is canonical on disk.
    try:
        saved_value = read_agent_section_content(wine_id, "dashboard_notes", data_dir)
    except Exception:
        pass

    return _TEMPLATES.TemplateResponse(
        request,
        "partials/dashboard_notes.html",
        context={
            "wine_id": wine_id,
            "dashboard_notes": saved_value,
            "saved": True,
        },
    )


async def wine_notes_partial(request: Request) -> HTMLResponse:
    """GET /cellar/{wine_id:int}/notes — return the editor fragment."""
    wine_id = int(request.path_params["wine_id"])
    data_dir = request.app.state.data_dir
    note = ""
    if data_dir:
        try:
            from cellarbrain.dossier_ops import read_agent_section_content

            note = read_agent_section_content(wine_id, "dashboard_notes", data_dir)
        except Exception:
            note = ""
    return _TEMPLATES.TemplateResponse(
        request,
        "partials/dashboard_notes.html",
        context={
            "wine_id": wine_id,
            "dashboard_notes": note,
            "saved": False,
        },
    )


async def bottle_mark_consumed(request: Request) -> HTMLResponse:
    """POST /cellar/{wine_id:int}/bottles/{bottle_id:int}/consumed.

    Adds the bottle to the consumed-pending sidecar and returns an updated
    button fragment. The bottle is NOT removed from Parquet — that happens
    only when the user re-exports Vinocell and the next ETL imports the
    matching ``bottles-gone`` row.
    """
    wine_id = int(request.path_params["wine_id"])
    bottle_id = int(request.path_params["bottle_id"])
    data_dir = request.app.state.data_dir
    if not data_dir:
        return PlainTextResponse("Cellar data not available.", status_code=503)

    payload = await _read_form(request)
    note = (payload.get("note") or "").strip() or None

    from .sidecars import add_consumed_pending

    add_consumed_pending(data_dir, bottle_id=bottle_id, wine_id=wine_id, note=note)

    return _TEMPLATES.TemplateResponse(
        request,
        "partials/consumed_button.html",
        context={
            "wine_id": wine_id,
            "bottle_id": bottle_id,
            "pending": True,
        },
    )


async def bottle_unmark_consumed(request: Request) -> HTMLResponse:
    """POST /cellar/{wine_id:int}/bottles/{bottle_id:int}/consumed/undo."""
    wine_id = int(request.path_params["wine_id"])
    bottle_id = int(request.path_params["bottle_id"])
    data_dir = request.app.state.data_dir
    if not data_dir:
        return PlainTextResponse("Cellar data not available.", status_code=503)

    from .sidecars import remove_consumed_pending

    remove_consumed_pending(data_dir, bottle_id=bottle_id)
    return _TEMPLATES.TemplateResponse(
        request,
        "partials/consumed_button.html",
        context={
            "wine_id": wine_id,
            "bottle_id": bottle_id,
            "pending": False,
        },
    )


async def reminders_banner(request: Request) -> HTMLResponse:
    """GET /reminders — banner fragment listing pending consumed entries."""
    data_dir = request.app.state.data_dir
    items: list[dict] = []
    if data_dir:
        try:
            from .sidecars import read_consumed_pending

            items = read_consumed_pending(data_dir)
        except Exception:
            items = []
    return _TEMPLATES.TemplateResponse(
        request,
        "partials/reminder_banner.html",
        context={"pending": items, "count": len(items)},
    )


async def pending_consumed_page(request: Request) -> HTMLResponse:
    """GET /pending-consumed — full page listing pending consumed bottles."""
    data_dir = request.app.state.data_dir
    rows: list[dict] = []
    if data_dir:
        try:
            from .sidecars import read_consumed_pending

            items = read_consumed_pending(data_dir)
        except Exception:
            items = []
        if items:
            con = request.app.state.cellar_con
            if con is not None:
                rows = cellar_q.get_pending_consumed_details(con, [int(it["bottle_id"]) for it in items])
                # merge marked_at + note from sidecar
                meta = {int(it["bottle_id"]): it for it in items}
                for r in rows:
                    m = meta.get(int(r["bottle_id"]))
                    if m:
                        r["marked_at"] = m.get("marked_at")
                        r["note"] = m.get("note")
    return _TEMPLATES.TemplateResponse(
        request,
        "pending_consumed.html",
        context={"rows": rows, "count": len(rows)},
    )


async def drink_tonight_page(request: Request) -> HTMLResponse:
    """GET /drink-tonight — list of wines staged for tonight."""
    data_dir = request.app.state.data_dir
    items: list[dict] = []
    rows: list[dict] = []
    if data_dir:
        try:
            from .sidecars import read_drink_tonight

            items = read_drink_tonight(data_dir)
        except Exception:
            items = []
        if items:
            con = request.app.state.cellar_con
            if con is not None:
                rows = cellar_q.get_wines_by_ids(con, [int(it["wine_id"]) for it in items])
                meta = {int(it["wine_id"]): it for it in items}
                for r in rows:
                    m = meta.get(int(r["wine_id"]))
                    if m:
                        r["added_at"] = m.get("added_at")
                        r["note"] = m.get("note")
    return _TEMPLATES.TemplateResponse(
        request,
        "drink_tonight.html",
        context={"rows": rows, "count": len(rows)},
    )


async def drink_tonight_sync(request: Request) -> JSONResponse:
    """POST /drink-tonight — replace the full server-side list.

    Accepts JSON body ``{"items": [{"wine_id": int, "added_at"?: str,
    "note"?: str}]}`` and returns the normalised list.
    """
    data_dir = request.app.state.data_dir
    if not data_dir:
        return JSONResponse({"error": "Cellar data not available."}, status_code=503)

    try:
        body = await request.json()
    except Exception:
        body = {}
    items = body.get("items") if isinstance(body, dict) else None
    if not isinstance(items, list):
        items = []

    from .sidecars import write_drink_tonight

    saved = write_drink_tonight(data_dir, items)
    return JSONResponse({"items": saved, "count": len(saved)})


async def drink_tonight_get(request: Request) -> JSONResponse:
    """GET /drink-tonight.json — server-side mirror of the list."""
    data_dir = request.app.state.data_dir
    if not data_dir:
        return JSONResponse({"items": [], "count": 0})
    from .sidecars import read_drink_tonight

    items = read_drink_tonight(data_dir)
    return JSONResponse({"items": items, "count": len(items)})


async def drink_tonight_add(request: Request) -> HTMLResponse:
    """POST /drink-tonight/add — append a wine, return updated button fragment."""
    data_dir = request.app.state.data_dir
    if not data_dir:
        return PlainTextResponse("Cellar data not available.", status_code=503)

    payload = await _read_form(request)
    try:
        wine_id = int(payload.get("wine_id", ""))
    except ValueError:
        return PlainTextResponse("Invalid wine_id", status_code=400)
    note = (payload.get("note") or "").strip() or None

    from .sidecars import add_drink_tonight

    add_drink_tonight(data_dir, wine_id=wine_id, note=note)
    return _TEMPLATES.TemplateResponse(
        request,
        "partials/drink_tonight_button.html",
        context={"wine_id": wine_id, "in_list": True},
    )


async def drink_tonight_remove(request: Request) -> HTMLResponse:
    """POST /drink-tonight/remove — drop a wine, return updated button fragment."""
    data_dir = request.app.state.data_dir
    if not data_dir:
        return PlainTextResponse("Cellar data not available.", status_code=503)

    payload = await _read_form(request)
    try:
        wine_id = int(payload.get("wine_id", ""))
    except ValueError:
        return PlainTextResponse("Invalid wine_id", status_code=400)

    from .sidecars import remove_drink_tonight

    remove_drink_tonight(data_dir, wine_id=wine_id)
    return _TEMPLATES.TemplateResponse(
        request,
        "partials/drink_tonight_button.html",
        context={"wine_id": wine_id, "in_list": False},
    )


# ---- Smart Recommendations -----------------------------------------------


async def recommend_page(request: Request) -> HTMLResponse:
    """GET /recommend — form + results page for smart recommendations."""
    from ..recommend import BUDGETS, OCCASIONS

    return _TEMPLATES.TemplateResponse(
        request,
        "recommend.html",
        context={
            "occasions": list(OCCASIONS.keys()),
            "budgets": list(BUDGETS.keys()),
            "results": [],
            "count": 0,
        },
    )


async def recommend_run(request: Request) -> HTMLResponse:
    """POST /recommend — score wines and return results fragment."""
    from ..recommend import (
        BUDGETS,
        OCCASIONS,
        RecommendParams,
        recommend,
    )

    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "partials/recommend_results.html",
            context={"results": [], "count": 0, "error": "Cellar data not available."},
        )

    payload = await _read_form(request)
    occasion = payload.get("occasion") or None
    cuisine = payload.get("cuisine") or None
    budget = payload.get("budget") or None
    try:
        guests = int(payload.get("guests", "")) if payload.get("guests") else None
    except ValueError:
        guests = None
    try:
        limit = int(payload.get("limit", "5"))
    except ValueError:
        limit = 5
    limit = min(max(limit, 1), 15)

    params = RecommendParams(
        occasion=occasion,
        cuisine=cuisine,
        guests=guests,
        budget=budget,
        limit=limit,
    )
    data_dir = request.app.state.data_dir
    results = recommend(con, params, data_dir=data_dir)

    # Check which wines are already on the drink-tonight list
    drink_tonight_ids: set[int] = set()
    if data_dir:
        try:
            from .sidecars import read_drink_tonight

            for item in read_drink_tonight(data_dir):
                try:
                    drink_tonight_ids.add(int(item["wine_id"]))
                except (KeyError, ValueError, TypeError):
                    pass
        except Exception:
            pass

    result_dicts = []
    for rec in results:
        result_dicts.append(
            {
                "wine_id": rec.wine_id,
                "wine_name": rec.wine_name,
                "vintage": rec.vintage,
                "winery_name": rec.winery_name,
                "category": rec.category,
                "drinking_status": rec.drinking_status,
                "total_score": rec.total_score,
                "reason": rec.reason,
                "in_list": rec.wine_id in drink_tonight_ids,
            }
        )

    return _TEMPLATES.TemplateResponse(
        request,
        "partials/recommend_results.html",
        context={
            "results": result_dicts,
            "count": len(result_dicts),
            "error": None,
            "occasions": list(OCCASIONS.keys()),
            "budgets": list(BUDGETS.keys()),
        },
    )


# ---- Phase E: Drinking-window timeline ------------------------------------


async def drinking_timeline_page(request: Request) -> HTMLResponse:
    """GET /drinking/timeline — Chart.js floating-bar drinking-window timeline."""
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )
    rows = cellar_q.get_drinking_window_dataset(con)
    return _TEMPLATES.TemplateResponse(
        request,
        "drinking_timeline.html",
        context={"rows": rows, "count": len(rows)},
    )


# ---- Phase F: Cellar heatmap ----------------------------------------------


async def cellar_heatmap_page(request: Request) -> HTMLResponse:
    """GET /cellars/heatmap — server-rendered SVG-grid coloured by status."""
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )
    cellars = cellar_q.get_heatmap_layout(con)
    return _TEMPLATES.TemplateResponse(
        request,
        "heatmap.html",
        context={"cellars": cellars},
    )


async def cellar_bottles(request: Request) -> HTMLResponse:
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    view = request.query_params.get("view", "stored")
    cellar_filter = request.query_params.get("cellar")
    category = request.query_params.get("category")
    sort = request.query_params.get("sort", "wine_name")
    desc = request.query_params.get("dir", "asc") == "desc"
    page = max(1, _int_or_none(request.query_params.get("page")) or 1)
    per_page = 50

    bottle_list, total = cellar_q.get_bottles(
        con,
        view=view,
        cellar=cellar_filter,
        category=category,
        sort=sort,
        desc=desc,
        limit=per_page,
        offset=(page - 1) * per_page,
    )
    cellar_names = cellar_q.get_cellar_names(con)
    total_pages = max(1, (total + per_page - 1) // per_page)

    template = "partials/bottle_rows.html" if _wants_partial(request) else "bottles.html"

    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "bottles": bottle_list,
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "view": view,
            "cellar_names": cellar_names,
            "cellar_filter": cellar_filter or "",
            "category": category or "",
            "sort": sort,
            "desc": desc,
        },
    )


async def drinking(request: Request) -> HTMLResponse:
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    wines = cellar_q.get_drinking_now(con)
    return _TEMPLATES.TemplateResponse(
        request,
        "drinking.html",
        context={
            "wines": wines,
        },
    )


# ---- Food pairing ---------------------------------------------------------


async def pairing_page(request: Request) -> HTMLResponse:
    """Food pairing interactive page — classify dish, retrieve candidates."""
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    results = None
    params: dict[str, str | None] = {}

    if request.method == "POST":
        form = await request.form()
        params = {
            "dish_description": form.get("dish_description", ""),
            "category": form.get("category") or None,
            "weight": form.get("weight") or None,
            "protein": form.get("protein") or None,
            "cuisine": form.get("cuisine") or None,
            "grapes": form.get("grapes") or None,
        }

        from cellarbrain import pairing

        grape_list = [g.strip() for g in params["grapes"].split(",")] if params["grapes"] else None
        results = pairing.retrieve_candidates(
            con,
            dish_description=params["dish_description"],
            category=params["category"],
            weight=params["weight"],
            protein=params["protein"],
            cuisine=params["cuisine"],
            grapes=grape_list,
            limit=15,
        )

    template = "partials/pairing_results.html" if _wants_partial(request) else "pairing.html"
    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={"results": results, "params": params},
    )


# ---- SQL playground -------------------------------------------------------

_QUICK_QUERIES = [
    {
        "label": "Wines by region",
        "sql": "SELECT country, region, COUNT(*) AS wines, SUM(bottles_stored) AS bottles FROM wines GROUP BY country, region ORDER BY bottles DESC LIMIT 20",
    },
    {
        "label": "Stock by cellar",
        "sql": "SELECT cellar_name, COUNT(*) AS bottles, ROUND(SUM(price), 0) AS value FROM bottles_stored GROUP BY cellar_name ORDER BY bottles DESC",
    },
    {
        "label": "Drinking now",
        "sql": "SELECT wine_name, vintage, winery_name, drinking_status, bottles_stored FROM wines WHERE drinking_status IN ('optimal', 'drinkable') ORDER BY vintage",
    },
    {"label": "Top wines", "sql": "SELECT wine_name, vintage, winery_name FROM wines ORDER BY wine_name LIMIT 20"},
    {
        "label": "Recent purchases",
        "sql": "SELECT wine_name, vintage, purchase_price, purchase_date, provider FROM bottles_full ORDER BY purchase_date DESC LIMIT 20",
    },
    {
        "label": "Price tiers",
        "sql": "SELECT CASE WHEN price < 20 THEN 'Under 20' WHEN price < 50 THEN '20-50' WHEN price < 100 THEN '50-100' ELSE '100+' END AS tier, COUNT(*) AS wines, SUM(bottles_stored) AS bottles FROM wines GROUP BY tier ORDER BY MIN(price)",
    },
    {
        "label": "Consumption history",
        "sql": "SELECT wine_name, vintage, consume_date, consume_reason FROM bottles_full WHERE status = 'consumed' ORDER BY consume_date DESC LIMIT 20",
    },
    {
        "label": "On order",
        "sql": "SELECT wine_name, vintage, provider, purchase_price FROM bottles_full WHERE is_in_transit ORDER BY wine_name",
    },
]


def _get_view_names(con: duckdb.DuckDBPyConnection) -> list[str]:
    """List all views in the agent connection."""
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW' ORDER BY table_name"
    ).fetchall()
    return [r[0] for r in rows]


async def sql_playground(request: Request) -> HTMLResponse:
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    sql = ""
    result = None
    error = None
    elapsed_ms = None
    columns: list[str] = []

    if request.method == "POST":
        form = await request.form()
        sql = form.get("sql", "").strip()
        max_rows = min(int(form.get("max_rows", "500") or "500"), 1000)

        if sql:
            try:
                from cellarbrain.query import validate_sql

                validate_sql(sql)
                import time

                t0 = time.monotonic()
                df = con.execute(sql).fetchdf()
                elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
                if len(df) > max_rows:
                    df = df.head(max_rows)
                columns = list(df.columns)
                result = df.to_dict(orient="records")
            except Exception as exc:
                error = str(exc)

    views = _get_view_names(con)

    template = "partials/sql_results.html" if _wants_partial(request) else "sql.html"

    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "sql": sql,
            "result": result,
            "columns": columns,
            "error": error,
            "elapsed_ms": elapsed_ms,
            "quick_queries": _QUICK_QUERIES,
            "views": views,
            "max_rows": max_rows if request.method == "POST" else 500,
        },
    )


# ---- Statistics dashboard -------------------------------------------------


async def stats(request: Request) -> HTMLResponse:
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    group_by = request.query_params.get("group_by", "country")
    sort = request.query_params.get("sort", "bottles")
    desc = request.query_params.get("dir", "desc") == "desc"

    overview = cellar_q.get_cellar_stats_overview(con)
    grouped = cellar_q.get_cellar_stats_grouped(
        con,
        group_by=group_by,
        sort=sort,
        desc=desc,
    )

    template = "partials/stats_content.html" if _wants_partial(request) else "stats.html"

    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "overview": overview,
            "grouped": grouped,
            "group_by": group_by,
            "sort": sort,
        },
    )


# ---- Tracked wines --------------------------------------------------------


async def tracked(request: Request) -> HTMLResponse:
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    wines = cellar_q.get_tracked_wines(con)
    return _TEMPLATES.TemplateResponse(
        request,
        "tracked.html",
        context={
            "wines": wines,
        },
    )


async def tracked_detail(request: Request) -> HTMLResponse:
    tracked_id = int(request.path_params["id"])
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    wine = cellar_q.get_tracked_wine_detail(con, tracked_id)
    if wine is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": f"Tracked wine #{tracked_id} not found."},
            status_code=404,
        )

    prices_chart = cellar_q.get_price_chart_data(con, tracked_id)
    prices_table = cellar_q.get_price_observations(con, tracked_id)

    # Companion dossier (best-effort)
    companion_data = None
    data_dir = request.app.state.data_dir
    if data_dir:
        try:
            from cellarbrain.companion_markdown import _find_existing_companion

            from .dossier_render import render_dossier

            companion_dir = pathlib.Path(data_dir) / "wines" / "tracked"
            path = _find_existing_companion(tracked_id, companion_dir)
            if path is not None and path.exists():
                companion_data = render_dossier(path)
        except Exception:
            pass

    return _TEMPLATES.TemplateResponse(
        request,
        "tracked_detail.html",
        context={
            "wine": wine,
            "prices_chart": prices_chart,
            "prices_table": prices_table,
            "companion": companion_data,
        },
    )


# ---- JSON API (Chart.js) -------------------------------------------------


async def api_volume(request: Request) -> JSONResponse:
    _, hours = _period(request)
    con = request.app.state.log_db
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_hourly_volume(con, hours, exclude_workbench=excl)
    return JSONResponse(data)


async def api_latency_hist(request: Request) -> JSONResponse:
    _, hours = _period(request)
    con = request.app.state.log_db
    tool = request.query_params.get("tool")
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_latency_histogram(con, hours, tool=tool, exclude_workbench=excl)
    return JSONResponse(data)


async def api_latency_ts(request: Request) -> JSONResponse:
    _, hours = _period(request)
    con = request.app.state.log_db
    tool = request.query_params.get("tool")
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_latency_timeseries(con, hours, tool=tool, exclude_workbench=excl)
    return JSONResponse(data)


async def api_top_tools(request: Request) -> JSONResponse:
    _, hours = _period(request)
    limit = int(request.query_params.get("limit", "10"))
    con = request.app.state.log_db
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_tool_usage(con, hours, limit=limit, exclude_workbench=excl)
    return JSONResponse(data)


# ---- Refresh endpoint (manual fallback) -----------------------------------


async def api_refresh(request: Request) -> JSONResponse:
    """Reopen the DuckDB log connection as a manual refresh fallback."""
    from cellarbrain.observability import open_log_reader

    app = request.app
    async with app.state.db_lock:
        try:
            app.state.log_db.close()
        except Exception:
            pass
        try:
            app.state.log_db = open_log_reader(
                app.state.data_dir or ".",
                app.state.log_db_path,
            )
        except FileNotFoundError:
            app.state.log_db = duckdb.connect(":memory:")
    return JSONResponse({"status": "ok"})


# ---- SSE live tail --------------------------------------------------------


def _sse_message(html: str) -> str:
    """Format an HTML snippet as an SSE message event."""
    lines = html.replace("\n", "")
    return f"event: message\ndata: {lines}\n\n"


def _render_event_card(event: dict) -> str:
    """Render a single event as an HTML snippet for the live tail."""
    status_class = " error" if event["status"] == "error" else ""
    ts = event["started_at"]
    if hasattr(ts, "strftime"):
        ts = ts.strftime("%H:%M:%S.%f")[:-3]
    params_html = ""
    if event.get("parameters"):
        from html import escape

        params_html = (
            '<details style="margin:0;font-size:0.75rem;">'
            f'<summary style="cursor:pointer;">params</summary>'
            f'<pre style="margin:0;white-space:pre-wrap;word-break:break-word;">'
            f"{escape(str(event['parameters']))}</pre></details>"
        )
    error_html = ""
    if event.get("error_type"):
        from html import escape

        error_html = f' <span style="color:var(--pico-del-color);">{escape(str(event["error_type"]))}</span>'
    return (
        f'<div class="event-card{status_class}">'
        f'<span class="ts">{ts}</span> '
        f'<span class="type">{event["event_type"]}</span> '
        f'<span class="name">{event["name"]}</span> '
        f'<span class="status">{event["status"]}</span> '
        f'<span class="ms">{event["duration_ms"]:.0f} ms</span>'
        f"{error_html}{params_html}"
        f"</div>"
    )


def _render_turn_boundary() -> str:
    """Render a turn separator line for the live tail."""
    return '<div class="turn-boundary">── new turn ──</div>'


async def api_live(request: Request) -> StreamingResponse:
    """Server-Sent Events stream of new tool events."""
    con = request.app.state.log_db
    lock = request.app.state.db_lock
    filter_type = request.query_params.get("filter_type") or None
    filter_status = request.query_params.get("filter_status") or None
    excl = not _include_wb(request)
    # _max_polls: testing hook — limits poll iterations (None = infinite)
    _max_polls_raw = request.query_params.get("_max_polls")
    max_polls = int(_max_polls_raw) if _max_polls_raw is not None else None

    async def event_generator():
        last_id = None
        # Initial batch (last 20 events)
        async with lock:
            events = obs_q.get_recent_events(
                con,
                limit=20,
                exclude_workbench=excl,
                event_type=filter_type,
                status=filter_status,
            )
        if events:
            last_id = events[-1]["event_id"]
            prev_turn = None
            for event in events:
                if prev_turn and event["turn_id"] != prev_turn:
                    yield _sse_message(_render_turn_boundary())
                prev_turn = event["turn_id"]
                yield _sse_message(_render_event_card(event))

        # Poll for new events
        polls = 0
        while max_polls is None or polls < max_polls:
            await asyncio.sleep(1)
            if await request.is_disconnected():
                break
            try:
                async with lock:
                    events = obs_q.get_recent_events(
                        con,
                        after_id=last_id,
                        limit=50,
                        exclude_workbench=excl,
                        event_type=filter_type,
                        status=filter_status,
                    )
            except Exception:
                continue
            if not events:
                yield ": keepalive\n\n"
            else:
                prev_turn = None
                for event in events:
                    if prev_turn and event["turn_id"] != prev_turn:
                        yield _sse_message(_render_turn_boundary())
                    prev_turn = event["turn_id"]
                    yield _sse_message(_render_event_card(event))
                last_id = events[-1]["event_id"]
            polls += 1

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ---- Workbench routes -----------------------------------------------------


async def workbench_list(request: Request) -> HTMLResponse:
    settings = request.app.state.settings
    all_tools = wb.discover_tools()
    tools_list = wb.filter_tools(
        all_tools,
        read_only=settings.workbench_read_only,
        allow_list=settings.workbench_allow or None,
    )
    grouped: dict[str, list] = {}
    for t in tools_list:
        grouped.setdefault(t.category, []).append(t)
    return _TEMPLATES.TemplateResponse(
        request,
        "workbench_list.html",
        context={
            "grouped": grouped,
            "total": len(tools_list),
        },
    )


async def workbench_tool(request: Request) -> HTMLResponse:
    tool_name = request.path_params["tool"]
    settings = request.app.state.settings

    tool = wb.get_tool(tool_name)
    if tool is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": f"Tool '{tool_name}' not found."},
            status_code=404,
        )

    # Safety check — is this tool allowed?
    if tool.is_write:
        allow = settings.workbench_allow or []
        if settings.workbench_read_only and tool_name not in allow:
            return _TEMPLATES.TemplateResponse(
                request,
                "error.html",
                context={"message": f"Tool '{tool_name}' is a write tool and is not enabled."},
                status_code=403,
            )

    result = None
    if request.method == "POST":
        form = await request.form()
        params = {p["name"]: form.get(p["name"], "") for p in tool.parameters}
        result = await wb.execute_tool(tool, params)

    template = "partials/workbench_response.html" if _wants_partial(request) and result else "workbench_tool.html"

    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "tool": tool,
            "result": result,
        },
    )


async def workbench_batch(request: Request) -> HTMLResponse:
    results = None
    selected = None

    if request.method == "POST":
        form = await request.form()
        seq_name = form.get("sequence")
        for seq in wb.BATCH_SEQUENCES:
            if seq.name == seq_name:
                selected = seq
                results = await wb.run_batch(seq)
                break

    return _TEMPLATES.TemplateResponse(
        request,
        "workbench_batch.html",
        context={
            "sequences": wb.BATCH_SEQUENCES,
            "selected": selected,
            "results": results,
        },
    )


# ---- Application factory --------------------------------------------------


def build_app(
    log_db_path: str | None,
    data_dir: str | None = None,
    dashboard_config: DashboardConfig | None = None,
    anomaly_config=None,
) -> Starlette:
    from cellarbrain.settings import AnomalyConfig, DashboardConfig

    app = Starlette(
        lifespan=_lifespan,
        routes=[
            # Observability pages
            Route("/", index),
            Route("/tools", tools),
            Route("/errors", errors),
            Route("/errors/export", errors_export),
            Route("/errors/{event_id}", error_detail),
            Route("/sessions", sessions),
            Route("/sessions/{session_id}", session_detail),
            Route("/sessions/{session_id}/{turn_id}", turn_events),
            Route("/latency", latency),
            Route("/live", live),
            Route("/anomalies", anomalies_page),
            Route("/anomalies/banner", anomalies_banner),
            Route("/ingest", ingest_page),
            # Cellar browser
            Route("/cellar", cellar),
            Route("/cellar/{wine_id:int}", wine_detail),
            Route("/cellar/{wine_id:int}/notes", wine_notes_partial, methods=["GET"]),
            Route("/cellar/{wine_id:int}/notes", wine_notes_save, methods=["POST"]),
            Route(
                "/cellar/{wine_id:int}/bottles/{bottle_id:int}/consumed",
                bottle_mark_consumed,
                methods=["POST"],
            ),
            Route(
                "/cellar/{wine_id:int}/bottles/{bottle_id:int}/consumed/undo",
                bottle_unmark_consumed,
                methods=["POST"],
            ),
            Route("/reminders", reminders_banner, methods=["GET"]),
            Route("/pending-consumed", pending_consumed_page, methods=["GET"]),
            Route("/drink-tonight", drink_tonight_page, methods=["GET"]),
            Route("/drink-tonight", drink_tonight_sync, methods=["POST"]),
            Route("/drink-tonight.json", drink_tonight_get, methods=["GET"]),
            Route("/drink-tonight/add", drink_tonight_add, methods=["POST"]),
            Route("/drink-tonight/remove", drink_tonight_remove, methods=["POST"]),
            Route("/recommend", recommend_page, methods=["GET"]),
            Route("/recommend", recommend_run, methods=["POST"]),
            Route("/bottles", cellar_bottles),
            Route("/drinking", drinking),
            Route("/drinking/timeline", drinking_timeline_page),
            Route("/cellars/heatmap", cellar_heatmap_page),
            Route("/pairing", pairing_page, methods=["GET", "POST"]),
            Route("/sql", sql_playground, methods=["GET", "POST"]),
            Route("/stats", stats),
            Route("/tracked", tracked),
            Route("/tracked/{id:int}", tracked_detail),
            # Workbench
            Route("/workbench", workbench_list),
            Route("/workbench/batch", workbench_batch, methods=["GET", "POST"]),
            Route("/workbench/{tool}", workbench_tool, methods=["GET", "POST"]),
            # JSON API
            Route("/api/volume", api_volume),
            Route("/api/latency-hist", api_latency_hist),
            Route("/api/latency-ts", api_latency_ts),
            Route("/api/top-tools", api_top_tools),
            Route("/api/refresh", api_refresh, methods=["POST"]),
            Route("/api/live", api_live),
            # Static files
            Mount("/static", StaticFiles(directory=str(_HERE / "static")), name="static"),
        ],
    )
    app.state.log_db_path = log_db_path
    app.state.data_dir = data_dir
    app.state.settings = dashboard_config or DashboardConfig()
    app.state.anomaly_config = anomaly_config or AnomalyConfig()
    return app
