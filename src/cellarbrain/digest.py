"""Cellar digest — proactive daily/weekly intelligence brief.

Aggregates urgency warnings, newly optimal wines, today's top recommendation,
inventory summary, recent ETL changes, and promotion matches into a single
formatted brief for agents and CLI consumers.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

import duckdb

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WineSummary:
    """Minimal wine reference for digest entries."""

    wine_id: int
    wine_name: str
    winery_name: str
    vintage: int | None
    bottles_stored: int
    drinking_status: str | None
    optimal_until: int | None = None
    optimal_from: int | None = None


@dataclass(frozen=True)
class InventorySummary:
    """High-level cellar inventory numbers."""

    total_wines: int
    total_bottles: int
    total_value: float | None
    past_optimal_count: int
    optimal_count: int
    too_young_count: int


@dataclass(frozen=True)
class EtlChange:
    """Summary of recent ETL activity."""

    last_run_timestamp: str | None
    last_run_mode: str | None
    inserts: int
    updates: int
    deletes: int


@dataclass(frozen=True)
class DigestResult:
    """Complete cellar digest output."""

    date: date
    period: str  # "daily" or "weekly"
    drink_soon: list[WineSummary] = field(default_factory=list)
    newly_optimal: list[WineSummary] = field(default_factory=list)
    top_pick: WineSummary | None = None
    top_pick_reason: str | None = None
    inventory: InventorySummary | None = None
    recent_changes: EtlChange | None = None
    promotion_matches: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

_DRINK_SOON_SQL = """\
SELECT wine_id, wine_name, winery_name, vintage, bottles_stored,
       drinking_status, optimal_until
FROM wines
WHERE bottles_stored > 0
  AND drinking_status = 'past_optimal'
ORDER BY optimal_until ASC NULLS LAST
LIMIT 5
"""

_NEWLY_OPTIMAL_SQL = """\
SELECT wine_id, wine_name, winery_name, vintage, bottles_stored,
       drinking_status, optimal_from
FROM wines
WHERE bottles_stored > 0
  AND drinking_status = 'optimal'
  AND optimal_from = ?
ORDER BY wine_name ASC
LIMIT 5
"""

_INVENTORY_SQL = """\
SELECT
    count(*) AS total_wines,
    sum(bottles_stored) AS total_bottles,
    sum(cellar_value) AS total_value,
    count(*) FILTER (WHERE drinking_status = 'past_optimal') AS past_optimal,
    count(*) FILTER (WHERE drinking_status = 'optimal') AS optimal,
    count(*) FILTER (WHERE drinking_status = 'too_young') AS too_young
FROM wines
WHERE bottles_stored > 0
"""

_LAST_ETL_SQL = """\
SELECT etl_timestamp, etl_mode, total_inserts, total_updates, total_deletes
FROM etl_run
ORDER BY etl_timestamp DESC
LIMIT 1
"""


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------


def build_digest(
    con: duckdb.DuckDBPyConnection,
    data_dir: str | None = None,
    *,
    today: date | None = None,
    period: str = "daily",
) -> DigestResult:
    """Compute the cellar digest from current data.

    Args:
        con: DuckDB connection with views available (internal connection).
        data_dir: Data directory for recommendation engine access.
        today: Override current date (for testing).
        period: "daily" or "weekly" — affects lookback for changes.

    Returns:
        DigestResult with all sections populated.
    """
    if today is None:
        today = date.today()
    current_year = today.year

    # --- Drink soon (past optimal wines) ---
    drink_soon: list[WineSummary] = []
    try:
        rows = con.execute(_DRINK_SOON_SQL).fetchall()
        for row in rows:
            drink_soon.append(
                WineSummary(
                    wine_id=row[0],
                    wine_name=row[1],
                    winery_name=row[2],
                    vintage=row[3],
                    bottles_stored=row[4],
                    drinking_status=row[5],
                    optimal_until=row[6],
                )
            )
    except Exception as exc:
        logger.debug("drink_soon query failed: %s", exc)

    # --- Newly optimal (wines entering optimal window this year) ---
    newly_optimal: list[WineSummary] = []
    try:
        rows = con.execute(_NEWLY_OPTIMAL_SQL, [current_year]).fetchall()
        for row in rows:
            newly_optimal.append(
                WineSummary(
                    wine_id=row[0],
                    wine_name=row[1],
                    winery_name=row[2],
                    vintage=row[3],
                    bottles_stored=row[4],
                    drinking_status=row[5],
                    optimal_from=row[6],
                )
            )
    except Exception as exc:
        logger.debug("newly_optimal query failed: %s", exc)

    # --- Top pick (today's recommendation) ---
    top_pick: WineSummary | None = None
    top_pick_reason: str | None = None
    try:
        from .recommend import RecommendParams, recommend

        params = RecommendParams(limit=1)
        results = recommend(con, params, today=today, data_dir=data_dir)
        if results:
            r = results[0]
            top_pick = WineSummary(
                wine_id=r.wine_id,
                wine_name=r.wine_name,
                winery_name=r.winery_name,
                vintage=r.vintage,
                bottles_stored=r.bottles_stored,
                drinking_status=r.drinking_status,
            )
            top_pick_reason = r.reason
    except Exception as exc:
        logger.debug("top_pick recommendation failed: %s", exc)

    # --- Inventory summary ---
    inventory: InventorySummary | None = None
    try:
        row = con.execute(_INVENTORY_SQL).fetchone()
        if row:
            inventory = InventorySummary(
                total_wines=row[0] or 0,
                total_bottles=row[1] or 0,
                total_value=row[2],
                past_optimal_count=row[3] or 0,
                optimal_count=row[4] or 0,
                too_young_count=row[5] or 0,
            )
    except Exception as exc:
        logger.debug("inventory query failed: %s", exc)

    # --- Recent ETL changes ---
    recent_changes: EtlChange | None = None
    try:
        row = con.execute(_LAST_ETL_SQL).fetchone()
        if row:
            recent_changes = EtlChange(
                last_run_timestamp=str(row[0]) if row[0] else None,
                last_run_mode=row[1],
                inserts=row[2] or 0,
                updates=row[3] or 0,
                deletes=row[4] or 0,
            )
    except Exception as exc:
        logger.debug("etl_run query failed: %s", exc)

    # --- Promotion matches (optional — graceful if not installed) ---
    promotion_matches: list[str] = []
    if data_dir:
        try:
            from .promotions.state import load_state

            state = load_state(data_dir)
            # State tracks processed UIDs — we look at recent report files
            # For simplicity, report promotions as available if state exists
            if state:
                promotion_matches.append("Promotion data available — run `scan_promotions` for latest deals")
        except Exception:
            pass  # promotions extra not installed or no state

    return DigestResult(
        date=today,
        period=period,
        drink_soon=drink_soon,
        newly_optimal=newly_optimal,
        top_pick=top_pick,
        top_pick_reason=top_pick_reason,
        inventory=inventory,
        recent_changes=recent_changes,
        promotion_matches=promotion_matches,
    )


# ---------------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------------


def format_digest(result: DigestResult) -> str:
    """Render a DigestResult as a human-readable Markdown brief.

    Examples:
        >>> from datetime import date
        >>> r = DigestResult(date=date(2026, 5, 11), period="daily")
        >>> "Cellar Digest" in format_digest(r)
        True
    """
    lines: list[str] = []
    period_label = "Daily" if result.period == "daily" else "Weekly"
    lines.append(f"# {period_label} Cellar Digest — {result.date.strftime('%d %B %Y')}")
    lines.append("")

    # --- Inventory ---
    if result.inventory:
        inv = result.inventory
        value_str = f" | CHF {inv.total_value:,.0f} value" if inv.total_value else ""
        lines.append(f"**Cellar:** {inv.total_wines} wines | {inv.total_bottles} bottles{value_str}")
        lines.append(
            f"**Status:** {inv.optimal_count} optimal | "
            f"{inv.past_optimal_count} past optimal | "
            f"{inv.too_young_count} too young"
        )
        lines.append("")

    # --- Drink Soon ---
    if result.drink_soon:
        lines.append("## 🚨 Drink Soon")
        lines.append("")
        lines.append("Wines past their optimal window — consider opening soon:")
        lines.append("")
        for w in result.drink_soon:
            until_str = f" (optimal until {w.optimal_until})" if w.optimal_until else ""
            lines.append(
                f"- **{w.winery_name} {w.wine_name}** {w.vintage or '—'}"
                f"{until_str} — {w.bottles_stored} bottle{'s' if w.bottles_stored != 1 else ''}"
            )
        lines.append("")

    # --- Newly Optimal ---
    if result.newly_optimal:
        lines.append("## ✨ Newly Optimal")
        lines.append("")
        lines.append("Wines that entered their optimal drinking window this year:")
        lines.append("")
        for w in result.newly_optimal:
            lines.append(
                f"- **{w.winery_name} {w.wine_name}** {w.vintage or '—'}"
                f" — {w.bottles_stored} bottle{'s' if w.bottles_stored != 1 else ''}"
            )
        lines.append("")

    # --- Top Pick ---
    if result.top_pick:
        lines.append("## 🍷 Today's Pick")
        lines.append("")
        pick = result.top_pick
        lines.append(f"**{pick.winery_name} {pick.wine_name}** {pick.vintage or '—'}")
        if result.top_pick_reason:
            lines.append(f"  _{result.top_pick_reason}_")
        lines.append("")

    # --- Recent Changes ---
    if result.recent_changes:
        ch = result.recent_changes
        lines.append("## 📋 Last ETL Run")
        lines.append("")
        lines.append(f"- **When:** {ch.last_run_timestamp or 'unknown'}")
        lines.append(f"- **Mode:** {ch.last_run_mode or 'unknown'}")
        changes_parts = []
        if ch.inserts:
            changes_parts.append(f"+{ch.inserts} new")
        if ch.updates:
            changes_parts.append(f"~{ch.updates} updated")
        if ch.deletes:
            changes_parts.append(f"-{ch.deletes} removed")
        if changes_parts:
            lines.append(f"- **Changes:** {', '.join(changes_parts)}")
        else:
            lines.append("- **Changes:** no changes")
        lines.append("")

    # --- Promotions ---
    if result.promotion_matches:
        lines.append("## 🏷️ Promotions")
        lines.append("")
        for pm in result.promotion_matches:
            lines.append(f"- {pm}")
        lines.append("")

    # --- Empty state ---
    if not result.drink_soon and not result.newly_optimal and not result.top_pick:
        lines.append("_No actionable items today. Your cellar is in good shape!_")
        lines.append("")

    return "\n".join(lines)
