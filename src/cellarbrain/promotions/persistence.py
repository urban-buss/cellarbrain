"""Persistence layer for promotion match results.

Stores scored promotion matches to year-partitioned Parquet files,
enabling historical querying and month-by-month trend analysis.
"""

from __future__ import annotations

import logging
import pathlib
from datetime import UTC, datetime

from .models import PromotionMatch

logger = logging.getLogger(__name__)


def save_matches(
    matches: list[PromotionMatch],
    scan_time: datetime,
    data_dir: pathlib.Path,
) -> int:
    """Persist scored promotion matches to year-partitioned Parquet.

    Returns the number of matches saved.
    """
    if not matches:
        return 0

    from .. import writer

    existing = writer.read_partitioned_parquet_rows("promotion_match", data_dir)
    max_id = max((r["match_id"] for r in existing), default=0)

    rows = [_match_to_row(m, scan_time, match_id=max_id + i + 1) for i, m in enumerate(matches)]

    writer.append_partitioned_parquet(
        "promotion_match",
        rows,
        data_dir,
        partition_field="scan_time",
    )
    logger.info("Saved %d promotion matches to Parquet", len(rows))
    return len(rows)


def load_matches(
    data_dir: pathlib.Path,
    months: int = 6,
) -> list[dict]:
    """Load promotion matches from Parquet, filtered by recency.

    Args:
        data_dir: Path to the data directory containing Parquet files.
        months: Only return matches from the last N months.

    Returns:
        List of match dicts sorted by scan_time descending.
    """
    from .. import writer

    all_rows = writer.read_partitioned_parquet_rows("promotion_match", data_dir)
    if not all_rows:
        return []

    cutoff = datetime.now(tz=UTC) - _timedelta_months(months)

    filtered = []
    for row in all_rows:
        scan_time = row.get("scan_time")
        if scan_time is None:
            continue
        # Ensure timezone-aware comparison
        if hasattr(scan_time, "timestamp"):
            ts = scan_time if scan_time.tzinfo else scan_time.replace(tzinfo=UTC)
            if ts >= cutoff:
                filtered.append(row)
        else:
            filtered.append(row)

    filtered.sort(key=lambda r: r.get("scan_time", datetime.min), reverse=True)
    return filtered


def _match_to_row(
    match: PromotionMatch,
    scan_time: datetime,
    match_id: int,
) -> dict:
    """Convert a PromotionMatch to a Parquet row dict."""
    promo = match.promotion
    return {
        "match_id": match_id,
        "scan_time": scan_time,
        "retailer_id": promo.retailer_id,
        "wine_name": promo.wine_name,
        "producer": promo.producer,
        "vintage": promo.vintage,
        "sale_price": promo.sale_price,
        "currency": promo.currency,
        "original_price": promo.original_price,
        "discount_pct": promo.discount_pct,
        "match_type": match.match_type,
        "match_category": match.match_category,
        "confidence": match.confidence,
        "wine_id": match.wine_id,
        "matched_wine_name": match.wine_name,
        "bottles_owned": match.bottles_owned,
        "reference_price": match.reference_price,
        "discount_vs_reference": match.discount_vs_reference,
        "similar_to_wine_id": match.similar_to_wine_id,
        "similarity_score": match.similarity_score,
        "value_score": match.value_score,
        "gap_dimension": match.gap_dimension or None,
        "gap_detail": match.gap_detail or None,
    }


def _timedelta_months(months: int) -> __import__("datetime").timedelta:
    """Approximate N months as days (30.44 days/month)."""
    from datetime import timedelta

    return timedelta(days=int(months * 30.44))
