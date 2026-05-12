"""Markdown report builder for promotion scan results."""

from __future__ import annotations

from .models import ExtractedPromotion, PromotionMatch, PromotionScanResult


def format_report(result: PromotionScanResult) -> str:
    """Format a PromotionScanResult as a Markdown report.

    Structure:
    - Scan summary (newsletters processed, promotions found)
    - Per-retailer breakdown
    - All extracted promotions table
    - Errors (if any)
    """
    lines: list[str] = []

    # Header
    lines.append("## Newsletter Promotion Scan")
    lines.append("")
    lines.append(
        f"**Scanned:** {result.newsletters_processed} newsletters · **Promotions found:** {result.total_promotions}"
    )
    lines.append("")

    # Per-retailer stats
    if result.retailer_stats:
        lines.append("### By Retailer")
        lines.append("")
        lines.append("| Retailer | Emails | Products |")
        lines.append("|----------|--------|----------|")
        for retailer_id, stats in sorted(result.retailer_stats.items()):
            lines.append(f"| {retailer_id} | {stats.get('emails', 0)} | {stats.get('products', 0)} |")
        lines.append("")

    # Promotions table
    if result.promotions:
        lines.append("### Extracted Promotions")
        lines.append("")
        lines.append("| Retailer | Producer | Wine | Vintage | Sale Price | Original | Discount | Rating |")
        lines.append("|----------|----------|------|---------|-----------|----------|----------|--------|")
        for promo in result.promotions:
            lines.append(_format_promo_row(promo))
        lines.append("")

    # Errors
    if result.errors:
        lines.append("### Errors")
        lines.append("")
        for error in result.errors:
            lines.append(f"- {error}")
        lines.append("")

    # Cellar matches
    if result.matches:
        lines.append("### Cellar Matches")
        lines.append("")
        lines.append("| Retailer | Promotion | Match | Confidence | Bottles | Promo Price | Cellar Price | vs Cellar |")
        lines.append("|----------|-----------|-------|------------|---------|-------------|-------------|-----------|")
        for match in result.matches:
            lines.append(_format_match_row(match))
        lines.append("")

    return "\n".join(lines)


def _format_promo_row(promo: ExtractedPromotion) -> str:
    """Format a single promotion as a Markdown table row."""
    vintage = str(promo.vintage) if promo.vintage else "—"
    sale = f"{promo.currency} {promo.sale_price}"
    original = f"{promo.currency} {promo.original_price}" if promo.original_price else "—"

    if promo.discount_pct is not None:
        discount = f"-{promo.discount_pct:.0f}%"
    elif promo.original_price and promo.original_price > 0:
        from .price_utils import compute_discount_pct

        pct = compute_discount_pct(promo.sale_price, promo.original_price)
        discount = f"-{pct:.0f}%"
    else:
        discount = "—"

    rating = f"{promo.rating_source} {promo.rating_score}" if promo.rating_score else "—"

    return (
        f"| {promo.retailer_id} | {promo.producer} | {promo.wine_name} | "
        f"{vintage} | {sale} | {original} | {discount} | {rating} |"
    )


def _format_match_row(match: PromotionMatch) -> str:
    """Format a single cellar match as a Markdown table row."""
    promo = match.promotion
    promo_price = f"{promo.currency} {promo.sale_price}"
    cellar_price = f"CHF {match.reference_price}" if match.reference_price else "—"
    vs_cellar = f"{match.discount_vs_reference:+.0f}%" if match.discount_vs_reference is not None else "—"
    conf = f"{match.confidence:.0%}"
    return (
        f"| {promo.retailer_id} | {promo.wine_name} | "
        f"{match.wine_name} | {conf} | {match.bottles_owned} | "
        f"{promo_price} | {cellar_price} | {vs_cellar} |"
    )


# ---------------------------------------------------------------------------
# Scored report — QW-5: categorised promotion matches
# ---------------------------------------------------------------------------


def format_scored_report(result: PromotionScanResult) -> str:
    """Format a PromotionScanResult with categorised scored matches.

    Groups scored matches into:
    - Re-buy Opportunities (cheaper than purchase price)
    - Similar to Your Collection (structural similarity)
    - Fill Cellar Gaps (underrepresented dimensions)

    Falls back to basic format_report() if no scored_matches present.
    """
    if not result.scored_matches:
        return format_report(result)

    lines: list[str] = []

    # Header (reuse scan summary)
    lines.append("## Newsletter Promotion Scan")
    lines.append("")
    lines.append(
        f"**Scanned:** {result.newsletters_processed} newsletters · "
        f"**Promotions found:** {result.total_promotions} · "
        f"**Actionable matches:** {len(result.scored_matches)}"
    )
    lines.append("")

    # Per-retailer stats
    if result.retailer_stats:
        lines.append("### By Retailer")
        lines.append("")
        lines.append("| Retailer | Emails | Products |")
        lines.append("|----------|--------|----------|")
        for retailer_id, stats in sorted(result.retailer_stats.items()):
            lines.append(f"| {retailer_id} | {stats.get('emails', 0)} | {stats.get('products', 0)} |")
        lines.append("")

    # Categorised scored matches
    rebuy = [m for m in result.scored_matches if m.match_category == "rebuy"]
    similar = [m for m in result.scored_matches if m.match_category == "similar"]
    gap_fill = [m for m in result.scored_matches if m.match_category == "gap_fill"]

    if rebuy:
        lines.append("### Re-buy Opportunities")
        lines.append("")
        lines.append("| Score | Retailer | Wine | Promo Price | Your Price | Savings | Bottles Owned |")
        lines.append("|-------|----------|------|-------------|-----------|---------|---------------|")
        for m in rebuy:
            lines.append(_format_rebuy_row(m))
        lines.append("")

    if similar:
        lines.append("### Similar to Your Collection")
        lines.append("")
        lines.append("| Score | Retailer | Promotion | Similar To | Similarity | Price |")
        lines.append("|-------|----------|-----------|-----------|-----------|-------|")
        for m in similar:
            lines.append(_format_similar_row(m))
        lines.append("")

    if gap_fill:
        lines.append("### Fill Cellar Gaps")
        lines.append("")
        lines.append("| Score | Retailer | Wine | Price | Gap | Detail |")
        lines.append("|-------|----------|------|-------|-----|--------|")
        for m in gap_fill:
            lines.append(_format_gap_row(m))
        lines.append("")

    # Errors
    if result.errors:
        lines.append("### Errors")
        lines.append("")
        for error in result.errors:
            lines.append(f"- {error}")
        lines.append("")

    return "\n".join(lines)


def _format_rebuy_row(match: PromotionMatch) -> str:
    """Format a re-buy match row."""
    promo = match.promotion
    promo_price = f"{promo.currency} {promo.sale_price}"
    cellar_price = f"CHF {match.reference_price}" if match.reference_price else "—"
    savings = f"{match.discount_vs_reference:+.0f}%" if match.discount_vs_reference is not None else "—"
    return (
        f"| {match.value_score:.2f} | {promo.retailer_id} | {promo.wine_name} | "
        f"{promo_price} | {cellar_price} | {savings} | {match.bottles_owned} |"
    )


def _format_similar_row(match: PromotionMatch) -> str:
    """Format a similarity match row."""
    promo = match.promotion
    promo_price = f"{promo.currency} {promo.sale_price}"
    sim_score = f"{match.similarity_score:.0%}" if match.similarity_score else "—"
    return (
        f"| {match.value_score:.2f} | {promo.retailer_id} | {promo.wine_name} | "
        f"{match.wine_name} | {sim_score} | {promo_price} |"
    )


def _format_gap_row(match: PromotionMatch) -> str:
    """Format a gap-fill match row."""
    promo = match.promotion
    promo_price = f"{promo.currency} {promo.sale_price}"
    return (
        f"| {match.value_score:.2f} | {promo.retailer_id} | {promo.wine_name} | "
        f"{promo_price} | {match.gap_dimension} | {match.gap_detail} |"
    )
