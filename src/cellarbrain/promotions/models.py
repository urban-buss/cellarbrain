"""Shared data models for the promotions pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


@dataclass
class ExtractedPromotion:
    """A single wine promotion extracted from a newsletter email."""

    wine_name: str
    producer: str
    sale_price: Decimal
    currency: str = "CHF"

    # Optional fields — parsers populate what they can
    original_price: Decimal | None = None
    discount_pct: float | None = None
    vintage: int | None = None
    appellation: str = ""
    color: str = ""
    bottle_size_ml: int = 750
    per_bottle_price: Decimal | None = None
    rating_score: str = ""
    rating_source: str = ""
    product_url: str = ""
    image_url: str = ""
    category: str = ""
    is_set: bool = False
    is_spirit: bool = False

    # Metadata (populated by orchestrator)
    retailer_id: str = ""
    email_date: datetime | None = None
    email_subject: str = ""


@dataclass
class PromotionMatch:
    """A promotion matched to a cellar or tracked wine."""

    promotion: ExtractedPromotion
    match_type: str  # "exact", "fuzzy", "tracked", "similar"
    confidence: float = 0.0

    # Cellar match
    wine_id: int | None = None
    wine_name: str = ""
    bottles_owned: int = 0

    # Tracked wine match
    tracked_wine_id: int | None = None

    # Reference pricing
    reference_price: Decimal | None = None
    reference_source: str = ""
    discount_vs_reference: float | None = None

    # Similarity match
    similar_to_wine_id: int | None = None
    similarity_score: float | None = None

    # Enhanced scoring (QW-5)
    match_category: str = ""  # "rebuy", "similar", "gap_fill"
    value_score: float = 0.0  # composite actionability score (0.0–1.0)
    gap_dimension: str = ""  # "region", "grape", "price_tier", "category"
    gap_detail: str = ""  # human-readable gap description
    cellar_bottles_in_category: int = 0


@dataclass
class PromotionScanResult:
    """Complete result of a promotion scan cycle."""

    scan_time: datetime
    newsletters_processed: int
    total_promotions: int
    promotions: list[ExtractedPromotion] = field(default_factory=list)
    matches: list[PromotionMatch] = field(default_factory=list)
    scored_matches: list[PromotionMatch] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    retailer_stats: dict[str, dict] = field(default_factory=dict)
