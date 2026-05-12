"""Newsletter promotion scanner.

Monitors an IMAP mailbox for wine retailer newsletters, extracts
promotions using retailer-specific parsers, matches them against
the cellar and tracked wines, evaluates deal quality, and returns
a structured report.

Public API:
    - ``scan_once()`` — single scan cycle
    - ``PromotionScanResult`` — result dataclass
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

from .models import ExtractedPromotion, PromotionMatch, PromotionScanResult

logger = logging.getLogger(__name__)


def scan_once(
    settings: object,
    *,
    dry_run: bool = False,
    retailer_filter: str | None = None,
) -> PromotionScanResult:
    """Execute a single promotion scan cycle.

    Steps:
    1. Discover and register parsers
    2. Resolve credentials and fetch emails
    3. Route each email to its parser and extract promotions
    4. Optionally archive raw .eml files
    5. Update processed-UID state
    6. Return structured result
    """
    from ..email_poll.credentials import resolve_credentials
    from .fetch import fetch_newsletters
    from .registry import discover_parsers, route_email
    from .state import load_state, save_state

    config = settings.promotions
    data_dir = Path(settings.paths.data_dir)

    discover_parsers()
    state = load_state(data_dir)
    processed_uids = set(state.get("processed_uids", []))

    # Resolve credentials
    user, password = resolve_credentials(scope=config.credential_scope)

    # Build sender whitelist from enabled retailers
    sender_patterns: list[str] = []
    for _rid, rcfg in config.retailers.items():
        if rcfg.enabled:
            sender_patterns.extend(rcfg.sender_patterns)

    # Fetch
    emails = fetch_newsletters(
        host=config.imap_host,
        port=config.imap_port,
        use_ssl=config.use_ssl,
        user=user,
        password=password,
        mailbox=config.mailbox,
        sender_patterns=sender_patterns,
        processed_uids=processed_uids,
        max_age_days=config.max_age_days,
        mark_color=config.processed_color if (config.mark_processed and not dry_run) else "",
    )

    # Parse
    all_promotions: list[ExtractedPromotion] = []
    errors: list[str] = []
    retailer_stats: dict[str, dict] = {}

    for email_msg in emails:
        if retailer_filter and not _matches_retailer(email_msg.sender, retailer_filter, config):
            continue

        result = route_email(email_msg.sender, email_msg.subject, email_msg.text_plain, email_msg.text_html, config)
        if result is None:
            continue

        retailer_id, parser = result
        try:
            promos = parser.extract(email_msg.text_plain, email_msg.text_html, email_msg.subject)
        except Exception as exc:
            errors.append(f"{retailer_id}: {exc}")
            logger.warning("Parser error for %s (UID %d): %s", retailer_id, email_msg.uid, exc)
            promos = []

        for p in promos:
            p.retailer_id = retailer_id
            p.email_date = email_msg.date
            p.email_subject = email_msg.subject
        all_promotions.extend(promos)

        # Track stats per retailer
        stats = retailer_stats.setdefault(retailer_id, {"emails": 0, "products": 0})
        stats["emails"] += 1
        stats["products"] += len(promos)

    # Archive raw emails if configured
    if config.archive_raw:
        _archive_emails(emails, config.archive_dir, config, data_dir)

    # Match against cellar
    matches: list[PromotionMatch] = []
    scored_matches: list[PromotionMatch] = []
    if all_promotions:
        try:
            from .matching import match_promotions

            matches = match_promotions(all_promotions, str(data_dir))
        except Exception as exc:
            logger.warning("Cellar matching failed: %s", exc)

        # Enhanced scoring (QW-5)
        try:
            from .matching import score_promotions

            scored_matches = score_promotions(all_promotions, str(data_dir))
        except Exception as exc:
            logger.warning("Promotion scoring failed: %s", exc)

        # Persist scored matches
        if scored_matches and not dry_run:
            try:
                from .persistence import save_matches

                save_matches(scored_matches, datetime.now(UTC), data_dir)
            except Exception as exc:
                logger.warning("Promotion match persistence failed: %s", exc)

    # Update state
    if not dry_run and config.mark_processed:
        new_uids = [e.uid for e in emails]
        state.setdefault("processed_uids", []).extend(new_uids)
        state["last_scan"] = datetime.now(UTC).isoformat()
        save_state(data_dir, state)

    return PromotionScanResult(
        scan_time=datetime.now(UTC),
        newsletters_processed=len(emails),
        total_promotions=len(all_promotions),
        promotions=all_promotions,
        matches=matches,
        scored_matches=scored_matches,
        errors=errors,
        retailer_stats=retailer_stats,
    )


def _matches_retailer(sender: str, retailer_filter: str, config: object) -> bool:
    """Check if a sender matches the filtered retailer."""
    import fnmatch

    rcfg = config.retailers.get(retailer_filter)
    if rcfg is None:
        return False
    sender_lower = sender.lower()
    return any(fnmatch.fnmatch(sender_lower, pat) for pat in rcfg.sender_patterns)


def _archive_emails(emails: list, archive_dir: str, config: object, data_dir: Path) -> None:
    """Save raw .eml files to the archive directory."""
    from .fetch import NewsletterEmail

    base = data_dir.parent / archive_dir
    for em in emails:
        if not isinstance(em, NewsletterEmail) or not em.raw_bytes:
            continue
        retailer_dir = base / _infer_retailer_dir(em.sender, config)
        retailer_dir.mkdir(parents=True, exist_ok=True)
        date_str = em.date.strftime("%Y-%m-%d") if em.date else "unknown"
        filename = f"{date_str}_{em.uid}.eml"
        path = retailer_dir / filename
        if not path.exists():
            path.write_bytes(em.raw_bytes)


def _infer_retailer_dir(sender: str, config: object) -> str:
    """Infer retailer directory name from sender address."""
    import fnmatch

    sender_lower = sender.lower()
    for retailer_id, rcfg in config.retailers.items():
        if any(fnmatch.fnmatch(sender_lower, pat) for pat in rcfg.sender_patterns):
            return retailer_id
    return "_unknown"


__all__ = ["ExtractedPromotion", "PromotionMatch", "PromotionScanResult", "scan_once"]
