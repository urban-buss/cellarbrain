"""Newsletter email fetching layer.

Wraps email_poll.imap.ImapClient with newsletter-specific logic:
- Searches by sender whitelist (not subject)
- Extracts both text/plain and text/html MIME parts
- Filters by already-processed UIDs
"""

from __future__ import annotations

import email
import email.policy
import email.utils
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

# Apple Mail color-flag keyword mapping (mirrors ImapClient._COLOR_KEYWORDS).
_COLOR_KEYWORDS: dict[str, list[bytes] | None] = {
    "orange": [b"$MailFlagBit0"],
    "red": [],
    "yellow": [b"$MailFlagBit1"],
    "blue": [b"$MailFlagBit0", b"$MailFlagBit1"],
    "green": [b"$MailFlagBit2"],
    "purple": [b"$MailFlagBit0", b"$MailFlagBit2"],
    "gray": [b"$MailFlagBit1", b"$MailFlagBit2"],
    "none": None,
}


@dataclass
class NewsletterEmail:
    """A fetched newsletter email ready for parsing."""

    uid: int
    sender: str
    subject: str
    date: datetime
    text_plain: str
    text_html: str
    raw_bytes: bytes


def fetch_newsletters(
    host: str,
    port: int,
    use_ssl: bool,
    user: str,
    password: str,
    mailbox: str,
    sender_patterns: list[str],
    processed_uids: set[int],
    max_age_days: int = 7,
    mark_color: str = "",
) -> list[NewsletterEmail]:
    """Connect to IMAP and fetch unprocessed newsletter emails.

    Reuses ``email_poll.imap.ImapClient`` for connection management.
    Searches for ALL messages in the mailbox, then filters client-side
    by sender patterns and already-processed UIDs.

    When *mark_color* is set, already-flagged emails are excluded at
    the IMAP level via ``UNKEYWORD``, and matched emails are marked
    with the given color flag before the connection closes.
    """
    from ..email_poll.imap import ImapClient

    results: list[NewsletterEmail] = []

    with ImapClient(host, port, use_ssl) as client:
        client.login(user, password)
        client.select_folder(mailbox)

        # Determine exclusion keywords from the processed color
        exclude_kw: list[bytes] | None = None
        if mark_color:
            kw = _COLOR_KEYWORDS.get(mark_color.lower())
            if kw:
                exclude_kw = kw

        # Search for all messages (IMAP FROM search is single-value;
        # we filter by sender pattern client-side)
        uids = client.search_all(exclude_keywords=exclude_kw)

        # Filter out already-processed
        new_uids = [uid for uid in uids if uid not in processed_uids]
        if not new_uids:
            logger.info("No new newsletter emails found")
            return results

        logger.info("Found %d new newsletter emails (of %d total)", len(new_uids), len(uids))

        # Fetch raw messages
        raw_messages = client.fetch_raw(new_uids)

        for uid, raw_bytes in raw_messages.items():
            msg = email.message_from_bytes(raw_bytes, policy=email.policy.default)

            # Extract sender
            from_header = msg.get("From", "")
            _, sender_addr = email.utils.parseaddr(from_header)
            sender_addr = sender_addr.lower()

            # Filter by sender patterns
            if not _matches_any_sender(sender_addr, sender_patterns):
                continue

            # Extract date
            date_header = msg.get("Date", "")
            msg_date = _parse_date(date_header)

            # Skip old messages
            if max_age_days > 0 and msg_date:
                age = (datetime.now(UTC) - msg_date).days
                if age > max_age_days:
                    continue

            # Extract subject
            subject = msg.get("Subject", "")

            # Extract MIME parts
            text_plain, text_html = _extract_mime_parts(msg)

            results.append(
                NewsletterEmail(
                    uid=uid,
                    sender=sender_addr,
                    subject=subject,
                    date=msg_date or datetime.now(UTC),
                    text_plain=text_plain,
                    text_html=text_html,
                    raw_bytes=raw_bytes,
                )
            )

        # Mark matched emails as processed on the IMAP server
        if mark_color and results:
            matched_uids = [r.uid for r in results]
            client.mark_processed(matched_uids, color=mark_color)

    logger.info("Fetched %d newsletter emails matching sender patterns", len(results))
    return results


def fetch_from_archive(
    archive_dir: str,
    retailer: str | None = None,
) -> list[NewsletterEmail]:
    """Load newsletter emails from the local .eml archive (for testing/re-parsing).

    Reads .eml files from ``archive_dir/{retailer}/`` and returns them as
    NewsletterEmail objects with uid=0.
    """
    from pathlib import Path

    base = Path(archive_dir)
    if not base.exists():
        return []

    results: list[NewsletterEmail] = []
    dirs = [base / retailer] if retailer else [d for d in base.iterdir() if d.is_dir()]

    for dir_path in dirs:
        if not dir_path.exists():
            continue
        for eml_path in sorted(dir_path.glob("*.eml")):
            raw_bytes = eml_path.read_bytes()
            msg = email.message_from_bytes(raw_bytes, policy=email.policy.default)

            from_header = msg.get("From", "")
            _, sender_addr = email.utils.parseaddr(from_header)

            date_header = msg.get("Date", "")
            msg_date = _parse_date(date_header)

            subject = msg.get("Subject", "")
            text_plain, text_html = _extract_mime_parts(msg)

            results.append(
                NewsletterEmail(
                    uid=0,
                    sender=sender_addr.lower(),
                    subject=subject,
                    date=msg_date or datetime.now(UTC),
                    text_plain=text_plain,
                    text_html=text_html,
                    raw_bytes=raw_bytes,
                )
            )

    return results


def _matches_any_sender(sender: str, patterns: list[str]) -> bool:
    """Check if sender matches any pattern in the list."""
    import fnmatch

    return any(fnmatch.fnmatch(sender, pattern) for pattern in patterns)


def _parse_date(date_str: str) -> datetime | None:
    """Parse an RFC 2822 date string into a timezone-aware datetime."""
    if not date_str:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(date_str)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed
    except (ValueError, TypeError):
        return None


def _extract_mime_parts(msg: email.message.Message) -> tuple[str, str]:
    """Extract text/plain and text/html parts from a MIME message."""
    text_plain = ""
    text_html = ""

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain" and not text_plain:
                payload = part.get_content()
                if isinstance(payload, str):
                    text_plain = payload
            elif content_type == "text/html" and not text_html:
                payload = part.get_content()
                if isinstance(payload, str):
                    text_html = payload
    else:
        content_type = msg.get_content_type()
        payload = msg.get_content()
        if isinstance(payload, str):
            if content_type == "text/plain":
                text_plain = payload
            elif content_type == "text/html":
                text_html = payload

    return text_plain, text_html
