"""IMAP client wrapper for fetching Vinocell CSV export emails.

Wraps ``imapclient.IMAPClient`` with Cellarbrain-specific search,
fetch, and mark-as-processed operations.  Uses stdlib ``email`` for
MIME parsing.
"""

from __future__ import annotations

import email
import email.policy
import email.utils
import imaplib
import logging
from datetime import UTC, datetime
from types import TracebackType
from typing import ClassVar

from .grouping import EmailMessage

logger = logging.getLogger(__name__)


class IMAPTransientError(Exception):
    """Raised when the IMAP connection is aborted by a transient server error."""


class ImapClient:
    """Thin wrapper around ``imapclient.IMAPClient``.

    Use as a context manager::

        with ImapClient(host, port, ssl) as client:
            client.login(user, password)
            uids = client.search_unseen("[VinoCell] CSV file")
            ...
    """

    def __init__(self, host: str, port: int, use_ssl: bool, *, timeout: int = 60) -> None:
        self._host = host
        self._port = port
        self._use_ssl = use_ssl
        self._timeout = timeout
        self._client: object = None  # imapclient.IMAPClient instance

    def __enter__(self) -> ImapClient:
        import imapclient

        self._client = imapclient.IMAPClient(
            self._host,
            port=self._port,
            ssl=self._use_ssl,
            timeout=self._timeout,
        )
        logger.debug("Connected to %s:%d (ssl=%s)", self._host, self._port, self._use_ssl)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._client is not None:
            try:
                self._client.logout()
            except Exception:
                logger.debug("IMAP logout failed", exc_info=True)
            self._client = None

    def login(self, user: str, password: str) -> None:
        """Authenticate with the IMAP server."""
        self._client.login(user, password)
        logger.debug("Authenticated as %s", user)

    def select_folder(self, folder: str) -> int:
        """Select an IMAP folder (mailbox) and return its UIDVALIDITY."""
        response = self._client.select_folder(folder, readonly=False)
        return response.get(b"UIDVALIDITY", 0)

    def search_unseen(
        self,
        subject_filter: str,
        sender_filter: str = "",
    ) -> list[int]:
        """Search for UNSEEN messages matching filters.

        Returns a list of IMAP UIDs.
        """
        criteria = ["UNSEEN"]
        if subject_filter:
            criteria.extend(["SUBJECT", subject_filter])
        if sender_filter:
            criteria.extend(["FROM", sender_filter])

        uids = self._client.search(criteria)
        logger.debug("IMAP SEARCH returned %d UIDs", len(uids))
        return list(uids)

    def fetch_messages(
        self,
        uids: list[int],
        expected_files: tuple[str, ...] | list[str],
        *,
        max_attachment_bytes: int = 0,
    ) -> list[tuple[EmailMessage, bytes]]:
        """Fetch messages and extract single-attachment metadata + data.

        Only messages with exactly one attachment whose filename is in
        *expected_files* are returned.  Others are silently skipped.

        Parameters
        ----------
        max_attachment_bytes:
            If > 0, skip attachments exceeding this size (bytes).

        Returns list of ``(EmailMessage, attachment_bytes)`` tuples.
        """
        if not uids:
            return []

        results: list[tuple[EmailMessage, bytes]] = []
        try:
            raw_responses = self._client.fetch(uids, ["BODY.PEEK[]", "INTERNALDATE"])
        except imaplib.IMAP4.abort as exc:
            logger.warning("IMAP connection aborted during FETCH: %s", exc)
            raise IMAPTransientError(str(exc)) from exc

        for uid, data in raw_responses.items():
            internal_date = data.get(b"INTERNALDATE")
            if internal_date is None:
                internal_date = datetime.now(UTC)
            elif internal_date.tzinfo is None:
                internal_date = internal_date.replace(tzinfo=UTC)

            rfc822 = data.get(b"BODY[]") or data.get(b"RFC822", b"")
            if not rfc822:
                continue
            msg = email.message_from_bytes(rfc822, policy=email.policy.default)

            attachments = _extract_attachments(msg)
            if len(attachments) != 1:
                continue

            filename, payload = attachments[0]
            if filename not in expected_files:
                continue

            if max_attachment_bytes and len(payload) > max_attachment_bytes:
                logger.warning(
                    "Attachment %s (%d bytes) exceeds limit — skipping UID %d",
                    filename,
                    len(payload),
                    uid,
                )
                continue

            # Extract sender from From: header
            from_header = msg.get("From", "")
            _, sender_addr = email.utils.parseaddr(from_header)

            em = EmailMessage(
                uid=int(uid),
                date=internal_date,
                filename=filename,
                size=len(payload),
                sender=sender_addr.lower(),
            )
            results.append((em, payload))

        logger.debug("Fetched %d valid attachment messages from %d UIDs", len(results), len(uids))
        return results

    # Apple Mail color-flag keyword mapping (iCloud PERMANENTFLAGS \* compatible).
    _COLOR_KEYWORDS: ClassVar[dict[str, list[bytes] | None]] = {
        "orange": [b"$MailFlagBit0"],
        "red": [],
        "yellow": [b"$MailFlagBit1"],
        "blue": [b"$MailFlagBit0", b"$MailFlagBit1"],
        "green": [b"$MailFlagBit2"],
        "purple": [b"$MailFlagBit0", b"$MailFlagBit2"],
        "gray": [b"$MailFlagBit1", b"$MailFlagBit2"],
        "none": None,
    }

    def mark_seen(self, uids: list[int]) -> None:
        """Mark messages as ``\\Seen`` (read) without removing existing flags."""
        if uids:
            self._client.add_flags(uids, [b"\\Seen"])
            logger.info("Marked %d messages as read (UIDs: %s)", len(uids), uids)

    def search_all(
        self,
        sender_filter: str = "",
        since: datetime | None = None,
        exclude_keywords: list[bytes] | None = None,
    ) -> list[int]:
        """Search for ALL messages matching filters (regardless of Seen/Unseen).

        Parameters
        ----------
        sender_filter
            Optional FROM filter.
        since
            Optional SINCE date filter (IMAP date, not exact datetime).
        exclude_keywords
            Optional list of IMAP keywords to exclude via ``UNKEYWORD``.
            Used to skip messages already flagged as processed.

        Returns a list of IMAP UIDs.
        """
        criteria: list = ["ALL"]
        if sender_filter:
            criteria.extend(["FROM", sender_filter])
        if since:
            criteria.extend(["SINCE", since.date()])
        if exclude_keywords:
            for kw in exclude_keywords:
                criteria.extend(["UNKEYWORD", kw])

        uids = self._client.search(criteria)
        logger.debug("IMAP SEARCH ALL returned %d UIDs", len(uids))
        return list(uids)

    def fetch_raw(self, uids: list[int]) -> dict[int, bytes]:
        """Fetch raw RFC822 message bytes for given UIDs.

        Returns a dict mapping UID → raw bytes.
        """
        if not uids:
            return {}

        raw_responses = self._client.fetch(uids, ["BODY.PEEK[]"])
        result: dict[int, bytes] = {}
        for uid, data in raw_responses.items():
            rfc822 = data.get(b"BODY[]") or data.get(b"RFC822", b"")
            if rfc822:
                result[int(uid)] = rfc822
        logger.debug("Fetched %d raw messages from %d UIDs", len(result), len(uids))
        return result

    def mark_processed(self, uids: list[int], color: str = "orange") -> None:
        """Mark messages as read and apply an Apple Mail color flag.

        Uses IMAP ``add_flags`` so existing user-set flags are preserved.
        Supported colors: orange, red, yellow, blue, green, purple, gray, none.
        """
        if not uids:
            return
        keywords = self._COLOR_KEYWORDS.get(color.lower())
        if keywords is None:
            # "none" — only mark as seen, no color flag
            self._client.add_flags(uids, [b"\\Seen"])
        else:
            flags: list[bytes] = [b"\\Seen", b"\\Flagged", *keywords]
            self._client.add_flags(uids, flags)
        logger.info("Marked %d messages processed (color=%s, UIDs: %s)", len(uids), color, uids)

    def move_messages(self, uids: list[int], folder: str) -> None:
        """Copy messages to *folder* and delete from current folder."""
        if not uids:
            return
        self._client.copy(uids, folder)
        self._client.delete_messages(uids)
        self._client.expunge()
        logger.info("Moved %d messages to %s (UIDs: %s)", len(uids), folder, uids)


def _extract_attachments(msg: email.message.EmailMessage) -> list[tuple[str, bytes]]:
    """Extract (filename, payload) pairs from a MIME message."""
    attachments: list[tuple[str, bytes]] = []
    for part in msg.walk():
        content_disposition = part.get_content_disposition()
        if content_disposition != "attachment":
            continue
        filename = part.get_filename()
        if not filename:
            continue
        payload = part.get_payload(decode=True)
        if payload is None:
            continue
        attachments.append((filename, payload))
    return attachments
