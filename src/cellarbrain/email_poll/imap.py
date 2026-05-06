"""IMAP client wrapper for fetching Vinocell CSV export emails.

Wraps ``imapclient.IMAPClient`` with Cellarbrain-specific search,
fetch, and mark-as-processed operations.  Uses stdlib ``email`` for
MIME parsing.
"""

from __future__ import annotations

import email
import email.policy
import email.utils
import logging
from datetime import UTC, datetime
from types import TracebackType

from .grouping import EmailMessage

logger = logging.getLogger(__name__)


class ImapClient:
    """Thin wrapper around ``imapclient.IMAPClient``.

    Use as a context manager::

        with ImapClient(host, port, ssl) as client:
            client.login(user, password)
            uids = client.search_unseen("[VinoCell] CSV file")
            ...
    """

    def __init__(self, host: str, port: int, use_ssl: bool) -> None:
        self._host = host
        self._port = port
        self._use_ssl = use_ssl
        self._client: object = None  # imapclient.IMAPClient instance

    def __enter__(self) -> ImapClient:
        import imapclient

        self._client = imapclient.IMAPClient(
            self._host,
            port=self._port,
            ssl=self._use_ssl,
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

    def select_folder(self, folder: str) -> None:
        """Select an IMAP folder (mailbox)."""
        self._client.select_folder(folder, readonly=False)

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
        raw_responses = self._client.fetch(uids, ["BODY.PEEK[]", "INTERNALDATE"])

        for uid, data in raw_responses.items():
            internal_date = data.get(b"INTERNALDATE")
            if internal_date is None:
                internal_date = datetime.now(UTC)

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

    def mark_seen(self, uids: list[int]) -> None:
        """Mark messages as ``\\Seen`` (read)."""
        if uids:
            self._client.set_flags(uids, [b"\\Seen"])
            logger.info("Marked %d messages as read (UIDs: %s)", len(uids), uids)

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
