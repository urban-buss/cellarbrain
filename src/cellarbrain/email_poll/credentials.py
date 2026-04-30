"""Credential resolution for the IMAP ingestion daemon.

Resolution order:
1. macOS Keychain / system keyring (via ``keyring`` library)
2. Environment variables (``CELLARBRAIN_IMAP_USER``, ``CELLARBRAIN_IMAP_PASSWORD``)

Credentials are never stored in TOML configuration files.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_KEYRING_SERVICE = "cellarbrain-ingest"
_ENV_USER = "CELLARBRAIN_IMAP_USER"
_ENV_PASSWORD = "CELLARBRAIN_IMAP_PASSWORD"


def resolve_credentials() -> tuple[str, str]:
    """Resolve IMAP username and password.

    Returns
    -------
    (username, password) tuple.

    Raises
    ------
    ValueError
        If credentials cannot be found in any source.
    """
    # 1. Try system keyring
    user, password = _try_keyring()
    if user and password:
        logger.debug("Credentials resolved from system keyring")
        return user, password

    # 2. Fall back to environment variables
    user = os.environ.get(_ENV_USER, "")
    password = os.environ.get(_ENV_PASSWORD, "")
    if user and password:
        logger.debug("Credentials resolved from environment variables")
        return user, password

    raise ValueError(
        "IMAP credentials not found. Provide them via:\n"
        f"  1. System keyring (service: {_KEYRING_SERVICE!r}), or\n"
        f"  2. Environment variables {_ENV_USER} and {_ENV_PASSWORD}\n"
        "Run 'cellarbrain ingest --setup' for interactive credential storage."
    )


def store_credentials(user: str, password: str) -> None:
    """Store credentials in the system keyring.

    Raises
    ------
    ImportError
        If the ``keyring`` package is not installed.
    """
    import keyring

    keyring.set_password(_KEYRING_SERVICE, "username", user)
    keyring.set_password(_KEYRING_SERVICE, "password", password)
    logger.info("Credentials stored in system keyring (service: %s)", _KEYRING_SERVICE)


def _try_keyring() -> tuple[str, str]:
    """Attempt to read credentials from the system keyring.

    Returns ("", "") if keyring is unavailable or credentials are not stored.
    """
    try:
        import keyring
    except ImportError:
        return "", ""

    try:
        user = keyring.get_password(_KEYRING_SERVICE, "username") or ""
        password = keyring.get_password(_KEYRING_SERVICE, "password") or ""
        return user, password
    except Exception:
        logger.debug("Keyring lookup failed", exc_info=True)
        return "", ""
