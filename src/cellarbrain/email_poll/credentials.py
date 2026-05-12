"""Credential resolution for IMAP connections.

Supports multiple credential scopes:
- ``"ingest"`` — CSV ingestion daemon (default, backward-compatible)
- ``"newsletter"`` — newsletter promotion scanner
- ``"shared"`` — tries newsletter scope first, falls back to ingest

Resolution order per scope:
1. macOS Keychain / system keyring (via ``keyring`` library)
2. Environment variables (``CELLARBRAIN_IMAP_USER_{SCOPE}`` / ``_PASSWORD_{SCOPE}``)
3. If scope == "shared": try "ingest" credentials as fallback

Credentials are never stored in TOML configuration files.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_KEYRING_SERVICE = "cellarbrain-ingest"
_ENV_USER = "CELLARBRAIN_IMAP_USER"
_ENV_PASSWORD = "CELLARBRAIN_IMAP_PASSWORD"

# Scope-specific configuration
_SCOPE_CONFIG: dict[str, tuple[str, str, str]] = {
    # scope: (keyring_service, env_user, env_password)
    "ingest": ("cellarbrain-ingest", "CELLARBRAIN_IMAP_USER", "CELLARBRAIN_IMAP_PASSWORD"),
    "newsletter": (
        "cellarbrain-newsletter",
        "CELLARBRAIN_IMAP_USER_NEWSLETTER",
        "CELLARBRAIN_IMAP_PASSWORD_NEWSLETTER",
    ),
}


def resolve_credentials(scope: str = "ingest") -> tuple[str, str]:
    """Resolve IMAP username and password for a given scope.

    Parameters
    ----------
    scope
        Credential scope: ``"ingest"``, ``"newsletter"``, or ``"shared"``.
        ``"shared"`` tries newsletter first, then falls back to ingest.

    Returns
    -------
    (username, password) tuple.

    Raises
    ------
    ValueError
        If credentials cannot be found in any source.
    """
    if scope == "shared":
        # Try newsletter first, then ingest
        try:
            return _resolve_for_scope("newsletter")
        except ValueError:
            return _resolve_for_scope("ingest")

    return _resolve_for_scope(scope)


def _resolve_for_scope(scope: str) -> tuple[str, str]:
    """Resolve credentials for a specific scope."""
    keyring_service, env_user, env_password = _SCOPE_CONFIG.get(scope, _SCOPE_CONFIG["ingest"])

    # 1. Try system keyring
    user, password = _try_keyring(keyring_service)
    if user and password:
        logger.debug("Credentials resolved from system keyring (scope=%s)", scope)
        return user, password

    # 2. Fall back to environment variables
    user = os.environ.get(env_user, "")
    password = os.environ.get(env_password, "")
    if user and password:
        logger.debug("Credentials resolved from environment variables (scope=%s)", scope)
        return user, password

    raise ValueError(
        f"IMAP credentials not found for scope '{scope}'. Provide them via:\n"
        f"  1. System keyring (service: {keyring_service!r}), or\n"
        f"  2. Environment variables {env_user} and {env_password}\n"
        "Run 'cellarbrain ingest --setup' for interactive credential storage."
    )


def store_credentials(user: str, password: str, scope: str = "ingest") -> None:
    """Store credentials in the system keyring.

    Parameters
    ----------
    scope
        Credential scope: ``"ingest"`` or ``"newsletter"``.
    """
    import keyring

    keyring_service = _SCOPE_CONFIG.get(scope, _SCOPE_CONFIG["ingest"])[0]
    keyring.set_password(keyring_service, "username", user)
    keyring.set_password(keyring_service, "password", password)
    logger.info("Credentials stored in system keyring (service: %s)", keyring_service)


def _try_keyring(service: str = _KEYRING_SERVICE) -> tuple[str, str]:
    """Attempt to read credentials from the system keyring.

    Returns ("", "") if credentials are not stored.
    """
    import keyring

    try:
        user = keyring.get_password(service, "username") or ""
        password = keyring.get_password(service, "password") or ""
        return user, password
    except Exception:
        logger.debug("Keyring lookup failed", exc_info=True)
        return "", ""
