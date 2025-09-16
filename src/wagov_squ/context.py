"""Simple resource management utilities."""

import logging
from collections.abc import Iterator
from contextlib import contextmanager

import httpx

from .settings import SquSettings

logger = logging.getLogger(__name__)


@contextmanager
def http_session(timeout: float = 30.0) -> Iterator[httpx.Client]:
    """Simple HTTP client context manager."""
    client = httpx.Client(timeout=timeout)
    try:
        yield client
    finally:
        client.close()


def validate_jira_config(settings: SquSettings) -> bool:
    """Check if Jira configuration is complete."""
    # Check both nested and legacy fields for backward compatibility
    jira_url = settings.jira.url or settings.jira_url
    jira_username = settings.jira.username or settings.jira_username
    jira_password = settings.jira.password or settings.jira_password

    return all([jira_url, jira_username, jira_password])


def validate_azure_config(settings: SquSettings) -> bool:
    """Check if Azure configuration is complete."""
    # Check both nested and legacy fields for backward compatibility
    tenant_id = settings.azure.tenant_id or settings.tenant_id
    return tenant_id is not None
