"""nbdev-squ - Python SIEM Query Utils with Ibis support."""

import logging

__version__ = "1.5.3"

# Configure default logging if not already configured
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

# Import main API components
from .api import Fmt, clients, list_securityinsights, list_workspaces, query_all
from .core import azcli, cache, logger, login
from .legacy import export_jira_issues

__all__ = [
    "clients",
    "cache",
    "logger",
    "login",
    "azcli",
    "list_workspaces",
    "list_securityinsights",
    "query_all",
    "export_jira_issues",
    "Fmt",  # Export format enum for ibis support
]
