"""Pydantic-based settings with backward compatibility."""

from __future__ import annotations

import re
import warnings
from typing import Any
from urllib.parse import urlparse

from pydantic import ConfigDict, Field, computed_field, field_validator
from pydantic_settings import BaseSettings


class AzureConfig(BaseSettings):
    """Azure-specific configuration."""

    tenant_id: str | None = Field(
        None,
        description="Azure Active Directory tenant ID (GUID format)"
    )
    vault_name: str | None = Field(
        None,
        description="Azure Key Vault name for configuration storage"
    )
    datalake_account: str | None = Field(
        None,
        description="Azure Data Lake Storage account URL"
    )
    datalake_container: str | None = Field(
        None,
        description="Azure Data Lake Storage container name"
    )

    @field_validator('tenant_id')
    @classmethod
    def validate_tenant_id(cls, v: str | None) -> str | None:
        """Validate tenant ID is a valid GUID format."""
        if v is None:
            return v
        guid_pattern = re.compile(
            r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        )
        if not guid_pattern.match(v):
            raise ValueError('tenant_id must be a valid GUID format')
        return v

    @field_validator('datalake_account')
    @classmethod
    def validate_datalake_account(cls, v: str | None) -> str | None:
        """Validate datalake account is a valid URL."""
        if v is None:
            return v
        parsed = urlparse(v)
        if not all([parsed.scheme, parsed.netloc]):
            raise ValueError('datalake_account must be a valid URL')
        return v


class JiraConfig(BaseSettings):
    """Jira-specific configuration."""

    url: str | None = Field(
        None,
        description="Jira instance URL (e.g., https://yourorg.atlassian.net)"
    )
    username: str | None = Field(
        None,
        description="Jira username or email address for authentication"
    )
    password: str | None = Field(
        None,
        description="Jira API token or password for authentication"
    )

    @field_validator('url')
    @classmethod
    def validate_jira_url(cls, v: str | None) -> str | None:
        """Validate Jira URL format."""
        if v is None:
            return v
        parsed = urlparse(v)
        if not all([parsed.scheme in ('http', 'https'), parsed.netloc]):
            raise ValueError('Jira URL must be a valid HTTP/HTTPS URL')
        return v.rstrip('/')  # Remove trailing slash

    @computed_field
    @property
    def base_api_url(self) -> str | None:
        """Compute base API URL for Jira REST API."""
        if not self.url:
            return None
        return f"{self.url}/rest/api/3"


class SquSettings(BaseSettings):
    """Pydantic settings with Azure Key Vault and environment variable support."""

    model_config = ConfigDict(
        env_prefix="SQU_",
        case_sensitive=False,
        extra="allow",  # Allow extra fields for backward compatibility
        validate_default=True
    )

    # Nested configuration models
    azure: AzureConfig = Field(default_factory=AzureConfig, description="Azure-specific settings")
    jira: JiraConfig = Field(default_factory=JiraConfig, description="Jira-specific settings")

    # Legacy flat fields for backward compatibility
    jira_url: str | None = Field(
        None,
        description="[DEPRECATED] Use jira.url instead. Jira instance URL"
    )
    jira_username: str | None = Field(
        None,
        description="[DEPRECATED] Use jira.username instead. Jira username"
    )
    jira_password: str | None = Field(
        None,
        description="[DEPRECATED] Use jira.password instead. Jira password"
    )
    tenant_id: str | None = Field(
        None,
        description="[DEPRECATED] Use azure.tenant_id instead. Azure tenant ID"
    )
    vault_name: str | None = Field(
        None,
        description="[DEPRECATED] Use azure.vault_name instead. Azure Key Vault name"
    )
    datalake_account: str | None = Field(
        None,
        description="[DEPRECATED] Use azure.datalake_account instead. Azure Data Lake account"
    )
    datalake_container: str | None = Field(
        None,
        description="[DEPRECATED] Use azure.datalake_container instead. Azure Data Lake container"
    )

    # External API settings
    runzero_apitoken: str | None = Field(
        None,
        description="RunZero API token for network discovery integration"
    )
    abuseipdb_api_key: str | None = Field(
        None,
        description="AbuseIPDB API key for IP reputation lookups"
    )
    tenable_access_key: str | None = Field(
        None,
        description="Tenable.io access key for vulnerability data"
    )
    tenable_secret_key: str | None = Field(
        None,
        description="Tenable.io secret key for vulnerability data"
    )

    @field_validator('runzero_apitoken', 'abuseipdb_api_key', 'tenable_access_key', 'tenable_secret_key')
    @classmethod
    def validate_api_keys(cls, v: str | None) -> str | None:
        """Validate API keys are not empty strings."""
        if v is not None and v.strip() == '':
            raise ValueError('API keys cannot be empty strings')
        return v

    def model_post_init(self, __context: Any) -> None:
        """Handle legacy field migration to nested models."""
        # Migrate legacy Jira fields
        if self.jira_url and not self.jira.url:
            self.jira.url = self.jira_url
        if self.jira_username and not self.jira.username:
            self.jira.username = self.jira_username
        if self.jira_password and not self.jira.password:
            self.jira.password = self.jira_password

        # Migrate legacy Azure fields
        if self.tenant_id and not self.azure.tenant_id:
            self.azure.tenant_id = self.tenant_id
        if self.vault_name and not self.azure.vault_name:
            self.azure.vault_name = self.vault_name
        if self.datalake_account and not self.azure.datalake_account:
            self.azure.datalake_account = self.datalake_account
        if self.datalake_container and not self.azure.datalake_container:
            self.azure.datalake_container = self.datalake_container


class LegacyConfigWrapper:
    """Backward-compatible wrapper that mimics benedict behavior."""

    def __init__(self, pydantic_settings: SquSettings):
        self._settings = pydantic_settings
        self._dict = pydantic_settings.model_dump()

    def __getattr__(self, name: str):
        """Provide dot notation access like benedict."""
        # Avoid recursion by checking internal attributes first
        if name.startswith('_'):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

        # Check the dict first to avoid potential recursion
        if hasattr(self, '_dict') and name in self._dict:
            return self._dict[name]

        # Check the Pydantic settings if available
        if hasattr(self, '_settings'):
            settings_dict = object.__getattribute__(self._settings, '__dict__')
            if name in settings_dict:
                return settings_dict[name]

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def __getitem__(self, key: str):
        """Provide dict-like access."""
        if hasattr(self, '_dict') and key in self._dict:
            return self._dict[key]
        raise KeyError(key)

    def get(self, key: str, default=None):
        """Dict-style get method."""
        try:
            return self[key]
        except KeyError:
            return default

    def standardize(self):
        """Backward compatibility - benedict had this method."""
        # This was used in the original code, now it's a no-op
        warnings.warn(
            "standardize() is deprecated and no longer needed with Pydantic settings",
            DeprecationWarning,
            stacklevel=2
        )

    def dict(self):
        """Return dict representation."""
        return self._dict

    def __contains__(self, key: str):
        """Support 'in' operator."""
        return hasattr(self, '_dict') and key in self._dict

    def keys(self):
        """Return dict keys for compatibility."""
        return self._dict.keys() if hasattr(self, '_dict') else []

    def values(self):
        """Return dict values for compatibility."""
        return self._dict.values() if hasattr(self, '_dict') else []

    def items(self):
        """Return dict items for compatibility."""
        return self._dict.items() if hasattr(self, '_dict') else []


def create_settings_from_dict(data: dict) -> LegacyConfigWrapper:
    """Create settings from dictionary data (for Key Vault compatibility)."""
    # Filter out None values to let defaults work
    filtered_data = {k: v for k, v in data.items() if v is not None}

    try:
        settings = SquSettings(**filtered_data)
    except Exception as e:
        # Fallback for unknown fields - use extra="allow"
        warnings.warn(f"Some config fields couldn't be validated: {e}")
        settings = SquSettings(**data)

    return LegacyConfigWrapper(settings)


def create_settings_from_env() -> LegacyConfigWrapper:
    """Create settings from environment variables."""
    settings = SquSettings()
    return LegacyConfigWrapper(settings)
