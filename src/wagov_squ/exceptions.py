"""Domain-specific exceptions for better error handling."""

from __future__ import annotations

from typing import Any


class SquError(Exception):
    """Base exception for all squ-related errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class ConfigurationError(SquError):
    """Raised when configuration is invalid or missing."""

    pass


class AuthenticationError(SquError):
    """Raised when authentication fails."""

    pass


class AzureError(SquError):
    """Raised when Azure operations fail."""

    pass


class JiraError(SquError):
    """Raised when Jira operations fail."""

    pass


class QueryError(SquError):
    """Raised when KQL queries fail."""

    pass


class ValidationError(SquError):
    """Raised when data validation fails."""

    pass


class ResourceError(SquError):
    """Raised when resource management fails."""

    pass


class RetryExhaustedError(SquError):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, message: str, attempts: int, last_error: Exception):
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error


class SquConfigurationError(ConfigurationError):
    """Legacy alias for ConfigurationError."""

    pass


class SquAuthenticationError(AuthenticationError):
    """Legacy alias for AuthenticationError."""

    pass


def validate_not_empty(value: Any, field_name: str) -> Any:
    """Validate that a value is not None or empty string."""
    if value is None:
        raise ValidationError(f"{field_name} cannot be None")
    if isinstance(value, str) and not value.strip():
        raise ValidationError(f"{field_name} cannot be empty")
    return value


def validate_config_required(config: Any, field_name: str) -> Any:
    """Validate that a required configuration field is present."""
    try:
        value = getattr(config, field_name)
        return validate_not_empty(value, field_name)
    except AttributeError:
        raise ConfigurationError(f"Required configuration field '{field_name}' is missing")


def require_authentication(func):
    """Decorator to ensure authentication is required for a function."""

    def wrapper(*args, **kwargs):
        # This could be expanded to check actual auth state
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "authentication" in str(e).lower() or "login" in str(e).lower():
                raise AuthenticationError(f"Authentication required: {e}")
            raise

    return wrapper
