"""Tests for core module."""

from wagov_squ.core import cache, dirs, logger


def test_logger_exists():
    """Test that logger is created."""
    assert logger is not None
    assert logger.name == "wagov_squ.core"


def test_cache_exists():
    """Test that cache is initialized."""
    assert cache is not None
    # Test basic cache operations
    cache.set("test_key", "test_value")
    assert cache.get("test_key") == "test_value"
    cache.delete("test_key")


def test_dirs_configured():
    """Test that platform dirs are configured."""
    assert dirs is not None
    assert "nbdev-squ" in str(dirs.user_cache_dir)
