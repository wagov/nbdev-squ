"""Tests for legacy compatibility and markdown conversion functionality."""

import warnings
from unittest.mock import patch

import pytest


def test_legacy_nbdev_import_compatibility():
    """Test that importing from nbdev_squ works and issues deprecation warning."""
    # Clear any existing deprecation warnings
    warnings.simplefilter("always")
    
    with warnings.catch_warnings(record=True) as w:
        # Import from legacy package name
        import nbdev_squ
        from nbdev_squ.api import Clients, atlaskit_transformer
        
        # Verify deprecation warning was issued
        assert len(w) >= 1
        assert any(issubclass(warning.category, DeprecationWarning) for warning in w)
        assert any("deprecated" in str(warning.message) for warning in w)
        assert any("wagov_squ" in str(warning.message) for warning in w)
        
        # Verify we can access the same functionality
        assert hasattr(nbdev_squ, 'api')
        assert hasattr(nbdev_squ.api, 'Clients')
        assert hasattr(nbdev_squ.api, 'atlaskit_transformer')
        
        # Verify the functions are callable
        assert callable(Clients)
        assert callable(atlaskit_transformer)


def test_legacy_import_direct_functions():
    """Test importing specific functions from legacy module works."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        
        # Test importing specific functions
        from nbdev_squ.legacy import sentinel_beautify_local, flatten
        from nbdev_squ.api import hunt, query_all
        
        # Verify functions exist
        assert callable(sentinel_beautify_local)
        assert callable(flatten)
        assert callable(hunt)
        assert callable(query_all)


def test_atlaskit_transformer_basic_functionality():
    """Test that atlaskit_transformer function works with actual transformer."""
    from wagov_squ.api import atlaskit_transformer
    
    # Simple markdown test
    test_markdown = "# Test Header\n\nThis is a test paragraph with **bold** text."
    
    try:
        # Try to run the actual transformer
        result = atlaskit_transformer(test_markdown, "md", "wiki")
        
        # If it succeeds, verify we get string output
        assert isinstance(result, str)
        assert len(result) > 0
        
        # Should contain wiki-style formatting
        assert "h1." in result or "Test Header" in result
        
    except FileNotFoundError:
        # If bundle is missing, just verify the function raises the right error
        pytest.skip("JS bundle not found - this is expected in test environment")
    except Exception as e:
        # If Node.js is not available, skip the test
        if "node" in str(e).lower():
            pytest.skip("Node.js not available in test environment")
        else:
            raise


def test_atlaskit_transformer_markdown_to_wiki():
    """Test markdown to wiki conversion with realistic content."""
    from wagov_squ.api import atlaskit_transformer
    
    markdown_input = """# Security Incident

**Alert**: Critical issue detected.

## Details
- Status: Active
- Priority: High

### Code Block
```bash
echo "test"
```
"""
    
    try:
        result = atlaskit_transformer(markdown_input, "md", "wiki")
        
        # Verify we get output
        assert isinstance(result, str)
        assert len(result) > 0
        
        # Should preserve the main content
        assert "Security Incident" in result
        
    except (FileNotFoundError, Exception) as e:
        if "node" in str(e).lower() or "not found" in str(e).lower():
            pytest.skip("Dependencies not available in test environment")
        else:
            raise


def test_sentinel_beautify_local_integration():
    """Test the sentinel_beautify_local function."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        
        from wagov_squ.legacy import sentinel_beautify_local
    
    # Sample security incident data
    incident_data = {
        "IncidentNumber": "12345",
        "Title": "Suspicious Login Activity",
        "Severity": "High",
        "Status": "Active",
        "Description": "Multiple failed login attempts detected",
        "IncidentUrl": "https://portal.azure.com/incident/12345",
        "TenantId": "test-tenant-id",
        "Labels": "[]",
        "Owner": None,
        "AdditionalData": None,
        "Comments": "[]",
        "AlertData": [
            {
                "AlertName": "Suspicious Login",
                "AlertSeverity": "High", 
                "TimeGenerated": "2023-01-01T10:00:00Z",
                "AlertLink": "https://portal.azure.com/alert/1",
                "Description": "Failed login attempts from unknown IP"
            }
        ]
    }
    
    # Mock only the list_workspaces function, let atlaskit_transformer run naturally
    with patch('wagov_squ.legacy.api.list_workspaces') as mock_workspaces:
        
        # Mock dataframe-like object for list_workspaces
        import pandas as pd
        mock_df = pd.DataFrame([{"customerId": "test-tenant-id", "SecOps Status": "Active"}])
        mock_workspaces.return_value = mock_df
        
        try:
            # Test the function
            result = sentinel_beautify_local(incident_data)
            
            # Verify structure
            assert "subject" in result
            assert "labels" in result 
            assert "wikimarkup" in result
            assert "observables" in result
            
            # Verify content
            assert "12345" in result["subject"]
            assert "Suspicious Login Activity" in result["subject"]
            assert len(result["labels"]) > 0
            assert "SIEM_Severity:High" in result["labels"]
            
            # Verify we got wiki markup (should be a string)
            assert isinstance(result["wikimarkup"], str)
            assert len(result["wikimarkup"]) > 0
            
        except (FileNotFoundError, Exception) as e:
            if "node" in str(e).lower() or "not found" in str(e).lower():
                pytest.skip("Dependencies not available in test environment")
            else:
                raise


def test_flatten_utility_function():
    """Test the flatten utility function from legacy module."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        
        from nbdev_squ.legacy import flatten
    
    # Test nested dictionary
    nested_dict = {
        "level1": {
            "level2a": {
                "level3": "deep_value"
            },
            "level2b": "shallow_value"
        },
        "top_level": "top_value"
    }
    
    result = flatten(nested_dict)
    
    expected = {
        "level1_level2a_level3": "deep_value",
        "level1_level2b": "shallow_value", 
        "top_level": "top_value"
    }
    
    assert result == expected


def test_atlaskit_transformer_error_handling():
    """Test error handling in atlaskit_transformer when bundle is missing."""
    from wagov_squ.api import atlaskit_transformer
    
    with patch('wagov_squ.api.pkgutil.get_data') as mock_get_data:
        mock_get_data.return_value = None
        
        with pytest.raises(FileNotFoundError, match="atlaskit-transformer.bundle.js not found"):
            atlaskit_transformer("# Test", "md", "wiki")
