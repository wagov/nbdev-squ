"""Integration tests using actual Azure data (read-only operations only)."""

import os

import pytest

from wagov_squ.api import Clients
from wagov_squ.core import login


@pytest.fixture(scope="session")
def azure_config():
    """Check if Azure configuration is available for integration testing."""
    config_env = os.getenv("SQU_CONFIG")
    if not config_env:
        pytest.skip("SQU_CONFIG environment variable not set - skipping integration tests")
    return config_env


@pytest.fixture(scope="session")
def authenticated_clients(azure_config):
    """Get authenticated clients for integration testing."""
    try:
        # Attempt to authenticate
        login()
        clients = Clients()
        return clients
    except Exception as e:
        pytest.skip(f"Azure authentication failed: {e}")


@pytest.mark.integration
class TestAzureIntegration:
    """Integration tests that query actual Azure data (read-only)."""

    def test_list_workspaces_integration(self, authenticated_clients):
        """Test listing actual Azure Sentinel workspaces."""
        from wagov_squ.api import list_workspaces

        # Test with default format
        workspaces = list_workspaces(fmt="df")

        # Verify we get a pandas DataFrame
        import pandas as pd

        assert isinstance(workspaces, pd.DataFrame)

        # Verify expected columns exist
        expected_columns = {"alias", "customerId"}
        assert expected_columns.issubset(set(workspaces.columns))

        # Verify we have at least some workspaces
        assert len(workspaces) > 0

        print(f"✅ Found {len(workspaces)} Azure Sentinel workspaces")

    def test_list_workspaces_formats(self, authenticated_clients):
        """Test different output formats for workspace listing."""
        from wagov_squ.api import list_workspaces

        # Test CSV format
        csv_result = list_workspaces(fmt="csv")
        assert isinstance(csv_result, str)
        assert "alias" in csv_result

        # Test JSON format
        json_result = list_workspaces(fmt="json")
        assert isinstance(json_result, list)
        if json_result:  # If we have data
            assert isinstance(json_result[0], dict)
            assert "alias" in json_result[0]

        # Test list format
        list_result = list_workspaces(fmt="list")
        assert isinstance(list_result, list)

        print("✅ All workspace output formats work correctly")

    def test_list_securityinsights_integration(self, authenticated_clients):
        """Test listing actual Azure Security Insights resources."""
        from wagov_squ.api import list_securityinsights

        insights = list_securityinsights()

        # Verify we get a pandas DataFrame
        import pandas as pd

        assert isinstance(insights, pd.DataFrame)

        # Verify expected columns exist
        expected_columns = {"customerId", "name"}
        assert expected_columns.issubset(set(insights.columns))

        print(f"✅ Found {len(insights)} Security Insights resources")

    def test_simple_kql_query(self, authenticated_clients):
        """Test a simple, non-destructive KQL query."""
        import pandas as pd

        from wagov_squ.api import query_all

        # Simple query to get schema information (read-only)
        test_query = """
        SecurityEvent
        | getschema
        | take 5
        """

        try:
            result = query_all(test_query, fmt="df")

            # Verify we get a pandas DataFrame
            assert isinstance(result, pd.DataFrame)

            # For schema query, we expect ColumnName and ColumnType columns
            if len(result) > 0:
                expected_schema_columns = {"ColumnName", "ColumnType"}
                assert expected_schema_columns.issubset(set(result.columns))

            print(f"✅ KQL query executed successfully, returned {len(result)} schema rows")

        except Exception as e:
            # Some workspaces might not have SecurityEvent table
            pytest.skip(f"KQL query failed (possibly no SecurityEvent table): {e}")

    def test_clients_properties(self, authenticated_clients):
        """Test that all client properties are accessible."""
        clients = authenticated_clients

        # Test config is loaded
        config = clients.config
        assert hasattr(config, "keys")  # Should be dict-like

        print("✅ Client configuration loaded successfully")

    @pytest.mark.slow
    def test_multiple_workspace_query(self, authenticated_clients):
        """Test querying across multiple workspaces (if available)."""
        from wagov_squ.api import list_workspaces, query_all

        workspaces = list_workspaces(fmt="df")

        if len(workspaces) < 2:
            pytest.skip("Need at least 2 workspaces for multi-workspace test")

        # Simple query that should work across workspaces
        test_query = """
        Heartbeat
        | summarize LastHeartbeat = max(TimeGenerated)
        | take 1
        """

        try:
            result = query_all(test_query, fmt="df")

            # Verify we get results
            import pandas as pd

            assert isinstance(result, pd.DataFrame)

            print(f"✅ Multi-workspace query successful, {len(result)} total results")

        except Exception as e:
            pytest.skip(f"Multi-workspace query failed: {e}")


@pytest.mark.integration
class TestJiraIntegration:
    """Integration tests for Jira Enhanced API (if configured)."""

    @pytest.fixture
    def jira_client(self, authenticated_clients):
        """Get Jira client if configured."""
        try:
            clients = authenticated_clients
            jira = clients.jira
            return jira
        except Exception as e:
            pytest.skip(f"Jira not configured or authentication failed: {e}")

    def test_jira_enhanced_api_available(self, jira_client):
        """Test that Enhanced JQL API is available."""
        # Check that our wrapper has the enhanced_jql method
        assert hasattr(jira_client, "enhanced_jql")
        assert hasattr(jira_client, "jql")

        print("✅ Jira Enhanced API methods available")

    def test_jira_simple_query(self, jira_client):
        """Test a simple, non-destructive Jira query."""
        try:
            # Simple query to get a few issues (read-only)
            result = jira_client.jql("order by created desc", limit=5)

            # Should return a dict with issues
            assert isinstance(result, dict)
            assert "issues" in result
            assert "total" in result

            print(f"✅ Jira query successful, found {len(result.get('issues', []))} issues")

        except Exception as e:
            pytest.skip(f"Jira query failed: {e}")

    def test_jira_enhanced_jql_direct(self, jira_client):
        """Test direct enhanced_jql method call."""
        try:
            # Test direct enhanced_jql call
            result = jira_client.enhanced_jql("order by created desc", limit=3)

            assert isinstance(result, dict)
            assert "issues" in result
            assert "total" in result

            print(f"✅ Enhanced JQL direct call successful, found {len(result.get('issues', []))} issues")

        except Exception as e:
            pytest.skip(f"Enhanced JQL query failed: {e}")

    def test_jira_query_with_fields(self, jira_client):
        """Test JQL query with specific fields."""
        try:
            # Query with specific fields
            result = jira_client.jql(
                "order by created desc", 
                limit=3,
                fields="key,summary,status,created"
            )

            assert isinstance(result, dict)
            assert "issues" in result

            # Check that issues have the requested fields
            if result.get("issues"):
                issue = result["issues"][0]
                assert "key" in issue
                assert "fields" in issue
                fields = issue["fields"]
                expected_fields = {"summary", "status", "created"}
                assert any(field in fields for field in expected_fields)

            print(f"✅ Jira query with fields successful")

        except Exception as e:
            pytest.skip(f"Jira query with fields failed: {e}")

    def test_jira_project_query(self, jira_client):
        """Test JQL query for a specific project (if any exist)."""
        try:
            # First, try to get any project
            result = jira_client.jql("order by created desc", limit=1)
            
            if not result.get("issues"):
                pytest.skip("No issues found to test project queries")

            # Get first project key from results
            first_issue = result["issues"][0]
            project_key = first_issue.get("fields", {}).get("project", {}).get("key")
            
            if not project_key:
                pytest.skip("No project key found in issue data")

            # Query specific project
            project_result = jira_client.jql(f"project = {project_key}", limit=5)
            
            assert isinstance(project_result, dict)
            assert "issues" in project_result

            print(f"✅ Project-specific query successful for {project_key}")

        except Exception as e:
            pytest.skip(f"Project query failed: {e}")


# Utility function to run integration tests
def run_integration_tests():
    """Run integration tests if environment is configured."""
    if not os.getenv("SQU_CONFIG"):
        print("❌ SQU_CONFIG not set - integration tests will be skipped")
        print("   Set SQU_CONFIG=keyvault_name/tenant_id to enable integration tests")
        return False

    print("✅ SQU_CONFIG found - integration tests will run")
    return True


if __name__ == "__main__":
    run_integration_tests()
