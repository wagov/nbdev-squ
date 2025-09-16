"""Tests for API module."""

from unittest.mock import Mock, patch

from wagov_squ.api import Clients


def test_clients_class_exists():
    """Test that Clients class is importable."""
    with patch("wagov_squ.api.login"), patch("wagov_squ.api.load_config", return_value={}):
        clients = Clients()
        assert clients is not None


def test_jira_client_cloud_support():
    """Test that Jira client is configured for cloud with enhanced_jql support."""
    with patch("wagov_squ.api.login"), patch("wagov_squ.api.load_config", return_value={}):
        clients = Clients()

        # Mock config to avoid actual auth
        with patch.object(clients, "config") as mock_config:
            mock_config.jira_url = "https://example.atlassian.net"
            mock_config.jira_username = "user"
            mock_config.jira_password = "pass"

            # Mock the Jira constructor
            with patch("wagov_squ.api.Jira") as mock_jira:
                mock_jira_instance = Mock()
                mock_jira.return_value = mock_jira_instance

                # Access the jira client
                clients.jira

                # Verify it was created with cloud=True for enhanced API support
                mock_jira.assert_called_once_with(
                    url="https://example.atlassian.net",
                    username="user",
                    password="pass",
                    cloud=True,
                )

                # Verify enhanced_jql method exists (library provides this)
                assert hasattr(mock_jira_instance, "enhanced_jql")


def test_jira_enhanced_api_usage():
    """Test that we can use the enhanced_jql method from atlassian-python-api."""
    from wagov_squ.api import Clients

    with patch("wagov_squ.api.login"), patch("wagov_squ.api.load_config", return_value={}):
        clients = Clients()

        # Mock the entire chain
        with (
            patch.object(clients, "config") as mock_config,
            patch("wagov_squ.api.Jira") as mock_jira,
        ):
            mock_config.jira_url = "https://example.atlassian.net"
            mock_config.jira_username = "user"
            mock_config.jira_password = "pass"

            mock_jira_instance = Mock()
            mock_jira.return_value = mock_jira_instance

            # Mock enhanced_jql response
            mock_response = {
                "issues": [{"id": "1", "key": "TEST-1"}],
                "startAt": 0,
                "maxResults": 100,
                "total": 1,
            }
            mock_jira_instance.enhanced_jql.return_value = mock_response

            jira_client = clients.jira

            # Test calling enhanced_jql (the recommended v3 API method)
            jira_client.enhanced_jql("project = TEST")

            # Verify the enhanced_jql method was called
            mock_jira_instance.enhanced_jql.assert_called_once_with("project = TEST")


def test_jira_backward_compatibility():
    """Test that the standard jql() method now uses enhanced APIs under the hood."""
    from wagov_squ.api import Clients

    with patch("wagov_squ.api.login"), patch("wagov_squ.api.load_config", return_value={}):
        clients = Clients()

        # Mock the entire chain
        with (
            patch.object(clients, "config") as mock_config,
            patch("wagov_squ.api.Jira") as mock_jira,
        ):
            mock_config.jira_url = "https://example.atlassian.net"
            mock_config.jira_username = "user"
            mock_config.jira_password = "pass"

            mock_jira_instance = Mock()
            mock_jira.return_value = mock_jira_instance

            # Mock enhanced_jql response (what gets called under the hood)
            mock_response = {
                "issues": [{"id": "1", "key": "TEST-1"}],
                "startAt": 0,
                "maxResults": 100,
                "total": 1,
            }
            mock_jira_instance.enhanced_jql.return_value = mock_response

            jira_client = clients.jira

            # Test calling the standard jql() method (should use enhanced APIs under hood)
            jira_client.jql("project = TEST", start=10, limit=50)

            # Verify that enhanced_jql was called instead of the deprecated jql method
            mock_jira_instance.enhanced_jql.assert_called_once_with(
                "project = TEST", start=10, limit=50, fields="*all", expand=None
            )

            # Verify jql method was NOT called (we're using enhanced under the hood)
            assert not mock_jira_instance.jql.called


def test_fmt_enum_import():
    """Test that Fmt enum can be imported and used."""
    from wagov_squ import Fmt

    assert Fmt.pandas == "df"
    assert Fmt.csv == "csv"
    assert Fmt.json == "json"
    assert Fmt.list == "list"
    assert Fmt.ibis == "ibis"
