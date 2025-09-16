"""Tests for API module."""

from unittest.mock import Mock, patch

from wagov_squ.api import Clients


def test_clients_class_exists():
    """Test that Clients class is importable."""
    with patch("wagov_squ.api.login"), patch("wagov_squ.api.load_config", return_value={}):
        clients = Clients()
        assert clients is not None


def test_jira_client_cloud_support():
    """Test that Jira client is configured for cloud."""
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

                # Verify it was created with cloud=True
                mock_jira.assert_called_once_with(
                    url="https://example.atlassian.net",
                    username="user",
                    password="pass",
                    cloud=True,
                )


def test_jira_basic_usage():
    """Test that we can use standard Jira API methods."""
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

            # Mock jql response
            mock_response = {
                "issues": [{"id": "1", "key": "TEST-1"}],
                "startAt": 0,
                "maxResults": 100,
                "total": 1,
            }
            mock_jira_instance.jql.return_value = mock_response

            jira_client = clients.jira

            # Test calling standard jql method
            jira_client.jql("project = TEST")

            # Verify the jql method was called
            mock_jira_instance.jql.assert_called_once_with("project = TEST")


def test_jira_client_creation():
    """Test that Jira client is created correctly."""
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

            jira_client = clients.jira

            # Verify that Jira was instantiated correctly
            mock_jira.assert_called_once_with(
                url="https://example.atlassian.net",
                username="user",
                password="pass",
                cloud=True,
            )

            # Verify we get the direct Jira client (not wrapped)
            assert jira_client is mock_jira_instance


def test_fmt_enum_import():
    """Test that Fmt enum can be imported and used."""
    from wagov_squ import Fmt

    assert Fmt.pandas == "df"
    assert Fmt.csv == "csv"
    assert Fmt.json == "json"
    assert Fmt.list == "list"
    assert Fmt.ibis == "ibis"
