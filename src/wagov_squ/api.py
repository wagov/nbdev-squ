"""API module for nbdev-squ package."""

__all__ = [
    "logger",
    "clients",
    "retryer",
    "columns_of_interest",
    "columns",
    "Clients",
    "list_workspaces_safe",
    "list_workspaces",
    "list_subscriptions",
    "list_securityinsights_safe",
    "list_securityinsights",
    "chunks",
    "loganalytics_query",
    "query_all",
    "finalise_query",
    "hunt",
    "atlaskit_transformer",
    "security_incidents",
    "security_alerts",
    "Plugin",
    "Fmt",  # Export format enum for ibis support
]

import json
import logging
import pkgutil
import time
from functools import cached_property
from importlib.metadata import version
from pathlib import Path
from subprocess import CalledProcessError, run

import pandas
from atlassian import Jira
from azure.identity import AzureCliCredential
from azure.monitor.query import LogsBatchQuery, LogsQueryClient, LogsQueryStatus
from benedict import benedict
from dbt.adapters.duckdb.plugins import BasePlugin, SourceConfig
from diskcache import memoize_stampede
from tenable.io import TenableIO

from .core import azcli, cache, chunks, datalake_path, dirs, httpx, load_config, login, retryer
from .frame import Fmt, as_pandas, format_output, memtable, read_parquet

logger = logging.getLogger(__name__)

class Clients:
    """
    Clients for various services, cached for performance
    """

    @cached_property
    def config(self):
        login()
        return cache.get("config", load_config())

    @cached_property
    def runzero(self):
        """
        Returns a runzero client
        """
        return httpx.Client(
            base_url="https://console.rumble.run/api/v1.0",
            headers={"Authorization": f"Bearer {self.config.runzero_apitoken}"},
        )

    @cached_property
    def abuseipdb(self):
        """
        Returns an abuseipdb client
        """
        from abuseipdb_wrapper import AbuseIPDB

        return AbuseIPDB(api_key=self.config.abuseipdb_api_key)

    @cached_property
    def jira(self):
        """Returns a Jira client for issue tracking and project management."""
        jira_client = Jira(
            url=self.config.jira_url,
            username=self.config.jira_username,
            password=self.config.jira_password,
            cloud=True,
        )
        return jira_client

    @cached_property
    def tio(self):
        """
        Returns a TenableIO client
        """
        return TenableIO(self.config.tenable_access_key, self.config.tenable_secret_key)


clients = Clients()


@memoize_stampede(cache, expire=60 * 60 * 3)  # cache for 3 hours
def list_workspaces_safe(
    fmt: str = "df",  # df, csv, json, list
    agency: str = "ALL",
):  # Agency alias or ALL
    path = datalake_path()

    # Use pandas for now (ibis migration in separate function)
    df = pandas.read_csv((path / "notebooks/lists/SentinelWorkspaces.csv").open())
    df = df.join(
        pandas.read_csv((path / "notebooks/lists/SecOps Groups.csv").open()).set_index("Alias"),
        on="SecOps Group",
        rsuffix="_secops",
    )
    df = df.rename(columns={"SecOps Group": "alias", "Domains and IPs": "domains"})
    df = df.dropna(subset=["customerId"]).sort_values(by="alias").convert_dtypes().reset_index()
    persisted = f"{dirs.user_cache_dir}/list_workspaces.parquet"
    df.to_parquet(persisted)
    return persisted


def list_workspaces(
    fmt: str = "df",  # df, csv, json, list, ibis
    agency: str = "ALL",
):  # Agency alias or ALL
    # Load cached data
    parquet_path = list_workspaces_safe(fmt, agency)
    expr = read_parquet(parquet_path)

    # Filter by agency if needed
    if agency != "ALL":
        df = as_pandas(expr)  # Use pandas for filtering for now
        expr = df[df["alias"] == agency]

    # Handle special list format (customerId unique)
    if fmt == "list":
        df = as_pandas(expr)
        return list(df["customerId"].unique())

    # Use frame abstraction for other formats
    return format_output(expr, fmt)


def list_subscriptions():
    return pandas.DataFrame(azcli(["account", "list"]))["id"].unique()


@memoize_stampede(cache, expire=60 * 60 * 3)  # cache for 3 hours
def list_securityinsights_safe():
    return azcli(
        [
            "graph",
            "query",
            "--first",
            "1000",
            "-q",
            """
        resources
        | where type =~ 'microsoft.operationsmanagement/solutions'
        | where name startswith 'SecurityInsights'
        | project wlid = tolower(tostring(properties.workspaceResourceId))
        | join kind=leftouter (
            resources | where type =~ 'microsoft.operationalinsights/workspaces' | extend wlid = tolower(id))
            on wlid
        | extend customerId = properties.customerId
        """,
        ]
    )["data"]


def list_securityinsights(fmt: str = "df"):
    """List Azure Security Insights resources."""
    data = list_securityinsights_safe()
    expr = memtable(data)
    return format_output(expr, fmt)


def loganalytics_query(
    queries: list[str],
    timespan=pandas.Timedelta("14d"),
    batch_size=190,
    batch_delay=32,
    sentinel_workspaces=None,
):
    """
    Run queries across all workspaces, in batches of `batch_size` with a minimum delay of `batch_delay` between batches.
    Returns a dictionary of queries and results
    """
    client = LogsQueryClient(AzureCliCredential())
    workspaces = list_workspaces(fmt="df")
    if sentinel_workspaces is None:
        sentinel_workspaces = list_securityinsights()
    query_requests, results = [], []
    for query in queries:
        for workspace_id in sentinel_workspaces["customerId"]:
            query_requests.append(
                LogsBatchQuery(workspace_id=workspace_id, query=query, timespan=timespan)
            )
    querytime = pandas.Timestamp("now")
    logger.info(f"Executing {len(query_requests)} queries at {querytime}")
    for request_batch in chunks(query_requests, batch_size):
        while cache.get("loganalytics_query_running"):
            time.sleep(1)
        batch_start = pandas.Timestamp("now")
        cache.set("loganalytics_query_running", True, batch_delay)
        results += client.query_batch(request_batch)
        duration = pandas.Timestamp("now") - batch_start
        logger.info(f"Completed {len(results)} (+{len(request_batch)}) in {duration}")
    dfs = {}
    for request, result in zip(query_requests, results):
        if result.status == LogsQueryStatus.PARTIAL:
            tables = result.partial_data
            tables = [pandas.DataFrame(table.rows, columns=table.columns) for table in tables]
        elif result.status == LogsQueryStatus.SUCCESS:
            tables = result.tables
            tables = [pandas.DataFrame(table.rows, columns=table.columns) for table in tables]
        else:
            tables = [pandas.DataFrame([result.__dict__])]
        df = pandas.concat(tables).dropna(axis=1, how="all")  # prune empty columns
        df["TenantId"] = request.workspace
        alias = workspaces.query(f'customerId == "{request.workspace}"')["alias"].str.cat()
        if alias == "":
            alias = sentinel_workspaces.query(f'customerId == "{request.workspace}"')[
                "name"
            ].str.cat()
        df["_alias"] = alias
        query = request.body["query"]
        if query in dfs:
            dfs[query].append(df)
        else:
            dfs[query] = [df]
    return {
        query: pandas.concat(results, ignore_index=True).convert_dtypes()
        for query, results in dfs.items()
    }


def query_all(query, fmt="df", timespan=pandas.Timedelta("14d")):
    """Execute KQL queries across Azure Sentinel workspaces."""
    try:
        # Check query is not a plain string and is iterable
        assert not isinstance(query, str)
        iter(query)
    except (AssertionError, TypeError):
        # if it is a plain string or it's not iterable, convert into a list of queries
        query = [query]

    # Execute queries and get results
    results = loganalytics_query(query, timespan)

    # Concatenate all results
    dfs = list(results.values())
    if len(dfs) == 1:
        expr = dfs[0]
    else:
        # Use pandas concat for now, could use ibis union later
        expr = pandas.concat(dfs, ignore_index=True)

    # Format output using frame abstraction
    return format_output(expr, fmt)


columns_of_interest = benedict(
    {
        "name": [
            "AADEmail",
            "AccountName",
            "AccountUPN",
            "AccountUpn",
            "Account",
            "CompromisedEntity",
            "DestinationUserName",
            "Computer",
            "DisplayName",
            "EmailSenderAddress",
            "FileName",
            "FilePath",
            "FolderPath",
            "FullyQualifiedSubjectUserName",
            "InitiatingProcessAccountUpn",
            "MailboxOwnerUPN",
            "Name",
            "NewProcessName",
            "Owner",
            "ParentProcessName",
            "Process",
            "CommandLine",
            "ProcessCommandLine",
            "RecipientEmailAddress",
            "RequesterUpn",
            "SenderMailFromAddress",
            "SourceIdentity",
            "SourceUserName",
            "SubjectUserName",
            "TargetUserName",
            "TargetUser",
            "Upn",
            "UserName",
            "userName",
            "UserPrincipalName",
        ],
        "guid": ["Caller", "DestinationUserID", "SourceUserID", "UserId"],
        "ip": [
            "CallerIpAddress",
            "DestinationIP",
            "DstIpAddr",
            "EmailSourceIpAddress",
            "IPAddresses",
            "IPAddress",
            "IpAddress",
            "NetworkDestinationIP",
            "NetworkIP",
            "NetworkSourceIP",
            "RemoteIP",
            "SourceIP",
            "SrcIpAddr",
        ],
        "url": ["DomainName", "FileOriginUrl", "FQDN", "RemoteUrl", "Url"],
        "hash": ["MD5", "SHA1", "SHA256", "FileHashValue", "FileHash", "InitiatingProcessSHA256"],
    }
)

columns = [column for area in columns_of_interest.values() for column in area]


def finalise_query(query, take):
    return f"{query} | take {take} | extend placeholder_=dynamic({{'':null}}) | evaluate bag_unpack(column_ifexists('pack_', placeholder_))"


def hunt(
    indicators,
    expression="has",
    columns=columns,
    workspaces=None,
    timespans=["1d", "14d", "90d", "700d"],
    take=5000,
):
    queries = []
    if workspaces is None:
        workspaces = list_securityinsights()
    else:
        df = list_securityinsights()
        workspaces = df[df["customerId"].isin(workspaces)]
    querylogged = False
    if expression in ["has_any"]:
        query = f"let indicators = dynamic({indicators}); "

        for count, column in enumerate(columns):
            if count == 0:
                query += f"find where {column} has_any (indicators)"
            else:
                query += f" or {column} has_any (indicators)"
        final_query = finalise_query(query, take)
        queries.append(final_query)
    else:
        for indicator in indicators:
            if expression not in ["has_all"]:
                indicator = f"'{indicator}'"  # wrap indicator in quotes unless expecting dynamic
            if not querylogged:
                logger.info(
                    f"Test Query: find where {columns[0]} {expression} {indicator} | take {take}"
                )
                querylogged = True
            for chunk in chunks([f"{column} {expression} {indicator}" for column in columns], 20):
                query = " or ".join(chunk)
                final_query = finalise_query(f"find where {query}", take)
                queries.append(final_query)
    for timespan in timespans:
        results = pandas.concat(
            loganalytics_query(
                queries, pandas.Timedelta(timespan), sentinel_workspaces=workspaces
            ).values()
        )
        if "placeholder_" in results.columns:
            results = results.drop("placeholder_", axis=1)
        if results.empty:
            logger.info(f"No results in {timespan}, extending hunt")
            continue
        logger.info(f"Found {indicators} in {timespan}, returning")
        return results
    else:
        raise Exception("No results found!")


def atlaskit_transformer(inputtext, inputfmt="md", outputfmt="wiki", runtime="node"):
    """Transform text using the atlaskit transformer bundle."""
    bundle_data = pkgutil.get_data("wagov_squ", "atlaskit-transformer.bundle.js")
    if bundle_data is None:
        raise FileNotFoundError("atlaskit-transformer.bundle.js not found in wagov_squ package")

    # Cache the bundle using the current package version
    transformer = dirs.user_cache_path / f"atlaskit-transformer.bundle_v{version('wagov_squ')}.js"
    if not transformer.exists():
        transformer.write_bytes(bundle_data)

    cmd = [runtime, str(transformer), inputfmt, outputfmt]
    logger.debug(" ".join(cmd))
    try:
        return run(cmd, input=inputtext, text=True, capture_output=True, check=True).stdout
    except CalledProcessError:
        run(cmd, input=inputtext, text=True, check=True)


def security_incidents(
    start=pandas.Timestamp("now", tz="UTC") - pandas.Timedelta("1d"),
    timedelta=pandas.Timedelta("1d"),
):
    # Queries for security incidents from `start` time for `timedelta` and returns a dataframe
    # Sorts by TimeGenerated (TODO)
    query = "SecurityIncident | summarize arg_max(TimeGenerated, *) by IncidentNumber"
    return query_all(query, timespan=(start.to_pydatetime(), timedelta))


def security_alerts(
    start=pandas.Timestamp("now", tz="UTC") - pandas.Timedelta("1d"),
    timedelta=pandas.Timedelta("1d"),
):
    # Queries for security alerts from `start` time for `timedelta` and returns a dataframe
    # Sorts by TimeGenerated (TODO)
    query = "SecurityAlert | summarize arg_max(TimeGenerated, *) by SystemAlertId"
    return query_all(query, timespan=(start.to_pydatetime(), timedelta))


class Plugin(BasePlugin):
    def initialize(self, config):
        """Initialize the plugin with Azure authentication."""
        try:
            login()
        except Exception as e:
            logger.warning(f"Authentication failed during plugin initialization: {e}")
            # Don't fail initialization, let individual queries handle auth errors

    def configure_cursor(self, cursor):
        """Configure database cursor - no special configuration needed for this plugin."""
        pass

    def load(self, source_config: SourceConfig):
        """
        Load data based on source configuration.
        
        Returns empty DataFrame for expected "no data" situations.
        Raises DbtRuntimeError for operational failures to let dbt handle transactions properly.
        """
        try:
            if "kql_path" in source_config:
                # Load and execute KQL query from file
                kql_path = source_config["kql_path"]
                kql_path = kql_path.format(**source_config.as_dict())
                
                if not Path(kql_path).exists():
                    logger.info(f"KQL file not found: {kql_path}")
                    return pandas.DataFrame()
                
                query = Path(kql_path).read_text()
                timespan = pandas.Timedelta(source_config.get("timespan", "14d"))
                return query_all(query, timespan=timespan)
                
            elif "list_workspaces" in source_config:
                # Return workspace listing
                return list_workspaces()
                
            elif "client_api" in source_config:
                # Call specific client API method
                api_method = source_config["client_api"]
                kwargs = json.loads(source_config.get("kwargs", "{}"))
                
                if not hasattr(clients, api_method):
                    raise ValueError(f"Unknown client API method: {api_method}")
                    
                api_result = getattr(clients, api_method)(**kwargs)
                
                # Ensure we return a DataFrame
                if isinstance(api_result, pandas.DataFrame):
                    return api_result
                else:
                    return pandas.DataFrame(api_result)
            else:
                raise ValueError(
                    "Invalid squ plugin configuration. Must specify one of: "
                    "kql_path, list_workspaces, or client_api"
                )
                
        except (FileNotFoundError, ValueError, json.JSONDecodeError) as e:
            # Expected "no data" or configuration issues - return empty DataFrame
            logger.info(f"No data or invalid config: {e}")
            return pandas.DataFrame()
        except Exception as e:
            # Unexpected operational failures - let dbt handle transaction rollback properly
            from dbt.exceptions import DbtRuntimeError
            raise DbtRuntimeError(f"squ plugin failed for config {source_config}: {e}") from e

    def default_materialization(self):
        """Default materialization strategy for this plugin."""
        return "view"
