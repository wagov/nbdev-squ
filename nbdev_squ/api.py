# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/01_api.ipynb.

# %% auto 0
__all__ = ['logger', 'clients', 'columns_of_interest', 'columns', 'Clients', 'list_workspaces', 'list_subscriptions',
           'list_securityinsights', 'chunks', 'loganalytics_query', 'query_all', 'finalise_query', 'hunt',
           'atlaskit_transformer', 'security_incidents', 'security_alerts']

# %% ../nbs/01_api.ipynb 3
import pandas, json, logging, time, requests, httpx_cache
from .core import *
from diskcache import memoize_stampede
from subprocess import run, CalledProcessError
from azure.monitor.query import LogsQueryClient, LogsBatchQuery, LogsQueryStatus
from azure.identity import AzureCliCredential
from benedict import benedict
from functools import cached_property
from abuseipdb_wrapper import AbuseIPDB
from atlassian import Jira
from tenable.io import TenableIO

# %% ../nbs/01_api.ipynb 5
logger = logging.getLogger(__name__)

# %% ../nbs/01_api.ipynb 7
class Clients:
    """
    Clients for various services, cached for performance
    """
    @cached_property
    def config(self):
        return cache.get("config", load_config())

    @cached_property
    def runzero(self):
        """
        Returns a runzero client
        """
        return httpx_cache.Client(base_url="https://console.rumble.run/api/v1.0", headers={"Authorization": f"Bearer {self.config.runzero_apitoken}"})

    @cached_property
    def abuseipdb(self):
        """
        Returns an abuseipdb client
        """
        return AbuseIPDB(API_KEY=self.config.abuseipdb_api_key)

    @cached_property
    def jira(self):
        """
        Returns a jira client
        """
        return Jira(url=self.config.jira_url, username=self.config.jira_username, password=self.config.jira_password)

    @cached_property
    def tio(self):
        """
        Returns a TenableIO client
        """
        return TenableIO(self.config.tenable_access_key, self.config.tenable_secret_key)


clients = Clients()

# %% ../nbs/01_api.ipynb 9
@memoize_stampede(cache, expire=60 * 60 * 3) # cache for 3 hours
def list_workspaces(fmt: str = "df", # df, csv, json, list
                    agency: str = "ALL"): # Agency alias or ALL
    path = datalake_path()
    df = pandas.read_csv((path / "notebooks/lists/SentinelWorkspaces.csv").open())
    df = df.join(pandas.read_csv((path / "notebooks/lists/SecOps Groups.csv").open()).set_index("Alias"), on="SecOps Group", rsuffix="_secops")
    df = df.rename(columns={"SecOps Group": "alias", "Domains and IPs": "domains"})
    df = df.dropna(subset=["customerId"]).sort_values(by="alias").convert_dtypes().reset_index()
    if agency != "ALL":
        df = df[df["alias"] == agency]
    if fmt == "df":
        return df
    elif fmt == "csv":
        return df.to_csv()
    elif fmt == "json":
        return df.fillna("").to_dict("records")
    elif fmt == "list":
        return list(df["customerId"].unique())
    else:
        raise ValueError("Invalid format")

# %% ../nbs/01_api.ipynb 13
@memoize_stampede(cache, expire=60 * 60 * 3) # cache for 3 hours
def list_subscriptions():
    return pandas.DataFrame(azcli(["account", "list"]))["id"].unique()

@memoize_stampede(cache, expire=60 * 60 * 3) # cache for 3 hours
def list_securityinsights():
    return pandas.DataFrame(azcli([
        "graph", "query", "--first", "1000", "-q", 
        """
        resources
        | where type =~ 'microsoft.operationsmanagement/solutions'
        | where name startswith 'SecurityInsights'
        | project wlid = tolower(tostring(properties.workspaceResourceId))
        | join kind=leftouter (
            resources | where type =~ 'microsoft.operationalinsights/workspaces' | extend wlid = tolower(id))
            on wlid
        | extend customerId = properties.customerId
        """
    ])["data"])

def chunks(items, size):
    # Yield successive `size` chunks from `items`
    for i in range(0, len(items), size):
        yield items[i:i + size]

@memoize_stampede(cache, expire=60 * 5) # cache for 5 mins
def loganalytics_query(queries: list[str], timespan=pandas.Timedelta("14d"), batch_size=190, batch_delay=32, sentinel_workspaces = None):
    """
    Run queries across all workspaces, in batches of `batch_size` with a minimum delay of `batch_delay` between batches.
    Returns a dictionary of queries and results
    """
    client = LogsQueryClient(AzureCliCredential())
    workspaces = list_workspaces(fmt="df")
    if sentinel_workspaces is None:
        sentinel_workspaces = list_securityinsights()
    requests, results = [], []
    for query in queries:
        for workspace_id in sentinel_workspaces["customerId"]:
            requests.append(LogsBatchQuery(workspace_id=workspace_id, query=query, timespan=timespan))
    querytime = pandas.Timestamp("now")
    logger.info(f"Executing {len(requests)} queries at {querytime}")
    for request_batch in chunks(requests, batch_size):
        while cache.get("loganalytics_query_running"):
            time.sleep(1)
        batch_start = pandas.Timestamp("now")
        cache.set("loganalytics_query_running", True, batch_delay)
        results += client.query_batch(request_batch)
        duration = pandas.Timestamp("now") - batch_start
        logger.info(f"Completed {len(results)} (+{len(request_batch)}) in {duration}")
    dfs = {}
    for request, result in zip(requests, results):
        if result.status == LogsQueryStatus.PARTIAL:
            tables = result.partial_data
            tables = [pandas.DataFrame(table.rows, columns=table.columns) for table in tables]
        elif result.status == LogsQueryStatus.SUCCESS:
            tables = result.tables
            tables = [pandas.DataFrame(table.rows, columns=table.columns) for table in tables]
        else:
            tables = [pandas.DataFrame([result.__dict__])]
        df = pandas.concat(tables).dropna(axis=1, how='all') # prune empty columns
        df["TenantId"] = request.workspace
        alias = workspaces.query(f'customerId == "{request.workspace}"')["alias"].str.cat()
        if alias == '':
            alias = sentinel_workspaces.query(f'customerId == "{request.workspace}"')["name"].str.cat()
        df["_alias"] = alias
        query = request.body["query"]
        if query in dfs:
            dfs[query].append(df)
        else:
            dfs[query] = [df]
    return {query: pandas.concat(results, ignore_index=True).convert_dtypes() for query, results in dfs.items()}

def query_all(query, fmt="df", timespan=pandas.Timedelta("14d")):
    try:
        # Check query is not a plain string and is iterable
        assert not isinstance(query, str)
        iter(query)
    except (AssertionError, TypeError):
        # if it is a plain string or it's not iterable, convert into a list of queries
        query = [query]
    df = pandas.concat(loganalytics_query(query, timespan).values())
    if fmt == "df":
        return df
    elif fmt == "csv":
        return df.to_csv()
    elif fmt == "json":
        return df.fillna("").to_dict("records")
    else:
        raise ValueError("Invalid format")

# %% ../nbs/01_api.ipynb 16
columns_of_interest = benedict({
    "name": ['AADEmail', 'AccountName', 'AccountUPN', 'AccountUpn', 'Account', 'CompromisedEntity', 'DestinationUserName', 
             "Computer", "DisplayName", "EmailSenderAddress", "FileName", 'FilePath', "FolderPath", 'FullyQualifiedSubjectUserName', 'InitiatingProcessAccountUpn',
            'MailboxOwnerUPN', 'Name', 'NewProcessName', 'Owner', 'ParentProcessName', 'Process', 'CommandLine', 'ProcessCommandLine', 'RecipientEmailAddress',
            'RequesterUpn', 'SenderMailFromAddress', 'SourceIdentity', 'SourceUserName', 'SubjectUserName', 'TargetUserName', 'TargetUser', 'Upn',
            'UserName', 'userName', 'UserPrincipalName'],
    "guid": ['Caller', "DestinationUserID", 'SourceUserID', 'UserId'],
    "ip": ['CallerIpAddress', 'DestinationIP', 'DstIpAddr', 'EmailSourceIpAddress', 'IPAddresses', 'IPAddress', 'IpAddress',
           'NetworkDestinationIP', 'NetworkIP', 'NetworkSourceIP', 'RemoteIP', 'SourceIP', 'SrcIpAddr'],
    "url": ["DomainName", 'FileOriginUrl', 'FQDN', 'RemoteUrl', 'Url'],
    "hash": ["MD5", "SHA1", "SHA256", 'FileHashValue', 'FileHash', 'InitiatingProcessSHA256']
})

columns = [column for area in columns_of_interest.values() for column in area]

def finalise_query(query, take):
    return f"{query} | take {take} | extend placeholder_=dynamic({{'':null}}) | evaluate bag_unpack(column_ifexists('pack_', placeholder_))"

def hunt(indicators, expression="has", columns=columns, workspaces=None, timespans=["1d", "14d", "90d", "700d"], take=5000):
    queries = []
    if workspaces is None:
        workspaces = list_securityinsights()
    else:
        df = list_securityinsights()
        workspaces = df[df["customerId"].isin(workspaces)]
    querylogged = False
    if expression in ['has_any']:
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
            if expression not in ['has_all']:
                indicator = f"'{indicator}'" # wrap indicator in quotes unless expecting dynamic
            if not querylogged:
                logger.info(f"Test Query: find where {columns[0]} {expression} {indicator} | take {take}")
                querylogged = True
            for chunk in chunks([f"{column} {expression} {indicator}" for column in columns], 20):
                query = " or ".join(chunk)
                final_query = finalise_query(f"find where {query}", take)
                queries.append(final_query)
    for timespan in timespans:
        results = pandas.concat(loganalytics_query(queries, pandas.Timedelta(timespan), sentinel_workspaces = workspaces).values())
        if 'placeholder_' in results.columns:
            results = results.drop('placeholder_', axis=1)
        if results.empty:
            logger.info(f"No results in {timespan}, extending hunt")
            continue
        logger.info(f"Found {indicators} in {timespan}, returning")
        return results
    else:
        raise Exception("No results found!")

# %% ../nbs/01_api.ipynb 18
def atlaskit_transformer(inputtext, inputfmt="md", outputfmt="wiki", runtime="node"):
    import nbdev_squ
    transformer = dirs.user_cache_path / f"atlaskit-transformer.bundle_v{nbdev_squ.__version__}.js"
    if not transformer.exists():
        transformer_url = f'{nbdev_squ._modidx.d["settings"]["git_url"]}/releases/download/v{nbdev_squ.__version__}/atlaskit-transformer.bundle.js'
        transformer.write_bytes(requests.get(transformer_url).content)
    cmd = [runtime, str(transformer), inputfmt, outputfmt]
    logger.debug(" ".join(cmd))
    try:
        return run(cmd, input=inputtext, text=True, capture_output=True, check=True).stdout
    except CalledProcessError:
        run(cmd, input=inputtext, text=True, check=True)

# %% ../nbs/01_api.ipynb 21
def security_incidents(start=pandas.Timestamp("now", tz="UTC") - pandas.Timedelta("1d"), timedelta=pandas.Timedelta("1d")):
    # Queries for security incidents from `start` time for `timedelta` and returns a dataframe
    # Sorts by TimeGenerated (TODO)
    query = "SecurityIncident | summarize arg_max(TimeGenerated, *) by IncidentNumber"
    return query_all(query, timespan=(start.to_pydatetime(), timedelta))

def security_alerts(start=pandas.Timestamp("now", tz="UTC") - pandas.Timedelta("1d"), timedelta=pandas.Timedelta("1d")):
    # Queries for security alerts from `start` time for `timedelta` and returns a dataframe
    # Sorts by TimeGenerated (TODO)
    query = "SecurityAlert | summarize arg_max(TimeGenerated, *) by SystemAlertId"
    return query_all(query, timespan=(start.to_pydatetime(), timedelta))
