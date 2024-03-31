# SIEM Query Utils


<!-- WARNING: THIS FILE WAS AUTOGENERATED! DO NOT EDIT! -->

## Status

[![GitHub Actions Workflow
Status](https://img.shields.io/github/actions/workflow/status/wagov/nbdev-squ/deploy.yaml.svg?logo=github)](https://github.com/wagov/nbdev-squ/actions/workflows/deploy.yaml)
[![Python Packaging Index -
Version](https://img.shields.io/pypi/v/nbdev-squ.svg?logo=pypi)](https://pypi.org/project/nbdev-squ/)
[![OpenSSF
Scorecard](https://img.shields.io/ossf-scorecard/github.com/wagov/nbdev-squ.svg?label=openssf%20scorecard&logo=data:image/png;base64,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.png)](https://securityscorecards.dev/viewer/?uri=github.com/wagov/nbdev-squ)

## Install

Below is how to install in a plain python 3.11+ environment

``` sh
pip install nbdev-squ
```

The installation can also be run in a notebook (we tend to use
[JupyterLab Desktop](https://github.com/jupyterlab/jupyterlab-desktop)
for local dev). The `SQU_CONFIG` env var indicates to nbdev_squ it
should load the json secret *squconfig-`my_keyvault_tenantid`* from the
`my_kevault_name` keyvault.

``` python
%pip install nbdev-squ
import os; os.environ["SQU_CONFIG"] = "{{ my_keyvault_name }}/{{ my_keyvault_tenantid }}" 

from nbdev_squ import api
# do cool notebook stuff with api
```

### Security considerations

The contents of the keyvault secret are loaded into memory and cached in
the
[user_cache_dir](https://platformdirs.readthedocs.io/en/latest/api.html#cache-directory)
which should be a temporary secure directory restricted to the single
user. Please ensure that the system this library is used on disallows
access and/or logging of the user cache directory to external locations,
and is on an encrypted disk (a common approach is to use isolated VMs
and workstations for sensitive activities).

## How to use

*Note: If you create/use a Github Codespace on any of the wagov repos,
SQU_CONFIG should be configured automatically.*

Before using, config needs to be loaded into `squ.core.cache`, which can
be done automatically from json in a keyvault by setting the env var
`SQU_CONFIG` to `"keyvault/tenantid"`.

``` bash
export SQU_CONFIG="{{ keyvault }}/{{ tenantid }}"
```

Can be done in python before import from nbdev_squ as well:

``` python
import os; os.environ["SQU_CONFIG"] = "{{ keyvault }}/{{ tenantid }}"
```

``` python
from nbdev_squ import api
import io, pandas

# Load workspace info from datalake blob storage
df = api.list_workspaces(fmt="df"); print(df.shape)

# Load workspace info from introspection of azure graph
df = api.list_securityinsights(); print(df.shape)

# Kusto query to Sentinel workspaces via Azure Lighthouse
df = api.query_all("SecurityIncident | take 20", fmt="df"); print(df.shape)

# Kusto queries to Sentinel workspaces via Azure Lighthouse (batches up to 100 queries at a time)
df = api.query_all(["SecurityAlert | take 20" for a in range(10)]); print(df.shape)

# Kusto query to ADX
#df = api.adxtable2df(api.adx_query("kusto query | take 20"))

# General azure cli cmd
api.azcli(["config", "set", "extension.use_dynamic_install=yes_without_prompt"])
print(len(api.azcli(["account", "list"])))

# Various pre-configured api clients

# RunZero
response = api.clients.runzero.get("/export/org/assets.csv", params={"search": "has_public:t AND alive:t AND (protocol:rdp OR protocol:vnc OR protocol:teamviewer OR protocol:telnet OR protocol:ftp)"})
pandas.read_csv(io.StringIO(response.text)).head(10)

# Jira
pandas.json_normalize(api.clients.jira.jql("updated > -1d")["issues"]).head(10)

# AbuseIPDB
api.clients.abuseipdb.check_ip("1.1.1.1")

# TenableIO
pandas.DataFrame(api.clients.tio.scans.list()).head(10)
```

``` python
badips_df = api.query_all("""
SecurityIncident
| where Classification == "TruePositive"
| mv-expand AlertIds
| project tostring(AlertIds)
| join SecurityAlert on $left.AlertIds == $right.SystemAlertId
| mv-expand todynamic(Entities)
| project Entities.Address
| where isnotempty(Entities_Address)
| distinct tostring(Entities_Address)
""", timespan=pandas.Timedelta("45d"))
```

``` python
df = api.query_all("find where ClientIP startswith '172.16.' | evaluate bag_unpack(pack_) | take 40000")
```

``` python
df = api.query_all("""union withsource="_table" *
| extend _ingestion_time_bin = bin(ingestion_time(), 1h)
| summarize take_any(*) by _table, _ingestion_time_bin
| project pack=pack_all(true)""")
```

``` python
import json
pandas.DataFrame(list(df["pack"].apply(json.loads)))
```

## Secrets template

The below json can be used as a template for saving your own json into
*`my_keyvault_name`/squconfig-`my_keyvault_tenantid`* to use with this
library:

``` json
{
  "config_version": "20240101 - added ??? access details",
  "datalake_blob_prefix": "https://???/???",
  "datalake_subscription": "???",
  "datalake_account": "???.blob.core.windows.net",
  "datalake_container": "???",
  "kql_baseurl": "https://raw.githubusercontent.com/???",
  "azure_dataexplorer": "https://???.???.kusto.windows.net/???",
  "tenant_id": "???",
  "jira_url": "https://???.atlassian.net",
  "jira_username": "???@???",
  "jira_password": "???",
  "runzero_apitoken": "???",
  "abuseipdb_api_key": "???",
  "tenable_access_key": "???",
  "tenable_secret_key": "???",
}
```
