# wagov-squ

Python SIEM Query Utils with Ibis support for scalable data processing.

[![GitHub Actions](https://img.shields.io/github/actions/workflow/status/wagov/nbdev-squ/release.yml.svg?logo=github)](https://github.com/wagov/nbdev-squ/actions/workflows/release.yml)
[![PyPI Version](https://img.shields.io/pypi/v/wagov-squ.svg?logo=pypi)](https://pypi.org/project/wagov-squ/)

## Features

- **Azure Sentinel Integration**: Query Log Analytics workspaces with KQL
- **Multiple Output Formats**: Support for pandas DataFrame, CSV, JSON, list, and Ibis expressions  
- **Scalable Processing**: Built on Ibis for efficient data transformations
- **Modern Architecture**: Built on uv for fast dependency management

## Install

```bash
pip install wagov-squ
```

### Development Setup
```bash
git clone https://github.com/wagov/nbdev-squ.git
cd nbdev-squ
uv sync --dev
```

## Configuration

Set the `SQU_CONFIG` environment variable to specify Azure Key Vault configuration:

```python
import os
os.environ["SQU_CONFIG"] = "keyvault_name/tenant_id"
```

## Quick Start

### Basic Usage

```python
from wagov_squ import list_workspaces, query_all, Fmt

# List Azure Sentinel workspaces (returns pandas DataFrame by default)
workspaces = list_workspaces()
print(workspaces)

# Get data in different formats
csv_data = list_workspaces(fmt=Fmt.csv)
json_data = list_workspaces(fmt=Fmt.json) 
list_data = list_workspaces(fmt=Fmt.list)

# Execute KQL queries
results = query_all("SecurityEvent | take 10")
```

### Ibis Support

```python
from wagov_squ import list_workspaces, Fmt

# Get data as Ibis expression for advanced processing
ibis_expr = list_workspaces(fmt=Fmt.ibis)

# Use Ibis for complex transformations
filtered = ibis_expr.filter(ibis_expr.alias == "MyAgency")
result = filtered.to_pandas()  # Convert back to pandas when needed
```

## API Reference

### Core Functions

- `list_workspaces(fmt="df", agency="ALL")` - List Azure Sentinel workspaces
- `list_securityinsights(fmt="df")` - List Security Insights resources  
- `query_all(query, fmt="df", timespan=14d)` - Execute KQL queries across workspaces
- `clients.jira` - Access Jira API client

### Output Formats

- `Fmt.pandas` or `"df"` - pandas DataFrame (default)
- `Fmt.csv` or `"csv"` - CSV string
- `Fmt.json` or `"json"` - List of dictionaries
- `Fmt.list` or `"list"` - List of lists  
- `Fmt.ibis` or `"ibis"` - Ibis expression for advanced processing

## Integration with dbt & SQLMesh

### dbt Integration

wagov-squ includes a built-in dbt-duckdb plugin for seamless integration. Configure your dbt `profiles.yml`:

```yaml
your_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'dbt.duckdb'
      plugins:
        - module: 'wagov_squ.api'
          alias: 'squ'
```

Then use in your dbt models:

```sql
-- models/security/persistence_hunting.sql
{{ config(materialized='view') }}

SELECT * FROM (
  SELECT * FROM squ(kql_path='queries/persistence_detection.kql', timespan='14d')
)
```

Create `queries/persistence_detection.kql`:
```kql
DeviceProcessEvents
| where ActionType == "ProcessCreated" 
| where ProcessCommandLine has_all (dynamic(['reg',' ADD', @'Software\Microsoft\Windows\CurrentVersion\Run']))
| where InitiatingProcessFileName !in (dynamic(['Discord.exe','Skype.exe']))
| project Timestamp, DeviceName, ProcessCommandLine, InitiatingProcessFileName
```

Or query workspaces directly:
```sql
SELECT * FROM squ(list_workspaces=true)
```

### SQLMesh Integration

```python
# models/security_events.py
from sqlmesh import model
from wagov_squ import query_all, Fmt

@model(
    "security.persistence_events",
    kind="view",
    cron="@daily"
)
def persistence_events(context):
    kql_query = """
    DeviceProcessEvents
    | where ActionType == "ProcessCreated"
    | where ProcessCommandLine contains "reg add"
    | project Timestamp, DeviceName, ProcessCommandLine
    """
    
    # Get data as Ibis expression for further processing
    data = query_all(kql_query, fmt=Fmt.ibis)
    return data.filter(data.Timestamp >= context.start_date)
```

## Security Considerations

Configuration secrets are cached securely in the [user_cache_dir](https://platformdirs.readthedocs.io/en/latest/api.html#cache-directory) with restricted permissions. Ensure:

- System runs on encrypted disk
- User cache directory access is restricted  
- No external logging of cache directory
- Use isolated VMs/workstations for sensitive activities

## Development

### Commands

```bash
just install          # Install development dependencies
just test             # Run all tests  
just test-fast        # Run unit tests only
just test-integration # Run integration tests (requires SQU_CONFIG)
just lint             # Format and lint code
just build            # Build package
just complexity       # Analyze code complexity
```

### Testing

Integration tests require Azure authentication via `SQU_CONFIG`:

```bash
export SQU_CONFIG="keyvault_name/tenant_id"  
just test-integration
```

## License

Apache-2.0

