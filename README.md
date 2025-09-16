# wagov-squ

Python SIEM Query Utils with Ibis support for scalable data processing.

[![GitHub Actions](https://img.shields.io/github/actions/workflow/status/wagov/nbdev-squ/deploy.yaml.svg?logo=github)](https://github.com/wagov/nbdev-squ/actions/workflows/deploy.yaml)
[![PyPI Version](https://img.shields.io/pypi/v/wagov-squ.svg?logo=pypi)](https://pypi.org/project/wagov-squ/)
[![OpenSSF Scorecard](https://img.shields.io/ossf-scorecard/github.com/wagov/nbdev-squ.svg?label=openssf%20scorecard)](https://securityscorecards.dev/viewer/?uri=github.com/wagov/nbdev-squ)

## Features

- **Azure Sentinel Integration**: Query Log Analytics workspaces with KQL
- **Multiple Output Formats**: Support for pandas DataFrame, CSV, JSON, list, and Ibis expressions  
- **Scalable Processing**: Built on Ibis for efficient data transformations
- **Backward Compatible**: Zero breaking changes for existing code
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

