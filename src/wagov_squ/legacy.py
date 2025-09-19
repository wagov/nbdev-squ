"""Legacy functionality for nbdev-squ package."""

__all__ = [
    "logger",
    "adx_query",
    "adxtable2df",
    "export_jira_issues",
    "flatten",
    "sentinel_beautify_local",
]

import json
import logging

import pandas
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

from . import api

logger = logging.getLogger(__name__)


def adx_query(kql: str | list[str]):
    """Run a Kusto query against Azure Data Explorer.

    Args:
        kql: Single query string or list of queries to execute as script

    Returns:
        Query results from primary result set
    """
    if isinstance(kql, list):
        kql = "\n".join([".execute script with (ContinueOnErrors=true) <|"] + kql)

    config = api.cache["config"]
    cluster, database = config.azure_dataexplorer.rsplit("/", 1)
    client = KustoClient(KustoConnectionStringBuilder.with_az_cli_authentication(cluster))

    return client.execute(database, kql.replace("\\", "\\\\")).primary_results[0]


def adxtable2df(table) -> pandas.DataFrame:
    """Convert ADX table to pandas DataFrame."""
    columns = [col.column_name for col in table.columns]
    return pandas.DataFrame(table.raw_rows, columns=columns)


def export_jira_issues(dry_run=False):
    """Exports all JIRA issues to the data lake for the last 7 days."""
    batch_size = 100
    days_to_export = 7

    jira_issues_path = api.datalake_path() / "jira_outputs" / "issues"

    def _get_jira_batch(start_at: int, jql: str) -> tuple[int, int, list]:
        """Get a batch of Jira issues and return (next_start, total, issues)."""
        response = api.clients.jira.jql(jql, start=start_at, limit=batch_size)

        if not response or not isinstance(response, dict):
            raise ValueError(f"Invalid Jira response: {type(response)}")

        # Handle standard Jira API format
        if "startAt" in response and "maxResults" in response:
            return (
                response["startAt"] + response["maxResults"],
                response["total"],
                response["issues"],
            )

        # Handle paginated API or fallback
        issues = response.get("issues", response.get("values", []))
        total = response.get("total", len(issues))
        next_start = start_at + batch_size if response.get("nextPageToken") else total

        return min(next_start, total), total, issues

    def _save_parquet(df: pandas.DataFrame, output_path) -> None:
        """Save DataFrame to parquet, handling both local and blob storage."""
        if str(output_path).startswith("az://"):
            # Blob storage - write directly without mkdir
            df.to_parquet(output_path)
        else:
            # Local storage - create directory if needed
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(output_path)

    def _export_day(date: pandas.Timestamp) -> pandas.DataFrame | None:
        """Export Jira issues for a single day."""
        next_date = date + pandas.Timedelta(days=1)
        jql = f"updated >= {date.date()} and updated < {next_date.date()} order by key"
        output = jira_issues_path / f"{date.date()}" / "issues.parquet"

        # Skip if already exists and not today
        if (
            not dry_run
            and output.exists()
            and date < pandas.Timestamp.now() - pandas.Timedelta(days=1)
        ):
            return None

        # Collect all pages
        dataframes = []
        start_at = 0
        total = -1

        while start_at != total:
            start_at, total, issues = _get_jira_batch(start_at, jql)
            if issues:
                dataframes.append(pandas.DataFrame(issues))
            if len(dataframes) == 1:  # Log after first batch
                logger.info(f"Loading {total} total issues for {date.date()}")

        if not dataframes:
            return None

        # Combine and save
        df = pandas.concat(dataframes, ignore_index=True)
        df["fields"] = df["fields"].apply(json.dumps)

        if not dry_run:
            _save_parquet(df, output)

        return df

    # Export last 7 days
    start_date = pandas.Timestamp.now() - pandas.Timedelta(days=days_to_export)
    current_date = start_date

    while current_date <= pandas.Timestamp.now():
        _export_day(current_date)
        current_date += pandas.Timedelta(days=1)


def flatten(nested_dict: dict, parent_key: str = "", sep: str = "_") -> dict:
    """Flatten a nested dictionary into a single level.

    Args:
        nested_dict: The nested dictionary to flatten
        parent_key: Parent key for current nesting level
        sep: Separator for flattened keys

    Returns:
        Flattened dictionary with concatenated keys
    """
    result = {}

    for key, value in nested_dict.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            result.update(flatten(value, new_key, sep))
        else:
            result[new_key] = value

    return result


def sentinel_beautify_local(
    data: dict,
    outputformat: str = "jira",
    default_status: str = "Onboard: MOU (T0)",
    default_orgid: int = 2,
):
    """Convert SecurityIncident to markdown and structured data for JIRA."""
    max_alerts = 10
    max_wiki_length = 32760

    # Parse JSON fields
    for field in ["Labels", "Owner", "AdditionalData", "Comments"]:
        if data.get(field) and isinstance(data[field], str):
            data[field] = json.loads(data[field])

    labels, incident_details = _extract_incident_info(data)
    comments = _extract_comments(data.get("Comments", []))
    alert_details, observables = _process_alerts(data.get("AlertData", [])[:max_alerts])

    title = f"SIEM Detection #{data['IncidentNumber']} Sev:{data['Severity']} - {data['Title']} (Status:{data['Status']})"

    mdtext = "\n".join(
        [
            f"# {title}",
            "",
            f"## [SecurityIncident #{data['IncidentNumber']} Details]({data['IncidentUrl']})",
            "",
            *incident_details,
            *comments,
            *alert_details,
        ]
    )

    # Clean and deduplicate labels
    clean_labels = set("".join(c for c in label if c.isalnum() or c in ".:_") for label in labels)

    # Get customer info
    customer = _get_customer_info(data["TenantId"], default_status, default_orgid)

    return {
        "subject": title,
        "labels": list(clean_labels),
        "observables": [dict(ts) for ts in set(tuple(i.items()) for i in observables)],
        "sentinel_data": data,
        "wikimarkup": api.atlaskit_transformer(mdtext)[:max_wiki_length],
        **customer,
    }


def _extract_incident_info(data: dict) -> tuple[list[str], list[str]]:
    """Extract labels and incident details from security incident data."""
    labels = [
        f"SIEM_Severity:{data['Severity']}",
        f"SIEM_Status:{data['Status']}",
        f"SIEM_Title:{data['Title']}",
    ]
    labels.extend(label["labelName"] for label in data.get("Labels", []))

    incident_details = [data["Description"], ""]

    # Add owner info
    if owner_data := data.get("Owner"):
        owner = owner_data.get("email") or owner_data.get("userPrincipalName")
        if owner:
            labels.append(f"SIEM_Owner:{owner}")
            incident_details.append(f"- **Sentinel Incident Owner:** {owner}")

    # Add classification info
    for field, label_prefix, detail_name in [
        ("Classification", "SIEM_Classification", "Alert Classification"),
        ("ClassificationReason", "SIEM_ClassificationReason", "Alert Classification Reason"),
        ("ProviderName", "SIEM_ProviderName", "Provider Name"),
    ]:
        if value := data.get(field):
            labels.append(f"{label_prefix}:{value}")
            incident_details.append(f"- **{detail_name}:** {value}")

    # Add additional data
    if additional := data.get("AdditionalData"):
        _process_additional_data(additional, labels, incident_details)

    return labels, incident_details


def _process_additional_data(additional: dict, labels: list, details: list) -> None:
    """Process AdditionalData fields for labels and details."""
    mappings = [
        ("alertProductNames", "SIEM_alertProductNames", "Product Names"),
        ("tactics", "SIEM_tactics", "[MITRE ATT&CK Tactics](https://attack.mitre.org/tactics/)"),
        (
            "techniques",
            "SIEM_techniques",
            "[MITRE ATT&CK Techniques](https://attack.mitre.org/techniques/)",
        ),
    ]

    for field, label_prefix, detail_name in mappings:
        if values := additional.get(field):
            joined_values = ",".join(values)
            labels.append(f"{label_prefix}:{joined_values}")
            details.append(f"- **{detail_name}:** {joined_values}")


def _extract_comments(comments: list) -> list[str]:
    """Extract and format comments."""
    if not comments:
        return []

    result = ["", "## Comments"]
    for comment in comments:
        result.extend(comment["message"].split("\n"))
    result.append("")
    return result


def _process_alerts(alerts: list) -> tuple[list[str], list[dict]]:
    """Process alerts and extract observables."""
    if not alerts:
        return [], []

    entity_mappings = {
        "host": "{HostName}",
        "account": "{Name}",
        "process": "{CommandLine}",
        "file": "{Name}",
        "ip": "{Address}",
        "url": "{Url}",
        "dns": "{DomainName}",
        "registry-key": "{Hive}{Key}",
        "filehash": "{Algorithm}{Value}",
    }

    class _DefaultDict(dict):
        def __missing__(self, key):
            return key

    alert_details = [
        "",
        "## Alert Details",
        "The last day of activity (up to 10 alerts) is summarised below from newest to oldest.",
    ]
    observables = []

    for alert in alerts:
        # Add alert header
        alert_details.append(
            f"### [{alert['AlertName']} (Severity:{alert['AlertSeverity']}) - "
            f"TimeGenerated {alert['TimeGenerated']}]({alert['AlertLink']})"
        )
        alert_details.append(alert["Description"])

        # Process alert fields
        for field in ["RemediationSteps", "ExtendedProperties", "Entities"]:
            if not alert.get(field):
                continue

            # Parse JSON strings
            value = alert[field]
            if isinstance(value, str) and value.startswith(("{", "[")):
                value = json.loads(value)

            # Extract entities as observables
            if field == "Entities":
                for entity in value:
                    observable = _extract_observable(entity, entity_mappings, _DefaultDict())
                    if observable:
                        observables.append(observable)

            # Format field for display
            _format_alert_field(alert_details, field, value)

    return alert_details, observables


def _extract_observable(entity: dict, mappings: dict, default_dict) -> dict | None:
    """Extract observable from entity data."""
    if "Type" not in entity:
        return {"type": "unknown", "value": repr(entity)}

    entity_type = entity["Type"]
    template = mappings.get(entity_type, "")

    try:
        value = template.format_map(default_dict(entity)) if template else repr(entity)
        return {"type": entity_type, "value": value} if value else None
    except (KeyError, ValueError):
        return {"type": entity_type, "value": repr(entity)}


def _format_alert_field(details: list, field: str, value) -> None:
    """Format alert field for markdown display."""
    if not value:
        return

    if isinstance(value, list) and value and isinstance(value[0], dict):
        # List of dicts - create sections
        for i, entry in enumerate(flatten(item) for item in value if len(item.keys()) > 1):
            details.extend(["", f"#### {field}.{i}"])
            for key, val in entry.items():
                if val:
                    details.append(f"- **{key}:** {val}")

    elif isinstance(value, dict):
        # Dict - show as bullet points
        details.extend(["", f"#### {field}"])
        for key, val in value.items():
            if not val:
                continue
            if len(str(val)) < 200:
                details.append(f"- **{key}:** {val}")
            else:
                details.extend([f"- **{key}:**", "", "```", str(val), "```", ""])

    else:
        # List or other - show as lines
        details.extend(
            ["", f"#### {field}"]
            + [str(item) for item in (value if isinstance(value, list) else [value])]
        )


def _get_customer_info(tenant_id: str, default_status: str, default_orgid: int) -> dict:
    """Get customer information from tenant ID."""
    workspaces_df = api.list_workspaces()
    customer_records = workspaces_df[workspaces_df["customerId"] == tenant_id].to_dict("records")
    customer = customer_records[0] if customer_records else {}

    return {
        "secops_status": customer.get("SecOps Status", default_status),
        "jira_orgid": customer.get("JiraOrgId", default_orgid),
        "customer": customer,
    }
