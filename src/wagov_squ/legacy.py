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


def export_jira_issues(
    dry_run=False, 
    force_refresh=False, 
    include_today=False,
    days_to_export=7,
    batch_size=100
):
    """Exports all JIRA issues to the data lake.
    
    Args:
        dry_run: If True, don't actually save files
        force_refresh: If True, re-process existing files (useful for merging new updates)
        include_today: If True, process today's date (useful for real-time updates)
        days_to_export: Number of days to export (default: 7)
        batch_size: Pagination batch size (default: 100)
    """

    logger.info(f"🚀 Starting JIRA issues export ({'DRY RUN' if dry_run else 'LIVE'})")
    logger.info(f"📅 Exporting {days_to_export} days of data")

    jira_issues_path = api.datalake_path() / "jira_outputs" / "issues"
    logger.info(f"📁 Output path: {jira_issues_path}")

    def _get_jira_batch(page_token: str | None, jql: str) -> tuple[str | None, int, list]:
        """Get a batch of Jira issues and return (next_page_token, total, issues)."""
        token_display = '<token>' if page_token else 'None'
        logger.debug(f"🔍 Fetching batch: page_token={token_display}, limit={batch_size}")
        
        # Use enhanced_jql for Cloud with nextPageToken pagination
        response = api.clients.jira.enhanced_jql(jql, limit=batch_size, nextPageToken=page_token)

        if not response or not isinstance(response, dict):
            raise ValueError(f"Invalid Jira response: {type(response)}")

        logger.debug(f"📥 Response keys: {list(response.keys())}")

        issues = response.get("issues", response.get("values", []))
        next_page_token = response.get("nextPageToken")
        
        # For enhanced_jql, we don't get total upfront, so estimate based on pagination
        if next_page_token:
            # More pages available, use a large number as placeholder
            total = 999999
        else:
            # Last page, we can't determine exact total but we know we're done
            total = len(issues) if page_token is None else 999999
        
        next_display = '<token>' if next_page_token else 'None'
        logger.debug(f"📊 Enhanced JQL: next_page_token={next_display}, total={total}, issues_in_batch={len(issues)}")
        return next_page_token, total, issues

    def _save_parquet(df: pandas.DataFrame, output_path) -> None:
        """Save DataFrame to parquet, handling both local and blob storage."""
        if str(output_path).startswith("az://"):
            # Blob storage - use UPath write_bytes to maintain auth context
            import io
            with io.BytesIO() as buffer:
                df.to_parquet(buffer, coerce_timestamps="ms", allow_truncated_timestamps=True)
                buffer.seek(0)
                output_path.write_bytes(buffer.read())
        else:
            # Local storage - create directory if needed
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(output_path)

    def _export_day(date: pandas.Timestamp) -> pandas.DataFrame | None:
        """Export Jira issues for a single day."""
        logger.info(f"📆 Processing {date.date()}")
        
        next_date = date + pandas.Timedelta(days=1)
        jql = f"updated >= {date.date()} and updated < {next_date.date()} order by key"
        output = jira_issues_path / f"{date.date()}" / "issues.parquet"

        # Determine if we should skip this date
        is_today = date.date() == pandas.Timestamp.now().date()
        
        # Skip today unless specifically requested
        if is_today and not include_today:
            logger.info(f"⏭️  Skipping {date.date()} (today) - use include_today=True to process")
            return None
        
        # Skip if file exists and we're not forcing refresh
        if not dry_run and output.exists() and not force_refresh and not is_today:
            logger.info(f"⏭️  Skipping {date.date()} - already exists (use force_refresh=True to re-process)")
            return None

        # Collect all pages
        dataframes = []
        page_token = None
        total = -1

        batch_count = 0
        while True:
            batch_count += 1
            page_token, total, issues = _get_jira_batch(page_token, jql)
            
            # Log progress every 20 batches instead of every batch
            if batch_count == 1 or batch_count % 20 == 0:
                token_display = '<token>' if page_token else 'None'
                logger.info(f"🔄 Batch {batch_count}: next_page_token: {token_display}, got {len(issues)} issues")
            
            if issues:
                dataframes.append(pandas.DataFrame(issues))
            if len(dataframes) == 1:  # Log after first batch
                if total > batch_size and total != 999999:  # Known total with multiple pages
                    logger.info(f"📊 Found {total} total issues for {date.date()} (fetching in {batch_size}-issue batches)")
                elif total == 999999:  # Paginated API without known total
                    logger.info(f"📊 Fetching issues for {date.date()} in {batch_size}-issue batches...")
                else:
                    logger.info(f"📊 Found {total} total issues for {date.date()}")
            elif len(dataframes) > 1 and len(dataframes) % 3 == 0:  # Progress every 3 batches
                loaded = sum(len(df) for df in dataframes)
                if total != 999999:
                    logger.info(f"📥 Progress: {loaded} / {total} issues loaded...")
                else:
                    logger.info(f"📥 Progress: {loaded} issues loaded (total unknown)...")
            
            # Stop when no more pages available
            if page_token is None:
                break
                
            # Additional safety: stop if we've done too many batches
            if batch_count > 500:  # More than 25,000 issues seems unlikely
                logger.error(f"⚠️  Too many batches ({batch_count}), stopping to prevent runaway")
                break

        if not dataframes:
            logger.info(f"📭 No issues found for {date.date()}")
            return None

        # Combine and save
        df = pandas.concat(dataframes, ignore_index=True)
        df["fields"] = df["fields"].apply(json.dumps)
        
        logger.info(f"🔄 Processing {len(df)} issues for {date.date()}")

        if not dry_run:
            _save_parquet(df, output)
            logger.info(f"💾 Saved {len(df)} issues to {output}")
        else:
            logger.info(f"🏃 DRY RUN: Would save {len(df)} issues")

        return df

    # Export last 7 days
    start_date = pandas.Timestamp.now() - pandas.Timedelta(days=days_to_export)
    end_date = pandas.Timestamp.now()
    current_date = start_date
    
    total_issues = 0
    days_processed = 0

    logger.info(f"📅 Processing dates from {start_date.date()} to {end_date.date()}")

    while current_date <= end_date:
        df = _export_day(current_date)
        if df is not None:
            total_issues += len(df)
        days_processed += 1
        current_date += pandas.Timedelta(days=1)

    logger.info(f"✅ Export complete! Processed {days_processed} days with {total_issues} total issues")


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
