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
from markdown import markdown

from . import api

logger = logging.getLogger(__name__)


def adx_query(kql):
    """
    Run a kusto query

    Args:
        kql (str or list): kusto query or list of queries

    Returns:
        json: query results
    """
    if isinstance(kql, list):
        kql = [".execute script with (ContinueOnErrors=true) <|"] + kql
        kql = "\n".join(kql)
    config = api.cache["config"]
    cluster, dx_db = config.azure_dataexplorer.rsplit("/", 1)
    dx_client = KustoClient(KustoConnectionStringBuilder.with_az_cli_authentication(cluster))
    return dx_client.execute(dx_db, kql.replace("\\", "\\\\")).primary_results[0]


def adxtable2df(table):
    """
    Return a pandas dataframe from an adx table
    """
    columns = [col.column_name for col in table.columns]
    frame = pandas.DataFrame(table.raw_rows, columns=columns)
    return frame


def export_jira_issues():
    """
    Exports all JIRA issues to the data lake.
    """
    jira_issues_path = api.datalake_path() / "jira_outputs" / "issues"

    def getissues(start_at, jql):
        # The jql() method now uses Enhanced JQL v3 APIs under the hood automatically
        response = api.clients.jira.jql(jql, start=start_at, limit=100)
        next_start = response["startAt"] + response["maxResults"]
        total_rows = response["total"]
        if next_start > total_rows:
            next_start = total_rows
        issues = response["issues"]
        return next_start, total_rows, issues

    def save_date_issues(after_date: pandas.Timestamp, path=jira_issues_path):
        fromdate = after_date
        todate = after_date + pandas.to_timedelta("1d")
        jql = f"updated >= {fromdate.date().isoformat()} and updated < {todate.date().isoformat()} order by key"
        output = path / f"{fromdate.date().isoformat()}" / "issues.parquet"
        if output.exists() and fromdate < pandas.Timestamp.now() - pandas.to_timedelta("1d"):
            # skip previously dumped days except for last day
            return None
        start_at, total_rows = 0, -1
        dataframes = []
        while start_at != total_rows:
            start_at, total_rows, issues = getissues(start_at, jql)
            dataframes.append(pandas.DataFrame(issues))
            if start_at == 100:
                logger.info(f"{total_rows} to load")
        if total_rows > 1:
            df = pandas.concat(dataframes)
            df["fields"] = df["fields"].apply(json.dumps)
            logger.info(f"saving {output}")
            try:
                df.to_parquet(output.open("wb"))
            except Exception as exc:
                print(exc)
            return df
        else:
            return None

    after = pandas.Timestamp.now() - pandas.to_timedelta("7d")
    until = pandas.Timestamp.now() + pandas.to_timedelta("1d")

    while after < until:
        save_date_issues(after)
        after += pandas.to_timedelta("1d")


def flatten(nested_dict, parent_key="", sep="_"):
    """
    Flatten a nested dictionary.

    Args:
        nested_dict (dict): The nested dictionary to flatten.
        parent_key (str, optional): The parent key for the current level of nesting.
        sep (str, optional): The separator to use for flattened keys.

    Returns:
        dict: The flattened dictionary.
    """
    flat_dict = {}

    for key, value in nested_dict.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            flat_dict.update(flatten(value, new_key, sep))
        else:
            flat_dict[new_key] = value

    return flat_dict


def sentinel_beautify_local(
    data: dict,
    outputformat: str = "jira",
    default_status: str = "Onboard: MOU (T0)",
    default_orgid: int = 2,
):
    """
    Takes a SecurityIncident including alerts as json and returns
    markdown, html and detailed json representation.
    """
    for jsonfield in ["Labels", "Owner", "AdditionalData", "Comments"]:
        if data.get(jsonfield):
            data[jsonfield] = json.loads(data[jsonfield])
    labels = [
        f"SIEM_Severity:{data['Severity']}",
        f"SIEM_Status:{data['Status']}",
        f"SIEM_Title:{data['Title']}",
    ]
    labels += [label["labelName"] for label in data["Labels"]]  # copy over labels from incident
    incident_details = [data["Description"], ""]

    if data.get("Owner"):
        owner = None
        if data["Owner"].get("email"):
            owner = data["Owner"]["email"]
        elif data["Owner"].get("userPrincipalName"):
            owner = data["Owner"]["userPrincipalName"]
        if owner:
            labels.append(f"SIEM_Owner:{owner}")
            incident_details.append(f"- **Sentinel Incident Owner:** {owner}")

    if data.get("Classification"):
        labels.append(f"SIEM_Classification:{data['Classification']}")
        incident_details.append(f"- **Alert Classification:** {data['Classification']}")

    if data.get("ClassificationReason"):
        labels.append(f"SIEM_ClassificationReason:{data['ClassificationReason']}")
        incident_details.append(
            f"- **Alert Classification Reason:** {data['ClassificationReason']}"
        )

    if data.get("ProviderName"):
        labels.append(f"SIEM_ProviderName:{data['ProviderName']}")
        incident_details.append(f"- **Provider Name:** {data['ProviderName']}")

    if data.get("AdditionalData"):
        if data["AdditionalData"].get("alertProductNames"):
            product_names = ",".join(data["AdditionalData"]["alertProductNames"])
            labels.append(f"SIEM_alertProductNames:{product_names}")
            incident_details.append(f"- **Product Names:** {product_names}")
        if data["AdditionalData"].get("tactics"):
            tactics = ",".join(data["AdditionalData"]["tactics"])
            labels.append(f"SIEM_tactics:{tactics}")
            incident_details.append(
                f"- **[MITRE ATT&CK Tactics](https://attack.mitre.org/tactics/):** {tactics}"
            )
        if data["AdditionalData"].get("techniques"):
            techniques = ",".join(data["AdditionalData"]["techniques"])
            labels.append(f"SIEM_techniques:{techniques}")
            incident_details.append(
                "- **[MITRE ATT&CK Techniques](https://attack.mitre.org/techniques/):**"
                f" {techniques}"
            )

    comments = []
    if data.get("Comments"):
        if len(data["Comments"]) > 0:
            comments += ["", "## Comments"]
            for comment in data["Comments"]:
                comments += comment["message"].split("\n")
            comments += [""]

    alert_details = []
    observables = []
    entity_type_value_mappings = {
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

    class Default(dict):
        """
        Default dict that returns the key if the key is not found
        Args:
            dict
        """

        def __missing__(self, key):
            return key

    for alert in data["AlertData"][:10]:  # Assumes alertdata is newest to oldest
        if not alert_details:
            alert_details += [
                "",
                "## Alert Details",
                (
                    "The last day of activity (up to 10 alerts) is summarised below from"
                    " newest to oldest."
                ),
            ]
        alert_details.append(
            f"### [{alert['AlertName']} (Severity:{alert['AlertSeverity']}) - "
            + f"TimeGenerated {alert['TimeGenerated']}]({alert['AlertLink']})"
        )
        alert_details.append(alert["Description"])
        for key in [
            "RemediationSteps",
            "ExtendedProperties",
            "Entities",
        ]:  # entities last as may get truncated
            if alert.get(key):
                if isinstance(alert[key], str) and alert[key][0] in ["{", "["]:
                    alert[key] = json.loads(alert[key])
                if key == "Entities":  # add the entity to our list of observables
                    for entity in alert[key]:
                        observable = {"value": None}
                        if "Type" in entity:
                            observable = {
                                "type": entity["Type"],
                                "value": entity_type_value_mappings.get(
                                    entity["Type"], ""
                                ).format_map(Default(entity)),
                            }
                        if not observable["value"]:  # dump whole dict as string if no mapping found
                            observable["value"] = repr(entity)
                        observables.append(observable)
                if alert[key] and isinstance(alert[key], list) and isinstance(alert[key][0], dict):
                    # if list of dicts, make a table
                    for index, entry in enumerate(
                        [flatten(item) for item in alert[key] if len(item.keys()) > 1]
                    ):
                        alert_details += ["", f"#### {key}.{index}"]
                        for entrykey, value in entry.items():
                            if value:
                                alert_details.append(f"- **{entrykey}:** {value}")
                elif isinstance(alert[key], dict):  # if dict display as list
                    alert_details += ["", f"#### {key}"]
                    for entrykey, value in alert[key].items():
                        if value and len(value) < 200:
                            alert_details.append(f"- **{entrykey}:** {value}")
                        elif value:  # break out long blocks
                            alert_details += [f"- **{entrykey}:**", "", "```", value, "```", ""]
                else:  # otherwise just add as separate lines
                    alert_details += ["", f"#### {key}"] + [item for item in alert[key]]

    title = (
        f"SIEM Detection #{data['IncidentNumber']} Sev:{data['Severity']} -"
        f" {data['Title']} (Status:{data['Status']})"
    )
    mdtext = (
        [
            f"# {title}",
            "",
            f"## [SecurityIncident #{data['IncidentNumber']} Details]({data['IncidentUrl']})",
            "",
        ]
        + incident_details
        + comments
        + alert_details
    )
    mdtext = "\n".join([str(line) for line in mdtext])
    # Convert markdown to HTML (returned for potential use)
    markdown(mdtext, extensions=["tables"])
    # remove special chars and deduplicate labels
    labels = set("".join(c for c in label if c.isalnum() or c in ".:_") for label in labels)

    response = {
        "subject": title,
        "labels": list(labels),
        "observables": [dict(ts) for ts in set(tuple(i.items()) for i in observables)],
        "sentinel_data": data,
    }
    workspaces_df = api.list_workspaces()
    customer = workspaces_df[workspaces_df["customerId"] == data["TenantId"]].to_dict("records")
    if len(customer) > 0:
        customer = customer[0]
    else:
        customer = {}
    # Grab wiki format for jira and truncate to 32767 chars
    response.update(
        {
            "secops_status": customer.get("SecOps Status") or default_status,
            "jira_orgid": customer.get("JiraOrgId") or default_orgid,
            "customer": customer,
            "wikimarkup": (api.atlaskit_transformer(mdtext)[:32760]),
        }
    )
    return response
