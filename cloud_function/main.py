from googleapiclient.discovery import build
from google.cloud import bigquery
from google.cloud import exceptions
from datetime import datetime
import json
import os
import base64

#TODO replace by your GCP project id
PROJECT_ID = "your_project_id"

#TODO replace by the dataset and table name of your configuration table
CONFIG_DATASET = "sandbox"
CONFIG_TABLE = "ga_reports_config"


def get_reports_configuration(bq_client, report_id=None):
    """Retrieves the reports configuration from a table in BigQuery for reports that are flagged to be generated.
    Configuration includes:
    - dimensions (separated by ', ')
    - metrics (separated by ', ')
    - filters (separated by ';')
    - destination dataset and table (where the results will be loaded to)

    Args:
        bq_client: BigQuery client object
    Returns:
        A list of reports configurations
    """
    try:

        config_query = (
            "SELECT id,"
            "       dimensions,"
            "       metrics,"
            "       filters,"
            "       period_start_date,"
            "       period_end_date,"
            "       destination_dataset,"
            "       destination_table,"
            "       view_id"
            "  FROM `{project}.{dataset}.{table}`"
            '  WHERE '.format(
                project=PROJECT_ID, dataset=CONFIG_DATASET, table=CONFIG_TABLE
            )
        )

        # if a specific report_id is passed, it is processed whether the active flag is true or not
        if report_id != "all":
            config_query = config_query + f" id = {report_id}"
        else:
            config_query = config_query + f" active IS TRUE"

        query_job = bq_client.query(config_query)

        print(
            f"Retrieving Google Analytics reports configuration from BQ table {CONFIG_DATASET}.{CONFIG_TABLE}"
        )

        try:
            # Waits for the query to finish
            iterator = query_job.result()
        except exceptions.GoogleCloudError as exc:
            print("Errors retrieving configuration from BQ table: ", query_job.errors)

        rows = list(iterator)

        reports_config = []

        for row in rows:

            dim_configs = []
            met_configs = []

            dims = row.dimensions.split(", ")

            # date should always be one of the dimensions
            if "ga:date" not in dims:
                dims.append("ga:date")

            for dim in dims:
                dim_config = {"name": dim}
                dim_configs.append(dim_config)

            for met in row.metrics.split(", "):
                met_config = {"expression": met}
                met_configs.append(met_config)

            # If the start and end date of the period was not provided in the configuration,
            # the last 3 days will be retrieved
            report_config = {
                "id": row.id,
                "dimensions": dim_configs,
                "metrics": met_configs,
                "filters": row.filters,
                "period_start_date": "3daysAgo"
                if row.period_start_date is None
                else row.period_start_date,
                "period_end_date": "today"
                if row.period_end_date is None
                else row.period_end_date,
                "destination_dataset": row.destination_dataset,
                "destination_table": row.destination_table,
                "view_id": row.view_id,
            }

            reports_config.append(report_config)

        return reports_config

    except Exception as ex:
        print(f"Exception: {ex}")
        raise


def initialize_analytics_reporting():
    """Initializes an Analytics Reporting API V4 service object.

    Returns:
        An authorized Analytics Reporting API V4 service object.
    """
    try:
        # Build the service object.
        service = build("analyticsreporting", "v4", cache_discovery=False)

        return service

    except Exception as ex:
        print(f"Error building the analyticsreporting API service object: {ex}")
        raise


def get_report(service, report_config, page_token=None):
    """Queries the Analytics Reporting API V4.

    Args:
        service: An authorized Analytics Reporting API V4 service object
        report_config: A dictionary with the report dimensions, metrics and other configurations
        page_token: results page to retrieve
    Returns:
    """
    try:
        return (
            service.reports()
            .batchGet(
                body={
                    "reportRequests": [
                        {
                            "viewId": report_config.get("view_id"),
                            "dateRanges": [
                                {
                                    "startDate": report_config.get("period_start_date"),
                                    "endDate": report_config.get("period_end_date"),
                                }
                            ],
                            "metrics": report_config.get("metrics"),
                            "dimensions": report_config.get("dimensions"),
                            "filtersExpression": report_config.get("filters"),
                            "pageSize": "10000",
                            "includeEmptyRows": "true",
                            "samplingLevel": "LARGE",
                            "pageToken": page_token,
                        }
                    ]
                }
            )
            .execute()
        )

    except Exception as ex:
        print(f"Exception: {ex}")
        raise


def get_report_data(service, report_config):
    """Calls function get_report (Google Analytics API) for all the pages in order to retrieve all the results.
    Parses the responses and creates a JSON object with the report data (dimensions, metrics and a timestamp of the
    extraction).

    Args:
        service: An authorized Analytics Reporting API V4 service object
        report_config: A dictionary object with the report configuration containing its dimensions and metrics
    Returns:
        JSON object containing all the rows of the report
    """
    try:
        print(
            f"Calling Google Analytics API for report with ID = {report_config.get('id')}"
        )

        response = get_report(service, report_config)

        # the API only returns 1 report per response
        report = response.get("reports", [])[0]

        column_header = report.get("columnHeader", {})
        dimension_headers = column_header.get("dimensions", [])
        metric_headers = column_header.get("metricHeader", {}).get(
            "metricHeaderEntries", []
        )
        rows = report.get("data", {}).get("rows", [])
        next_page_token = report.get("nextPageToken")

        # the API will be executed for all the pages in order to retrieve all the results
        # the attribute nextPageToken indicates the index of the last record retrieved
        while next_page_token != None:
            response = get_report(service, report_config, next_page_token)
            report = response.get("reports", [])[0]
            rows.extend(report.get("data", {}).get("rows", []))
            next_page_token = report.get("nextPageToken")

        # list of dictionaries that will hold all the rows of the report
        # dictionaries will be used because they don't require any conversion to load data to BQ in JSON format

        print("Processing report data")

        report_data = []

        metrics = []
        dimensions = []

        execution_timestamp = datetime.now()

        for m in metric_headers:
            metrics.append(m.get("name").replace("ga:", ""))

        for d in dimension_headers:
            dimensions.append(d.replace("ga:", ""))

        for row in rows:
            dimensions_values = row.get("dimensions", [])

            metrics_values = row.get("metrics", [])[0].get("values", {})

            table_columns = dimensions + metrics

            row_values = dimensions_values + metrics_values

            table_dict = dict(zip(table_columns, row_values))

            # every record will have the same execution timestamp
            table_dict["extracted_at"] = execution_timestamp.__str__()

            if table_dict.get("date") != None:
                table_dict["date"] = str(
                    datetime.strptime(str(table_dict["date"]), "%Y%m%d")
                )

            report_data.append(table_dict)

        return report_data

    except Exception as ex:
        print(f"Exception: {ex}")
        raise


def load_raw_data(bq_client, report_data, report_config):
    """Loads the report data to BigQuery (destination dataset and table specified in its configuration).
    Data is appended to the existing table.

    Args:
        bq_client: BigQuery client object
        report_data: JSON object with all the rows to insert
        report_config: Dictionary object of the report configuration containing its destination dataset and table
    """

    dataset = bq_client.dataset(report_config.get("destination_dataset"))

    # the raw tables will hold the data from all the extractions
    table = dataset.table(report_config.get("destination_table") + "_raw")

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    # date is always on of the dimensions of the report and is used as the partitioning field
    job_config.time_partitioning = bigquery.table.TimePartitioning(field="date")

    report_dimensions = []

    for report_dimension in report_config.get("dimensions", []):
        report_dimensions.append(report_dimension.get("name"))

    job_config.autodetect = True

    job = bq_client.load_table_from_json(report_data, table, job_config=job_config)

    try:
        print(
            "Loading report data to raw data table {table}".format(
                table=report_config.get("destination_dataset")
                + "."
                + report_config.get("destination_table")
                + "_raw"
            )
        )
        job.result()
    except exceptions.GoogleCloudError as exc:
        print("Errors: ", job.errors)


def recreate_all_report_results(bq_client, report_config):
    """The final output table is recreated every time with the most up-to-date results.
    The dimensions are considered to be the Primary Key and the metrics are inserted / updated.
    I.e. if the report has a dimension 'ga:date' and a metric 'ga:sessions', this function will fetch all the records
    for a given 'ga:date' that were loaded in the past, but will only load to the final table the ones from the present
    extraction with the latest number of sessions. If a value (or set of values) for the dimension(s) was not retrieved
    on the present extraction it will still be loaded to the final table.

    Args:
        bq_client: BigQuery client object
        report_config: Dictionary object of the report configuration containing its destination dataset and table
    """

    dimensions_str = ""
    dimensions = report_config.get("dimensions", [])
    for dimension in dimensions:
        dimensions_str += dimension.get("name").replace("ga:", "") + ", "

    # remove the last unnecessary ','
    dimensions_str = dimensions_str[:-2]

    dataset = report_config.get("destination_dataset")
    table = report_config.get("destination_table") + "_raw"

    final_results_query = (
        "SELECT t.* except(rk) "
        "FROM"
        "   (SELECT t.*,"
        "        rank () OVER (PARTITION BY {dimensions}"
        "                    ORDER BY extracted_at DESC) AS rk"
        "    FROM `{project}.{dataset}.{table}` t) t"
        "    WHERE rk = 1;".format(
            dimensions=dimensions_str, project=PROJECT_ID, dataset=dataset, table=table
        )
    )

    job_config = bigquery.QueryJobConfig()
    destination_dataset = bq_client.dataset(dataset)
    job_config.destination = destination_dataset.table(
        report_config.get("destination_table")
    )

    # table is deleted first to work around an error with the date partition
    bq_client.delete_table(job_config.destination, not_found_ok=True)

    job_config.time_partitioning = bigquery.table.TimePartitioning(field="date")

    # If the table doesn't exist yet, it will be created
    job_config.create_disposition = "CREATE_IF_NEEDED"

    # All the results will be recreated
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.use_legacy_sql = False
    job_config.allow_large_results = True

    # Start the query
    job = bq_client.query(final_results_query, job_config=job_config)

    try:
        print(
            "Loading report data to final output table {table}".format(
                table=report_config.get("destination_dataset")
                + "."
                + report_config.get("destination_table")
            )
        )
        # Wait for the query to finish
        job.result()
    except exceptions.GoogleCloudError as exc:
        print("Errors reloading all the reports results to BQ table: ", job.errors)


def main(event, context):

    try:

        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_json: dict = json.loads(pubsub_message)

        print(f"Pub/Sub message: {pubsub_message}")

        report_id = message_json["report_id"]

        if report_id != "all":
            print("Processing report with ID = {id}".format(id=report_id))

        bq_client = bigquery.Client()

        reports_config = get_reports_configuration(bq_client, report_id)
        service = initialize_analytics_reporting()

        for config in reports_config:

            report_data = get_report_data(service, config)
            load_raw_data(bq_client, report_data, config)
            recreate_all_report_results(bq_client, config)

        print("All reports processed successfully")

        return "OK", 200

    except Exception as ex:
        print("Google Analytics pipeline finished with errors")
        return "ERROR", 500
