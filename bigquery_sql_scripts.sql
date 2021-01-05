CREATE TABLE sandbox.ga_reports_config (
id INT64,
dimensions STRING,
metrics STRING,
filters STRING,
period_start_date STRING,
period_end_date STRING,
destination_dataset STRING,
destination_table STRING,
view_id STRING,
active BOOL
);

INSERT INTO your_dataset_name.ga_reports_config VALUES
(1, 'ga:date, ga:browser, ga:deviceCategory', 'ga:sessions', 'ga:Country==Germany', '30daysAgo', 'today', 'sandbox', 'sessions_report', '111111111', TRUE);
