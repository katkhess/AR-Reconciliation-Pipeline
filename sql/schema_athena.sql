-- Athena DDL (exported from console via SHOW CREATE TABLE, then normalized)
-- Data bucket: s3://ar-reconciliation-project-katkhess/
-- Query results bucket (workgroup output): s3://kathessbucket/metadatahealthcare/athena-query-results/

CREATE DATABASE IF NOT EXISTS ar_project;

CREATE EXTERNAL TABLE IF NOT EXISTS ar_project.customers (
  customer_id   string,
  customer_name string,
  industry      string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('quoteChar'='\"', 'separatorChar'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ar-reconciliation-project-katkhess/customers'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS ar_project.invoices (
  invoice_id     string,
  customer_id    string,
  invoice_date   string,
  due_date       string,
  invoice_amount string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('quoteChar'='\"', 'separatorChar'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ar-reconciliation-project-katkhess/invoices'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS ar_project.payments (
  payment_id      string,
  customer_id     string,
  payment_amount  string,
  payment_date    string,
  reference_notes string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('quoteChar'='\"', 'separatorChar'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ar-reconciliation-project-katkhess/payments'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS ar_project.returns (
  return_id           string,
  customer_id         string,
  original_invoice_id string,
  return_sku          string,
  credit_amount       string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('quoteChar'='\"', 'separatorChar'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ar-reconciliation-project-katkhess/returns'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS ar_project.credits (
  credit_id          string,
  customer_id        string,
  credit_amount      string,
  applied_invoice_id string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('quoteChar'='\"', 'separatorChar'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ar-reconciliation-project-katkhess/credits'
TBLPROPERTIES ('skip.header.line.count'='1');