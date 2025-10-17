-- Bronze table for customers (Lakehouse Raw -> Bronze)
-- CREATE OR REFRESH STREAMING LIVE TABLE as used by Databricks Lakehouse (Lakeflow)

CREATE OR REFRESH STREAMING LIVE TABLE bronze_customers
COMMENT 'Bronze table from raw customers CSV'
TBLPROPERTIES (
  'quality' = 'bronze',
  'source' = 'raw_public/customers.csv'
)
AS
SELECT
  CAST(customer_id AS STRING) AS customer_id,
  name,
  documento,
  segmento,
  pais,
  estado,
  current_timestamp() AS ingested_at,
  input_file_name() AS arquivo_origem
FROM
  read_csv('/Volumes/lakehouse/raw_public/customers.csv', header=true, inferSchema=true)
;