-- Bronze table for customers (Lakehouse Raw -> Bronze)
-- CREATE OR REFRESH STREAMING LIVE TABLE as used by Databricks Lakehouse (Lakeflow)

CREATE OR REFRESH STREAMING LIVE TABLE lakehouse.bronze.customers
COMMENT 'Bronze table from raw customers CSV'
TBLPROPERTIES (
  'quality' = 'bronze',
  'source' = 'raw_public/customers'
)
AS
SELECT
  CAST(customer_id AS STRING) AS customer_id,
  customer_name,
  documento,
  segmento,
  pais,
  estado,
  current_timestamp() AS ingested_at,
  _metadata.file_path AS arquivo_origem
FROM
  cloud_files('/Volumes/lakehouse/raw_public/customers', 'csv', map('header', 'true','inferSchema', 'true'))
;