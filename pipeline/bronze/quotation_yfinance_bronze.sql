-- Bronze table for yfinance quotations (commodities, forex, etc.)
CREATE OR REFRESH STREAMING LIVE TABLE lakehouse.bronze.quotation_yfinance
COMMENT 'Bronze table from raw yfinance CSV'
TBLPROPERTIES (
  'quality' = 'bronze',
  'source' = 'raw_public/quotation_yfinance'
)
AS
SELECT
  ativo,
  CAST(preco AS DOUBLE) AS preco,
  moeda,
  CAST(horario_coleta AS TIMESTAMP) AS horario_coleta,
  date_trunc('hour', CAST(horario_coleta AS TIMESTAMP)) AS horario_coleta_aproximado,
  current_timestamp() AS ingested_at,
  _metadata.file_path AS arquivo_origem
FROM
  cloud_files('/Volumes/lakehouse/raw_public/quotation_yfinance', 'csv', map('header', 'true',
      'inferSchema', 'true'))
;