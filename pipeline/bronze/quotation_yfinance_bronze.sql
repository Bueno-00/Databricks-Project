-- Bronze table for yfinance quotations (commodities, forex, etc.)
CREATE OR REFRESH STREAMING LIVE TABLE bronze_quotation_yfinance
COMMENT 'Bronze table from raw yfinance CSV'
TBLPROPERTIES (
  'quality' = 'bronze',
  'source' = 'raw_public/quotation_yfinance.csv'
)
AS
SELECT
  ativo,
  CAST(preco AS DOUBLE) AS preco,
  moeda,
  CAST(horario_coleta AS TIMESTAMP) AS horario_coleta,
  date_trunc('hour', CAST(horario_coleta AS TIMESTAMP)) AS horario_coleta_aproximado,
  current_timestamp() AS ingested_at,
  input_file_name() AS arquivo_origem
FROM
  read_csv('/Volumes/lakehouse/raw_public/quotation_yfinance.csv', header=true, inferSchema=true)
;