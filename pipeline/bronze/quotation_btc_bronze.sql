-- Bronze table for Bitcoin quotations
CREATE OR REFRESH STREAMING LIVE TABLE lakehouse.bronze.quotation_btc
COMMENT 'Bronze table from raw BTC quotation CSV'
TBLPROPERTIES (
  'quality' = 'bronze',
  'source' = 'raw_public/quotation_btc'
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
  cloud_files('/Volumes/lakehouse/raw_public/quotation_btc', 'csv', map('header', 'true',
      'inferSchema', 'true'))
;