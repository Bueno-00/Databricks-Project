-- Bronze table for commodity transactions (transaction_commodities.csv)
CREATE OR REFRESH STREAMING LIVE TABLE lakehouse.bronze.transaction_commodities
COMMENT 'Bronze table from raw commodities transactions CSV'
TBLPROPERTIES (
  'quality' = 'bronze',
  'source' = 'raw_public/transaction_commodities'
)
AS
SELECT
  transaction_id,
  CAST(data_hora AS TIMESTAMP) AS data_hora,
  commodity_code AS ativo,
  CAST(quantidade AS DOUBLE) AS quantidade,
  tipo_operacao,
  moeda,
  cliente_id,
  canal,
  mercado,
  arquivo_origem,
  CAST(importado_em AS TIMESTAMP) AS importado_em,
  current_timestamp() AS ingested_at,
  _metadata.file_path AS arquivo_source
FROM
  cloud_files('/Volumes/lakehouse/raw_public/transaction_commodities', 'csv' ,
  map(
      'header', 'true',
      'inferSchema', 'true'
      )
    )
;