-- Bronze table for BTC transactions
CREATE OR REFRESH STREAMING LIVE TABLE lakehouse.bronze.transaction_btc
COMMENT 'Bronze table from raw BTC transactions CSV'
TBLPROPERTIES (
  'quality' = 'bronze',
  'source' = 'raw_public/transacation_btc'
)
AS
SELECT
  transaction_id,
  CAST(data_hora AS TIMESTAMP) AS data_hora,
  ativo,
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
cloud_files('/Volumes/lakehouse/raw_public/transaction_btc', 'csv', map('header', 'true',
      'inferSchema', 'true'))
;