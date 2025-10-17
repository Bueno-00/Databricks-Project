-- Bronze table for BTC transactions
CREATE OR REFRESH STREAMING LIVE TABLE bronze_transaction_btc
COMMENT 'Bronze table from raw BTC transactions CSV'
TBLPROPERTIES (
  'quality' = 'bronze',
  'source' = 'raw_public/transacation_btc.csv'
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
  input_file_name() AS arquivo_source
FROM
  read_csv('/Volumes/lakehouse/raw_public/transaction_btc.csv', header=true, inferSchema=true)
;