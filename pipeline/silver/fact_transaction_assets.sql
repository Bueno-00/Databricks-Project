-- Silver: fact_transaction_assets
-- Une transações BTC e commodities, normaliza símbolos e aplica constraints de qualidade

CREATE OR REFRESH STREAMING LIVE TABLE silver_fact_transaction_assets
COMMENT 'Silver: unified transaction events (BTC + commodities)'
TBLPROPERTIES ('quality' = 'silver', 'layer' = 'silver')
AS
SELECT
  transaction_id,
  CAST(data_hora AS TIMESTAMP) AS data_hora,
  date_trunc('hour', CAST(data_hora AS TIMESTAMP)) AS data_hora_aproximada,
  UPPER(COALESCE(ativo, commodity_code)) AS raw_asset,
  CASE
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('BTC','BTC-USD') THEN 'BTC'
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('GOLD','GC=F') THEN 'GOLD'
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('OIL','CL=F') THEN 'OIL'
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('SILVER','SI=F') THEN 'SILVER'
    ELSE 'UNKNOWN'
  END AS asset_symbol,
  CAST(quantidade AS DOUBLE) AS quantidade,
  tipo_operacao,
  moeda,
  cliente_id,
  canal,
  mercado,
  arquivo_origem,
  CAST(importado_em AS TIMESTAMP) AS importado_em,
  current_timestamp() AS ingested_at
FROM STREAM(bronze_transaction_btc)

UNION ALL

SELECT
  transaction_id,
  CAST(data_hora AS TIMESTAMP) AS data_hora,
  date_trunc('hour', CAST(data_hora AS TIMESTAMP)) AS data_hora_aproximada,
  UPPER(COALESCE(ativo, commodity_code)) AS raw_asset,
  CASE
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('BTC','BTC-USD') THEN 'BTC'
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('GOLD','GC=F') THEN 'GOLD'
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('OIL','CL=F') THEN 'OIL'
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('SILVER','SI=F') THEN 'SILVER'
    ELSE 'UNKNOWN'
  END AS asset_symbol,
  CAST(quantidade AS DOUBLE) AS quantidade,
  tipo_operacao,
  moeda,
  cliente_id,
  canal,
  mercado,
  arquivo_origem,
  CAST(importado_em AS TIMESTAMP) AS importado_em,
  current_timestamp() AS ingested_at
FROM STREAM(bronze_transaction_commodities)

-- Data quality constraints
CONSTRAINT quantidade_positive EXPECT (quantidade > 0) ON VIOLATION DROP ROW;
CONSTRAINT data_hora_not_null EXPECT (data_hora IS NOT NULL) ON VIOLATION DROP ROW;
CONSTRAINT tipo_operacao_valid EXPECT (tipo_operacao IN ('COMPRA','VENDA')) ON VIOLATION DROP ROW;
CONSTRAINT asset_symbol_valid EXPECT (asset_symbol IN ('BTC','GOLD','OIL','SILVER')) ON VIOLATION DROP ROW;
