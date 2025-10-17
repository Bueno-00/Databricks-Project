-- Silver: fact_quotation_assets
-- Une cotações de BTC e yfinance, normaliza símbolos e aplica constraints de qualidade

CREATE OR REFRESH STREAMING LIVE TABLE silver_fact_quotation_assets
COMMENT 'Silver: unified quotations (BTC + yfinance)'
TBLPROPERTIES ('quality' = 'silver', 'layer' = 'silver')
AS
SELECT
  ativo,
  CAST(preco AS DOUBLE) AS preco,
  moeda,
  CAST(horario_coleta AS TIMESTAMP) AS horario_coleta,
  date_trunc('hour', CAST(horario_coleta AS TIMESTAMP)) AS horario_coleta_aproximado,
  CASE
    WHEN UPPER(ativo) IN ('BTC','BTC-USD') THEN 'BTC'
    WHEN UPPER(ativo) IN ('GC=F') THEN 'GOLD'
    WHEN UPPER(ativo) IN ('CL=F') THEN 'OIL'
    WHEN UPPER(ativo) IN ('SI=F') THEN 'SILVER'
    ELSE 'UNKNOWN'
  END AS asset_symbol,
  current_timestamp() AS ingested_at
FROM STREAM(bronze_quotation_btc)

UNION ALL

SELECT
  ativo,
  CAST(preco AS DOUBLE) AS preco,
  moeda,
  CAST(horario_coleta AS TIMESTAMP) AS horario_coleta,
  date_trunc('hour', CAST(horario_coleta AS TIMESTAMP)) AS horario_coleta_aproximado,
  CASE
    WHEN UPPER(ativo) IN ('BTC','BTC-USD') THEN 'BTC'
    WHEN UPPER(ativo) IN ('GC=F') THEN 'GOLD'
    WHEN UPPER(ativo) IN ('CL=F') THEN 'OIL'
    WHEN UPPER(ativo) IN ('SI=F') THEN 'SILVER'
    ELSE 'UNKNOWN'
  END AS asset_symbol,
  current_timestamp() AS ingested_at
FROM STREAM(bronze_quotation_yfinance)

-- Data quality constraints
CONSTRAINT preco_positive EXPECT (preco > 0) ON VIOLATION DROP ROW;
CONSTRAINT horario_coleta_not_future EXPECT (horario_coleta <= current_timestamp()) ON VIOLATION DROP ROW;
CONSTRAINT ativo_not_null EXPECT (ativo IS NOT NULL AND ativo != '') ON VIOLATION DROP ROW;
CONSTRAINT moeda_usd EXPECT (moeda = 'USD') ON VIOLATION DROP ROW;
