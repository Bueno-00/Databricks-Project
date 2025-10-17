-- Silver: fact_transaction_revenue
-- Join entre transações, cotações e clientes para calcular gross_value, sinal e fee

CREATE OR REFRESH STREAMING LIVE TABLE silver_fact_transaction_revenue
COMMENT 'Silver: transaction revenue (joined with quotations and clients)'
TBLPROPERTIES ('quality' = 'silver', 'layer' = 'silver')
AS
WITH t AS (
  SELECT * FROM STREAM(silver_fact_transaction_assets)
),
q AS (
  SELECT horario_coleta_aproximado, asset_symbol, preco FROM STREAM(silver_fact_quotation_assets)
),
c AS (
  SELECT customer_id FROM STREAM(silver_dim_clientes)
)
SELECT
  t.transaction_id,
  t.data_hora,
  t.data_hora_aproximada,
  t.asset_symbol,
  t.quantidade,
  t.tipo_operacao,
  q.preco AS preco_cotacao,
  (t.quantidade * q.preco) AS gross_value,
  CASE
    WHEN t.tipo_operacao = 'VENDA' THEN (t.quantidade * q.preco)
    WHEN t.tipo_operacao = 'COMPRA' THEN -(t.quantidade * q.preco)
    ELSE 0
  END AS gross_value_sinal,
  (t.quantidade * q.preco) * 0.0025 AS fee_revenue,
  t.cliente_id,
  current_timestamp() AS ingested_at
FROM t
LEFT JOIN q ON q.horario_coleta_aproximado = t.data_hora_aproximada AND q.asset_symbol = t.asset_symbol
LEFT JOIN c ON c.customer_id = t.cliente_id

-- Constraints
CONSTRAINT preco_cotacao_positive EXPECT (preco_cotacao > 0) ON VIOLATION DROP ROW;
CONSTRAINT fee_revenue_positive EXPECT (fee_revenue > 0) ON VIOLATION DROP ROW;
CONSTRAINT customer_sk_not_null EXPECT (cliente_id IS NOT NULL) ON VIOLATION DROP ROW;
CONSTRAINT cotacao_timestamp_before_tx EXPECT (q.horario_coleta_aproximado <= t.data_hora) ON VIOLATION DROP ROW;
