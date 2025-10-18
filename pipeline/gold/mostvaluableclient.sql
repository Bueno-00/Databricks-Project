-- Gold: mostvaluableclient
-- Agrega métricas por cliente e calcula ranking/classificação

CREATE OR REFRESH STREAMING LIVE TABLE gold_mostvaluableclient
COMMENT 'Gold: top clients metrics and segmentation'
TBLPROPERTIES ('quality' = 'gold', 'layer' = 'gold')
AS
WITH revenue AS (
  SELECT * FROM STREAM(silver_fact_transaction_revenue)
),
max_dt AS (
  SELECT MAX(data_hora) AS max_data_hora FROM revenue
),
metrics AS (
  SELECT
    cliente_id,
    COUNT(*) AS total_transacoes,
    SUM(gross_value) AS valor_total,
    AVG(gross_value) AS ticket_medio,
    MIN(data_hora) AS primeira_transacao,
    MAX(data_hora) AS ultima_transacao,
    SUM(fee_revenue) AS comissao_total,
    MAX(data_hora) as latest_tx
  FROM revenue
  GROUP BY cliente_id
),
metrics_30 AS (
  SELECT
    m.*,
    (SELECT COUNT(1) FROM revenue r WHERE r.cliente_id = m.cliente_id AND r.data_hora >= (SELECT max_data_hora FROM max_dt) - INTERVAL 30 DAYS) AS transacoes_ultimos_30_dias
  FROM metrics m
)
SELECT
  m.cliente_id AS customer_id,
  m.total_transacoes,
  m.valor_total,
  m.ticket_medio,
  m.primeira_transacao,
  m.ultima_transacao,
  m.transacoes_ultimos_30_dias,
  m.comissao_total,
  RANK() OVER (ORDER BY m.total_transacoes DESC) AS ranking_por_transacoes,
  CASE
    WHEN RANK() OVER (ORDER BY m.total_transacoes DESC) = 1 THEN 'Top 1'
    WHEN RANK() OVER (ORDER BY m.total_transacoes DESC) = 2 THEN 'Top 2'
    WHEN RANK() OVER (ORDER BY m.total_transacoes DESC) = 3 THEN 'Top 3'
    ELSE 'Outros'
  END AS classificacao_cliente,
  current_timestamp() AS calculated_at
FROM metrics_30 m
ORDER BY total_transacoes DESC
;