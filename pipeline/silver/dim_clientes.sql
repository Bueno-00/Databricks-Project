-- Silver: dim_clientes
-- Anonimização e validação de clientes

CREATE OR REFRESH STREAMING LIVE TABLE silver_dim_clientes
COMMENT 'Silver: dimension of clients with PII anonymized'
TBLPROPERTIES ('quality' = 'silver', 'layer' = 'silver')
AS
SELECT
  customer_id,
  customer_name,
  documento,
  SHA2(documento, 256) AS documento_hash,
  segmento,
  pais,
  estado,
  cidade,
  CAST(created_at AS TIMESTAMP) AS created_at,
  current_timestamp() AS ingested_at
FROM STREAM(bronze_customers)

-- Constraints
CONSTRAINT customer_id_not_null EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW;
CONSTRAINT segmento_valid EXPECT (segmento IN ('Financeiro', 'Indústria', 'Varejo', 'Tecnologia')) ON VIOLATION DROP ROW;
CONSTRAINT pais_valid EXPECT (pais IN ('Brasil', 'Alemanha', 'Estados Unidos')) ON VIOLATION DROP ROW;
