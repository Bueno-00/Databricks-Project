Pipeline Bronze - Lakeflow (Databricks Live Tables)

Arquivos nesta pasta:
- `customers_bronze.sql` - Cria a tabela Bronze `bronze_customers` a partir de `/Volumes/lakehouse/raw_public/customers.csv`
- `quotation_btc_bronze.sql` - Cria a tabela Bronze `bronze_quotation_btc` a partir de `/Volumes/lakehouse/raw_public/quotation_btc.csv`
- `quotation_yfinance_bronze.sql` - Cria a tabela Bronze `bronze_quotation_yfinance` a partir de `/Volumes/lakehouse/raw_public/quotation_yfinance.csv`
- `transaction_btc_bronze.sql` - Cria a tabela Bronze `bronze_transaction_btc` a partir de `/Volumes/lakehouse/raw_public/transacation_btc.csv`
- `transaction_commodities_bronze.sql` - Cria a tabela Bronze `bronze_transaction_commodities` a partir de `/Volumes/lakehouse/raw_public/transaction_commodities.csv`

Instruções - Deploy no Databricks (Lakeflow / Live Tables):

1. Crie um novo Pipeline no Databricks usando Live Tables (Lakeflow).
2. Configure o storage root/output para um caminho Delta no seu workspace (ex: dbfs:/user/hive/warehouse/lakehouse/).
3. Adicione os arquivos SQL desta pasta ao pipeline como notebooks ou arquivos SQL (cada arquivo define uma tabela streaming `CREATE OR REFRESH STREAMING LIVE TABLE`).
4. Garanta permissões e Unity Catalog se aplicável.
5. Para ingestão incremental, Databricks recomenda usar `cloud_files()` com Auto Loader. Se pretende usar `cloud_files()`, ajuste as fontes para `COPY INTO` ou `FROM STREAM(read_stream(...))` conforme documentação oficial.

Referências:
- Live Tables / Lakeflow docs: https://docs.databricks.com

Observações:
- Estes scripts usam a função hipotética `read_csv()` para leitura direta de CSV em uma tabela Live; no Databricks você deverá substituir pelas APIs recomendadas (ex: `SELECT * FROM cloud_files('/Volumes/lakehouse/raw_public/...', format='csv', header='true')` ou usar `COPY INTO` para arquivos estáticos). Ajustei para clareza; verifique e substitua por `cloud_files()` conforme sua política de ingestão.

Próximos passos:
- Implementar Silver (transformações, validações) e Gold (métricas).
- Incluir constraints de qualidade conforme `requisitos/requisitos.md`.
