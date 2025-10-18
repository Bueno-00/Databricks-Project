# 🚀 Databricks Project: Pipeline Financeiro e Crypto Lakehouse

Este repositório documenta a implementação de um pipeline de dados completo na plataforma **Databricks**, utilizando a arquitetura **Lakehouse** e o framework **Lakeflow Declarative Pipelines** (substituto do Delta Live Tables). O foco é a ingestão e modelagem de dados de múltiplas fontes, incluindo informações de clientes, cotações de mercado (financeiro e Bitcoin) e transações, visando a criação de um modelo de dados analítico robusto e a identificação de métricas de alto valor.

## 🎯 Objetivo

O projeto foi construído para demonstrar a proficiência na **Engenharia de Dados em um ambiente Lakehouse**, focando em:

1.  **Ingestão de Dados:** Coletar dados de diferentes APIs e/ou fontes de entrada (clientes, cotações, transações).
2.  **Modelagem Dimensional:** Aplicar transformações para criar um modelo de dados (`dim` e `fact`) otimizado para consultas analíticas e *Business Intelligence* (BI).
3.  **Qualidade de Dados:** Utilizar as *Expectations* do **Lakeflow Declarative Pipelines** para garantir a validade e a confiabilidade dos dados em cada estágio.
4.  **Automação e Orquestração:** Implementar o **Lakeflow Declarative Pipelines** para a orquestração declarativa do pipeline, simplificando a manutenção e a linhagem dos dados.

## 🧱 Arquitetura de Referência (Medallion Architecture)

O pipeline segue o padrão de camadas conhecido como **Medallion Architecture** (Bronze, Silver, Gold), gerenciado dentro do ambiente Databricks, aproveitando o **Delta Lake** para confiabilidade e performance.

$$\text{Sistemas de Origem} \rightarrow \text{Bronze (Raw)} \rightarrow \text{Silver (Refined)} \rightarrow \text{Gold (Curated)}$$

### Camadas do Projeto

| Camada | Descrição | Principais Tabelas |
| :--- | :--- | :--- |
| **Bronze** | Dados brutos e não processados. Ingestão direta das fontes (APIs/arquivos). | `customers`, `quotation_btc`, `quotation_finance`, `transaction_btc`, `transaction_commodities` |
| **Silver** | Dados limpos, padronizados e enriquecidos. Criação de dimensões e fatos intermediários. | `dim_clientes`, `fact_quotation_assets`, `fact_transaction_assets`, `fact_transaction_revenue` |
| **Gold** | Dados prontos para consumo analítico. Fatos agregados e **KPIs de negócio de alto valor**. | `mostvaluableclient` |

### Detalhe da Tabela Gold: `mostvaluableclient`

Esta tabela na camada Gold é o resultado final da agregação e análise, destinada a identificar os clientes mais valiosos, fornecendo uma visão 360º para o consumo de BI.

| Coluna | Descrição |
| :--- | :--- |
| `customer_sk` | Chave substituta do cliente. |
| `total_transacoes` | Contagem total de transações realizadas. |
| `valor_total` | Valor financeiro total transacionado. |
| `ticket_medio` | Valor médio das transações do cliente. |
| `primeira_transacao` | Data da primeira transação registrada. |
| `ultima_transacao` | Data da última transação registrada. |
| `transacoes_ultimos_30_dias` | Contagem de transações nos últimos 30 dias. |
| `comissao_total` | Total de comissão gerada pelo cliente. |
| `ranking_por_transacoes` | Classificação do cliente com base no volume de transações. |
| `classificacao_cliente` | Classificação de valor (ex: Bronze, Silver, Gold, Platinum). |
| `calculated_at` | Timestamp de quando o registro foi processado/calculado. |

## ⚙️ Tecnologias Utilizadas

| Tecnologia | Função no Projeto |
| :--- | :--- |
| **Databricks** | Plataforma principal para desenvolvimento e execução do pipeline. |
| **Lakeflow Declarative Pipelines** | Framework declarativo para a construção e orquestração do pipeline de ETL. |
| **Apache Spark** | Motor de processamento distribuído subjacente, garantindo escalabilidade. |
| **Delta Lake** | Formato de armazenamento que garante transações ACID e qualidade na Lakehouse. |
| **Python / SQL** | Linguagens utilizadas na definição das transformações e do pipeline. |
| **Git e GitHub** | Versionamento e controle do código-fonte. |

## 📊 Grafo do Pipeline Lakeflow

O **Lakeflow Declarative Pipelines** automatiza o fluxo de trabalho e dependências, gerando o gráfico de linhagem abaixo. Ele visualiza como as fontes de dados (`bronze`) se transformam, unem e progridem até as tabelas de consumo (`gold`), com a aplicação de *Expectations* de qualidade em cada etapa.

A imagem se encontra no arquivo "pipeline_graph.jpg"

### Fluxo de Dependência Principal:

1.  **Ingestão Bronze:** Múltiplas tabelas de origem (clientes, cotações e transações) alimentam o pipeline.
2.  **Modelagem Silver (Dimensões/Fatos Intermediários):**
    * `customers` $\rightarrow$ `dim_clientes`
    * `quotation_btc` e `quotation_finance` $\rightarrow$ `fact_quotation_assets`
    * `transaction_btc` e `transaction_commodities` $\rightarrow$ `fact_transaction_assets`
    * Fatos e Dimensões Silver $\rightarrow$ `fact_transaction_revenue`
3.  **Camada Gold (Consumo Analítico/KPIs):**
    * Tabelas Silver $\rightarrow$ `mostvaluableclient`

## 🏁 Resultados Esperados

Ao final deste projeto, você demonstra a capacidade de:

* Configurar um ambiente Databricks completo.
* Ingerir dados de APIs com scripts Python.
* Calcular KPIs e modelar camadas **Silver** e **Gold** de acordo com requisitos de negócio.
* Aplicar boas práticas de governança e versionamento usando GitHub.
* Implementar pipelines declarativos utilizando **Lakeflow Declarative Pipelines** para automação e qualidade de dados.

## 🔑 Contato

| Email | GitHub |
| :--- | :--- |
| **rafabuenode@gmail.com** | [Bueno-00](https://github.com/Bueno-00) |
