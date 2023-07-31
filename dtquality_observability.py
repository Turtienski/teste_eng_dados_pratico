import pandas as pd
import sqlite3
from datetime import datetime
from grafana_api.grafana_face import GrafanaFace

# Função para criar o banco de dados SQLite e inserir os dados
def create_and_populate_database(df):
    # Criando a conexão com o banco de dados SQLite
    conn = sqlite3.connect('sales_data.db')

    # Criando a tabela no banco de dados
    df.to_sql('sales_data', conn, if_exists='replace', index=False)

    # Fechando a conexão com o banco de dados SQLite
    conn.close()

# Função para realizar as verificações de qualidade de dados
def perform_data_quality_checks(df):
    # Verificando se todos os IDs de usuários são únicos
    if df['user_id'].nunique() == df.shape[0]:
        print("Todos os IDs de usuários são únicos.")
    else:
        print("Existem IDs de usuários duplicados.")

    # Verificando se os valores de vendas não são negativos
    if (df['sale_value'] >= 0).all():
        print("Todos os valores de vendas são não negativos.")
    else:
        print("Existem valores de vendas negativos.")

    # Verificando se todas as entradas têm timestamps válidos
    if pd.to_datetime(df['date'], errors='coerce').notnull().all():
        print("Todos os timestamps são válidos.")
    else:
        print("Existem timestamps inválidos.")

# Função para criar as métricas de observabilidade no Grafana
def create_grafana_dashboard():
    # Configurando a conexão com o Grafana
    grafana_api = GrafanaFace(auth=('admin', 'admin'), host='http://localhost', port=3000, protocol='http')

    # Criando o painel no Grafana
    dashboard = {
        "id": None,
        "title": "ETL Observability Dashboard",
        "panels": [
            {
                "title": "Tempo de Processamento ETL",
                "type": "graph",
                "span": 6,
                "targets": [
                    {
                        "query": "SELECT time, processing_time FROM etl_metrics WHERE $timeFilter",
                        "alias": "Tempo de Processamento",
                        "format": "ms",
                    }
                ],
            },
            {
                "title": "Quantidade de Linhas Ingeridas",
                "type": "singlestat",
                "span": 6,
                "targets": [
                    {
                        "query": "SELECT value FROM etl_metrics WHERE metric='rows_ingested' ORDER BY time DESC LIMIT 1",
                        "format": "none",
                        "refId": "A",
                    }
                ],
            },
        ],
    }

    # Criando o dashboard no Grafana
    grafana_api.dashboard.update_dashboard(dashboard)

# Carregando os dados do arquivo CSV
df = pd.read_csv('sales_data.csv')

# Limpando os dados
df.drop_duplicates(inplace=True)
df.fillna(value=0, inplace=True)
df['sale_value_usd'] = df['sale_value'] * 0.75
df['date'] = pd.to_datetime(df['date'])

# Realizando as verificações de qualidade de dados
perform_data_quality_checks(df)

# Criando e populando o banco de dados SQLite
create_and_populate_database(df)

# Criando as métricas de observabilidade no Grafana
create_grafana_dashboard()
