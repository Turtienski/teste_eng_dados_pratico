import pandas as pd
import sqlite3

# Carregando os dados do arquivo sales_data.csv em um DataFrame
df = pd.read_csv('sales_data.csv', sep='\t')

# 1. Limpeza dos dados

# Removendo linhas duplicadas com base em todas as colunas
df = df.drop_duplicates()

# Verificando e tratando valores ausentes (caso haja)
# Por exemplo, podemos preencher os valores ausentes com 0 ou com a média dos valores
# Neste caso, como todos os valores são numéricos, podemos preencher com 0.
df = df.fillna(0)

# 2. Transformação do valor de venda para USD usando a taxa de conversão

# Definindo a taxa de conversão de 1 FICT = 0.75 USD
taxa_conversao = 0.75

# Aplicando a conversão para USD
df['sale_value_usd'] = df['sale_value'] * taxa_conversao

# 3. Validando os dados tratados

# Verificando se os valores de venda não são negativos
if (df['sale_value'] < 0).any():
    raise ValueError("Há valores de venda negativos no DataFrame.")

# Verificando se todos os IDs de usuários (seller_id) são únicos
if df['seller_id'].duplicated().any():
    raise ValueError("Existem IDs de usuários (seller_id) duplicados no DataFrame.")

# Convertendo a coluna 'date' para o tipo datetime para verificar se os timestamps são válidos
df['date'] = pd.to_datetime(df['date'], errors='coerce')

# Verificando se há registros com timestamps inválidos (NaT)
if df['date'].isnull().any():
    raise ValueError("Existem timestamps inválidos (NaN) no DataFrame.")

# 4. Carregando os dados limpos e transformados em um banco de dados relacional

# Conectando ao banco de dados (caso não exista, será criado um novo)
con = sqlite3.connect('sales_data.db')

# Gravando o DataFrame no banco de dados (a tabela será chamada 'sales_data')
df.to_sql('sales_data', con, if_exists='replace', index=False)

# Fechando a conexão com o banco de dados
con.close()
