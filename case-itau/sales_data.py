import mysql.connector
from pyspark.shell import spark
from pyspark.sql.functions import *


con = mysql.connector.connect(host='localhost', database='dbTeste', user='root', password='')


# Caminho para o arquivo CSV
file = '/Users/thaissales/Documents/case-test/teste_eng_dados_pratico/sales_data.csv'

# Usando Spark para ler o arquivo CSV e criar um DataFrame
dados = spark.read.csv(file, header=True, inferSchema=True)

# Exibindo os primeiros registros do DataFrame
dados.show(50)

# Contar linhas duplicadas com base em todas as colunas

dados.groupBy(dados.columns).count().filter(col("count") > 1).show()

# Remove linhas duplicadas com base em todas as colunas

df_drop = dados.dropDuplicates()
df_drop.show(50)

# Adicionar uma nova coluna com o valor da venda em USD
conversion_rate = 0.75
df_converted = df_drop.withColumn("sale_value_usd", col("sale_value") * conversion_rate)

# Mostrar o DataFrame com a conversão feita
df_converted.show(50)


# Estabeleça a conexão com o banco de dados MySQL
try:
    cursor = con.cursor()

    # Criar a tabela no banco de dados
    create_table_query = """
    CREATE TABLE sales_data (
        transaction_id INT PRIMARY KEY,
        date DATE,
        product_id INT,
        seller_id INT,
        sale_value DECIMAL(10, 2),
        currency VARCHAR(100),
        sale_value_usd DECIMAL(10, 2)
    )
    """
    cursor.execute(create_table_query)

    # Inserir os dados do DataFrame na tabela
    for row in df_converted.collect():
        insert_query = """
        INSERT INTO sales_data (transaction_id, date, product_id, seller_id, sale_value, currency, sale_value_usd)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            row["transaction_id"], row["date"], row["product_id"],
            row["seller_id"], row["sale_value"], row["currency"],
            row["sale_value_usd"]
        )
        cursor.execute(insert_query, values)

    # Efetive as alterações
    con.commit()

    print("Tabela criada e dados inseridos no MySQL com sucesso!")

except mysql.connector.Error as e:
    print("Erro ao conectar, criar tabela ou inserir dados:", e)

finally:
    # Encerre a conexão
    if con.is_connected():
        cursor.close()
        con.close()
        print("Conexão MySQL encerrada")



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('Fim!!!')

