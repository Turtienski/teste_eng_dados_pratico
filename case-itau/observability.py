import mysql.connector
from pyspark.shell import spark, sc
from datetime import datetime


# Conectar ao banco de dados
con = mysql.connector.connect(host='localhost', database='dbTeste', user='root', password='')
cursor = con.cursor()

# Consulta SQL para selecionar todos os registros da tabela
select_query = "SELECT * FROM sales_data"

# Executar a consulta
cursor.execute(select_query)

# Recuperar os resultados
resultados = cursor.fetchall()

# Criar um RDD a partir dos resultados
rdd = sc.parallelize(resultados)

# Filtrar entradas com timestamps válidos
dt_invalid = rdd.filter(lambda row: isinstance(row[1], datetime))
if dt_invalid:
    print("Timestamp válidos.")
else:
    print("Timestamps inválidos.")

# Verificar se os valores de vendas não são negativos
values = rdd.filter(lambda row: row[4] >= 0)
if values:
    print("Os valores de vendas não são negativos.")
else:
    print("Há valores de vendas negativos.")

# Verificar se todos os registros são únicos
registros_unicos = rdd.distinct().count() == rdd.count()
if registros_unicos:
    print("Todos os registros são únicos.")
else:
    print("Existem registros duplicados.")

# Carregar o arquivo CSV como RDD
rdd_csv = sc.textFile("/Users/thaissales/Documents/case-test/teste_eng_dados_pratico/sales_data.csv").filter(lambda line: "transaction_id" not in line)

# Contar a quantidade de linhas no RDD do banco de dados
count_db = rdd.count()
print('Total Registro:', count_db)

# Contar a quantidade de linhas no RDD do CSV
count_csv = rdd_csv.count()
print('Total Registro:', count_csv)

# Verificar se a quantidade de linhas é igual
if count_db == count_csv:
    print("A quantidade de linhas ingeridas no banco de dados é igual à quantidade de linhas do CSV.")
else:
    print("A quantidade de linhas ingeridas no banco de dados não é igual à quantidade de linhas do CSV.")

# Encerrar a conexão
if con.is_connected():
    cursor.close()
    con.close()
    print("Conexão MySQL encerrada")


if __name__ == '__main__':
    print('Fim!!!')