import pyspark.sql 
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, avg
from pyspark.sql.window import Window

# Criando uma sessão Spark
spark = SparkSession.builder \
    .appName("Website Logs Analysis") \
    .getOrCreate()

# Carregando os dados do arquivo website_logs.csv em um DataFrame Spark
df = spark.read.csv('website_logs.csv', header=True, inferSchema=True)

# 1. Identificando as 10 páginas mais visitadas

# Agrupando os dados pela coluna 'page_url' e contando a ocorrência de cada página
page_counts = df.groupBy('page_url').count()

# Ordenando os resultados em ordem decrescente pelo número de visitas
top_10_pages = page_counts.orderBy(desc('count')).limit(10)

# Mostrando o resultado
print("Top 10 páginas mais visitadas:")
top_10_pages.show()

# 2. Calculando a média de duração das sessões dos usuários

# Criando uma janela (particionada por 'user_id' e ordenada por 'date') para calcular a média de duração
window_spec = Window.partitionBy('user_id').orderBy('date')

# Adicionando uma coluna 'session_duration_prev' com a duração da sessão anterior de cada usuário
df_with_prev_duration = df.withColumn('session_duration_prev', (df['date'].cast('long') - \
                                                               df['date'].lag().over(window_spec)).cast('integer'))

# Calculando a média de duração das sessões de cada usuário
avg_session_duration = df_with_prev_duration.groupBy('user_id').agg(avg('session_duration_prev').alias('avg_duration'))

# Mostrando o resultado
print("\nMédia de duração das sessões dos usuários:")
avg_session_duration.show()

# Encerrando a sessão Spark
spark.stop()
