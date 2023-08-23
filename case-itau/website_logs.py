from pyspark.shell import spark
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Caminho para o arquivo CSV
file = '/Users/thaissales/Documents/case-test/teste_eng_dados_pratico/website_logs.csv'

## Ler o arquivo CSV e criar um DataFrame
dados = spark.read.csv(file, header=True, inferSchema=True)

# Calcular a contagem de visitas por página
contagem_paginas = dados.groupBy("page_url").count()

# Ordenar as páginas pela contagem de visitas em ordem decrescente
paginas_ordenadas = contagem_paginas.orderBy(col("count").desc())

# Criar uma janela para calcular o ranking das páginas mais visitadas
janela = Window.orderBy(col("count").desc())

# Adicionar uma coluna de ranking às páginas ordenadas
paginas_ordenadas_com_ranking = paginas_ordenadas.withColumn("ranking", rank().over(janela))

# Selecionar as 10 primeiras páginas mais visitadas
top_10_paginas = paginas_ordenadas_com_ranking.filter(col("ranking") <= 10)

# Exibir as 10 páginas mais visitadas
top_10_paginas.show()

###########################################################################

# Calcular a média de duração das sessões dos usuários
media_duracao = dados.select(avg(col("session_duration")))

# Exibir a média de duração das sessões dos usuários
media_duracao.show()

###########################################################################

# Converter a coluna de data para um carimbo de data/hora UNIX
dados = dados.withColumn("unix_timestamp", unix_timestamp(col("date")))

# Criar uma janela para calcular a contagem de visitas por usuário e por semana
janela = Window.partitionBy("user_id").orderBy("unix_timestamp")

# Calcular a contagem de visitas por usuário e por semana
dados_com_contagem = dados.withColumn("visitas_na_semana", count("user_id").over(janela))

# Filtrar os usuários que retornam mais de uma vez por semana
usuarios_retornantes = dados_com_contagem.filter(col("visitas_na_semana") > 1)

# Contar a quantidade de usuários retornantes
quantidade_usuarios_retornantes = usuarios_retornantes.select("user_id").distinct().count()

# Exibir a quantidade de usuários retornantes
print("Quantidade de usuários retornantes:", quantidade_usuarios_retornantes)

# Selecionar os usuários retornantes e exibir
usuarios_retornantes.select("user_id").distinct().show()



if __name__ == '__main__':
    print('Fim!!!')