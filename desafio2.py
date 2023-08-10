import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Função para carregar os dados do S3 como um DynamicFrame
def load_data_from_s3(glueContext):
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database = "default",
        table_name = "website_logs_csv",
        transformation_ctx = "data_frame"
    )
    return dynamic_frame.toDF()

# Função principal
def main():
    df = load_data_from_s3(glueContext)

    # Registrar o DataFrame como uma tabela temporária para consultas SparkSQL
    df.createOrReplaceTempView("user_sessions")

    # Identificar as 10 páginas mais visitadas
    top_pages_query = """
        SELECT page_url, COUNT(user_id) AS qtd_visitas
        FROM user_sessions
        GROUP BY page_url
        ORDER BY qtd_visitas DESC
        LIMIT 10
    """
    top_pages = spark.sql(top_pages_query)

    # Calcular a média de duração das sessões dos usuários
    average_duration_query = """
        SELECT user_id, AVG(session_duration) AS avg_duration
        FROM user_sessions
        GROUP BY user_id
    """

    """
    ou a média geral:
    SELECT AVG(session_duration) AS avg_duration
    FROM user_sessions
    """
    average_duration = spark.sql(average_duration_query)

    # Determinar quantos usuários retornam mais de uma vez por semana
    # repeat_users_query = """
    #     SELECT DATE_TRUNC('week', date) AS week, COUNT(DISTINCT user_id) AS repeat_user_count
    #     FROM user_sessions
    #     WHERE user_id IN (
    #         SELECT user_id
    #         FROM user_sessions
    #     )
    #     GROUP BY week
    # """
    # repeat_users = spark.sql(repeat_users_query)

    print("Top 10 páginas mais visitadas:")
    top_pages.show()

    print("Média de duração das sessões dos usuários:")
    average_duration.show()

    # print("Usuários que retornam mais de uma vez por semana:")
    # repeat_users.show()

if __name__ == "__main__":
    main()

job.commit()