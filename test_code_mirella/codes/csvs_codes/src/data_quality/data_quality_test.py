from pyspark.sql.functions import col, to_date

from infrastructure.sessions.spark_session import get_spark_session


class DataQualityTest:

    def __init__(self):
        self.__spark = get_spark_session()
        self.__sales_csv = self.__spark.read.csv(path='../../../../sales_data.csv', sep=',', header=True)

    def validate_unique_ids(self):
        distinct_count = self.__sales_csv.select(col('seller_id')).distinct().count()
        total_users = self.__sales_csv.select(col('seller_id')).count()
        if distinct_count == total_users:
            print("TODOS USUÁRIOS SÃO ÚNICOS!")
        else:
            print("EXISTEM USUÁRIOS DUPLICADOS!")

    def validate_not_null_sale_value(self):
        filtered_df = self.__sales_csv.filter(col('sale_value').isNull()).count()
        if filtered_df and filtered_df > 0:
            print("EXISTEM VALORES DE VENDA NULOS!")
        else:
            print("NÃO EXISTEM VALORES DE VENDA NULOS!")

    def validate_sale_date(self):
        date_format = 'yyyy-MM-dd'
        new_valid_column = self.__sales_csv.withColumn('valid_date', to_date('date', date_format).isNotNull())
        count_false_rows = new_valid_column.filter(col('valid_date') == 'false').count()
        if count_false_rows and count_false_rows > 0:
            print("EXISTEM DATAS DE VENDA INVÁLIDAS")
        else:
            print("TODAS AS DATAS DE VENDAS SÃO VÁLIDAS")