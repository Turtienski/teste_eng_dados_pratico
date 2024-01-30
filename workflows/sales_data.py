from utils.conn_utils import *

source_path = "./source_files"
workflow_name = "sales_data.csv"

spark = pyspark_session()

df = pyspark_read_csv(spark, file_path=f"{source_path}/{workflow_name}")

clean_df = df.drop_duplicates().dropna()
converted_df = clean_df.withColumn("sale_value_USD", (clean_df['sale_value'] * 0.75))

conn = PostgreSQLHandler()
conn.insert_spark_df_data(clean_df, "sales_data")


