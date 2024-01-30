from pyspark.sql import SparkSession

def pyspark_session(app_name='App',
                       jars_path="./workflows/utils/jars/postgresql-42.7.1.jar"):
    try:
        return (SparkSession.builder
                .appName(app_name)
                .config("spark.jars", jars_path)
                .getOrCreate())
    except Exception as e:
        return e

def pyspark_postgres_connection(app_name='App',
                       jars_path="./workflows/utils/jars/postgresql-42.7.1.jar"):
    try:
        return (SparkSession.builder
                .appName(app_name)
                .config("spark.jars", jars_path)
                .getOrCreate())
    except Exception as e:
        return e

def pyspark_read_csv(spark, file_path, header='true',
                     inferSchema='true', delimiter=',', nullValue='NA'):
    try:
        return (spark.read
                .option("header", header)
                .option("inferSchema", inferSchema)
                .option("delimiter", delimiter)
                .option("nullValue", nullValue)
                .csv(file_path)
               )
    except Exception as e:
        if "flag can be true or false" in str(e):
            return "Error in parameters"
        else:
            return e

class PostgreSQLHandler:
    def __init__(self):
        self.db_url = "jdbc:postgresql://localhost:5432/mydatabase"
        self.db_properties = {
            "user": "myuser",
            "password": "mypassword",
            "driver": "org.postgresql.Driver"
        }

    def insert_spark_df_data(self, df_pyspark, table_name, write_mode="overwrite"):
        # Write PySpark DataFrame to PostgreSQL
        try:
            (df_pyspark.write
             .mode(write_mode)
             .jdbc(url=self.db_url, table=table_name, properties=self.db_properties))
        except Exception as e:
            return e

