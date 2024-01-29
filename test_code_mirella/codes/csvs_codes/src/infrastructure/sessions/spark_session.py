from pyspark.sql import SparkSession


def get_spark_session():
    return SparkSession.builder.master('local[1]').appName('TEST_MIRELLA') \
        .config("spark.jars", "infrastructure/jars/postgresql-9.2.jdbc3-20131022.000327-1.jar") \
        .getOrCreate()
