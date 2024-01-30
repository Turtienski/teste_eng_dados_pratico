from pyspark.sql.functions import desc, avg, col, weekofyear

from utils.conn_utils import *

source_path = "./source_files"
workflow_name = "website_logs.csv"

spark = pyspark_session()

df = pyspark_read_csv(spark, file_path=f"{source_path}/{workflow_name}")
clean_df = df.drop_duplicates().dropna().withColumn("date", df["date"].cast("date"))

## Analysing Top Pages
top_pages = (clean_df
             .select("page_url")
             .groupby("page_url").count()
             .orderBy(desc("count"))
             .limit(10)
             )

print("##-----------------------------------##")
print("TOP PAGES:")
top_pages.show()
print("##-----------------------------------##\n")


## Analysing Session Average Duration
avg_duration = (clean_df
                .agg(avg("session_duration").alias("avg_duration"))
                .select("avg_duration"))

print("##-----------------------------------##")
print("Session Average Duration:")
avg_duration.show()
print("##-----------------------------------##\n")

## Analysing Total of Returning Users on the same week
clean_dates_df = (clean_df.withColumn("week_of_year", weekofyear(clean_df["date"])))
user_access_count = (clean_dates_df
                     .select("user_id", "week_of_year")
                     .groupBy("user_id", "week_of_year").count()
                     .orderBy(col("count").desc())
                     .filter("count > 1")
                     ).selectExpr("count('user_id') as total_returning_users")

print("##-----------------------------------##")
print("Total of Returning Users Per Week:")
user_access_count.show()
print("##-----------------------------------##\n")

### Recording findings into Postgres
conn = PostgreSQLHandler()
conn.insert_spark_df_data(clean_df, "website_logs")

