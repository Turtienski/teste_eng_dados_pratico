from utils.conn_utils import pyspark_session, PostgreSQLHandler
from utils.api_utils import *
import json

base_url = "https://api.openweathermap.org/data/3.0/onecall"
api_key = "c30b9c0cf3150a9d29d2c2ed2e7da90b"

days = 7
lat = "-23.2028"
long = "-47.2863"
api_args = f"?lat={lat}&lon={long}&cnt={days}&appid={api_key}"

# print(f"{base_url}{api_args}")
# results = get_url(f"{base_url}{api_args}")
# print(results)

spark = pyspark_session()
df = spark.read.option("multiLine", "true").json("./source_files/api_sample.json")

conn = PostgreSQLHandler()
conn.insert_spark_df_data(df, "weather_salto_sp")
