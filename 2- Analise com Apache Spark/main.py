import pandas as pd
import yaml
import logging
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as sql


# Main class to extract, load and transform sales.csv file

class Main:

	def __init__(self):
		try:
			with open("conf\\website_logs.yaml") as f:
				self.config = yaml.load(f, Loader=yaml.FullLoader)
				logging.basicConfig(level=logging.INFO, filename = self.config['path_log'], format="%(asctime)s - %(levelname)s - %(message)s")
				logging.info(f"Creating SparkSession")
				self.spark = SparkSession.builder.master("local").appName(self.config['application']).getOrCreate()
		except Exception as e:
			print("SparkSession could not be created")
			logging.error(e)

		try:
			logging.info(f"Reading data")
			self.data = self.spark.read.csv(self.config['path_data'], header = True, inferSchema = True)
			self.data.printSchema()
		except Exception as e:
			print("Data could not be read")
			logging.error(e)

	def run(self):
		try:
			# most visited websites
			# SELECT page_url, count(*) FROM website_logs
			# GROUP BY page_url
			# order by count(*) DESC
			# LIMIT 10
			print("Most visited websites")
			result = self.data.groupBy("page_url").count().sort('count', ascending = False).limit(10)
			result.show()

			# average length of users sessions
			# SELECT mean(session_duration) FROM website_logs

			print("Average length of users sessions")
			result = self.data.select(sql.mean('session_duration'))
			result.show()

			# how many users return to the site more than once a week
			# SELECT * FROM website_logs as a
			# JOIN website_logs as b
			# ON a.user_id = b.user_id
			# AND a.date < b.date
			# AND datediff(b.date, a.date) <= 7

			print("How many users return to the site more than once a week")
			df1 = self.data.withColumn('final_date', sql.date_add(self.data['date'], 7))
			df2 = df1
			df1_a = df1.alias("df1_a")
			df2_a = df2.alias("df2_a")

			result = df1_a.join(df2_a, (sql.col('df1_a.user_id') == sql.col('df2_a.user_id'))
					& (sql.col('df1_a.page_url') == sql.col('df2_a.page_url'))
					& (sql.col('df1_a.date') < sql.col('df2_a.date'))
					& (sql.col('df2_a.date') <= sql.col('df1_a.final_date'))
					).select('df1_a.user_id', 'df1_a.page_url').groupBy('df1_a.page_url').agg(sql.countDistinct("user_id"))

			result.show()

		except Exception as e:
			print("DataQuality failed")
			logging.error(e)


if __name__ == '__main__':
	main = Main()
	main.run()