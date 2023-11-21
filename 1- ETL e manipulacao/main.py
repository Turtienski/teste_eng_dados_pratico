import pandas as pd
import yaml
import logging
from modules.data_quality import DataQuality
from modules.db_connection import DBConnection
from modules.send_alert import SendAlert

# Main class to extract, load and transform sales.csv file

class Main:

	def __init__(self):
		try:
			with open("conf\\sales_etl.yaml") as f:
				self.config = yaml.load(f, Loader=yaml.FullLoader)
				logging.basicConfig(level=logging.INFO, filename = self.config['path_log'], format="%(asctime)s - %(levelname)s - %(message)s")
				logging.info(f"Reading csv")
				self.data = pd.read_csv(self.config['path_data'])
		except Exception as e:
			print("File not found")
			logging.error(e)

	def run(self):
		try:
			# applying data quality rules
			logging.info(f"Applying data quality rules")
			dq = DataQuality(self.data)
			error = dq.check_data_rules()
			duplicated_rows = dq.remove_duplicates()
			self.data = dq.handle_missing_values()
			self.data = dq.data_transformation()
			if not error.empty:
				alert = SendAlert(error, self.config['recipients'], self.config['subject'], self.config['efrom'])
				alert.send_error_message()
				logging.error(error.values)
				exit()
		except Exception as e:
			print("DataQuality failed")
			logging.error(e)

		try:
			# connecting to mysql and loading data to stage
			logging.info(f"Connecting to mysql and loading data to stage")
			db = DBConnection()
			db.load_stage(self.data)
		except Exception as e:
			print("Failed to load data")
			logging.error(e)

		try:
			# data manipulation
			logging.info(f"Data manipulation")
			db.execute_script('scripts\\1-dimension_tables.sql')
			db.execute_script('scripts\\2-fact_tables.sql')
			db.execute_script('scripts\\3-truncate_stage.sql')
			nrows = len(self.data.index)
			id_count = db.get_query_result('scripts\\4-validate_entries.sql')
			del db
			if nrows != id_count:
				raise Exception("Not all records were loaded")
			logging.info(f"ETL executed successfully")
		except Exception as e:
			print("Failed to transform data")
			logging.error(e)

if __name__ == '__main__':
	main = Main()
	main.run()