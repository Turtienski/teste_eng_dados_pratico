import yaml
import logging
import requests
import pymongo


# Main class to call OpenWheatherMap API and save the results into MongoDB
# Why mongo-db?
# - schema less (more info could come from api)
# - large volumes of rapidly changing data

class Main:

	def __init__(self):
		try:
			with open("conf\\openwheathermap.yaml") as f:
				self.config = yaml.load(f, Loader=yaml.FullLoader)
				logging.basicConfig(level=logging.INFO, filename = self.config['path_log'], format="%(asctime)s - %(levelname)s - %(message)s")
				self.url = 'https://api.openweathermap.org/data/2.5/weather?lat='+self.config['latitude']+'&lon='+self.config['longitude']+'&appid='+self.config['api_key']
			try:
				# creating mongodb instance and a collection with city name
				logging.info("Creating mongodb connection")
				self.mongo = pymongo.MongoClient(self.config['mongo_url'])
				self.db = self.mongo[self.config["db_name"]]
				self.collection = self.db[self.config["city"]]
			except Exception as e:
				print("Error while creating mongodb connection")
				logging.error(e)
		except Exception as e:
			print("Error while reading config file")
			logging.error(e)

	def run(self):
		try:
			logging.info("Making API call")
			response = requests.get(self.url)
			# status_code = 200 means a sucessfully api call
			if response.status_code != 200:
				raise Exception("Unable to make API call - status_code: "+response.status_code)
			elif response.json() == '':
				raise Exception('Empty api call response')
			else:
				logging.info("Save results to mongodb")
				try:
					# just adding records to maintain history for future analysis
					result = self.collection.insert_one(response.json())
					print(result)
				except Exception as e:
					print("Erro while inserting api call response into mongodb")
					logging.error(e)
		except Exception as e:
			print("API call failed")
			logging.error(e)


if __name__ == '__main__':
	main = Main()
	main.run()