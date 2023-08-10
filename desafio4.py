from datetime import datetime
import requests
import logging
import boto3
import sys

class WeatherDataProcessor:
    def __init__(self, api_key_secret_name, region='us-east-1', tablename='weatherLab'):
        self.api_key_secret_name = api_key_secret_name
        self.region = region
        self.tablename = tablename
        self.logger = logging.getLogger(__name__)

    def get_secret(self, secret_name):
        client = boto3.client('secretsmanager', region_name=self.region)
        response = client.get_secret_value(SecretId=secret_name)
        secret = response['SecretString']
        return secret

    def store_weather_data_in_dynamodb(self, api_key, city, temperature, weather_description):
        try:
            dynamodb_table_name = self.tablename
            dynamodb = boto3.client('dynamodb', region_name=self.region)

            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            dynamodb.put_item(
                TableName=dynamodb_table_name,
                Item={
                    'City': {'S': city},
                    'Timestamp': {'S': timestamp},
                    'Temperature': {'N': str(temperature)},
                    'WeatherDescription': {'S': weather_description}
                }
            )
            self.logger.info("Dados armazenados no DynamoDB.")

        except Exception as e:
            self.logger.error(f"Erro ao armazenar dados no DynamoDB: {str(e)}")

    def process_weather_data(self, city):
        try:
            api_key = self.get_secret(self.api_key_secret_name)
            url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'

            response = requests.get(url)
            weather_data = response.json()

            temperature = weather_data['main']['temp']
            weather_description = weather_data['weather'][0]['description']

            self.store_weather_data_in_dynamodb(api_key, city, temperature, weather_description)

        except Exception as e:
            self.logger.error(f"Erro ao obter ou armazenar dados: {str(e)}")

def main():
    # Configurar o logger
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    api_key_secret_name = sys.argv[1] # ou 'weatherSecret'  # Substitua pelo nome do seu segredo no Secrets Manager
    city = sys.argv[2]  # ou 'London' - Substitua pela cidade desejada

    processor = WeatherDataProcessor(api_key_secret_name)
    processor.process_weather_data(city)

if __name__ == "__main__":
    main()
