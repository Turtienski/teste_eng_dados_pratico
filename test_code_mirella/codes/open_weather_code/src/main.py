import json
import time
import logging
import datetime

from config.read_config import read_yml_config
from engine.api_requests import get_weather
from test_code_mirella.codes.open_weather_code.src.infrastructure.write_data.write import save_data_to_s3


def main():
    logging.basicConfig(level=logging.INFO)
    config = read_yml_config(file_path='config/config.yml')
    api_key = config.get('api_key')
    city = config.get('city')

    # Buscando os dados
    start_get_data = time.time()
    weather_data = get_weather(api_key, city)
    end_get_data = time.time()
    get_data_duration = end_get_data - start_get_data
    logging.info(f"ETL_METRICS: Buscar os dados levou {get_data_duration} segundos.")

    date_now = datetime.datetime.now()

    if weather_data:
        # Salvando os dados
        start_save_data = time.time()
        weather_json = json.dumps(weather_data)
        save_data_to_s3(date_now=date_now, city=city, json=weather_json)
        end_save_data = time.time()
        save_data_duration = end_save_data - start_save_data
        logging.info(f"ETL_METRICS: Salvar os dados levou {save_data_duration} segundos.")

    else:
        logging.info("Não foram encontrados dados")
        exit(0)


if __name__ == '__main__':
    main()
