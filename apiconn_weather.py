import requests
import sqlite3
from datetime import datetime

# Chave de API do OpenWeatherMap (você precisa obter sua própria chave)
api_key = ''

# URL da API do OpenWeatherMap para a cidade do Rio de Janeiro
url = f'http://api.openweathermap.org/data/2.5/weather?q=Rio de Janeiro,BR&appid={api_key}'

# Faz a requisição à API do OpenWeatherMap
response = requests.get(url)
data = response.json()

# Extrai as informações relevantes da API
temperature = data['main']['temp']
weather_description = data['weather'][0]['description']
humidity = data['main']['humidity']
wind_speed = data['wind']['speed']
timestamp = datetime.fromtimestamp(data['dt'])

# Conexão e criação do banco de dados SQLite
conn = sqlite3.connect('weather_data.db')
cursor = conn.cursor()

# Criação da tabela 'weather_data' se ela não existir
cursor.execute('''CREATE TABLE IF NOT EXISTS weather_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT,
                    temperature REAL,
                    description TEXT,
                    humidity INTEGER,
                    wind_speed REAL,
                    timestamp TEXT
                )''')

# Insere os dados obtidos da API no banco de dados
cursor.execute('''INSERT INTO weather_data (city, temperature, description, humidity, wind_speed, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)''', ('Rio de Janeiro', temperature, weather_description, humidity, wind_speed, timestamp))

# Salva as alterações no banco de dados e fecha a conexão
conn.commit()
conn.close()

print('Dados da previsão do tempo foram coletados e armazenados no banco de dados SQLite.')
