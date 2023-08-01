# Imports
import requests
import pandas as pd
from datetime import datetime
import json

def lambda_handler(api_key, cidade):
    link = f'https://api.openweathermap.org/data/2.5/weather?q={cidade}&appid={api_key}'
    
    # Requisição
    requisicao = requests.get(link)
    
    # Retornando json de tempo
    return {
        'statusCode': requests.get(link).json()['cod'],
        'body': json.dumps(requisicao.json())
    }

# Credenciais
api_key = '1a0925695880013271c26100e80f7e3d'
cidade = 'santo andre'

# Informações de clima e geografia
weather = json.loads(lambda_handler(api_key, cidade)['body'])

infos = ['coord',
        'weather',
        'main',
        'wind',
        'clouds',
        'sys'
        ]
# Conexão com o BD
conexao = sqlite3.connect('itau.db')

general_infos = pd.DataFrame([[weather['base'],
            weather['visibility'],
            weather['dt'],
            weather['timezone'],
            weather['id'],
            weather['name'],
            weather['cod']]], columns=['base', 'visibility', 'dt', 'timezone', 'id', 'name', 'cod'])

# Salvando no BD tabelas com informações geográficas e climáticas
for i in infos:
    print(i)
    try:
        df = pd.DataFrame.from_dict(weather[i]) 
    except:
        df = pd.DataFrame.from_dict(weather[i], orient='index', columns=[i])
    df.to_sql(i + '_data_SA_api', conexao, if_exists='replace')
    print('ok')

general_infos.to_sql('general_infos_data_SA_api', conexao, if_exists='replace')

# Fechando conexão com o banco de dados
conexao.close()