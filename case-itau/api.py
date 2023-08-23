import sys
from datetime import datetime
import requests
import time
import mysql.connector



# Chave de API da OpenWeatherMap
api_key = 'e9f3c15f27ca3c97cf3e3b993ba71a2b'

# URL da API de previsão do tempo
url = "http://api.openweathermap.org/data/2.5/weather"

cidade = "Uberlandia"
pais = "br"

def coletar_dados():
    try:
        # Faz a requisição à API
        result = requests.get(url, params={"q": "{},{}".format(cidade, pais),
                                           "appid": api_key,
                                           "units": "metric",
                                           "lang": "pt_br"})

        # Verifica se a requisição foi bem-sucedida e retorna os dados
        if result.status_code < 300:
            return result.json()
        else:
            print("Erro: {}".format(result.status_code), file=sys.stderr)
            return None

    except requests.exceptions.RequestException as e:
        print(f'Erro na requisição: {e}')
        return None

try:
    con = mysql.connector.connect(host='localhost', database='dbTeste', user='root', password='')
    cursor = con.cursor()

    # Cria a tabela se ela não existir
    create_table_query = """
    CREATE TABLE IF NOT EXISTS previsao_tempo_api (
        data_ingestao DATETIME,
        cidade VARCHAR(255),
        temperatura_atual DECIMAL(5, 2),
        temperatura_minima DECIMAL(5, 2),
        temperatura_maxima DECIMAL(5, 2),
        humidade INT
    )
    """
    cursor.execute(create_table_query)
    con.commit()

    # Intervalo de coleta em segundos
    intervalo = 2

    while True:
        dados_previsao = coletar_dados()
        if dados_previsao:
            # Captura a data e hora atual
            data_ingestao = datetime.now()

            # Insere os dados coletados na tabela
            insert_query = """
            INSERT INTO previsao_tempo_api (data_ingestao, cidade, temperatura_atual, temperatura_minima, temperatura_maxima, humidade)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            values = (
                data_ingestao,
                dados_previsao["name"],
                dados_previsao["main"]["temp"],
                dados_previsao["main"]["temp_min"],
                dados_previsao["main"]["temp_max"],
                dados_previsao["main"]["humidity"]
            )
            cursor.execute(insert_query, values)
            con.commit()

            print("Dados inseridos no MySQL com sucesso!")

        time.sleep(intervalo)

except mysql.connector.Error as e:
    print("Erro ao conectar, criar tabela ou inserir dados:", e)

finally:
    # Encerra a conexão
    if con.is_connected():
        cursor.close()
        con.close()
        print("Conexão MySQL encerrada")


if __name__ == '__main__':
    print('Fim!!!')
