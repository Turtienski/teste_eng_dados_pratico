import unittest
import sqlite3
import requests

# Parte 1: ETL e Manipulação de Dados
def calculate_usd_sale_value(sale_value, currency):
    # Definir taxa de conversão
    exchange_rate = 0.75

    # Converter o valor de venda para USD
    if currency == "FICT":
        return sale_value * exchange_rate
    else:
        return sale_value

# Parte 2: Análise com Apache Spark
def get_most_visited_pages(logs_data, n=10):
    # Obter contagem de visitas por página
    page_counts = logs_data['page_url'].value_counts()

    # Obter as n páginas mais visitadas
    most_visited_pages = page_counts.nlargest(n)

    return most_visited_pages.index.tolist()

# Parte 4: Codificação
def get_weather_data(city):
    api_key = "YOUR_API_KEY"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"

    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Parte 5: Data Quality & Observability
def check_data_quality(data):
    # Verificar se todos os IDs de usuários são únicos
    if len(data['user_id']) != len(set(data['user_id'])):
        return False

    # Verificar se os valores de vendas não são negativos
    if any(data['sale_value'] < 0):
        return False

    # Verificar se todos os timestamps são válidos
    try:
        pd.to_datetime(data['date'], format='%Y-%m-%d', errors='raise')
    except:
        return False

    return True

# Testes unitários
class TestETLFunctions(unittest.TestCase):

    def test_calculate_usd_sale_value(self):
        # Teste de conversão de moeda fictícia para USD
        self.assertAlmostEqual(calculate_usd_sale_value(150, "FICT"), 112.5)
        self.assertAlmostEqual(calculate_usd_sale_value(300, "FICT"), 225)

        # Teste sem conversão de moeda (outra moeda)
        self.assertAlmostEqual(calculate_usd_sale_value(200, "USD"), 200)

    def test_get_most_visited_pages(self):
        # Teste com dados fictícios de logs
        logs_data = {
            'user_id': [1001, 1002, 1003, 1001, 1002, 1003, 1001, 1002, 1003, 1004],
            'page_url': ['homepage.html', 'product_page.html', 'checkout.html'] * 3,
            'session_duration': [15, 120, 45, 10, 95, 20, 150, 25, 85, 50]
        }
        self.assertEqual(get_most_visited_pages(logs_data, n=2), ['homepage.html', 'product_page.html'])

    def test_get_weather_data(self):
        # Teste com cidade válida
        city = "Rio de Janeiro"
        data = get_weather_data(city)
        self.assertIsNotNone(data)
        self.assertIn('main', data)
        self.assertIn('temp', data['main'])
        self.assertIn('humidity', data['main'])

        # Teste com cidade inválida
        invalid_city = "InvalidCityName"
        data = get_weather_data(invalid_city)
        self.assertIsNone(data)

    def test_check_data_quality(self):
        # Teste com dados válidos
        data_valid = {
            'user_id': [1001, 1002, 1003, 1004],
            'sale_value': [150, 300, 160, 210],
            'date': ['2023-07-25', '2023-07-25', '2023-07-25', '2023-07-25']
        }
        self.assertTrue(check_data_quality(data_valid))

        # Teste com dados inválidos (IDs duplicados)
        data_invalid1 = {
            'user_id': [1001, 1002, 1003, 1001],
            'sale_value': [150, 300, 160, 210],
            'date': ['2023-07-25', '2023-07-25', '2023-07-25', '2023-07-25']
        }
        self.assertFalse(check_data_quality(data_invalid1))

        # Teste com dados inválidos (valores de venda negativos)
        data_invalid2 = {
            'user_id': [1001, 1002, 1003, 1004],
            'sale_value': [-150, 300, 160, 210],
            'date': ['2023-07-25', '2023-07-25', '2023-07-25', '2023-07-25']
        }
        self.assertFalse(check_data_quality(data_invalid2))

        # Teste com dados inválidos (timestamp inválido)
        data_invalid3 = {
            'user_id': [1001, 1002, 1003, 1004],
            'sale_value': [150, 300, 160, 210],
            'date': ['2023-07-25', '25-07-2023', '2023-07-25', '2023-07-25']
        }
        self.assertFalse(check_data_quality(data_invalid3))

if __name__ == '__main__':
    unittest.main()
