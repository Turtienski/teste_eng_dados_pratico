import requests


def get_weather(api_key, city):
    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': api_key
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout as error:
        print(f"Timeout Error: {error}")
    except requests.exceptions.HTTPError as error:
        print(f"HTTP Error: {error}")
