
def get_url(api_url):
    import requests
    # Make a GET request to the API
    response = requests.get(api_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        print(response.json())
    else:
        # Print an error message if the request was not successful
        print(f"Error: {response.status_code} - {response.text}")