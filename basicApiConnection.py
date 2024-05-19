import requests

# Section 2o
api_key = 'fb598271fef4c3b6cb0030c61751e2d7' 
lat = '48.85341' # Use the latitude of your desired location
lon = '2.3488' # Use the longitude of your desired location
url = "http://api.openweathermap.org/data/2.5/forecast" # This the only change compared to the previous API call.url = 'https://api.openweathermap.org/data/2.5/weather'
# url = 'https://api.openweathermap.org/data/2.5/weather'

complete_url = f"{url}?lat={lat}&lon={lon}&appid={api_key}&units=metric"
response = requests.get(complete_url)
    
# Read response:
if response.status_code == 200:
    response_json = response.json()
    # print(response_json)
else:
    print(f"Error: Unable to fetch data. Status code {response.status_code}")
    raise requests.exceptions.HTTPError(response.text)
    
