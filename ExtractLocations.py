import requests
import pandas as pd

    

#.. Section 3o..
#η πρωτη συναρτηση που θελει να παιρνει τις πληροφοριες με ΑPI για κάθε τοποθεσία με τη χρηση for και μιας function
def fetch_api_data(url: str) -> dict:
    response = requests.get(url)
    response.raise_for_status()  # Raise exception for non-200 status codes
    return response.json()


def get_weather_data(locations: dict, api_key: str) -> dict:
    base_url = "https://api.openweathermap.org/data/2.5/"
    weather_data = {"current": {}, "forecast": {}}

    for location_name, location in locations.items():
        try:
            # Construct URLs with common base and location data
            current_url = f"{base_url}weather?lat={location['lat']}&lon={location['lon']}&appid={api_key}&units=metric"
            forecast_url = f"{base_url}forecast?lat={location['lat']}&lon={location['lon']}&appid={api_key}&units=metric"

            # Fetch and store data using fetch_api_data function
            weather_data["current"][location_name] = fetch_api_data(current_url)
            weather_data["forecast"][location_name] = fetch_api_data(forecast_url)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for location {location_name}: {e}")

    return weather_data

#API KEY
api_key = 'fb598271fef4c3b6cb0030c61751e2d7' 

# Create a dictionary with the coordinates of 5 locations:
locations_dict = {
    'Thessaloniki, GR': {'lat': '40.6403', 'lon': '22.9439'},
    'Paris, FR': {'lat': '48.85341', 'lon': '2.3488'},
    'London, GB': {'lat': '51.50853', 'lon': '-0.12574'},
    'Dubai, AE': {'lat': '25.276987', 'lon': '55.296249'},
    'Los Angeles, US': {'lat': '34.0522', 'lon': '-118.2437'},
}

weather_data = get_weather_data(locations_dict, api_key)

#----edw exe merika paradeigmata print-----
# print(weather_data["current"]["Thessaloniki, GR"]["weather"])# simantiko me auto to tropo exw prosvasi se edwterika kleidai tou lexikou
# print(weather_data["current"])



# #..Section 4o ..
#Current Data
#Πρώτα, έχετε τη συνάρτηση transform_current_weather_data, η οποία μετατρέπει τα δεδομένα του τρέχοντος καιρού 
#για μια συγκεκριμένη πόλη από ένα λεξικό σε ένα DataFrame. Αυτό το DataFrame περιλαμβάνει μόνο τα δεδομένα για τη συγκεκριμένη πόλη.

def transform_current_weather_data(data_dict: dict) -> pd.DataFrame:
  
  # Preprocess the value of 'weather' key (in some cases the API returns a list instead of a dictionary)
  if isinstance(data_dict['weather'], list):  # Check if 'weather' is a list and select the first element
    if data_dict['weather']:  # Check if the list is not empty
     data_dict['weather'] = data_dict['weather'][0]
    else:
     data_dict['weather'] = {}  # # Assign an empty dictionary if the list is empty
 
  # Flatten data structure
  flattened_data = {}
  for key, value in data_dict.items():
    if isinstance(value, dict):
      for sub_key, sub_value in value.items():
        flattened_data[f"{key}_{sub_key}"] = sub_value
    else:
      flattened_data[key] = value

  # Convert DataFrame and handle datetime if necessary
  data_df = pd.DataFrame([flattened_data])
  if 'dt' in data_df.columns:
    data_df['dt_txt'] = pd.to_datetime(data_df['dt'], unit='s')
    data_df['dt_txt'] = data_df['dt_txt'].dt.strftime('%Y-%m-%d %H:%M:%S')


  return data_df

current_weather = pd.DataFrame()

# Iterate over the weather data for all cities and concatenate the transformed data
for key, value in weather_data['current'].items():
    current_weather = pd.concat([current_weather, transform_current_weather_data(value)])

pd.set_option('display.max_columns', None)

# Print the DataFrame containing the current weather data for all cities
# print(current_weather)


def convert_weather_api_dict_to_dataframe(data_dict: dict) -> pd.DataFrame:
  extracted_data = {}
  for key, value in data_dict.items():
    if isinstance(value, dict):
      for sub_key, sub_value in value.items():
        extracted_data[f"{key}_{sub_key}"] = sub_value
    else:
      extracted_data[key] = value

  return pd.DataFrame([extracted_data])
# print(current_weather)


#Έπειτα, έχετε τη συνάρτηση transform_forecasted_weather_data, η οποία
# μετατρέπει τα προβλεπόμενα δεδομένα καιρού για μια συγκεκριμένη πόλη από ένα 
#λεξικό σε ένα DataFrame. Αυτό το DataFrame περιλαμβάνει τα προβλεπόμενα δεδομένα
# για κάθε ώρα σε ένα χρονικό πλαίσιο και επίσης περιλαμβάνει δεδομένα για τη συγκεκριμένη πόλη.
def transform_forecasted_weather_data(data_dict: dict) -> pd.DataFrame:
 
  city_dict = data_dict['city']
  city_df = convert_weather_api_dict_to_dataframe(city_dict)

  forecasts_dict = data_dict['list']
  forecast_df = pd.DataFrame()
  for forecast_item in forecasts_dict:
    forecast_item['weather'] = forecast_item['weather'][0]
    forecast_item_df = convert_weather_api_dict_to_dataframe(forecast_item)
    forecast_df = pd.concat([forecast_df, forecast_item_df], ignore_index=True)

  return pd.concat([forecast_df, city_df], axis=1)

forecast_weather = pd.DataFrame()



for key, value in weather_data['forecast'].items():
    forecast_weather = pd.concat([forecast_weather, transform_forecasted_weather_data(value)])

pd.set_option('display.max_columns', None)


print(forecast_weather)
