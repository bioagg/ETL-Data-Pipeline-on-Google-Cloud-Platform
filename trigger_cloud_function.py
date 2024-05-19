#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests

# Replace with the actual URL of your Cloud Function
url = "https://europe-west8-weather-api-data-analysis.cloudfunctions.net/etl_function_api"

# Optional: Add data to be sent in the POST request body (as a dictionary)
data = {"": ""}

# Send the POST request
response = requests.post(url, json=data)  # Use json=data if sending data

# Check for successful response
if response.status_code == 200:
  print("POST request successful!")
  print(response.text)
  # Access the response content (optional)
  # print(response.text)
else:
  print(f"Error: {response.status_code}")
  print(response.text)