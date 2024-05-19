# This variable will be True only when running the code from our local machine
is_local_environment = True # Set to False before deploying to Cloud Functions

cloud_storage_service_account = "./Keys/cloud-storage-admin-service-account.json"
bigquery_service_account = "./Keys/bigquery-admin-service-account.json"

api_key = 'fb598271fef4c3b6cb0030c61751e2d7'
project_id = 'weather-api-data-analysis'
dataset_id= 'weather_api'
bucket_name = 'data_api_weather'