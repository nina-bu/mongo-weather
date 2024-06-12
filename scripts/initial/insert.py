import os

import numpy as np
import pandas as pd
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['weatherDB']

# Cities
cities_file_path = '../../dataset/cities.csv'
cities_data = pd.read_csv(cities_file_path)

cities_data_dict = cities_data.to_dict('records')
cities_collection = db['cities']
cities_collection.insert_many(cities_data_dict)
print("Cities data inserted successfully")

# Countries
countries_file_path = '../../dataset/countries.csv'
countries_data = pd.read_csv(countries_file_path)

countries_data_dict = countries_data.to_dict('records')
countries_collection = db['countries']
countries_collection.insert_many(countries_data_dict)
print("Countries data inserted successfully")


# Weather
weather_file_path = '../../dataset/daily_weather.parquet'
output_dir = 'dataset/json_output'
os.makedirs(output_dir, exist_ok=True)

weather_data = pd.read_parquet(weather_file_path)
weather_data['date'] = pd.to_datetime(weather_data['date'])
weather_data = weather_data[weather_data['date'].dt.year >= 2019]
weather_data = weather_data.replace({np.nan: None})

weather_data_dict = weather_data.to_dict('records')
for record in weather_data_dict:
    record['date'] = record['date'].to_pydatetime()

weather_collection = db['weather']
weather_collection.insert_many(weather_data_dict)
print("Weather data inserted successfully")