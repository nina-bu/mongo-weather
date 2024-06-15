from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27018/')
db = client['weatherDB']
og_collection = db['weatherV0']

pipeline = [
    {
        "$addFields": {
            "year": { "$year": "$date" },
            "month": { "$month": "$date" }
        }
    },
    {
        "$group": {
            "_id": {
                "station_id": "$station_id",
                "city_name": "$city_name",
                "year": "$year",
                "month": "$month"
            },
            "min_temp_c": { "$min": "$min_temp_c" },
            "max_temp_c": { "$max": "$max_temp_c" },
            "avg_temp_c": { "$avg": "$avg_temp_c" },
            "readings": { 
                "$push": {
                    "date": "$date",
                    "season": "$season",
                    "min_temp_c": "$min_temp_c",
                    "max_temp_c": "$max_temp_c",
                    "avg_temp_c": "$avg_temp_c",
                    "precipitation_mm": "$precipitation_mm",
                    "snow_depth_mm": "$snow_depth_mm",
                    "avg_wind_dir_deg": "$avg_wind_dir_deg",
                    "avg_wind_speed_kmh": "$avg_wind_speed_kmh",
                    "peak_wind_gust_kmh": "$peak_wind_gust_kmh",
                    "avg_sea_level_pres_hpa": "$avg_sea_level_pres_hpa",
                    "sunshine_total_min": "$sunshine_total_min"
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "station_id": "$_id.station_id",
            "city_name": "$_id.city_name",
            "year": "$_id.year",
            "month": "$_id.month",
            "min_temp_c": 1,
            "max_temp_c": 1,
            "avg_temp_c": 1,
            "readings": 1
        }
    }
]

updated_collection = db['weatherV1']

result = og_collection.aggregate(pipeline)
updated_collection.insert_many(result)

# og_collection.drop()

print("Weather collection scheme updated.")
