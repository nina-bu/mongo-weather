# Query 4
## Determine the top 5 European cities for winter sports, based on greatest snow depth, lowest average temperatures, and weakest wind conditions during January

```javascript
db.weatherV2.aggregate([
    {
        "$lookup": {
            "from": "countries",
            "localField": "country_name",
            "foreignField": "country",
            "as": "country_info"
        }
    },
    {
        "$match": {
            "$expr": {
                "$and": [
                    { "$eq": [{ "$month": { "$arrayElemAt": ["$readings.date", 0] } }, 1] },
                    { "$eq": [{ "$arrayElemAt": ["$country_info.continent", 0] }, "Europe"] }
                ]
            }
        }
    },
    {
        "$unwind": "$readings"
    },
    {
        "$group": {
            "_id": {
                "station_id": "$station_id",
                "city_name": "$city_name",
                "year": "$readings.date.year"
            },
            "avg_temp_c": { "$avg": "$readings.avg_temp_c" },
            "max_temp_c": { "$max": "$readings.max_temp_c" },
            "min_temp_c": { "$min": "$readings.min_temp_c" },
            "avg_snow_depth_mm": { "$avg": { "$ifNull": ["$readings.snow_depth_mm", 0] } },
            "avg_wind_speed_kmh": { "$avg": { "$ifNull": ["$readings.avg_wind_speed_kmh", 0] } }
        }
    },
    {
        "$sort": {
            "avg_snow_depth_mm": -1,
            "avg_temp_c": 1,
            "avg_wind_speed_kmh": 1
        }
    },
    {
        "$group": {
            "_id": {
                "station_id": "$_id.station_id",
                "city_name": "$_id.city_name"
            },
            "yearly_data": {
                "$push": {
                    "year": "$_id.year",
                    "avg_temp_c": "$avg_temp_c",
                    "max_temp_c": "$max_temp_c",
                    "min_temp_c": "$min_temp_c",
                    "avg_snow_depth_mm": "$avg_snow_depth_mm",
                    "avg_wind_speed_kmh": "$avg_wind_speed_kmh"
                }
            },
            "avg_temp_c": { "$avg": "$avg_temp_c" },
            "max_temp_c": { "$max": "$max_temp_c" },
            "min_temp_c": { "$min": "$min_temp_c" },
            "avg_snow_depth_mm": { "$avg": "$avg_snow_depth_mm" },
            "avg_wind_speed_kmh": { "$avg": "$avg_wind_speed_kmh" }
        }
    },
    {
        "$sort": {
            "avg_snow_depth_mm": -1,
            "avg_temp_c": 1,
            "avg_wind_speed_kmh": 1
        }
    },
    {
        "$limit": 5
    },
    {
        "$lookup": {
            "from": "cities",
            "localField": "_id.station_id",
            "foreignField": "station_id",
            "as": "city"
        }
    },
    {
        "$unwind": "$city"
    },
    {
        "$project": {
            "_id": 0,
            "city_name": "$_id.city_name",
            "years_ranks": "$yearly_data.year",
            "country": "$city_info.country",
            "longitude": "$city_info.longitude",
            "latitude": "$city_info.latitude",
            "yearly_data": 1,
            "avg_temp_c": 1,
            "max_temp_c": 1,
            "min_temp_c": 1,
            "avg_snow_depth_mm": 1,
            "avg_wind_speed_kmh": 1
        }
    }
], { "allowDiskUse": true });
```

## Statistics
