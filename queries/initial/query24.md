# Query 4
## Determine the top 5 European cities for winter sports, based on greatest snow depth, lowest average temperatures, and weakest wind conditions during January

```javascript
db.weather.aggregate([
    {
        "$lookup": {
            "from": "cities",
            "localField": "station_id",
            "foreignField": "station_id",
            "as": "city_info"
        }
    },
    { "$unwind": "$city_info" },
    { // Lookup to join with countries collection based on city_info.iso3
        "$lookup": {
            "from": "countries",
            "localField": "city_info.iso3",
            "foreignField": "iso3",
            "as": "country_info"
        }
    },
    {    // Match stage to filter for European cities and January data
        "$match": {
            "$expr": {
                "$and": [
                    { "$eq": [{ "$month": { "$arrayElemAt": ["$readings.date", 0] } }, 1] },  
                    { "$eq": [{ "$arrayElemAt": ["$country_info.continent", 0] }, "Europe"] }
                ]
            }
        }
    },
    { "$unwind": "$readings" },
    {
        "$group": {
            "_id": {
                "station_id": "$station_id",
                "city_name": "$city_name",
                "year": "$year"
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
            "avg_sunshine_min": -1,
            "total_precipitation_mm": 1,
            "avg_temp_c": 1
        }
    },
    { "$limit": 5 },
    {
        "$lookup": {
            "from": "cities",
            "localField": "_id.station_id",
            "foreignField": "station_id",
            "as": "city"
        }
    },
    { "$unwind": "$city" },
    {
        "$project": {
            "_id": 0,
            "city_name": "$_id.city_name",
            "years_ranks": "$yearly_data.year",
            "country": "$city.country",
            "longitude": "$city.longitude",
            "latitude": "$city.latitude",
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
![image]()

## Bottlenecks & Optimization
- $lookup - add an extended reference to the country for every weather document, add an extended reference to the city's longitude and latitude for every weather document