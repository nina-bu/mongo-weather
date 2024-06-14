# Query 5
## Find the year in which the most cities in Europe had an average annual temperature above 20°C, and analyze which countries contributed the most to this number.

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
    { "$unwind": "$country_info" },
    {
        "$match": {
            "country_info.continent": "Europe"
        }
    },
    { "$unwind": "$readings" },
    {
        "$group": {
            "_id": {
                "station_id": "$station_id",
                "city_name": "$city_name",
                "country": "$country_info.country",
                "year": "$year"
            },
            "avg_temp_c": { "$avg": { "$ifNull": ["$readings.avg_temp_c", 0] } },
            "max_temp_c": { "$max": { "$ifNull": ["$readings.max_temp_c", 0] } },
            "min_temp_c": { "$min": { "$ifNull": ["$readings.min_temp_c", 0] } },
            "sunshine_total_min": { "$avg": { "$ifNull": ["$readings.sunshine_total_min", 0] } },
            "precipitation_mm": { "$avg": { "$ifNull": ["$readings.precipitation_mm", 0] } },
            "avg_snow_depth_mm": { "$avg": { "$ifNull": ["$readings.snow_depth_mm", 0] } },
            "avg_wind_speed_kmh": { "$avg": { "$ifNull": ["$readings.avg_wind_speed_kmh", 0] } }
        }
    },
    {
        "$match": {
            "avg_temp_c": { "$gt": 20 }
        }
    },
    {
        "$group": {
            "_id": {
                "year": "$_id.year",
                "country": "$_id.country",
            },
            "cities_count": { "$sum": 1 },
            "cities": {
                "$push": {
                    "station_id": "$_id.station_id",
                    "city_name": "$_id.city_name",
                    "avg_temp_c": "$avg_temp_c",
                    "max_temp_c": "$max_temp_c",
                    "min_temp_c": "$min_temp_c",
                    "sunshine_total_min": "$sunshine_total_min",
                    "precipitation_mm": "$precipitation_mm",
                    "avg_snow_depth_mm": "$avg_snow_depth_mm",
                    "avg_wind_speed_kmh": "$avg_wind_speed_kmh"
                }
            },
        }
    },
    {
        "$sort": { "cities_count": -1 }
    },
    {
        "$group": {
            "_id": {
                "year": "$_id.year"
            },
            "total_cities_count": { "$sum":  "$cities_count"},
            "countries": {
                 "$push": {
                    "country": "$_id.country",
                    "cities_count": "$cities_count",
                    "cities": "$cities"
                }}
        }
    },
    {
        "$sort": { "total_cities_count": -1 }
    },
    {
        "$project": {
            "_id": 0,
            "year": "$_id.year",
            "total_cities_count": 1,
            "countries": 1
        }
    }
], { "allowDiskUse": true });
```

## Statistics
![image]()

## Bottlenecks & Optimization
- $lookup - add an extended reference to the country for every weather document