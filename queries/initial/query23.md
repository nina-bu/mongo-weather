# Query 3
## Determine the top 5 best cities for summer vacations - sunniest with the least rainfall in August.

```javascript
db.weatherV1.aggregate([
    {
        "$match": {
            "month": { "$eq": 8 }
        }
    },
    { "$unwind": "$readings" },
    {
        "$group": {
            "_id": {
                "station_id": "$station_id",
                "city_name": "$city_name",
                "date": { "$dateToString": { "format": "%Y-%m-%d", "date": "$readings.date" } },
                "year": "$year"
            },
            "avg_temp_c": { "$avg": "$readings.avg_temp_c" },
            "max_temp_c": { "$max": "$readings.max_temp_c" },
            "min_temp_c": { "$min": "$readings.min_temp_c" },
            "sunshine_total_min": { "$avg": { "$ifNull": ["$readings.sunshine_total_min", 0] } },
            "precipitation_mm": { "$avg": { "$ifNull": ["$readings.precipitation_mm", 0] } }
        }
    },
    {
        "$group": {
            "_id": {
                "station_id": "$_id.station_id",
                "city_name": "$_id.city_name",
                "year": "$_id.year",
            },
            "avg_temp_c": { "$avg": "$avg_temp_c" },
            "max_temp_c": { "$max": "$max_temp_c" },
            "min_temp_c": { "$min": "$min_temp_c" },
            "avg_sunshine_min": { "$avg": "$sunshine_total_min" },
            "total_precipitation_mm": { "$sum": "$precipitation_mm" }
        }
    },
    {
        "$sort": {
            "avg_sunshine_min": -1,
            "total_precipitation_mm": 1,
            "avg_temp_c": 1
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
                    "avg_sunshine_min": "$avg_sunshine_min",
                    "total_precipitation_mm": "$total_precipitation_mm"
                }
            },
            "avg_temp_c": { "$avg": "$avg_temp_c" },
            "max_temp_c": { "$max": "$max_temp_c" },
            "min_temp_c": { "$min": "$min_temp_c" },
            "avg_sunshine_min": { "$avg": "$avg_sunshine_min" },
            "total_precipitation_mm": { "$sum": "$total_precipitation_mm" }
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
            "avg_sunshine_min": 1,
            "total_precipitation_mm": 1
        }
    }
], { "allowDiskUse": true });
```

## Statistics
![query23](https://github.com/nina-bu/mongo-weather/assets/116906239/b6105427-b80c-4791-b217-f8aa8b31ab05)


## Bottlenecks & Optimization
- $lookup - add an extended reference to the city's longitude and latitude for every weather document
