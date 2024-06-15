# Query 2
## Find the top 10 cities and their countries by the number of exteremely hot days from last year - when the maximum temperature exceeded 30 degrees Celsius and minimum temperature wasn't under 25.

```javascript
db.weatherV2.aggregate([
    {
        "$match": {
            "readings.date": { "$gte": new Date(new Date().getFullYear() - 1, new Date().getMonth(), new Date().getDate()) }
        }
    },
    { "$unwind": "$readings" },
    {
        "$group": {
            "_id": {
                "station_id": "$station_id",
                "city_name": "$city_name",
                "date": { "$dateToString": { "format": "%Y-%m-%d", "date": "$readings.date" } }
            },
            "avg_temp_c": { "$avg": "$readings.avg_temp_c" },
            "max_temp_c": { "$max": "$readings.max_temp_c" },
            "min_temp_c": { "$min": "$readings.min_temp_c" }
        }
    },
    { "$sort": { "date": -1 } },
    {
        "$match": {
            "$and": [
                { "max_temp_c": { "$gte": 30 } },
                { "min_temp_c": { "$gte": 25 } }
            ]
        }
    },
    {
        "$group": {
            "_id": {
                "station_id": "$_id.station_id",
                "city_name": "$_id.city_name"
            },
            "count_hottest_days": { "$sum": 1 },
            "all_days": {
                "$push": {
                    "date": "$_id.date",
                    "min_temp_c": "$min_temp_c",
                    "max_temp_c": "$max_temp_c",
                    "avg_temp_c": "$avg_temp_c"
                }
            }
        }
    },
    {
        "$addFields": {
            "all_days": {
                "$sortArray": {
                    "input": "$all_days",
                    "sortBy": { "date": 1 }
                }
            }
        }
    },
    { "$sort": { "count_hottest_days": -1 } },
    { "$limit": 10 },
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
            "country": "$country_name",
            "longitude": "$city.longitude",
            "latitude": "$city.latitude",
            "count_hottest_days": 1,
            "all_days": 1
        }
    }
], { "allowDiskUse": false });
```

## Statistics
![r_query22](https://github.com/nina-bu/mongo-weather/assets/116906239/b9091f45-af5e-49cb-810a-62f86bc75045)
