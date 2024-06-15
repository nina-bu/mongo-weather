# Query 1
## How has the average monthly temperature changed for each region over the past five years?

```javascript
db.weatherV2.aggregate([
    {
        "$match": {
            "year": { "$gte": new Date().getFullYear() - 5 }
        }
    },
    {
        "$lookup": {
            "from": "countries",
            "localField": "country_name",
            "foreignField": "country",
            "as": "country_info"
        }
    },
    { "$unwind": "$country_info" },
    {
        "$group": {
            "_id": {
                "region": "$country_info.region",
                "country": "$country_name",
                "station_id": "$station_id",
                "city_name": "$city_name",
                "year": "$year",
                "month": "$month",
                "avg_temp_c": "$avg_temp_c"
            },
        }
    },
    {
        "$sort": {
            "year": 1,
            "month": 1
        }
    },
    {
        "$group": {
            "_id": {
                "region": "$_id.region",
                "country": "$_id.country",
                "station_id": "$_id.station_id",
                "city_name": "$_id.city_name",
            },
            "average_temperatures": {
                "$push": {
                    "year": "$_id.year",
                    "month": "$_id.month",
                    "avg_temp_c": "$_id.avg_temp_c"
                }
            }
        }
    },
    {
        "$project": {
            "_id": 1,
            "average_temperatures": {
                "$sortArray": {
                    "input": "$average_temperatures",
                    "sortBy": { "year": 1, "month": 1 }
                }
            }
        }
    },
    {
        "$group": {
            "_id": {
                "region": "$_id.region",
                "country": "$_id.country",
            },
            "cities": {
                "$push": {
                    "station_id": "$_id.station_id",
                    "city_name": "$_id.city_name",
                    "average_temperatures": "$average_temperatures"
                }
            }
        }
    },
    {
        "$group": {
            "_id": "$_id.region",
            "avg_temp_c": { "$avg": { "$avg": "$average_temperatures.avg_temp_c" } },
            "countries": {
                "$push": {
                    "_id": "$_id.country",
                    "cities": "$cities"
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "region": "$_id",
            "countries": 1
        }
    },
    { "$sort": { "region": 1 } }
], { "allowDiskUse": true });
```

## Statistics
