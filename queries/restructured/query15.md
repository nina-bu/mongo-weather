# Query #5
## What are the temperature change trends (warming, cooling, stable) categorized by population size and density for each city?

```javascript
db.weather.aggregate([
    {
        "$group": {
            "_id": {
                "station_id": "$station_id",
                "year": "$year"
            },
            "avg_temp": { "$avg": "$avg_temp_c" },
            "city_name": { "$first": "$city_name" },
            "country_name": { "$first": "$country_name" }
        }
    },
    {
        "$sort": {
            "_id.station_id": 1,
            "_id.year": -1
        }
    },
    {
        "$group": {
            "_id": "$_id.station_id",
            "annual_temp_c_sorted": {
                "$push": {
                    "year": "$_id.year",
                    "avg_temp": "$avg_temp",
                    "city_name": "$city_name",
                    "country_name": "$country_name"
                }
            }
        }
    },
    {
        "$lookup": {
            "from": "countries",
            "localField": "annual_temp_c_sorted.country_name",
            "foreignField": "country",
            "as": "country_info"
        }
    },
    {
        "$project": {
            "station_id": "$_id",
            "annual_temp_c": {
                "$map": {
                    "input": { "$slice": ["$annual_temp_c_sorted", 1, 1] },
                    "in": {
                        "year": "$$this.year",
                        "avg_temp": "$$this.avg_temp",
                        "last_year_temp_diff": {
                            "$subtract": [
                                "$$this.avg_temp",
                                { "$arrayElemAt": ["$annual_temp_c_sorted.avg_temp", { "$add": [{ "$indexOfArray": ["$annual_temp_c_sorted.year", "$$this.year"] }, 1] }] }
                            ]
                        }
                    }
                }
            },
            "city_name": { "$arrayElemAt": ["$annual_temp_c_sorted.city_name", 0] },
            "country": { "$arrayElemAt": ["$annual_temp_c_sorted.country_name", 0] },
            "population": { "$arrayElemAt": ["$country_info.population", 0] },
        }
    },
    {
        "$project": {
            "station_id": 1,
            "annual_temp_c": 1,
            "city_name": 1,
            "country": 1,
            "population": 1,
            "avg_temp_diff": { "$avg": "$annual_temp_c.last_year_temp_diff" }
        }
    },
    {
        "$project": {
            "_id": 0,
            "city_name": 1,
            "country": 1,
            "population": 1,
            "avg_temp_diff": 1,
            "temp_change_category": {
                "$switch": {
                    "branches": [
                        { "case": { "$gt": ["$avg_temp_diff", 0] }, "then": "Warming" },
                        { "case": { "$lt": ["$avg_temp_diff", 0] }, "then": "Cooling" },
                        { "case": { "$eq": ["$avg_temp_diff", 0] }, "then": "Stable" }
                    ],
                    "default": "Unknown"
                }
            },
            "population_size": {
                "$switch": {
                    "branches": [
                        { "case": { "$gte": ["$population", 100000000] }, "then": "Very High" },
                        { "case": { "$gte": ["$population", 50000000] }, "then": "High" },
                        { "case": { "$gte": ["$population", 10000000] }, "then": "Medium" },
                        { "case": { "$gte": ["$population", 1000000] }, "then": "Low" },
                        { "case": { "$gt": ["$population", 0] }, "then": "Very Low" }
                    ],
                    "default": "Unknown"
                }
            }
        }
    }
])
```
## Statistics
![image](https://github.com/nina-bu/mongo-weather/assets/116764953/af4cca95-1ea2-4698-9110-36f1c3273395)

