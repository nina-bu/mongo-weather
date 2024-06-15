# Query #4
## Find countries in the southern hemisphere where cold waves (consecutive days with temperatures at or below freezing) have occurred, and the duration of the longest cold wave for each country?"
```javascript
db.weather.aggregate([
  {
    "$match": {
      "min_temp_c": { "$lte": 0 } 
    }
  },
  {
    "$lookup": {
      "from": "cities",
      "localField": "station_id",
      "foreignField": "station_id",
      "as": "city_info"
    }
  },
  {
    "$match": {
      "city_info.latitude": { "$lt": 0 }, 
    }
  },
  {
    "$project": {
      "station_id": 1,
      "year": 1,
      "month": 1,
      "city_name": 1,
      "temps_c": "$readings.min_temp_c",
      "country_name": 1 
    }
  },
  {
    "$addFields": {
      "consecutive_days_below_minus_1": {
        "$reduce": {
          "input": "$temps_c",
          "initialValue": { "count": 0, "max_count": 0 },
          "in": {
            "$cond": [
              { "$lte": ["$$this", 0] },
              { "count": { "$add": ["$$value.count", 1] }, "max_count": { "$max": ["$$value.count", "$$value.max_count"] } },
              { "count": 0, "max_count": { "$max": ["$$value.count", "$$value.max_count"] } }
            ]
          }
        }
      }
    }
  },
  {
    "$project": {
      "_id": 0,
      "year": 1,
      "month": 1,
      "station_id": 1,
      "city_name": 1,
      "country_name": 1,
      "cold_days": "$consecutive_days_below_minus_1.max_count"
    }
  },
  {
    "$match": {
      "cold_days": { "$gte": 3 }  
    }
  },
  {
    "$sort": {
      "cold_days": -1 
    }
  },
  {
    "$group": {
      "_id": "$country_name",  
      "weather_info": { "$first": "$$ROOT" }
    }
  },
  {
    "$replaceRoot": { "newRoot": "$weather_info" } 
  }
])
```
## Statistics
![image](https://github.com/nina-bu/mongo-weather/assets/116764953/7bd52536-5dd1-431e-ba79-7e9c24d31764)

