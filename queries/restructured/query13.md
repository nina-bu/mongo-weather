
# Query #3
## What are the top 10 country-year pairs with the largest difference between the average temperatures of the hottest and coldest cities?

```javascript
db.weather.aggregate([
  {
    $group: {
      _id: {
        station_id: "$station_id",
        year: "$year",
        country: "$country",
        city_name: "$city_name"
      },
      annual_avg_temp: { $avg: "$avg_temp_c" }
    }
  },
  {
    $group: {
      _id: {
        country: "$_id.country",
        year: "$_id.year"
      },
      hottest_city: {
        $max: {
          avg_temp: "$annual_avg_temp",
          city_name: "$_id.city_name"
        }
      },
      coldest_city: {
        $min: {
          avg_temp: "$annual_avg_temp",
          city_name: "$_id.city_name"
        }
      }
    }
  },
  {
    $addFields: {
      temp_diff: {
        $subtract: ["$hottest_city.avg_temp", "$coldest_city.avg_temp"]
      }
    }
  },
  {
    $sort: { temp_diff: -1 } 
  },
  {
    $limit: 10 
  },
  {
    $project: {
      _id: 0,
      country: "$_id.country",
      year: "$_id.year",
      hottest_city: "$hottest_city.city_name",
      hottest_temp: "$hottest_city.avg_temp",
      coldest_city: "$coldest_city.city_name",
      coldest_temp: "$coldest_city.avg_temp",
      temp_diff: 1
    }
  }
])
```
## Statistics
![image](https://github.com/nina-bu/mongo-weather/assets/116764953/bee802e4-005e-43c8-9795-fe8fc18ba977)

