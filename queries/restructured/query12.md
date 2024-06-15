
# Query #2
## For each capital in Europe, find the worst time of year to travel, defined as the season with the strongest wind and the highest precipitation

```javascript
db.weather.aggregate([
    {
        $lookup: {
            from: "countries",
            localField: "city_name",
            foreignField: "capital",
            as: "country_info"
        }
    },
    {
        $match: {
            $expr: {
                $and: [
                    { $eq: ["$city_name", { $arrayElemAt: ["$country_info.capital", 0] }] },
                    { $eq: ["Europe", { $arrayElemAt: ["$country_info.continent", 0] }] }
                ]
            }
        }
    },
    {
        $project: {
            _id: 0,
            capital: "$city_name",
            country: { $arrayElemAt: ["$country_info.country", 0] },
            year: 1,
            readings: {
                $map: {
                    input: "$readings",
                    as: "reading",
                    in: {
                        season: "$$reading.season",
                        avg_wind_speed_kmh: "$$reading.avg_wind_speed_kmh",
                        precipitation_mm: "$$reading.precipitation_mm"
                    }
                }
            }
        }
    },
    {
        $unwind: "$readings"
    },
    {
        $group: {
            _id: {
                capital: "$capital",
                country: "$country",
                season: "$readings.season"
            },
            maxWindSpeed: { $max: "$readings.avg_wind_speed_kmh" },
            totalPrecipitation: { $sum: { $ifNull: ["$readings.precipitation_mm", 0] } }
        }
    },
    {
        $sort: {
            "_id.capital": 1,
            totalPrecipitation: -1,
            maxWindSpeed: -1

        }
    }
    ,
    {
        $group: {
            _id: { capital: "$_id.capital", country: "$_id.country" },
            worstSeason: { $first: "$_id.season" },
            maxWindSpeed: { $first: "$maxWindSpeed" },
            totalPrecipitation: { $first: "$totalPrecipitation" }
        }
    },
    {
        $project: {
            _id: 0,
            capital: "$_id.capital",
            country: "$_id.country",
            worstSeason: 1,
            maxWindSpeed: 1,
            totalPrecipitation: 1
        }
    }
])
```

## Statistics
![image](https://github.com/nina-bu/mongo-weather/assets/116764953/0f67e73c-6d90-41c2-a0e1-8893ae45af0c)
