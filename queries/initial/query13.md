
# Query #3
## What are the top 10 country-year pairs with the largest difference between the average temperatures of the hottest and coldest cities?

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
    },
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
![image](https://github.com/nina-bu/mongo-weather/assets/116764953/8f7f504c-0b2a-4387-892f-bf28bb3520d3)

## Bottlenecks & Optimization
- $lookup - add an extended reference to the country for every weather document
