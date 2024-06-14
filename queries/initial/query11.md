# Query #1
## For each season and country, find the rainiest city over the previous 5 years
```javascript 
db.weather.aggregate([
    {
        $match: {
            year: { $gte: new Date().getFullYear() - 5 }
        }
    },
    {
        $lookup: {
            from: "cities",
            localField: "station_id",
            foreignField: "station_id",
            as: "city"
        }
    },
    {
        $unwind: "$readings"
    },
    {
        $match: {
            "readings.precipitation_mm": { $ne: null }
        }
    },
    {
        $project: {
            season: "$readings.season",
            station_id: 1,
            precipitation_mm: "$readings.precipitation_mm",
            city_name: 1,
            country_name: { $arrayElemAt: ["$city.country", 0] },
            year: 1,
            month: 1
        }
    },
    {
        $group: {
            _id: {
                season: "$season",
                country: "$country_name",
                station_id: "$station_id",
                city: "$city_name"
            },
            totalPrecipitation: { $sum: "$precipitation_mm" }
        }
    },
    {
        $sort: {
            "_id.country": 1,
            "_id.season": 1,
            totalPrecipitation: -1

        }
    },
    {
        $group: {
            _id: {
                country: "$_id.country",
                season: "$_id.season"
            },
            rainiestCity: { $first: "$_id.city" },
            totalPrecipitation: { $first: "$totalPrecipitation" }
        }
    },
    {
        $project: {
            _id: 0,
            country: "$_id.country",
            season: "$_id.season",
            rainiestCity: 1,
            totalPrecipitation: 1
        }
    }
])
```
## Statistics
![image](https://github.com/nina-bu/mongo-weather/assets/116764953/5d394ceb-6751-4f4c-b0b4-325643c6228f)

## Bottlenecks & Optimization
- $unwind - precompute total precipitation for each document in weather collection
- $lookup - add an extended reference to the country for every weather document
