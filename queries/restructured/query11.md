# Query #1
## For each season and country, find the rainiest city over the previous 5 years
```javascript 
db.weather.aggregate([
    {
        $match: {
            year: { $gte: new Date().getFullYear() - 5 },
            "readings.precipitation_mm": { $ne: null }
        }
    },
    {
        $unwind: "$readings"
    },
    {
        $group: {
            _id: {
                station_id: "$station_id",
                season: "$readings.season",
                country: "$country_name",
                city: "$city_name"
            },
            total_precipitation_mm: { $sum: "$readings.precipitation_mm" }
        }
    },
    {
        $sort: {
            "_id.country": 1,
            "_id.season": 1,
            total_precipitation_mm: -1
        }
    },
    {
        $group: {
            _id: {
                country: "$_id.country",
                season: "$_id.season"
            },
            rainiestCity: { $first: "$_id.city" },
            total_precipitation_mm: { $first: "$total_precipitation_mm" }
        }
    },
    {
        $project: {
            _id: 0,
            country: "$_id.country",
            season: "$_id.season",
            rainiestCity: 1,
            total_precipitation_mm: 1
        }
    }
])
```
## Statistics
![image](https://github.com/nina-bu/mongo-weather/assets/116764953/665f714a-36b4-47dc-9cab-01c707df8672)
