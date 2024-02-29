db.ExternalForecastingScalarTimeSeries.count()
db.ExternalForecastingScalarTimeSeries.distinct("moduleInstanceId")
db.ExternalForecastingScalarTimeSeries.aggregate([
    {"$sort": {"moduleInstanceId" : 1, "forecastTime" : 1}},
    {"$group": {"_id": {"moduleInstanceId": "$moduleInstanceId", "forecastTime": "$forecastTime"}, "count": {"$sum": 1}}},
    {"$group": {"_id": {"moduleInstanceId": "$_id.moduleInstanceId", "year": {"$year": "$_id.forecastTime"}}, "count": {"$sum": "$count"}}},
    {"$project": {"_id": 0, "moduleInstanceId": "$_id.moduleInstanceId", "year": "$_id.year", "count": "$count"}}
]);

let r = [];
db.ExternalForecastingScalarTimeSeries.distinct("moduleInstanceId").forEach(m => {
    db.ExternalForecastingScalarTimeSeries.distinct("parameterId", {"moduleInstanceId": m}).forEach(p => {
        db.ExternalForecastingScalarTimeSeries.distinct("locationId", {"moduleInstanceId": m, "parameterId": p}).forEach(l => {
           db.ExternalForecastingScalarTimeSeries.aggregate([
            {"$match": {"moduleInstanceId": m, "parameterId": p, "locationId": l}},
            {"$group": {"_id": "$forecastTime", "count": {"$sum": 1}}},
            {"$group": {"_id": {"year": {"$year": "$_id"}}, "count": {"$sum": "$count"}}},
            {"$project": {"moduleInstanceId": m, "parameterId": p, "locationId": l, "year": "$_id.year", "count": "$count"}},
            {"$project": {"_id": 0}}
            ]).forEach(x => {
                r.push(x);
            });
        });
    });
});