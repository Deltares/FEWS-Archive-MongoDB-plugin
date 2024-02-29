db.SimulatedHistoricalScalarTimeSeries.count()
db.SimulatedHistoricalScalarTimeSeries.distinct("moduleInstanceId")
db.SimulatedHistoricalScalarTimeSeries.aggregate([
    {"$sort": {"moduleInstanceId" : 1, "forecastTime" : 1}},
    {"$group": {"_id": {"moduleInstanceId": "$moduleInstanceId", "forecastTime": "$forecastTime"}, "count": {"$sum": 1}}},
    {"$group": {"_id": {"moduleInstanceId": "$_id.moduleInstanceId", "year": {"$year": "$_id.forecastTime"}}, "count": {"$sum": "$count"}}},
    {"$project": {"_id": 0, "moduleInstanceId": "$_id.moduleInstanceId", "year": "$_id.year", "count": "$count"}}
]);

let r = [];
db.SimulatedHistoricalScalarTimeSeries.distinct("moduleInstanceId").forEach(m => {
    db.SimulatedHistoricalScalarTimeSeries.distinct("parameterId", {"moduleInstanceId": m}).forEach(p => {
        db.SimulatedHistoricalScalarTimeSeries.distinct("locationId", {"moduleInstanceId": m, "parameterId": p}).forEach(l => {
           db.SimulatedHistoricalScalarTimeSeries.aggregate([
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
