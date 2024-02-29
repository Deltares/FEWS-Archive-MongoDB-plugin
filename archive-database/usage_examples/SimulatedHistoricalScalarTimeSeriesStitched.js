db.SimulatedHistoricalScalarTimeSeriesStitched.count()
db.SimulatedHistoricalScalarTimeSeriesStitched.distinct("moduleInstanceId")
db.SimulatedHistoricalScalarTimeSeriesStitched.aggregate([
    {"$sort": {"moduleInstanceId" : 1, "startTime" : 1, "endTime" : 1}},
    {"$group": {"_id": {"moduleInstanceId": "$moduleInstanceId", "startTime": "$startTime", "endTime": "$endTime"}, "count": {"$sum": 1}}},
    {"$group": {"_id": {"moduleInstanceId": "$_id.moduleInstanceId", "year": {"$year": "$_id.startTime"}}, "count": {"$sum": "$count"}}},
    {"$project": {"_id": 0, "moduleInstanceId": "$_id.moduleInstanceId", "year": "$_id.year", "count": "$count"}}
]);

let r = [];
db.SimulatedHistoricalScalarTimeSeriesStitched.distinct("moduleInstanceId").forEach(m => {
    db.SimulatedHistoricalScalarTimeSeriesStitched.distinct("parameterId", {"moduleInstanceId": m}).forEach(p => {
        db.SimulatedHistoricalScalarTimeSeriesStitched.distinct("locationId", {"moduleInstanceId": m, "parameterId": p}).forEach(l => {
           db.SimulatedHistoricalScalarTimeSeriesStitched.aggregate([
            {"$match": {"moduleInstanceId": m, "parameterId": p, "locationId": l}},
            {"$group": {"_id": "$startTime", "count": {"$sum": 1}}},
            {"$group": {"_id": {"year": {"$year": "$_id"}}, "count": {"$sum": "$count"}}},
            {"$project": {"moduleInstanceId": m, "parameterId": p, "locationId": l, "year": "$_id.year", "count": "$count"}},
            {"$project": {"_id": 0}}
            ]).forEach(x => {
                r.push(x);
            });
        });
    });
});
