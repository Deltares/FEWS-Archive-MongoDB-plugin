const mongodb = require("mongodb");

const client = new mongodb.MongoClient("mongodb://mongo:[PASSWORD]@mongo.infisys.net/admin");
const db = client.db("FEWS_ARCHIVE");

async function main(){
	const count = await db.collection("ExternalForecastingScalarTimeSeries").countDocuments();
	const modules = await db.collection("ExternalForecastingScalarTimeSeries").distinct("moduleInstanceId");
	const module_counts = await db.collection("ExternalForecastingScalarTimeSeries").aggregate([
		{"$sort": {"moduleInstanceId": 1, "forecastTime": 1}},
		{"$group": {"_id": {"moduleInstanceId": "$moduleInstanceId", "forecastTime": "$forecastTime"}, "count": {"$sum": 1}}},
		{"$group": {"_id": {"moduleInstanceId": "$_id.moduleInstanceId", "year": {"$year": "$_id.forecastTime"}}, "count": {"$sum": "$count"}}},
		{"$project": {"_id": 0, "moduleInstanceId": "$_id.moduleInstanceId", "year": "$_id.year", "count": "$count"}}
	]).toArray();

	console.log([count, modules, module_counts])

	//PARALLEL THREADS
	const module_param_loc_counts = (await Promise.all((await db.collection("ExternalForecastingScalarTimeSeries").distinct("moduleInstanceId")).map(async m => {
		return (await Promise.all((await db.collection("ExternalForecastingScalarTimeSeries").distinct("parameterId", {"moduleInstanceId": m})).map( async p => {
			return (await Promise.all((await db.collection("ExternalForecastingScalarTimeSeries").distinct("locationId", {"moduleInstanceId": m, "parameterId": p})).map(async l => {
				return db.collection("ExternalForecastingScalarTimeSeries").aggregate([
					{"$match": {"moduleInstanceId": m, "parameterId": p, "locationId": l}},
					{"$group": {"_id": "$forecastTime", "count": {"$sum": 1}}},
					{"$group": {"_id": {"year": {"$year": "$_id"}}, "count": {"$sum": "$count"}}},
					{"$project": {"moduleInstanceId": m, "parameterId": p, "locationId": l, "year": "$_id.year", "count": "$count"}},
					{"$project": {"_id": 0}}
				]).toArray();
			}))).flat();
		}))).flat();
	}))).flat();

	console.log(module_param_loc_counts)
}

await main();
