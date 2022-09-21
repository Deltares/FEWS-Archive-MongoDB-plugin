## Example Queries for MongoDB FEWS_ARCHIVE
### Native Shell Script Examples
- **Forecasting** 
    - Forecast collections are organized at the forecast date granularity, containing one complete forecast per database entry. These collections include:
    - ExternalForecastingScalarTimeSeries(.js)
    - SimulatedForecastingScalarTimeSeries(.js)
    - SimulatedHistoricalScalarTimeSeries(.js)
- **Observed / Bucketed**
  - Observed or 'Bucketed' collections are composed of continuous data, as opposed to the finite term of a forecast. These are organized at an arbitrary bucket-granularity, designed to minimize storage efficiency. These collections include:
    - ExternalHistoricalScalarTimeSeries(.js)
    - SimulatedHistoricalScalarTimeSeriesStitched(.js)
- **Examples Provided** 
  - Each of the example files provided illustrate general statistical queries retrieving the size of the data at various granularities.
    - The first query simply gets the total documents in a given collection: `db.ExternalForecastingScalarTimeSeries.count()`
    - The second lists the unique members of a single field: `db.ExternalForecastingScalarTimeSeries.distinct("moduleInstanceId")`
    - The third query counts the moduleInstanceId forecasts or buckets by year using a direct query approach: <pre>`db.ExternalForecastingScalarTimeSeries.aggregate([
    {"$sort": {"moduleInstanceId" : 1, "forecastTime" : 1}},
    {"$group": {"_id": {"moduleInstanceId": "$moduleInstanceId", "forecastTime": "$forecastTime"}, "count": {"$sum": 1}}},
    {"$group": {"_id": {"moduleInstanceId": "$_id.moduleInstanceId", "year": {"$year": "$_id.forecastTime"}}, "count": {"$sum": "$count"}}},
    {"$project": {"_id": 0, "moduleInstanceId": "$_id.moduleInstanceId", "year": "$_id.year", "count": "$count"}}
]);
-OR-
db.ExternalHistoricalScalarTimeSeries.aggregate([
    {"$sort": {"moduleInstanceId" : 1, "startTime" : 1, "endTime" : 1}},
    {"$group": {"_id": {"moduleInstanceId": "$moduleInstanceId", "startTime": "$startTime", "endTime": "$endTime"}, "count": {"$sum": 1}}},
    {"$group": {"_id": {"moduleInstanceId": "$_id.moduleInstanceId", "year": {"$year": "$_id.startTime"}}, "count": {"$sum": "$count"}}},
    {"$project": {"_id": 0, "moduleInstanceId": "$_id.moduleInstanceId", "year": "$_id.year", "count": "$count"}}
])`</pre>
    - The fourth query provides a more scalable programmatic alternative for lower granlarities and larger collections. It counts the forecasts or buckets by year, but at the moduleInstanceId/parameterId/locationId granularity:<pre>`let r = [];
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
-OR-
let r = [];
db.ExternalHistoricalScalarTimeSeries.distinct("moduleInstanceId").forEach(m => {
    db.ExternalHistoricalScalarTimeSeries.distinct("parameterId", {"moduleInstanceId": m}).forEach(p => {
        db.ExternalHistoricalScalarTimeSeries.distinct("locationId", {"moduleInstanceId": m, "parameterId": p}).forEach(l => {
           db.ExternalHistoricalScalarTimeSeries.aggregate([
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
});`</pre> 
- **Technique**
  - **Indexes** 
    - Initial queries in an aggregation pipeline must use indexes or a collection scan will occur. Indexes can be viewed in Robo3T, other database management tools, or directly queried.
    - Only one index is used per query. The initial $match should seek to reduce the set size as much as possible
    - @group only uses indexes if it is preceded by a sort using the same fields in the same order
    - Once the initial query in a pipeline reduces the set size, additional filters and grouping can be done without the risk of a full collection scan
    - Queries should be written to use the included indexes. Adding additional indexes consumes space and will make data inserts less performant. If you need to query at a granularity that is not indexed, first use a broader index to reduce the set size, followed by secondary pipeline filters to reduce the data to the desired filter granularity. 
    - Index overhead is realized at data insert time. Therefore, there is typically no need for periodic re-indexing.
  - **Performance**
    - Only one thread is used on the server to service a given query. Use parallelism and decomposed queries to achieve better performance.
    - Large queries spanning the entire database or returning very large result sets do not perform as well as many smaller queries along indexes, even when single threaded.
    - If you are warned about allowDiskUse, then you are probably not using indexes in your query correctly. You should almost never have to use the disk for queries.
    - Queries are lazy. The data is not retrieved until it is read from the connection. 
### Parallel Examples
- **NodeJS**
  - JavaScript using NodeJS is used with asynchronous function calls to achieve a parallel query that is much more performant then the shell script version seen earlier. 
  - The following query `ExternalForecastingScalarTimeSeries.node.js` illustrates a NodeJS alternative to the native queries contained in `ExternalForecastingScalarTimeSeries.js`. The query is very similar in syntax, but manages all queries asynchronously - awaiting the results from a secondary thread <pre>`1  const mongodb = require("mongodb");
2  const client = new mongodb.MongoClient("mongodb://mongo:[PASSWORD]@mongo.infisys.net/admin");
3  const db = client.db("FEWS_ARCHIVE");
4
5  async function main(){
6    const count = await db.collection("ExternalForecastingScalarTimeSeries").countDocuments();
7    const modules = await db.collection("ExternalForecastingScalarTimeSeries").distinct("moduleInstanceId");
8    const module_counts = await db.collection("ExternalForecastingScalarTimeSeries").aggregate([
9        {"$sort": {"moduleInstanceId": 1, "forecastTime": 1}},
10       {"$group": {"_id": {"moduleInstanceId": "$moduleInstanceId", "forecastTime": "$forecastTime"}, "count": {"$sum": 1}}},
11       {"$group": {"_id": {"moduleInstanceId": "$_id.moduleInstanceId", "year": {"$year": "$_id.forecastTime"}}, "count": {"$sum": "$count"}}},
12       {"$project": {"_id": 0, "moduleInstanceId": "$_id.moduleInstanceId", "year": "$_id.year", "count": "$count"}}
13   ]).toArray();
14
15   console.log([count, modules, module_counts])
16
17   //PARALLEL THREADS
18   const module_param_loc_counts = (await Promise.all((await db.collection("ExternalForecastingScalarTimeSeries").distinct("moduleInstanceId")).map(async m => {
19       return (await Promise.all((await db.collection("ExternalForecastingScalarTimeSeries").distinct("parameterId", {"moduleInstanceId": m})).map( async p => {
20           return (await Promise.all((await db.collection("ExternalForecastingScalarTimeSeries").distinct("locationId", {"moduleInstanceId": m, "parameterId": p})).map(async l => {
21               return db.collection("ExternalForecastingScalarTimeSeries").aggregate([
22                   {"$match": {"moduleInstanceId": m, "parameterId": p, "locationId": l}},
23                   {"$group": {"_id": "$forecastTime", "count": {"$sum": 1}}},
24                   {"$group": {"_id": {"year": {"$year": "$_id"}}, "count": {"$sum": "$count"}}},
25                   {"$project": {"moduleInstanceId": m, "parameterId": p, "locationId": l, "year": "$_id.year", "count": "$count"}},
26                   {"$project": {"_id": 0}}
27               ]).toArray();
28           }))).flat();
29       }))).flat();
30   }))).flat();
31
32   console.log(module_param_loc_counts)
33 }
34
35 await main();`</pre>
- **Python**
  - Python provide process-based parallelism that achieves the same performant results as the NodeJS threading.
  - `parallel.py` illustrate this approach <pre>`1  import pymongo
2  import multiprocessing as mp
3
4  client = pymongo.MongoClient("mongodb://mongo:[PASSWORD]@mongo.infisys.net/admin")
5  db = client["FEWS_ARCHIVE"]
6
7
8  def main():
9  	module_param_loc = [
10		(m, p, l)
11		for m in db["SimulatedForecastingScalarTimeSeries"].distinct("moduleInstanceId")
12		for p in db["SimulatedForecastingScalarTimeSeries"].distinct("parameterId", {"moduleInstanceId": m})
13		for l in db["SimulatedForecastingScalarTimeSeries"].distinct("locationId", {"moduleInstanceId": m, "parameterId": p})]
14
15	processes = 32
16	with mp.Pool(processes) as pool:
17		results = pool.map(_parallel_count, partition(module_param_loc, processes).values())
18	forecasts_per_module_param_loc = sorted([item for r in results for item in r], key=lambda s: s["count"], reverse=True)
19	print(forecasts_per_module_param_loc)
20
21
22 def _parallel_count(args):
23	parallel_client = pymongo.MongoClient("mongodb://mongo:[PASSWORD]@mongo.infisys.net/admin")
24	parallel_db = parallel_client["FEWS_ARCHIVE"]
25	results = []
26	for m, p, l in args:
27		for result in parallel_db["SimulatedForecastingScalarTimeSeries"].aggregate([
28			{"$match": {"moduleInstanceId": m, "parameterId": p, "locationId": l}},
29			{"$group": {"_id": "$forecastTime", "count": {"$sum": 1}}},
30			{"$group": {"_id": {"year": {"$year": "$_id"}}, "count": {"$sum": "$count"}}},
31			{"$project": {"moduleInstanceId": m, "parameterId": p, "locationId": l, "year": "$_id.year", "count": "$count"}},
32			{"$project": {"_id": 0}}
33		]):
34			results.append(result)
35	return results
36
37
38 def partition(items, num_partitions):
39	partitions = {p: [] for p in range(1, num_partitions + 1)}
40	[partitions[(i % num_partitions) + 1].append(s) for i, s in enumerate(items)]
41	[partitions.pop(k) for k in [k for k in partitions if len(partitions[k]) == 0]]
42	return partitions
43
44
45 if __name__ == "__main__":
46	main()`</pre>
### Pandas / DataFrame / Xarray Example
- **Forecast Verification**
  - The following example illustrates a real-world use cae where data is retrieved, loaded into a data frame, joined with data from another query, and placed into a xarray.
  - Several verification statistics are then calculated as proof of concept.
  - `forecast_verification.py` provides a working example for this data manipulation and calculation<pre>`1  import pymongo
2  import pandas as pd
3  from datetime import datetime
4
5  client = pymongo.MongoClient("mongodb://mongo:[PASSWORD]@mongo.infisys.net/admin")
6  db = client["FEWS_ARCHIVE"]
7
8
9  def main():
10	forecast = pd.DataFrame.from_records(db["ExternalForecastingScalarTimeSeries"].aggregate([
11		{"$match": {
12			"moduleInstanceId": "QPF_to_MAP",
13			"parameterId": "MAP",
14			"qualifierId": '["NAEFS"]',
15			"encodedTimeStepId": "SETS360",
16			"forecastTime": {"$gte": datetime.strptime("2020-01-01", "%Y-%m-%d"), "$lt": datetime.strptime("2020-02-01", "%Y-%m-%d")}}},
17		{"$unwind": "$timeseries"},
18		{"$project": {
19			"_id": 0,
20			"locationId": 1,
21			"forecastTime": 1,
22			"time": "$timeseries.t",
23			"forecast": "$timeseries.v",
24			"hour": {"$dateDiff": {"startDate": "$forecastTime", "endDate": "$timeseries.t", "unit": "hour"}}}}
25	]))
26	forecast = forecast.set_index(["locationId", "time"])
27	min_time = datetime.utcfromtimestamp(forecast.index.get_level_values("time").min().value/1000000000)
28	max_time = datetime.utcfromtimestamp(forecast.index.get_level_values("time").max().value/1000000000)
29
30	observed = pd.DataFrame.from_records(db["ExternalHistoricalScalarTimeSeries"].aggregate([
31		{"$match": {
32			"moduleInstanceId": "QPE_to_MAP",
33			"parameterId": "MAP",
34			"qualifierId": '["LMRFC_QPE"]',
35			"encodedTimeStepId": "DTOD_0_6_12_18TZ_CST",
36			"startTime": {"$lte": max_time},
37			"endTime": {"$gte": min_time}
38		}},
39		{"$unwind": "$timeseries"},
40		{"$match": {"timeseries.t": {"$gte": min_time, "$lte": max_time}}},
41		{"$project": {
42			"_id": 0,
43			"locationId": 1,
44			"time": "$timeseries.t",
45			"observed": "$timeseries.v"}}
46	]))
47	observed = observed.set_index(["locationId", "time"])
48	forecast_observed = forecast.join(observed).reset_index().set_index(["locationId", "forecastTime", "hour"]).dropna().drop(columns=["time"]).to_xarray()
49	mean_error_per_location = (forecast_observed.forecast - forecast_observed.observed).sum(["forecastTime", "hour"]) / (forecast_observed.dims["forecastTime"] * forecast_observed.dims["hour"])
50	average_over_per_location = (forecast_observed.forecast - forecast_observed.observed).where((forecast_observed.forecast - forecast_observed.observed) > 0).mean(["forecastTime", "hour"])
51	average_under_per_location = (forecast_observed.forecast - forecast_observed.observed).where((forecast_observed.forecast - forecast_observed.observed) < 0).mean(["forecastTime", "hour"])
52
53
54 if __name__ == "__main__":
55	main()`</pre>