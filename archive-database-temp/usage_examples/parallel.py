import pymongo
import multiprocessing as mp

client = pymongo.MongoClient("mongodb://mongo:[PASSWORD]@mongo.infisys.net/admin")
db = client["FEWS_ARCHIVE"]


def main():
	module_param_loc = [
		(m, p, l)
		for m in db["SimulatedForecastingScalarTimeSeries"].distinct("moduleInstanceId")
		for p in db["SimulatedForecastingScalarTimeSeries"].distinct("parameterId", {"moduleInstanceId": m})
		for l in db["SimulatedForecastingScalarTimeSeries"].distinct("locationId", {"moduleInstanceId": m, "parameterId": p})]

	processes = 32
	with mp.Pool(processes) as pool:
		results = pool.map(_parallel_count, partition(module_param_loc, processes).values())
	forecasts_per_module_param_loc = sorted([item for r in results for item in r], key=lambda s: s["count"], reverse=True)
	print(forecasts_per_module_param_loc)


def _parallel_count(args):
	parallel_client = pymongo.MongoClient("mongodb://mongo:[PASSWORD]@mongo.infisys.net/admin")
	parallel_db = parallel_client["FEWS_ARCHIVE"]
	results = []
	for m, p, l in args:
		for result in parallel_db["SimulatedForecastingScalarTimeSeries"].aggregate([
			{"$match": {"moduleInstanceId": m, "parameterId": p, "locationId": l}},
			{"$group": {"_id": "$forecastTime", "count": {"$sum": 1}}},
			{"$group": {"_id": {"year": {"$year": "$_id"}}, "count": {"$sum": "$count"}}},
			{"$project": {"moduleInstanceId": m, "parameterId": p, "locationId": l, "year": "$_id.year", "count": "$count"}},
			{"$project": {"_id": 0}}
		]):
			results.append(result)
	return results


def partition(items, num_partitions):
	partitions = {p: [] for p in range(1, num_partitions + 1)}
	[partitions[(i % num_partitions) + 1].append(s) for i, s in enumerate(items)]
	[partitions.pop(k) for k in [k for k in partitions if len(partitions[k]) == 0]]
	return partitions


if __name__ == "__main__":
	main()
