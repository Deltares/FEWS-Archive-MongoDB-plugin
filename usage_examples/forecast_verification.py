import pymongo
import pandas as pd
from datetime import datetime

client = pymongo.MongoClient("mongodb://mongo:[PASSWORD]@mongo.infisys.net/admin")
db = client["FEWS_ARCHIVE"]


def main():
	forecast = pd.DataFrame.from_records(db["ExternalForecastingScalarTimeSeries"].aggregate([
		{"$match": {
			"moduleInstanceId": "QPF_to_MAP",
			"parameterId": "MAP",
			"qualifierId": '["NAEFS"]',
			"encodedTimeStepId": "SETS360",
			"forecastTime": {"$gte": datetime.strptime("2020-01-01", "%Y-%m-%d"), "$lt": datetime.strptime("2020-02-01", "%Y-%m-%d")}}},
		{"$unwind": "$timeseries"},
		{"$project": {
			"_id": 0,
			"locationId": 1,
			"forecastTime": 1,
			"time": "$timeseries.t",
			"forecast": "$timeseries.v",
			"hour": {"$dateDiff": {"startDate": "$forecastTime", "endDate": "$timeseries.t", "unit": "hour"}}}}
	]))
	forecast = forecast.set_index(["locationId", "time"])
	min_time = datetime.utcfromtimestamp(forecast.index.get_level_values("time").min().value/1000000000)
	max_time = datetime.utcfromtimestamp(forecast.index.get_level_values("time").max().value/1000000000)

	observed = pd.DataFrame.from_records(db["ExternalHistoricalScalarTimeSeries"].aggregate([
		{"$match": {
			"moduleInstanceId": "QPE_to_MAP",
			"parameterId": "MAP",
			"qualifierId": '["LMRFC_QPE"]',
			"encodedTimeStepId": "DTOD_0_6_12_18TZ_CST",
			"startTime": {"$lte": max_time},
			"endTime": {"$gte": min_time}
		}},
		{"$unwind": "$timeseries"},
		{"$match": {"timeseries.t": {"$gte": min_time, "$lte": max_time}}},
		{"$project": {
			"_id": 0,
			"locationId": 1,
			"time": "$timeseries.t",
			"observed": "$timeseries.v"}}
	]))
	observed = observed.set_index(["locationId", "time"])
	forecast_observed = forecast.join(observed).reset_index().set_index(["locationId", "forecastTime", "hour"]).dropna().drop(columns=["time"]).to_xarray()
	mean_error_per_location = (forecast_observed.forecast - forecast_observed.observed).sum(["forecastTime", "hour"]) / (forecast_observed.dims["forecastTime"] * forecast_observed.dims["hour"])
	average_over_per_location = (forecast_observed.forecast - forecast_observed.observed).where((forecast_observed.forecast - forecast_observed.observed) > 0).mean(["forecastTime", "hour"])
	average_under_per_location = (forecast_observed.forecast - forecast_observed.observed).where((forecast_observed.forecast - forecast_observed.observed) < 0).mean(["forecastTime", "hour"])


if __name__ == "__main__":
	main()
