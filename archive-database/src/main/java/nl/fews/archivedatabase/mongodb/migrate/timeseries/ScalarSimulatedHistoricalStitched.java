package nl.fews.archivedatabase.mongodb.migrate.timeseries;

import nl.fews.archivedatabase.mongodb.migrate.utils.TimeSeriesUtil;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.BucketUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

public final class ScalarSimulatedHistoricalStitched {

	/**
	 * Static class
	 */
	private ScalarSimulatedHistoricalStitched(){}

	/**
	 *
	 */
	public static void stitchGroups(){
		try {
			ForkJoinPool pool = new ForkJoinPool(Settings.get("dbThreads"));
			ArrayList<Callable<Void>> tasks = new ArrayList<>();
			TimeSeriesUtil.getTimeSeriesGroups(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL)).forEach(timeSeriesGroup -> tasks.add(() -> {
				stitchGroup(timeSeriesGroup);
				return null;
			}));
			List<Future<Void>> results = pool.invokeAll(tasks);

			for (Future<Void> x : results) {
				x.get();
			}
			pool.shutdown();
		}
		catch (Exception ex){
			Thread.currentThread().interrupt();
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param timeSeriesGroup timeSeriesGroup
	 */
	public static void stitchGroup(Document timeSeriesGroup){
		Document bucket = BucketUtil.getBucket(timeSeriesGroup);
		List<Document> stitchedTimeSeries = TimeSeriesUtil.getStitchedTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));

		boolean collapse = bucket.getBoolean("collapse");
		if(collapse)
			stitchedTimeSeries = TimeSeriesUtil.getCollapsedTimeSeries(stitchedTimeSeries);

		Map<Integer, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(stitchedTimeSeries, BucketSize.valueOf(bucket.getString("bucketSize")));
		List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));

		for (Document d: timeSeriesDocuments) {
			d.remove("runInfo");
			d.remove("taskRunId");
			d.remove("forecastTime");
			d.remove("localForecastTime");
		}

		TimeSeriesUtil.removeTimeSeriesDocuments(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED));
		TimeSeriesUtil.saveTimeSeriesDocuments(timeSeriesDocuments, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED));
	}
}
