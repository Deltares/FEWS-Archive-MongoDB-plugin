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

public final class ScalarExternalHistoricalBucket {

	/**
	 * Static class
	 */
	private ScalarExternalHistoricalBucket(){}

	/**
	 *
	 */
	public static void bucketGroups(){
		try {
			ForkJoinPool pool = new ForkJoinPool(Settings.get("dbThreads"));
			ArrayList<Callable<Void>> tasks = new ArrayList<>();
			TimeSeriesUtil.getTimeSeriesGroups(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)).forEach(timeSeriesGroup -> tasks.add(() -> {
				bucketGroup(timeSeriesGroup);
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
	public static void bucketGroup(Document timeSeriesGroup){
		Document bucket = BucketUtil.getBucket(timeSeriesGroup);
		List<Document> timeSeries = TimeSeriesUtil.getTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));

		boolean collapse = bucket.getBoolean("collapse");
		if(collapse)
			timeSeries = TimeSeriesUtil.getCollapsedTimeSeries(timeSeries);

		Map<Integer, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, BucketSize.valueOf(bucket.getString("bucketSize")));
		List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
		TimeSeriesUtil.removeTimeSeriesDocuments(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET));
		TimeSeriesUtil.saveTimeSeriesDocuments(timeSeriesDocuments, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET));
	}
}
