package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.LogUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;

public final class BucketScalarExternalHistorical {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(BucketScalarExternalHistorical.class);

	/**
	 * Static class
	 */
	private BucketScalarExternalHistorical(){}

	/**
	 *
	 */
	public static void bucketGroups(){
		ForkJoinPool pool = new ForkJoinPool(Settings.get("databaseBaseThreads"));
		ArrayList<Callable<Void>> tasks = new ArrayList<>();
		TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL).forEach(timeSeriesGroup -> tasks.add(() -> {
			bucketGroup(timeSeriesGroup.get("timeSeriesGroup", Document.class), BucketSize.valueOf(timeSeriesGroup.getString("bucketSize")));
			return null;
		}));
		pool.invokeAll(tasks);
		pool.shutdown();
	}

	/**
	 *
	 * @param bucketSize bucketSize
	 * @param timeSeriesGroup timeSeriesGroup
	 */
	public static void bucketGroup(Document timeSeriesGroup, BucketSize bucketSize){
		try {
			List<Document> timeSeries = TimeSeriesUtil.getTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
			Map<Long, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
			List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, bucketSize, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
			TimeSeriesUtil.removeTimeSeriesDocuments(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET));
			TimeSeriesUtil.saveTimeSeriesDocuments(timeSeriesDocuments, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET));
		}
		catch (Exception ex){
			logger.warn(LogUtil.getLogMessageJson(ex, Map.of("timeSeriesGroup", timeSeriesGroup, "bucketSize", bucketSize.toString())).toJson(), ex);
		}
	}

	/**
	 *
	 */
	public static void replaceTimeSeriesWithBucketCollection(){
		String tempCollectionName = String.format("TEMP_%s", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
		Database.renameCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), tempCollectionName);
		Database.renameCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
		Database.dropCollection(tempCollectionName);
	}
}