package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.interfaces.BucketHistorical;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.BucketUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.LogUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

public abstract class BucketHistoricalBase implements BucketHistorical {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(BucketHistoricalBase.class);

	/**
	 *
	 */
	private static int progressCurrent = 0;

	/**
	 *
	 */
	private static int progressExpected = 0;

	/**
	 *
	 */
	private static final Object mutex = new Object();

	/**
	 *
	 */
	public void bucketGroups(String singletonCollection, String bucketCollection){
		try {
			List<String> bucketKeyFields = BucketUtil.getBucketKeyFields(bucketCollection);
			ForkJoinPool pool = new ForkJoinPool(Settings.get("databaseBaseThreads"));
			ArrayList<Callable<Void>> tasks = new ArrayList<>();
			List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(singletonCollection, bucketKeyFields);
			progressExpected = timeSeriesGroups.size();
			progressCurrent = 0;
			timeSeriesGroups.forEach(timeSeriesGroup -> tasks.add(() -> {
				bucketGroup(timeSeriesGroup.get("timeSeriesGroup", Document.class), BucketSize.valueOf(timeSeriesGroup.getString("bucketSize")), singletonCollection, bucketCollection);
				return null;
			}));
			List<Future<Void>> results = pool.invokeAll(tasks);
			for (Future<Void> x : results) {
				x.get();
			}
			pool.shutdown();
			logger.info("{} Progress: {}/{} {}, Pool Size: {}", getClass().getSimpleName(), progressCurrent, progressExpected, String.format("%,.2f%%", ((double)progressCurrent/progressExpected*100)), 0);
		}
		catch (Exception ex){
			logger.error(LogUtil.getLogMessageJson(ex, Map.of("singletonCollection", singletonCollection, "bucketCollection", bucketCollection)).toJson(), ex);
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param bucketSize bucketSize
	 */
	private void bucketGroup(Document bucketKeyDocument, BucketSize bucketSize, String singletonCollection, String bucketCollection){
		try{
			synchronized (mutex){
				if (++progressCurrent % 100 == 0)
					logger.info("{} Progress: {}/{} {}%", getClass().getSimpleName(), progressCurrent, progressExpected, String.format("%,.2f%%", ((double)progressCurrent/progressExpected*100)));
			}

			List<Document> timeSeries = getUnwoundTimeSeries(bucketKeyDocument, singletonCollection);
			Map<Long, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
			List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(bucketKeyDocument, timeSeriesBuckets, bucketSize, singletonCollection);

			BucketSize resizedBucketSize = BucketUtil.ensureBucketSize(bucketCollection, timeSeriesDocuments);
			if(resizedBucketSize != null){
				timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, resizedBucketSize);
				timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(bucketKeyDocument, timeSeriesBuckets, resizedBucketSize, singletonCollection);
			}

			for (Document d : timeSeriesDocuments) {
				d.remove("runInfo");
				d.remove("taskRunId");
				d.remove("forecastTime");
				d.remove("localForecastTime");
			}
			Database.deleteMany(bucketCollection, bucketKeyDocument);
			if(!timeSeriesDocuments.isEmpty())
				Database.insertMany(bucketCollection, timeSeriesDocuments);
		}
		catch (Exception ex){
			logger.error(LogUtil.getLogMessageJson(ex, Map.of("bucketKeyDocument", bucketKeyDocument)).toJson(), ex);
		}
	}

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param singletonCollection singletonCollection
	 * @return List<Document>
	 */
	protected abstract List<Document> getUnwoundTimeSeries(Document bucketKeyDocument, String singletonCollection);
}
