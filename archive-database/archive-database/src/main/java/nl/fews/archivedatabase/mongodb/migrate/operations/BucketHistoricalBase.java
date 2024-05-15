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
import java.util.concurrent.*;

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
			ExecutorService pool = Executors.newFixedThreadPool(Settings.get("databaseBaseThreads"));
			List<String> bucketKeyFields = BucketUtil.getBucketKeyFields(bucketCollection);
			List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(singletonCollection, bucketKeyFields);
			ArrayList<Callable<Void>> tasks = new ArrayList<>();
			progressExpected = timeSeriesGroups.size();
			progressCurrent = 0;

			class BucketGroup implements Callable<Void> {
				private final Document timeSeriesGroup;
				private final BucketSize bucketSize;
				private final String singletonCollection;
				private final String bucketCollection;

				public BucketGroup(Document timeSeriesGroup, BucketSize bucketSize, String singletonCollection, String bucketCollection) {
					this.timeSeriesGroup = timeSeriesGroup;
					this.bucketSize = bucketSize;
					this.singletonCollection = singletonCollection;
					this.bucketCollection = bucketCollection;
				}

				@Override
				public Void call() {
					bucketGroup(timeSeriesGroup, bucketSize, singletonCollection, bucketCollection);
					synchronized (mutex){
						if (++progressCurrent % 100 == 0)
							logger.info("{} Progress: {}/{} {}", getClass().getSimpleName(), progressCurrent, progressExpected, String.format("%,.2f%%", ((double)progressCurrent/progressExpected*100)));
					}
					return null;
				}
			}

			timeSeriesGroups.forEach(timeSeriesGroup -> tasks.add(new BucketGroup(timeSeriesGroup.get("timeSeriesGroup", Document.class), BucketSize.valueOf(timeSeriesGroup.getString("bucketSize")), singletonCollection, bucketCollection)));
			List<Future<Void>> results = pool.invokeAll(tasks);
			for (Future<Void> x : results) {
				x.get();
			}
			pool.shutdown();

			logger.info("{} Progress: {}/{} {}", getClass().getSimpleName(), progressCurrent, progressExpected, String.format("%,.2f%%", ((double)progressCurrent/progressExpected*100)));
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
