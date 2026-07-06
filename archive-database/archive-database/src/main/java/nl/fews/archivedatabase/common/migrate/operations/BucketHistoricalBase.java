package nl.fews.archivedatabase.common.migrate.operations;

import nl.fews.archivedatabase.common.migrate.interfaces.BucketHistorical;
import nl.fews.archivedatabase.common.shared.interfaces.DatabaseBridge;
import nl.fews.archivedatabase.common.shared.enums.BucketSize;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.common.shared.utils.BucketUtil;
import nl.fews.archivedatabase.common.shared.utils.LogUtil;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.*;

@SuppressWarnings("unchecked")
public abstract class BucketHistoricalBase implements BucketHistorical {

	/**
	 *
	 */
	protected final DatabaseBridge database;

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
	public BucketHistoricalBase() {
		try{
			database = (DatabaseBridge)Class.forName(String.format(Settings.DATABASE_NAMESPACE, Settings.get("databaseType", String.class).toLowerCase())).getConstructor().newInstance();
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 */
	public void bucketGroups(String singletonCollection, String bucketCollection){
		try {
			var pool = Executors.newFixedThreadPool(Settings.get("databaseBaseThreads"));
			var bucketKeyFields = BucketUtil.getBucketKeyFields(bucketCollection);
			var timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(singletonCollection, bucketKeyFields);
			var tasks = new ArrayList<Callable<Void>>();
			progressExpected = timeSeriesGroups.size();
			progressCurrent = 0;

			class BucketGroup implements Callable<Void> {
				private final Map<String, Object> timeSeriesGroup;
				private final BucketSize bucketSize;
				private final String singletonCollection;
				private final String bucketCollection;

				public BucketGroup(Map<String, Object> timeSeriesGroup, BucketSize bucketSize, String singletonCollection, String bucketCollection) {
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

			timeSeriesGroups.forEach(timeSeriesGroup -> tasks.add(new BucketGroup((Map<String, Object>)timeSeriesGroup.get("timeSeriesGroup"), BucketSize.valueOf((String)timeSeriesGroup.get("bucketSize")), singletonCollection, bucketCollection)));
			for (Future<Void> x : pool.invokeAll(tasks))
				x.get();
			pool.shutdown();

			logger.info("{} Progress: {}/{} {}", getClass().getSimpleName(), progressCurrent, progressExpected, String.format("%,.2f%%", ((double)progressCurrent/progressExpected*100)));
		}
		catch (Exception ex){
			logger.error(new JSONObject(LogUtil.getLogMessageJson(ex, Map.of("singletonCollection", singletonCollection, "bucketCollection", bucketCollection))).toString(), ex);
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param bucketSize bucketSize
	 */
	private void bucketGroup(Map<String, Object> bucketKeyDocument, BucketSize bucketSize, String singletonCollection, String bucketCollection){
		try{
			var timeSeries = getUnwoundTimeSeries(bucketKeyDocument, singletonCollection);
			var timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
			var timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(bucketKeyDocument, timeSeriesBuckets, bucketSize, singletonCollection);

			var resizedBucketSize = BucketUtil.ensureBucketSize(bucketCollection, timeSeriesDocuments);
			if(resizedBucketSize != null){
				timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, resizedBucketSize);
				timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(bucketKeyDocument, timeSeriesBuckets, resizedBucketSize, singletonCollection);
			}

			for (var d : timeSeriesDocuments) {
				d.remove("runInfo");
				d.remove("taskRunId");
				d.remove("forecastTime");
				d.remove("localForecastTime");
			}
			database.deleteMany(bucketCollection, bucketKeyDocument);
			if(!timeSeriesDocuments.isEmpty())
				database.insertMany(bucketCollection, timeSeriesDocuments);
		}
		catch (Exception ex){
			logger.error(new JSONObject(LogUtil.getLogMessageJson(ex, Map.of("bucketKeyDocument", bucketKeyDocument))).toString(), ex);
		}
	}

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param singletonCollection singletonCollection
	 * @return List<Map<String, Object>>
	 */
	protected abstract List<Map<String, Object>> getUnwoundTimeSeries(Map<String, Object> bucketKeyDocument, String singletonCollection);
}
