package nl.fews.archivedatabase.mongodb.migrate.operations;

import com.mongodb.client.AggregateIterable;
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

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public final class StitchScalarSimulatedHistorical {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(StitchScalarSimulatedHistorical.class);

	/**
	 * Static class
	 */
	private StitchScalarSimulatedHistorical(){}

	/**
	 *
	 */
	public static void stitchGroups(){
		try {
			ForkJoinPool pool = new ForkJoinPool(Settings.get("databaseBaseThreads"));
			ArrayList<Callable<Void>> tasks = new ArrayList<>();
			TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL).forEach(timeSeriesGroup -> tasks.add(() -> {
				stitchGroup(timeSeriesGroup.get("timeSeriesGroup", Document.class), BucketSize.valueOf(timeSeriesGroup.getString("bucketSize")));
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
	 * @param bucketSize bucketSize
	 * @param timeSeriesGroup timeSeriesGroup
	 */
	public static void stitchGroup(Document timeSeriesGroup, BucketSize bucketSize){
		try{
			List<Document> stitchedTimeSeries = getStitchedTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));

			Map<Long, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(stitchedTimeSeries, bucketSize);
			List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, bucketSize, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));

			for (Document d : timeSeriesDocuments) {
				d.remove("runInfo");
				d.remove("taskRunId");
				d.remove("forecastTime");
				d.remove("localForecastTime");
			}
			TimeSeriesUtil.removeTimeSeriesDocuments(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED));
			TimeSeriesUtil.saveTimeSeriesDocuments(timeSeriesDocuments, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED));
		}
		catch (Exception ex){
			logger.warn(LogUtil.getLogMessageJson(ex, Map.of("timeSeriesGroup", timeSeriesGroup, "bucketSize", bucketSize.toString())).toJson(), ex);
		}
	}

	/**
	 *
	 * @param timeSeriesGroup timeSeriesGroup
	 * @param collection collection
	 * @return List<Document>
	 */
	private static List<Document> getStitchedTimeSeries(Document timeSeriesGroup, String collection){
		Map<Date, Document> d = new HashMap<>();

		AggregateIterable<Document> results = Database.aggregate(collection, List.of(
				new Document("$match", timeSeriesGroup),
				new Document("$sort", new Document("forecastTime", 1)),
				new Document("$project", new Document("_id", 0).append("timeseries", 1)))).allowDiskUse(true);

		for (Document result: results)
			d.putAll(result.getList("timeseries", Document.class).stream().collect(Collectors.toMap(x -> x.getDate("t"), x-> x)));

		return d.values().stream().sorted(Comparator.comparing(s -> s.getDate("t"))).collect(Collectors.toList());
	}
}
