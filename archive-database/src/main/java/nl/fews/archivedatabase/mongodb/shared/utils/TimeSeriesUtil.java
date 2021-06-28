package nl.fews.archivedatabase.mongodb.shared.utils;

import com.mongodb.client.AggregateIterable;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

public final class TimeSeriesUtil {

	/**
	 * Static Class
	 */
	private TimeSeriesUtil(){}

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @return List<Document>
	 */
	public static List<Document> getTimeSeriesGroups(TimeSeriesType timeSeriesType){
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		List<String> keys = Database.getCollectionKeys(collection);
		List<Document> timeSeriesGroups = new ArrayList<>();
		Database.aggregate(collection, List.of(
				new Document("$sort", new Document(keys.stream().filter(s -> !s.equals("bucketSize") && !s.equals("bucket")).collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))),
				new Document("$group", new Document("_id", new Document(keys.stream().filter(s -> !s.equals("bucketSize") && !s.equals("bucket")).collect(Collectors.toMap(s -> s, s -> String.format("$%s", s), (k, v) -> v, LinkedHashMap::new)))).
						append("moduleInstanceId", new Document("$first", "moduleInstanceId")).
						append("locationId", new Document("$first", "locationId")).
						append("parameterId", new Document("$first", "parameterId")).
						append("qualifierId", new Document("$first", "qualifierId")).
						append("encodedTimeStepId", new Document("$first", "encodedTimeStepId")).
						append("timeStepMinutes", new Document("$first", "$metaData.timeStepMinutes")))
		)).allowDiskUse(true).forEach(timeSeriesGroup -> {
			BucketSize bucketSize = timeSeriesGroup.getString("encodedTimeStepId").equals("nonequidistant") ?
					BucketUtil.getBucketSize(new Document(Database.getCollectionIndexes(Settings.get("bucketSizeCollection"))[0].keySet().stream().filter(s -> !s.equals("unique")).collect(Collectors.toMap(s -> s, timeSeriesGroup::get, (k, v) -> v, LinkedHashMap::new)))) :
					BucketUtil.getBucketSize(timeSeriesGroup.getInteger("timeStepMinutes"));
			timeSeriesGroups.add(new Document("timeSeriesGroup", timeSeriesGroup.get("_id", Document.class)).append("bucketSize", bucketSize.toString()));
		});
		return timeSeriesGroups;
	}

	/**
	 *
	 * @param timeSeriesGroup timeSeriesGroup
	 * @param collection collection
	 */
	public static void removeTimeSeriesDocuments(Document timeSeriesGroup, String collection){
		Database.deleteMany(collection, timeSeriesGroup);
	}

	/**
	 *
	 * @param timeSeriesGroup timeSeriesGroup
	 * @param timeSeriesBuckets timeSeriesBuckets
	 * @param bucketSize bucketSize
	 * @param collection collection
	 */
	public static List<Document> getTimeSeriesDocuments(Document timeSeriesGroup, Map<Long, List<Document>> timeSeriesBuckets, BucketSize bucketSize, String collection){
		List<Document> timeSeriesDocuments = new ArrayList<>();
		Document baseDocument = Database.findOne(collection, timeSeriesGroup, new Document("timeseries", 0).append("_id", 0).append("localStartTime", 0).append("localEndTime", 0));
		if(baseDocument != null){
			String baseDocumentJson = baseDocument.toJson();
			timeSeriesBuckets.forEach((bucketValue, timeSeries) -> {
				Document document = Document.parse(baseDocumentJson);
				document.append("bucketSize", bucketSize.toString());
				document.append("bucket", bucketValue);
				document.append("timeseries", timeSeries);
				document.append("startTime", timeSeries.get(0).getDate("t"));
				document.append("endTime", timeSeries.get(timeSeries.size() - 1).getDate("t"));
				if (timeSeries.get(0).containsKey("localStartTime")) document.append("localStartTime", timeSeries.get(0).getDate("lt"));
				if (timeSeries.get(0).containsKey("localEndTime")) document.append("localEndTime", timeSeries.get(timeSeries.size() - 1).getDate("lt"));
				timeSeriesDocuments.add(document);
			});
		}
		return timeSeriesDocuments;
	}

	/**
	 *
	 * @param timeSeriesDocuments timeSeries
	 * @param collection collection
	 */
	public static void saveTimeSeriesDocuments(List<Document> timeSeriesDocuments, String collection){
		Database.insertMany(collection, timeSeriesDocuments);
	}

	/**
	 *
	 * @param timeSeriesGroup timeSeriesGroup
	 * @param collection collection
	 * @return List<Document>
	 */
	public static List<Document> getTimeSeries(Document timeSeriesGroup, String collection){
		List<Document> timeSeries = new ArrayList<>();
		AggregateIterable<Document> results = Database.aggregate(collection, List.of(
				new Document("$match", timeSeriesGroup),
				new Document("$unwind", "$timeseries"),
				new Document("$sort", new Document("timeseries.t", 1)),
				new Document("$replaceRoot", new Document("newRoot", "$timeseries")))).allowDiskUse(true);
		for (Document result: results)
			timeSeries.add(result);
		return timeSeries;
	}

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param bucketSize bucketSize
	 * @return Map<Integer, List<Document>>
	 */
	public static Map<Long, List<Document>> getTimeSeriesBuckets(List<Document> timeSeries, BucketSize bucketSize){
		Map<Long, List<Document>> timeSeriesBuckets = new HashMap<>();
		for (Document ts : timeSeries) {
			long bucketValue = BucketUtil.getBucketValue(ts.getDate("t"), bucketSize);
			timeSeriesBuckets.putIfAbsent(bucketValue, new ArrayList<>());
			timeSeriesBuckets.get(bucketValue).add(ts);
		}
		return timeSeriesBuckets;
	}
}