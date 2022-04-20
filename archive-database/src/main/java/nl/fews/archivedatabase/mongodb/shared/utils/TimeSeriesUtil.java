package nl.fews.archivedatabase.mongodb.shared.utils;

import com.mongodb.client.AggregateIterable;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

public final class TimeSeriesUtil {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(TimeSeriesUtil.class);

	/**
	 * Static Class
	 */
	private TimeSeriesUtil(){}

	/**
	 *
	 * @param collection collection
	 * @param bucketKeyFields bucketKeyFields
	 * @return List<Document>
	 */
	public static List<Document> getTimeSeriesGroups(String collection, List<String> bucketKeyFields){
		List<Document> timeSeriesGroups = new ArrayList<>();

		List<Document> query = List.of(
			new Document("$sort", new Document(bucketKeyFields.stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))),
			new Document("$group", new Document("_id", new Document(bucketKeyFields.stream().collect(Collectors.toMap(s -> s, s -> String.format("$%s", s), (k, v) -> v, LinkedHashMap::new)))).append("timeStepMinutes", new Document("$first", "$metaData.timeStepMinutes")))
		);
		Document hint = new Document(bucketKeyFields.stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("metaData.timeStepMinutes", 1);

		Database.aggregate(collection, query).hint(hint).allowDiskUse(true).forEach(timeSeriesGroup -> {
			Document groupResults = timeSeriesGroup.get("_id", Document.class);
			BucketSize bucketSize = groupResults.getString("encodedTimeStepId").equals("NETS") ?
					BucketUtil.getNetsBucketSize(collection, BucketUtil.getBucketKey(bucketKeyFields, groupResults)) :
					BucketUtil.getBucketSize(timeSeriesGroup.getInteger("timeStepMinutes"));
			timeSeriesGroups.add(new Document("timeSeriesGroup", groupResults).append("bucketSize", bucketSize.toString()));
		});

		return timeSeriesGroups;
	}

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param timeSeriesBuckets timeSeriesBuckets
	 * @param bucketSize bucketSize
	 * @param collection collection
	 */
	public static List<Document> getTimeSeriesDocuments(Document bucketKeyDocument, Map<Long, List<Document>> timeSeriesBuckets, BucketSize bucketSize, String collection){
		List<Document> timeSeriesDocuments = new ArrayList<>();

		Document baseDocument = Database.findOne(collection, bucketKeyDocument, new Document("timeseries", 0).append("_id", 0).append("localStartTime", 0).append("localEndTime", 0));
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
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param collection collection
	 * @return List<Document>
	 */
	public static List<Document> getStitchedTimeSeries(Document bucketKeyDocument, String collection){
		Map<Date, Document> timeSeries = new HashMap<>();

		AggregateIterable<Document> results = Database.aggregate(collection, List.of(
				new Document("$match", bucketKeyDocument),
				new Document("$sort", new Document("forecastTime", 1)),
				new Document("$project", new Document("_id", 0).append("timeseries", 1)))).allowDiskUse(true);

		for (Document result: results)
			timeSeries.putAll(result.getList("timeseries", Document.class).stream().collect(Collectors.toMap(x -> x.getDate("t"), x-> x)));

		return timeSeries.values().stream().sorted(Comparator.comparing(s -> s.getDate("t"))).collect(Collectors.toList());
	}

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param collection collection
	 * @return List<Document>
	 */
	public static List<Document> getUnwoundTimeSeries(Document bucketKeyDocument, String collection){
		List<Document> timeSeries = new ArrayList<>();

		AggregateIterable<Document> results = Database.aggregate(collection, List.of(
				new Document("$match", bucketKeyDocument),
				new Document("$unwind", "$timeseries"),
				new Document("$replaceRoot", new Document("newRoot", "$timeseries"))));

		for (Document result: results)
			timeSeries.add(result);

		List<Document> timeSeriesDeduplicate = timeSeries.stream().collect(Collectors.groupingBy(t -> t.getDate("t"))).values().stream().map(s -> s.get(0)).sorted(Comparator.comparing(s -> s.getDate("t"))).collect(Collectors.toList());

		if(timeSeries.size() != timeSeriesDeduplicate.size()){
			Exception ex = new Exception(String.format("Duplicate event dates found and removed -> [%s]", timeSeries.stream().collect(Collectors.groupingBy(t -> t.getDate("t"))).entrySet().stream().filter(f -> f.getValue().size() > 1).map(s -> String.format("%s: %s", s.getKey().toString(), s.getValue().size())).collect(Collectors.joining(","))));
			logger.warn(LogUtil.getLogMessageJson(ex, Map.of("collection", collection, "bucketKeyDocument", bucketKeyDocument)).toJson(), ex);
		}

		return timeSeriesDeduplicate;
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