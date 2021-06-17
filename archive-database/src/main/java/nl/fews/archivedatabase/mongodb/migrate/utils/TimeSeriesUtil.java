package nl.fews.archivedatabase.mongodb.migrate.utils;

import com.mongodb.client.AggregateIterable;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.utils.BucketUtil;
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
	 * @param fields fields
	 * @param collection collection
	 * @return List<Document>
	 */
	public static List<Document> getTimeSeriesGroups(List<String> fields, String collection){
		List<Document> timeSeriesGroups = new ArrayList<>();
		Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).aggregate(List.of(
				new Document("$sort", new Document(fields.stream().collect(Collectors.toMap(s -> s, s -> 1)))),
				new Document("$group", new Document("_id", new Document(fields.stream().collect(Collectors.toMap(s -> s, s -> String.format("$%s", s)))))),
				new Document("$replaceRoot", new Document("newRoot", "$_id"))
		)).allowDiskUse(true).forEach(timeSeriesGroups::add);
		return timeSeriesGroups;
	}

	/**
	 *
	 * @param timeSeriesGroup timeSeriesGroup
	 * @param collection collection
	 */
	public static void removeTimeSeriesDocuments(Document timeSeriesGroup, String collection){
		Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).deleteMany(timeSeriesGroup);
	}

	/**
	 *
	 * @param timeSeriesGroup timeSeriesGroup
	 * @param timeSeriesBuckets timeSeriesBuckets
	 * @param collection collection
	 */
	public static List<Document> getTimeSeriesDocuments(Document timeSeriesGroup, Map<Integer, List<Document>> timeSeriesBuckets, String collection){
		List<Document> timeSeriesDocuments = new ArrayList<>();
		Document baseDocument = Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).find(timeSeriesGroup).projection(new Document("timeseries", 0).append("_id", 0).append("localStartTime", 0).append("localEndTime", 0)).limit(1).first();
		if(baseDocument != null){
			String baseDocumentJson = baseDocument.toJson();
			timeSeriesBuckets.forEach((b, t) -> {
				Document document = Document.parse(baseDocumentJson);
				document.append("bucket", b);
				document.append("timeseries", t);
				document.append("startTime", t.get(0).getDate("t"));
				document.append("endTime", t.get(t.size() - 1).getDate("t"));
				if (t.get(0).containsKey("localStartTime")) document.append("localStartTime", t.get(0).getDate("lt"));
				if (t.get(0).containsKey("localEndTime")) document.append("localEndTime", t.get(t.size() - 1).getDate("lt"));
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
		Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).insertMany(timeSeriesDocuments);
	}

	/**
	 *
	 * @param timeSeriesGroup timeSeriesGroup
	 * @param collection collection
	 * @return List<Document>
	 */
	public static List<Document> getTimeSeries(Document timeSeriesGroup, String collection){
		List<Document> timeSeries = new ArrayList<>();
		AggregateIterable<Document> results = Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).aggregate(List.of(
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
	 * @return List<Document>
	 */
	public static List<Document> getCollapsedTimeSeries(List<Document> timeSeries){
		List<Document> collapsedTimeSeries = new ArrayList<>();
		int prev = -1;
		for (Document t: timeSeries) {
			if (
				prev == -1 || Double.compare(collapsedTimeSeries.get(prev).getDouble("v") == null ? Double.NaN : collapsedTimeSeries.get(prev).getDouble("v"), t.getDouble("v") == null ? Double.NaN : t.getDouble("v")) != 0) {
				collapsedTimeSeries.add(t);
				prev++;
			}
		}
		return collapsedTimeSeries;
	}

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param bucketSize bucketSize
	 * @return Map<Integer, List<Document>>
	 */
	public static Map<Integer, List<Document>> getTimeSeriesBuckets(List<Document> timeSeries, BucketSize bucketSize){
		Map<Integer, List<Document>> timeSeriesBuckets = new HashMap<>();
		for (Document t : timeSeries) {
			int b = BucketUtil.getBucketValue(t.getDate("t"), bucketSize);
			timeSeriesBuckets.putIfAbsent(b, new ArrayList<>());
			timeSeriesBuckets.get(b).add(t);
		}
		return timeSeriesBuckets;
	}

	/**
	 *
	 * @param timeSeriesGroup timeSeriesGroup
	 * @param collection collection
	 * @return List<Document>
	 */
	public static List<Document> getStitchedTimeSeries(Document timeSeriesGroup, String collection){
		Map<Date, Document> d = new HashMap<>();

		AggregateIterable<Document> results = Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).aggregate(List.of(
				new Document("$match", timeSeriesGroup),
				new Document("$sort", new Document("forecastTime", 1)),
				new Document("$project", new Document("_id", 0).append("timeseries", 1)))).allowDiskUse(true);

		for (Document result: results)
			d.putAll(result.getList("timeseries", Document.class).stream().collect(Collectors.toMap(x -> x.getDate("t"), x-> x)));

		return d.values().stream().sorted(Comparator.comparing(s -> s.getDate("t"))).collect(Collectors.toList());
	}
}