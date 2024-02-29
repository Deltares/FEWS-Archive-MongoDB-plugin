package nl.fews.archivedatabase.mongodb.export.utils;

import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.utils.BucketUtil;
import org.bson.Document;
import org.javatuples.Pair;

import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public final class DatabaseBucketUtil {

	/**
	 * Static Class
	 */
	private DatabaseBucketUtil(){}

	/**
	 * merges the passed documents with the existing (or new) bucketedDocument for the given bin (bucket) - after deleting any existing values between (inclusive) each document timeseries event's date range
	 * updates startTime, endTime, localStartTime, and localEndTime accordingly - leaving the returned document fully self-consistent
	 * @param existingDocument a populated document matching last passed bucket-intersecting document, preserving the _id, timeseries, and metaData.archiveTime fields, if already existing in mongo db
	 * @param document the timeseries-event-sorted, intersecting documents (at least one timeseries event entry time entry intersects the bucket)
	 * @return a single Document representing the merged documents passes + any existing values, if this bucket was already in mongo db
	 */
	public static Document mergeExistingDocument(Document existingDocument, Document document){
		existingDocument.append("timeseries", getNonIntersectingExistingTimeseries(existingDocument, document));
		Map<Date, Document> timeSeries = existingDocument.getList("timeseries", Document.class).stream().collect(Collectors.toMap(s -> s.getDate("t"), s -> s));
		for (Document event:document.getList("timeseries", Document.class)){
			timeSeries.put(event.getDate("t"), event);
		}
		if(timeSeries.isEmpty())
			return existingDocument;

		List<Document> sortedTimeseries = timeSeries.values().stream().sorted(Comparator.comparing(s -> s.getDate("t"))).toList();

		existingDocument.append("startTime", sortedTimeseries.get(0).getDate("t"));
		existingDocument.append("endTime", sortedTimeseries.get(sortedTimeseries.size()-1).getDate("t"));
		if(sortedTimeseries.get(0).containsKey("lt")) existingDocument.append("localStartTime", sortedTimeseries.get(0).getDate("lt"));
		if(sortedTimeseries.get(sortedTimeseries.size()-1).containsKey("lt")) existingDocument.append("localEndTime", sortedTimeseries.get(sortedTimeseries.size()-1).getDate("lt"));
		existingDocument.append("timeseries", sortedTimeseries);

		return existingDocument;
	}

	/**
	 * Gets the bucket intersecting documents for each set of mongo unique key fields (less the bucket).
	 * This will result in documents being used multiple times for each bucket they intersect.
	 * The entire document is needed to determine the original range within the timeseries for deletions
	 * @param timeSeries the entire list of all documents passed to this instance
	 * @param bucketCollection the document key fields matching the database unique collection key
	 * @return Map<String, Map<Integer, List<Document>>>
	 *     JSON string key representation of mongo unique key fields (less the bucket) =>
	 *     distinct buckets having the parent key =>
	 *     all documents having timeseries range that intersects the parent bucket.
	 */
	public static Map<String, Map<Pair<BucketSize, Long>, Document>> getDocumentsByKeyBucket(Document timeSeries, String bucketCollection){
		Map<String, Map<Pair<BucketSize, Long>, Document>> keyBucketDocuments = new HashMap<>();

		String bucketKey = Database.getKey(Database.getKeyDocument(BucketUtil.getBucketKeyFields(bucketCollection), timeSeries));
		BucketSize bucketSize = timeSeries.getString("encodedTimeStepId").equals("NETS") ?
				BucketUtil.getNetsBucketSize(bucketCollection, bucketKey) :
				BucketUtil.getBucketSize(timeSeries.get("metaData", Document.class).getInteger("timeStepMinutes"));

		keyBucketDocuments.putIfAbsent(bucketKey, new HashMap<>());
		timeSeries.getList("timeseries", Document.class).forEach(s -> {
			Pair<BucketSize, Long> bucket = new Pair<>(bucketSize, BucketUtil.getBucketValue(s.getDate("t"), bucketSize));
			if(!keyBucketDocuments.get(bucketKey).containsKey(bucket)){
				keyBucketDocuments.get(bucketKey).put(bucket, new Document(timeSeries).append("timeseries", new ArrayList<Document>()));
			}
			keyBucketDocuments.get(bucketKey).get(bucket).getList("timeseries", Document.class).add(s);
		});

		return keyBucketDocuments;
	}

	/**
	 * returns any existing values outside the min and max range of the new incoming values.
	 * This effectively accomplishes deletes where any value missing between the min and max
	 * range of newly passed insert / updates will be removed.
	 * @param existingDocument the existing timeseries document, if found, else the last passed document
	 * @param document the timeseries documents having a range of values that intersect the existing document
	 */
	public static List<Document> getNonIntersectingExistingTimeseries(Document existingDocument, Document document){
		Map<Date, Document> existingTimeseries = existingDocument.getList("timeseries", Document.class).stream().collect(Collectors.toMap(s -> s.getDate("t"), s -> s));
		List<Date> existingDates = new ArrayList<>(existingTimeseries.keySet());

		List<Document> events = document.getList("timeseries", Document.class);
		Date[] dateRange = new Date[]{events.get(0).getDate("t"), events.get(events.size()-1).getDate("t")};

		List<Document> trimmedExistingTimeseries = new ArrayList<>();
		for (Date existingDate:existingDates) {
			if(existingDate.compareTo(dateRange[0]) < 0 || existingDate.compareTo(dateRange[1]) > 0){
				trimmedExistingTimeseries.add(existingTimeseries.get(existingDate));
			}
		}
		return trimmedExistingTimeseries;
	}
}
