package nl.fews.archivedatabase.mongodb.export.utils;

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
	 * @param bucketValue the year or year-month integer-represented bin or 'bucket' over which to merge documents from intersecting timeseries
	 * @param existingDocument a populated document matching last passed bucket-intersecting document, preserving the _id, timeseries, and metaData.archiveTime fields, if already existing in mongo db
	 * @param documents the timeseries-event-sorted, intersecting documents (at least one timeseries event entry time entry intersects the bucket)
	 * @return a single Document representing the merged documents passes + any existing values, if this bucket was already in mongo db
	 */
	public static Document mergeDocuments(long bucketValue, Document existingDocument, List<Document> documents, String bucketCollection){
		removeExistingTimeseries(existingDocument, documents);
		Map<Date, Document> timeSeries = existingDocument.getList("timeseries", Document.class).stream().collect(Collectors.toMap(s -> s.getDate("t"), s -> s));
		for (Document document: documents) {
			BucketSize bucketSize = document.getString("encodedTimeStepId").equals("NETS") ?
					BucketUtil.getNetsBucketSize(bucketCollection, BucketUtil.getBucketKey(bucketCollection, document)) :
					BucketUtil.getBucketSize(document.get("metaData", Document.class).getInteger("timeStepMinutes"));
			for (Document event:document.getList("timeseries", Document.class)){
				if (bucketValue == BucketUtil.getBucketValue(event.getDate("t"), bucketSize)){
					timeSeries.put(event.getDate("t"), event);
				}
			}
		}
		if(timeSeries.isEmpty())
			return existingDocument;

		List<Document> sortedTimeseries = timeSeries.values().stream().sorted(Comparator.comparing(s -> s.getDate("t"))).collect(Collectors.toList());

		existingDocument.append("startTime", sortedTimeseries.get(0).getDate("t"));
		existingDocument.append("endTime", sortedTimeseries.get(sortedTimeseries.size()-1).getDate("t"));
		if(existingDocument.containsKey("localStartTime")) existingDocument.append("localStartTime", sortedTimeseries.get(0).getDate("lt"));
		if(existingDocument.containsKey("localEndTime")) existingDocument.append("localEndTime", sortedTimeseries.get(sortedTimeseries.size()-1).getDate("lt"));
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
	public static Map<String, Map<Pair<BucketSize, Long>, List<Document>>> getDocumentsByKeyBucket(List<Document> timeSeries, String bucketCollection){
		Map<String, Map<Pair<BucketSize, Long>, List<Document>>> keyBucketDocuments = new HashMap<>();
		for (Document document:timeSeries) {
			String bucketKey = BucketUtil.getBucketKey(bucketCollection, document);
			BucketSize bucketSize = document.getString("encodedTimeStepId").equals("NETS") ?
					BucketUtil.getNetsBucketSize(bucketCollection, bucketKey) :
					BucketUtil.getBucketSize(document.get("metaData", Document.class).getInteger("timeStepMinutes"));
			List<Pair<BucketSize, Long>> buckets = document.getList("timeseries", Document.class).stream().map(s -> new Pair<>(bucketSize, BucketUtil.getBucketValue(s.getDate("t"), bucketSize))).distinct().collect(Collectors.toList());

			keyBucketDocuments.putIfAbsent(bucketKey, new HashMap<>());
			for (Pair<BucketSize, Long> bucket:buckets){
				keyBucketDocuments.get(bucketKey).putIfAbsent(bucket, new ArrayList<>());
				keyBucketDocuments.get(bucketKey).get(bucket).add(document);
			}
		}
		return keyBucketDocuments;
	}

	/**
	 * in-place deletes any existing values between (inclusive) the min and max range of new incoming values.
	 * This effectively accomplishes deletes where any value missing between the min and max range of newly
	 * passed insert / updates will be removed.
	 * @param existingDocument the existing timeseries document, if found, else the last passed document
	 * @param documents the timeseries documents having a range of values that intersect the existing document
	 */
	public static void removeExistingTimeseries(Document existingDocument, List<Document> documents){
		Map<Date, Document> timeseries = existingDocument.getList("timeseries", Document.class).stream().collect(Collectors.toMap(s -> s.getDate("t"), s -> s));
		List<Date[]> dateRanges = documents.stream().map(s -> s.getList("timeseries", Document.class)).map(s -> new Date[]{s.get(0).getDate("t"), s.get(s.size()-1).getDate("t")}).collect(Collectors.toList());
		List<Date> dates = new ArrayList<>(timeseries.keySet());

		List<Document> trimmedTimeseries = new ArrayList<>();
		for (Date date:dates) {
			if(dateRanges.stream().noneMatch(s -> date.compareTo(s[0]) >= 0 && date.compareTo(s[1]) <= 0)){
				trimmedTimeseries.add(timeseries.get(date));
			}
		}
		existingDocument.append("timeseries", trimmedTimeseries);
	}
}
