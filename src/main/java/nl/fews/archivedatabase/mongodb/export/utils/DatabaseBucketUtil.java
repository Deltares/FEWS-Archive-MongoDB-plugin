package nl.fews.archivedatabase.mongodb.export.utils;

import nl.fews.archivedatabase.mongodb.export.enums.BucketSize;
import org.bson.Document;
import org.json.JSONArray;

import java.util.*;
import java.util.stream.Collectors;

public class DatabaseBucketUtil {

    private DatabaseBucketUtil(){
    }

    /**
     * merges the passed documents with the existing (or new) bucketedDocument for the given bin (bucket) - after deleting any existing values between (inclusive) each document timeseries event's date range
     * updates startTime, endTime, localStartTime, and localEndTime accordingly - leaving the returned document fully self-consistent
     * @param bucket the year or year-month integer-represented bin or 'bucket' over which to merge documents from intersecting timeseries
     * @param existingDocument a populated document matching last passed bucket-intersecting document, preserving the _id, timeseries, and metaData.archiveTime fields, if already existing in mongo db
     * @param documents the timeseries-event-sorted, intersecting documents (at least one timeseries event entry time entry intersects the bucket)
     * @return a single Document representing the merged documents passes + any existing values, if this bucket was already in mongo db
     */
    public static Document mergeDocuments(int bucket, Document existingDocument, List<Document> documents){
        removeExistingTimeseries(existingDocument, documents);
        Map<Date, Document> timeSeries = existingDocument.getList("timeseries", Document.class).stream().collect(Collectors.toMap(s -> s.getDate("t"), s -> s));
        for (Document document: documents) {
            BucketSize bucketSize = BucketUtil.getBucketSize(document);
            for (Document event:document.getList("timeseries", Document.class)){
                if (bucket == BucketUtil.getBucket(event.getDate("t"), bucketSize)){
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
     * @param timeseries the entire list of all documents passed to this instance
     * @param keys the document key fields matching the database unique collection key
     * @return Map<String, Map<Integer, List<Document>>>
     *     JSON string key representation of mongo unique key fields (less the bucket) =>
     *     distinct buckets having in the parent key =>
     *     all documents having timeseries range that intersects the parent bucket.
     */
    public static Map<String, Map<Integer, List<Document>>> getDocumentsByKeyBucket(List<Document> timeseries, List<String> keys){
        keys = new ArrayList<>(keys);
        keys.remove("bucket");

        Map<String, Map<Integer, List<Document>>> keyBucketDocuments = new HashMap<>();
        for (Document document:timeseries) {
            BucketSize bucketSize = BucketUtil.getBucketSize(document);
            String key = new JSONArray(keys.stream().map(document::get).collect(Collectors.toList())).toString();
            List<Integer> buckets = document.getList("timeseries", Document.class).stream().map(s -> BucketUtil.getBucket(s.getDate("t"), bucketSize)).distinct().collect(Collectors.toList());

            keyBucketDocuments.computeIfAbsent(key, x -> new HashMap<>());
            for (Integer bucket:buckets){
                keyBucketDocuments.get(key).computeIfAbsent(bucket, x -> new ArrayList<>());
                keyBucketDocuments.get(key).get(bucket).add(document);
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
        LinkedList<Document> timeseries = new LinkedList<>(existingDocument.getList("timeseries", Document.class));
        for (int i = 0; i < timeseries.size(); i++) {
            Date existingDate = timeseries.get(i).getDate("t");
            if(documents.stream().anyMatch(s ->
                    existingDate.compareTo(s.getList("timeseries", Document.class).get(0).getDate("t")) >= 0 &&
                    existingDate.compareTo(s.getList("timeseries", Document.class).get(s.getList("timeseries", Document.class).size()-1).getDate("t")) <= 0)){
                timeseries.remove(i--);
            }
        }
        existingDocument.append("timeseries", new ArrayList<>(timeseries));
    }
}
