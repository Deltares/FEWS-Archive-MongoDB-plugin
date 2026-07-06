package nl.fews.archivedatabase.common.export.operations;

import nl.fews.archivedatabase.common.export.interfaces.Synchronize;
import nl.fews.archivedatabase.common.shared.database.Index;
import nl.fews.archivedatabase.common.shared.enums.BucketSize;
import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.tuple.Pair;
import nl.fews.archivedatabase.common.shared.tuple.Triplet;
import nl.fews.archivedatabase.common.shared.utils.BucketUtil;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesTypeUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({"unused", "unchecked"})
public final class SynchronizeBuckets extends SynchronizeBase implements Synchronize {

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param timeSeriesType timeSeriesType
	 * @return Triplet<List<Map<String, Object>>, List<Map<String, Object>>, List<Map<String, Object>>>
	 */
	@Override
	protected Map<String, Triplet<List<Map<String, Object>>, List<Map<String, Object>>, List<Map<String, Object>>>> getInsertUpdateRemove(Map<String, Object> timeSeries, TimeSeriesType timeSeriesType){
		var bucketCollection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		var bucketResized = false;

		var insertUpdateRemove = _getInsertUpdateRemove(timeSeries, timeSeriesType);
		for (var e: insertUpdateRemove.entrySet()) {
			if (TimeSeriesTypeUtil.getTimeSeriesTypeBucket(timeSeriesType)) {
				var insert = e.getValue().getValue0();
				var replace = e.getValue().getValue1();
				var bucketSize = BucketUtil.ensureBucketSize(bucketCollection, Stream.of(insert, replace).flatMap(Collection::stream).toList());
				bucketResized = bucketResized || bucketSize != null;
			}
		}

		if(bucketResized)
			insertUpdateRemove = getInsertUpdateRemove(timeSeries, timeSeriesType);

		return insertUpdateRemove;
	}

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param timeSeriesType timeSeriesType
	 * @return Triplet<List<Map<String, Object>>, List<Map<String, Object>>, List<Map<String, Object>>>
	 */
	private Map<String, Triplet<List<Map<String, Object>>, List<Map<String, Object>>, List<Map<String, Object>>>> _getInsertUpdateRemove(Map<String, Object> timeSeries, TimeSeriesType timeSeriesType){
		var bucketCollection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		var insertUpdateRemove = new ConcurrentHashMap<String, Triplet<List<Map<String, Object>>, List<Map<String, Object>>, List<Map<String, Object>>>>();

		getDocumentsByKeyBucket(timeSeries, bucketCollection).forEach((key, buckets) -> {

			var insert = new ArrayList<Map<String, Object>>();
			var replace = new ArrayList<Map<String, Object>>();
			var remove = new ArrayList<Map<String, Object>>();

			buckets.forEach((bucket, document) -> {

				var bucketSize = bucket.getValue0();
				var bucketValue = bucket.getValue1();

				document.put("bucketSize", bucketSize.toString());
				document.put("bucket", bucketValue);

				var query = Index.getKeyDocument(Index.getKeys(bucketCollection), document);
				query.put("bucketSize", bucketSize.toString());
				query.put("bucket", bucketValue);
				var existingDocument = database.findOne(bucketCollection, query);

				if (existingDocument == null) {
					var d = new LinkedHashMap<>(document);
					d.put("timeseries", new ArrayList<Map<String, Object>>());
					var mergedDocument = mergeExistingDocument(d, document);
					if (!((List<Map<?, ?>>)mergedDocument.get("timeseries")).isEmpty())
						insert.add(mergedDocument);
				}
				else {
					((Map<String, Object>)document.get("metaData")).put("archiveTime", ((Map<String, Object>)existingDocument.get("metaData")).get("archiveTime"));
					var d = new LinkedHashMap<>(document);
					d.put("_id", existingDocument.get("_id"));
					d.put("timeseries", existingDocument.get("timeseries"));
					var mergedDocument = mergeExistingDocument(d, document);
					if (((List<Map<?, ?>>)mergedDocument.get("timeseries")).isEmpty())
						remove.add(mergedDocument);
					else
						replace.add(mergedDocument);
				}
			});
			insertUpdateRemove.put(key, new Triplet<>(insert, replace, remove));
		});
		return insertUpdateRemove;
	}

	/**
	 * Gets the bucket intersecting documents for each set of unique key fields (less the bucket).
	 * This will result in documents being used multiple times for each bucket they intersect.
	 * The entire document is needed to determine the original range within the timeseries for deletions
	 * @param timeSeries the entire list of all documents passed to this instance
	 * @param bucketCollection the document key fields matching the database unique collection key
	 * @return Map<String, Map<Integer, List<Map<String, Object>>>>
	 *     JSON string key representation of unique key fields (less the bucket) =>
	 *     distinct buckets having the parent key =>
	 *     all documents having timeseries range that intersects the parent bucket.
	 */
	private Map<String, Map<Pair<BucketSize, Long>, Map<String, Object>>> getDocumentsByKeyBucket(Map<String, Object> timeSeries, String bucketCollection){
		var keyBucketDocuments = new LinkedHashMap<String, Map<Pair<BucketSize, Long>, Map<String, Object>>>();

		var bucketKey = Index.getKey(Index.getKeyDocument(BucketUtil.getBucketKeyFields(bucketCollection), timeSeries));
		var bucketSize = timeSeries.get("encodedTimeStepId").equals("NETS") ?
			BucketUtil.getNetsBucketSize(bucketCollection, bucketKey) :
			BucketUtil.getBucketSize((int)((Map<?, ?>)timeSeries.get("metaData")).get("timeStepMinutes"));

		keyBucketDocuments.putIfAbsent(bucketKey, new LinkedHashMap<>());
		((List<Map<String, Object>>)timeSeries.get("timeseries")).forEach(s -> {
			Pair<BucketSize, Long> bucket = new Pair<>(bucketSize, BucketUtil.getBucketValue((Date)s.get("t"), bucketSize));
			if(!keyBucketDocuments.get(bucketKey).containsKey(bucket)){
				var t = new LinkedHashMap<>(timeSeries);
				t.put("timeseries", new ArrayList<Map<String, Object>>());
				keyBucketDocuments.get(bucketKey).put(bucket, t);
			}
			((List<Map<String, Object>>)keyBucketDocuments.get(bucketKey).get(bucket).get("timeseries")).add(s);
		});

		return keyBucketDocuments;
	}

	/**
	 * merges the passed documents with the existing (or new) bucketedDocument for the given bin (bucket) - after deleting any existing values between (inclusive) each document timeseries event's date range
	 * updates startTime, endTime, localStartTime, and localEndTime accordingly - leaving the returned document fully self-consistent
	 * @param existingDocument a populated document matching the last passed bucket-intersecting document, preserving the _id, timeseries, and metaData.archiveTime fields, if already existing in db
	 * @param document the timeseries-event-sorted, intersecting documents (at least one timeseries event entry time entry intersects the bucket)
	 * @return a single Map<String, Object> representing the merged documents passes + any existing values, if this bucket was already in db
	 */
	private Map<String, Object> mergeExistingDocument(Map<String, Object> existingDocument, Map<String, Object> document){
		existingDocument.put("timeseries", getNonIntersectingExistingTimeseries(existingDocument, document));
		var timeSeries = ((List<Map<String, Object>>)existingDocument.get("timeseries")).stream().collect(Collectors.toMap(s -> (Date)s.get("t"), s -> s));
		((List<Map<String, Object>>)document.get("timeseries")).forEach(event -> timeSeries.put((Date)event.get("t"), event));
		if(timeSeries.isEmpty())
			return existingDocument;

		var sortedTimeseries = timeSeries.values().stream().sorted(Comparator.comparing(s -> (Date)s.get("t"))).toList();

		existingDocument.put("startTime", sortedTimeseries.get(0).get("t"));
		existingDocument.put("endTime", sortedTimeseries.get(sortedTimeseries.size()-1).get("t"));
		if(sortedTimeseries.get(0).containsKey("lt")) existingDocument.put("localStartTime", sortedTimeseries.get(0).get("lt"));
		if(sortedTimeseries.get(sortedTimeseries.size()-1).containsKey("lt")) existingDocument.put("localEndTime", sortedTimeseries.get(sortedTimeseries.size()-1).get("lt"));
		existingDocument.put("timeseries", sortedTimeseries);

		return existingDocument;
	}

	/**
	 * returns any existing values outside the min and max range of the new incoming values.
	 * This effectively accomplishes deletes where any value missing between the min and max
	 * range of newly passed insert / updates will be removed.
	 * @param existingDocument the existing timeseries document, if found, else the last passed document
	 * @param document the timeseries documents having a range of values that intersect the existing document
	 */
	private List<Map<String, Object>> getNonIntersectingExistingTimeseries(Map<String, Object> existingDocument, Map<String, Object> document){
		var existingTimeseries = ((List<Map<String, Object>>)existingDocument.get("timeseries")).stream().collect(Collectors.toMap(s -> (Date)s.get("t"), s -> s));
		var existingDates = new ArrayList<>(existingTimeseries.keySet());

		var events = (List<Map<String, Object>>)document.get("timeseries");
		var dateRange = new Date[]{(Date)events.get(0).get("t"), (Date)events.get(events.size()-1).get("t")};

		var trimmedExistingTimeseries = new ArrayList<Map<String, Object>>();
		existingDates.forEach(existingDate -> {
			if(existingDate.compareTo(dateRange[0]) < 0 || existingDate.compareTo(dateRange[1]) > 0){
				trimmedExistingTimeseries.add(existingTimeseries.get(existingDate));
			}
		});
		return trimmedExistingTimeseries;
	}
}
