package nl.fews.archivedatabase.mongodb.export.operations;

import nl.fews.archivedatabase.mongodb.export.interfaces.Synchronize;
import nl.fews.archivedatabase.mongodb.export.utils.DatabaseBucketUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.utils.BucketUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;
import org.javatuples.Triplet;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public final class SynchronizeBuckets extends SynchronizeBase implements Synchronize {

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param timeSeriesType timeSeriesType
	 * @return Triplet<List<Document>, List<Document>, List<Document>>
	 */
	@Override
	protected Map<String, Triplet<List<Document>, List<Document>, List<Document>>> getInsertUpdateRemove(List<Document> timeSeries, TimeSeriesType timeSeriesType){
		String bucketCollection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		boolean bucketResized = false;

		Map<String, Triplet<List<Document>, List<Document>, List<Document>>> insertUpdateRemove = _getInsertUpdateRemove(timeSeries, timeSeriesType);
		for (Map.Entry<String, Triplet<List<Document>, List<Document>, List<Document>>> e: insertUpdateRemove.entrySet()) {
			if (TimeSeriesTypeUtil.getTimeSeriesTypeBucket(timeSeriesType)) {
				List<Document> insert = e.getValue().getValue0();
				List<Document> replace = e.getValue().getValue1();
				BucketSize bucketSize = BucketUtil.ensureBucketSize(bucketCollection, Stream.of(insert, replace).flatMap(Collection::stream).collect(Collectors.toList()));
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
	 * @return Triplet<List<Document>, List<Document>, List<Document>>
	 */
	private Map<String, Triplet<List<Document>, List<Document>, List<Document>>> _getInsertUpdateRemove(List<Document> timeSeries, TimeSeriesType timeSeriesType){
		String bucketCollection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		List<String> keys = Database.getCollectionKeys(bucketCollection);
		Map<String, Triplet<List<Document>, List<Document>, List<Document>>> insertUpdateRemove = new HashMap<>();
		DatabaseBucketUtil.getDocumentsByKeyBucket(timeSeries, bucketCollection).forEach((key, buckets) -> {

			List<Document> insert = new ArrayList<>();
			List<Document> replace = new ArrayList<>();
			List<Document> remove = new ArrayList<>();
			buckets.forEach((bucket, documents) -> {

				BucketSize bucketSize = bucket.getValue0();
				long bucketValue = bucket.getValue1();

				Document document = new Document(documents.get(documents.size() - 1)).append("bucketSize", bucketSize.toString()).append("bucket", bucketValue);
				Document existingDocument = Database.findOne(bucketCollection, new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get, (k, v) -> v, LinkedHashMap::new))).append("bucketSize", bucketSize.toString()).append("bucket", bucketValue));

				if (existingDocument == null) {
					document = DatabaseBucketUtil.mergeDocuments(bucket.getValue1(), document.append("timeseries", new ArrayList<Document>()), documents, bucketCollection);
					if (!document.getList("timeseries", Document.class).isEmpty())
						insert.add(document);
				}
				else {
					document.get("metaData", Document.class).append("archiveTime", existingDocument.get("metaData", Document.class).get("archiveTime"));
					document = DatabaseBucketUtil.mergeDocuments(bucket.getValue1(), document.append("_id", existingDocument.get("_id")).append("timeseries", existingDocument.get("timeseries")), documents, bucketCollection);
					if (document.getList("timeseries", Document.class).isEmpty())
						remove.add(document);
					else
						replace.add(document);
				}
			});
			insertUpdateRemove.put(key, new Triplet<>(insert, replace, remove));
		});
		return insertUpdateRemove;
	}
}
