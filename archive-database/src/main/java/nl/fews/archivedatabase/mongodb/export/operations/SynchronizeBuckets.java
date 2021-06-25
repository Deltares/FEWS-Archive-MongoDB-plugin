package nl.fews.archivedatabase.mongodb.export.operations;

import nl.fews.archivedatabase.mongodb.export.interfaces.Synchronize;
import nl.fews.archivedatabase.mongodb.export.utils.DatabaseBucketUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import org.bson.Document;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public final class SynchronizeBuckets extends SynchronizeBase implements Synchronize {

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param collection collection
	 * @param keys keys
	 * @return Triplet<List<Document>, List<Document>, List<Document>>
	 */
	@Override
	protected Triplet<List<Document>, List<Document>, List<Document>> synchronize(List<Document> timeSeries, String collection, List<String> keys){
		List<Document> insert = new ArrayList<>();
		List<Document> replace = new ArrayList<>();
		List<Document> remove = new ArrayList<>();

		DatabaseBucketUtil.getDocumentsByKeyBucket(timeSeries, keys).forEach((key, buckets) -> buckets.forEach((bucket, documents) -> {
			BucketSize bucketSize = bucket.getValue0();
			long bucketValue = bucket.getValue1();

			Document document = new Document(documents.get(documents.size() - 1)).append("bucketSize", bucketSize.toString()).append("bucket", bucketValue);
			Document existingDocument = Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).find(
					new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get, (k, v) -> v, LinkedHashMap::new))).append("bucketSize", bucketSize.toString()).append("bucket", bucketValue)).first();

			if (existingDocument == null) {
				document = DatabaseBucketUtil.mergeDocuments(bucket.getValue1(), document.append("timeseries", new ArrayList<Document>()), documents);
				if (!document.getList("timeseries", Document.class).isEmpty())
					insert.add(document);
			}
			else {
				document.get("metaData", Document.class).append("archiveTime", existingDocument.get("metaData", Document.class).get("archiveTime"));
				document = DatabaseBucketUtil.mergeDocuments(bucket.getValue1(), document.append("_id", existingDocument.get("_id")).append("timeseries", existingDocument.get("timeseries")), documents);
				if (document.getList("timeseries", Document.class).isEmpty())
					remove.add(document);
				else
					replace.add(document);
			}
		}));
		return new Triplet<>(insert, replace, remove);
	}
}
