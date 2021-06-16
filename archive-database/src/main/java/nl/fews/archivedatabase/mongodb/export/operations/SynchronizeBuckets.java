package nl.fews.archivedatabase.mongodb.export.operations;

import nl.fews.archivedatabase.mongodb.export.interfaces.Synchronize;
import nl.fews.archivedatabase.mongodb.export.utils.DatabaseBucketUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class SynchronizeBuckets implements Synchronize {

	/**
	 * Inserts, updates or replaces data for bucketed (observed) timeseries
	 * @param timeSeries the entire list of all documents passed to this instance
	 * @param timeSeriesType timeSeriesType
	 */
	public void synchronize(List<Document> timeSeries, TimeSeriesType timeSeriesType){
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		List<String> keys = Database.getCollectionKeys(collection);

		Map<String, Document> existingDocuments = getExistingDocuments(collection, getExistingQueries(timeSeries, keys), keys);

		List<Document> insert = new ArrayList<>();
		List<Document> replace = new ArrayList<>();
		List<Document> remove = new ArrayList<>();

		DatabaseBucketUtil.getDocumentsByKeyBucket(timeSeries, keys).forEach((key, buckets) -> buckets.forEach((bucketValue, documents) -> {
			Document document = new Document(documents.get(documents.size() - 1)).append("bucket", bucketValue);
			String existingQuery = new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get))).append("bucket", bucketValue).toJson();

			Document existingDocument = existingDocuments.getOrDefault(existingQuery, null);
			if (existingDocument == null) {
				document = DatabaseBucketUtil.mergeDocuments(bucketValue, document.append("timeseries", new ArrayList<Document>()), documents);
				if(!document.getList("timeseries", Document.class).isEmpty())
					insert.add(document);
			}
			else {
				document.get("metaData", Document.class).append("archiveTime", existingDocument.get("metaData", Document.class).get("archiveTime"));
				document = DatabaseBucketUtil.mergeDocuments(bucketValue, document.append("_id", existingDocument.get("_id")).append("timeseries", existingDocument.get("timeseries")), documents);
				if(document.getList("timeseries", Document.class).isEmpty())
					remove.add(document);
				else
					replace.add(document);
			}
		}));
		Database.synchronize(collection, insert, replace, remove);
	}

	/**
	 *
	 * @param collection collection
	 * @param existingQueries existingQueries
	 * @param keys keys
	 * @return existingDocuments
	 */
	private Map<String, Document> getExistingDocuments(String collection, List<Document> existingQueries, List<String> keys){
		Map<String, Document> existingDocuments = new HashMap<>();
		if(!existingQueries.isEmpty()) {
			for (Document document : Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).find(new Document("$or", existingQueries))) {
				existingDocuments.put(new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get))).toJson(), document);
			}
		}
		return existingDocuments;
	}

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param keys keys
	 * @return existingQueries
	 */
	private List<Document> getExistingQueries(List<Document> timeSeries, List<String> keys){
		List<Document> existingQueries = new ArrayList<>();
		for (Map.Entry<String, Map<Integer, List<Document>>> key : DatabaseBucketUtil.getDocumentsByKeyBucket(timeSeries, keys).entrySet()) {
			for (Map.Entry<Integer, List<Document>> bucket : key.getValue().entrySet()) {
				Document document = new Document(bucket.getValue().get(bucket.getValue().size() - 1)).append("bucket", bucket.getKey());
				existingQueries.add(new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get))).append("bucket", bucket.getKey()));
			}
		}
		return existingQueries;
	}
}
