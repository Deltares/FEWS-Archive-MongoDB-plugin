package nl.fews.archivedatabase.mongodb.export.operations;

import nl.fews.archivedatabase.mongodb.export.interfaces.Synchronize;
import nl.fews.archivedatabase.mongodb.export.utils.DatabaseSingletonUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.TimeSeriesUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.utils.BucketUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class SynchronizeSingletons implements Synchronize {

	/**
	 * Inserts, updates or replaces data for forecast timeseries
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

		DatabaseSingletonUtil.getDocumentsByKey(timeSeries, keys).forEach((key, documents) -> {
			Document document = documents.get(documents.size() - 1);
			String existingQuery = new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get))).toJson();

			boolean collapse = BucketUtil.getBucket(document).getBoolean("collapse");
			if(collapse)
				document.append("timeseries", TimeSeriesUtil.getCollapsedTimeSeries(document.getList("timeseries", Document.class)));

			Document existingDocument = existingDocuments.getOrDefault(existingQuery, null);
			if (existingDocument == null) {
				if(!document.getList("timeseries", Document.class).isEmpty())
					insert.add(document);
			}
			else {
				document.get("metaData", Document.class).append("archiveTime", existingDocument.get("metaData", Document.class).getDate("archiveTime"));
				document.append("_id", existingDocument.get("_id"));
				if(document.getList("timeseries", Document.class).isEmpty())
					remove.add(document);
				else
					replace.add(document);
			}
		});
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
			for (Document document : Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).find(new Document("$or", existingQueries)).projection(new Document("timeseries", 0))) {
				String key = new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get))).toJson();
				if(document.get("metaData", Document.class).getDate("archiveTime").compareTo(existingDocuments.getOrDefault(key, document).get("metaData", Document.class).getDate("archiveTime")) >= 0)
					existingDocuments.put(key, document);
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
		for (Map.Entry<String, List<Document>> key : DatabaseSingletonUtil.getDocumentsByKey(timeSeries, keys).entrySet()) {
			Document document = key.getValue().get(key.getValue().size() - 1);
			existingQueries.add(new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get))));
		}
		return existingQueries;
	}
}
