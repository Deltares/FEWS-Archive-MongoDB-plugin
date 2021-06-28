package nl.fews.archivedatabase.mongodb.export.operations;

import nl.fews.archivedatabase.mongodb.export.interfaces.Synchronize;
import nl.fews.archivedatabase.mongodb.export.utils.DatabaseSingletonUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import org.bson.Document;
import org.javatuples.Triplet;

import java.util.*;
import java.util.stream.Collectors;

public final class SynchronizeSingletons extends SynchronizeBase implements Synchronize {

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param collection collection
	 * @param keys keys
	 * @return Triplet<List<Document>, List<Document>, List<Document>>
	 */
	@Override
	protected Triplet<List<Document>, List<Document>, List<Document>> synchronize(List<Document> timeSeries, String collection, List<String> keys){
		Map<String, Document> existingDocuments = getExistingDocuments(collection, getExistingQueries(timeSeries, keys), keys);

		List<Document> insert = new ArrayList<>();
		List<Document> replace = new ArrayList<>();
		List<Document> remove = new ArrayList<>();

		DatabaseSingletonUtil.getDocumentsByKey(timeSeries, keys).forEach((key, documents) -> {
			Document document = documents.get(documents.size() - 1);
			String existingQuery = new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get, (k, v) -> v, LinkedHashMap::new))).toJson();

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
		return new Triplet<>(insert, replace, remove);
	}

	/**
	 *
	 * @param collection collection
	 * @param existingQueries existingQueries
	 * @param keys keys
	 * @return existingDocuments
	 */
	private static Map<String, Document> getExistingDocuments(String collection, List<Document> existingQueries, List<String> keys){
		Map<String, Document> existingDocuments = new HashMap<>();
		if(!existingQueries.isEmpty()) {
			for (Document document : Database.find(collection, new Document("$or", existingQueries), new Document("timeseries", 0))) {
				String key = new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get, (k, v) -> v, LinkedHashMap::new))).toJson();
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
	private static List<Document> getExistingQueries(List<Document> timeSeries, List<String> keys){
		List<Document> existingQueries = new ArrayList<>();
		for (Map.Entry<String, List<Document>> key : DatabaseSingletonUtil.getDocumentsByKey(timeSeries, keys).entrySet()) {
			Document document = key.getValue().get(key.getValue().size() - 1);
			existingQueries.add(new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get, (k, v) -> v, LinkedHashMap::new))));
		}
		return existingQueries;
	}
}
