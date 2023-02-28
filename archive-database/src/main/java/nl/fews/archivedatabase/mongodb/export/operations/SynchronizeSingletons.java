package nl.fews.archivedatabase.mongodb.export.operations;

import nl.fews.archivedatabase.mongodb.export.interfaces.Synchronize;
import nl.fews.archivedatabase.mongodb.export.utils.DatabaseSingletonUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;
import org.javatuples.Triplet;

import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public final class SynchronizeSingletons extends SynchronizeBase implements Synchronize {

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param timeSeriesType timeSeriesType
	 * @return Triplet<List<Document>, List<Document>, List<Document>>
	 */
	@Override
	protected Map<String, Triplet<List<Document>, List<Document>, List<Document>>> getInsertUpdateRemove(Document timeSeries, TimeSeriesType timeSeriesType){
		Map<String, Triplet<List<Document>, List<Document>, List<Document>>> insertUpdateRemove = new HashMap<>();
		Map<String, Document> existingDocuments = getExistingDocuments(getExistingQueries(timeSeries, timeSeriesType), timeSeriesType);

		DatabaseSingletonUtil.getDocumentsByKey(timeSeries, timeSeriesType).forEach((key, documents) -> {
			List<Document> insert = new ArrayList<>();
			List<Document> replace = new ArrayList<>();
			List<Document> remove = new ArrayList<>();

			Document document = documents.get(documents.size() - 1);
			Document existingDocument = existingDocuments.getOrDefault(key, null);
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
			insertUpdateRemove.put(key, new Triplet<>(insert, replace, remove));
		});
		return insertUpdateRemove;
	}

	/**
	 *
	 * @param existingQueries existingQueries
	 * @param timeSeriesType timeSeriesType
	 * @return existingDocuments
	 */
	private static Map<String, Document> getExistingDocuments(List<Document> existingQueries, TimeSeriesType timeSeriesType){
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		List<String> keys = Database.getCollectionKeys(collection);
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
	 * @param timeSeriesType timeSeriesType
	 * @return existingQueries
	 */
	private static List<Document> getExistingQueries(Document timeSeries, TimeSeriesType timeSeriesType){
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		List<String> keys = Database.getCollectionKeys(collection);
		List<Document> existingQueries = new ArrayList<>();
		for (Map.Entry<String, List<Document>> key : DatabaseSingletonUtil.getDocumentsByKey(timeSeries, timeSeriesType).entrySet()) {
			Document document = key.getValue().get(key.getValue().size() - 1);
			existingQueries.add(new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get, (k, v) -> v, LinkedHashMap::new))));
		}
		return existingQueries;
	}
}
