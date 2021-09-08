package nl.fews.archivedatabase.mongodb.export.utils;

import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

public class DatabaseSingletonUtil {

	/**
	 * Static Class
	 */
	private DatabaseSingletonUtil(){}

	/**
	 * Gets the documents for each set of mongo unique key fields.
	 * @param timeSeries the entire list of all documents passed to this instance
	 * @param timeSeriesType the document key fields matching the database unique collection key
	 * @return Map<String, List<Document>> document map keyed by their JSON string key representation of mongo unique key fields.
	 */
	public static Map<String, List<Document>> getDocumentsByKey(List<Document> timeSeries, TimeSeriesType timeSeriesType){
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		List<String> keys = Database.getCollectionKeys(collection);
		Map<String, List<Document>> keyBucketDocuments = new HashMap<>();
		for (Document document:timeSeries) {
			String key = new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get, (k, v) -> v, LinkedHashMap::new))).toJson();
			keyBucketDocuments.putIfAbsent(key, new ArrayList<>());
			keyBucketDocuments.get(key).add(document);
		}
		return keyBucketDocuments;
	}
}
