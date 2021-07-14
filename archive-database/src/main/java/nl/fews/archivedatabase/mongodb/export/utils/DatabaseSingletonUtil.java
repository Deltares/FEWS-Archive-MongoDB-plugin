package nl.fews.archivedatabase.mongodb.export.utils;

import org.bson.Document;
import org.json.JSONArray;

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
	 * @param keys the document key fields matching the database unique collection key
	 * @return Map<String, List<Document>> document map keyed by their JSON string key representation of mongo unique key fields.
	 */
	public static Map<String, List<Document>> getDocumentsByKey(List<Document> timeSeries, List<String> keys){
		Map<String, List<Document>> keyBucketDocuments = new HashMap<>();
		for (Document document:timeSeries) {
			String key = new JSONArray(keys.stream().map(s -> document.get(s) instanceof Date ? document.getDate(s).toInstant() : document.get(s)).collect(Collectors.toList())).toString();
			keyBucketDocuments.putIfAbsent(key, new ArrayList<>());
			keyBucketDocuments.get(key).add(document);
		}
		return keyBucketDocuments;
	}
}
