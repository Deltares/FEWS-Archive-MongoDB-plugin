package nl.fews.archivedatabase.mongodb.export.utils;

import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;

import java.util.*;

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
	public static Map<String, List<Document>> getDocumentsByKey(Document timeSeries, TimeSeriesType timeSeriesType){
		Map<String, List<Document>> keyBucketDocuments = new HashMap<>();
		String key = Database.getKey(Database.getKeyDocument(Database.getCollectionKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType)), timeSeries));
		keyBucketDocuments.putIfAbsent(key, new ArrayList<>());
		keyBucketDocuments.get(key).add(timeSeries);
		return keyBucketDocuments;
	}
}
