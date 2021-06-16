package nl.fews.archivedatabase.mongodb.shared.database;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public final class Database {

	/**
	 *
	 */
	public enum Collection {
		MigrateMetaData,
		MigrateLog
	}

	/**
	 *
	 */
	private static MongoClient mongoClient = null;

	/**
	 *
	 */
	private static String lastConnectionString = null;

	/**
	 * Static Class
	 */
	private Database(){}

	/**
	 *
	 * @return MongoClient
	 */
	public static synchronized MongoClient create(){
		String connectionString = Settings.get("connectionString");
		if (Database.mongoClient == null || lastConnectionString == null || !lastConnectionString.equals(connectionString)) {
			Database.mongoClient = MongoClients.create(connectionString);

			if(lastConnectionString == null || !lastConnectionString.equals(connectionString)){
				ensureCollections();
			}
			lastConnectionString = connectionString;
		}
		return mongoClient;
	}

	/**
	 *
	 * @return databaseName
	 */
	public static String getDatabaseName(){
		return getDatabaseName(Settings.get("connectionString", String.class));
	}

	/**
	 *
	 * @param connectionString connectionString
	 * @return databaseName
	 */
	public static String getDatabaseName(String connectionString){
		return new ConnectionString(connectionString).getDatabase();
	}

	/**
	 *
	 */
	private static void ensureCollections(){
		for (TimeSeriesType timeSeriesType:TimeSeriesType.values()) {
			String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
			if(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType) == null || getCollectionIndexes(collection).length == 0)
				continue;
			ensureCollection(collection);
		}
		Arrays.stream(Collection.values()).forEach(s -> ensureCollection(s.toString()));
	}

	/**
	 *
	 * @param collection collection
	 */
	public static void ensureCollection(String collection){
		MongoCollection<Document> mongoCollection = mongoClient.getDatabase(Database.getDatabaseName()).getCollection(collection);
		Map<String, Object> indexes = new HashMap<>();
		mongoCollection.listIndexes().forEach(index -> indexes.put(index.get("key", Document.class).keySet().stream().sorted().collect(Collectors.joining("_")), index.get("key", Document.class)));

		for (Document document: getCollectionIndexes(collection)) {
			Object o = document.remove("unique");
			boolean unique = o != null && (o.equals(true) || !o.equals(0));
			String key = document.keySet().stream().sorted().collect(Collectors.joining("_"));
			if(!indexes.containsKey(key)) {
				mongoCollection.createIndex(document, new IndexOptions().unique(unique));
			}
		}
	}

	/**
	 * Bulk operations
	 * @param collection collection
	 * @param insert insert documents
	 * @param replace replace documents
	 * @param remove remove documents
	 */
	public static void synchronize(String collection, List<Document> insert, List<Document> replace, List<Document> remove){
		if(!insert.isEmpty())
			Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).insertMany(insert);

		if(!replace.isEmpty()) {
			Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).deleteMany(new Document("_id", new Document("$in", replace.stream().map(s -> s.get("_id")).collect(Collectors.toList()))));
			Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).insertMany(replace);
		}

		if (!remove.isEmpty())
			Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).deleteMany(new Document("_id", new Document("$in", remove.stream().map(s -> s.get("_id")).collect(Collectors.toList()))));
	}

	/**
	 *
	 * @param collection collection
	 */
	public static void dropCollection(String collection){
		mongoClient.getDatabase(Database.getDatabaseName()).getCollection(collection).drop();
	}

	/**
	 *
	 * @param collection collection
	 * @return the durable key members matching the insert data type collection's unique key
	 */
	public static List<String> getCollectionKeys(String collection) {
		return collectionKeys.get(collection);
	}

	/**
	 *
	 * @param collection collection
	 * @return The array of document representing default indexes to be applied to the given collection.  The first entry is unique
	 */
	public static Document[] getCollectionIndexes(String collection) {
		return collectionIndex.get(collection);
	}

	/**
	 * lookup for string representation of each collection
	 */
	private static final Map<String, List<String>> collectionKeys = Map.of(
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucket"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucket"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "bucket")
	);

	/**
	 * default indexes for each collection
	 */
	private static final Map<String, Document[]> collectionIndex = Map.of(
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING)).stream().collect(Collectors.toMap(k -> k, k -> 1))).append("unique", 1),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1,"forecastTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"forecastTime",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1,"forecastTime",1)),
					new Document(Map.of("locationId",1,"parameterId",1,"forecastTime",1)),
					new Document(Map.of("moduleInstanceId",1,"forecastTime",1)),
					new Document(Map.of("locationId",1,"forecastTime",1)),
					new Document(Map.of("parameterId",1,"forecastTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1)),
					new Document(Map.of("locationId",1,"parameterId",1)),
					new Document(Map.of("moduleInstanceId",1)),
					new Document(Map.of("locationId",1)),
					new Document(Map.of("parameterId",1)),
					new Document(Map.of("forecastTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("locationId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("locationId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("forecastTime",1,"startTime",1,"endTime",1)),
					new Document(Map.of("startTime",1,"endTime",1)),
					new Document(Map.of("committed",1))
			},
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)).stream().collect(Collectors.toMap(k -> k, k -> 1))).append("unique", 1),
					new Document(Map.of("moduleInstanceId", 1,"locationId", 1, "parameterId", 1, "qualifierId", 1, "encodedTimeStepId", 1)),
					new Document(Map.of("moduleInstanceId", 1,"locationId", 1, "parameterId", 1, "startTime", 1, "endTime", 1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("locationId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("locationId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1)),
					new Document(Map.of("locationId",1,"parameterId",1)),
					new Document(Map.of("moduleInstanceId",1)),
					new Document(Map.of("locationId",1)),
					new Document(Map.of("parameterId",1)),
					new Document(Map.of("startTime",1,"endTime",1)),
					new Document(Map.of("committed",1))
			},
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET)).stream().collect(Collectors.toMap(k -> k, k -> 1))).append("unique", 1),
					new Document(Map.of("moduleInstanceId", 1,"locationId", 1, "parameterId", 1, "qualifierId", 1, "encodedTimeStepId", 1)),
					new Document(Map.of("moduleInstanceId", 1,"locationId", 1, "parameterId", 1, "startTime", 1, "endTime", 1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("locationId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("locationId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1)),
					new Document(Map.of("locationId",1,"parameterId",1)),
					new Document(Map.of("moduleInstanceId",1)),
					new Document(Map.of("locationId",1)),
					new Document(Map.of("parameterId",1)),
					new Document(Map.of("startTime",1,"endTime",1)),
					new Document(Map.of("committed",1))
			},
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING)).stream().collect(Collectors.toMap(k -> k, k -> 1))),
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING)).stream().filter(s -> !s.equals("taskRunId")).collect(Collectors.toMap(k -> k, k -> 1))).append("unique", 1),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1,"forecastTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"forecastTime",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1,"forecastTime",1)),
					new Document(Map.of("locationId",1,"parameterId",1,"forecastTime",1)),
					new Document(Map.of("moduleInstanceId",1,"forecastTime",1)),
					new Document(Map.of("locationId",1,"forecastTime",1)),
					new Document(Map.of("parameterId",1,"forecastTime",1)),
					new Document(Map.of("taskRunId",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1)),
					new Document(Map.of("locationId",1,"parameterId",1)),
					new Document(Map.of("moduleInstanceId",1)),
					new Document(Map.of("locationId",1)),
					new Document(Map.of("parameterId",1)),
					new Document(Map.of("forecastTime",1)),
					new Document(Map.of("taskRunId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("locationId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("locationId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("startTime",1,"endTime",1)),
					new Document(Map.of("committed",1))
			},
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL)).stream().collect(Collectors.toMap(k -> k, k -> 1))).append("unique", 1),
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL)).stream().filter(s -> !s.equals("taskRunId")).collect(Collectors.toMap(k -> k, k -> 1))),
					new Document(Map.of("moduleInstanceId", 1,"locationId", 1, "parameterId", 1, "qualifierId", 1, "encodedTimeStepId", 1, "ensembleId", 1, "ensembleMemberId", 1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1,"forecastTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"forecastTime",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1,"forecastTime",1)),
					new Document(Map.of("locationId",1,"parameterId",1,"forecastTime",1)),
					new Document(Map.of("moduleInstanceId",1,"forecastTime",1)),
					new Document(Map.of("locationId",1,"forecastTime",1)),
					new Document(Map.of("parameterId",1,"forecastTime",1)),
					new Document(Map.of("taskRunId",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1)),
					new Document(Map.of("locationId",1,"parameterId",1)),
					new Document(Map.of("moduleInstanceId",1)),
					new Document(Map.of("locationId",1)),
					new Document(Map.of("parameterId",1)),
					new Document(Map.of("forecastTime",1)),
					new Document(Map.of("taskRunId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("locationId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("locationId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("startTime",1,"endTime",1)),
					new Document(Map.of("committed",1))
			},
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)).stream().collect(Collectors.toMap(k -> k, k -> 1))).append("unique", 1),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1)),
					new Document(Map.of("locationId",1,"parameterId",1)),
					new Document(Map.of("moduleInstanceId",1)),
					new Document(Map.of("locationId",1)),
					new Document(Map.of("parameterId",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"locationId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("locationId",1,"parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("moduleInstanceId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("locationId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("parameterId",1,"startTime",1,"endTime",1)),
					new Document(Map.of("startTime",1,"endTime",1)),
					new Document(Map.of("committed",1))
			},
			Collection.MigrateMetaData.toString(), new Document[]{
					new Document(Map.of("metaDataFileRelativePath", 1)).append("unique", 1),
					new Document(Map.of("committed", 1))
			},
			Collection.MigrateLog.toString(), new Document[]{
					new Document(Map.of("date", 1)),
					new Document(Map.of("y", 1))
			}
	);
}
