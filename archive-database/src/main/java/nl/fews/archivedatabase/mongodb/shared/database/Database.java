package nl.fews.archivedatabase.mongodb.shared.database;

import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;

import java.util.*;
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
		MigrateLog,
		BucketSize
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
	 *
	 * @param collection collection
	 */
	public static void dropCollection(String collection){
		Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).drop();
	}

	/**
	 *
	 * @param collection collection
	 * @param newName newName
	 */
	public static void renameCollection(String collection, String newName){
		Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).renameCollection(new MongoNamespace(Database.getDatabaseName(), newName));
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
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucketSize", "bucket"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucketSize", "bucket"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "bucketSize", "bucket")
	);

	/**
	 * default indexes for each collection
	 */
	private static final Map<String, Document[]> collectionIndex = Map.of(
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING)).stream().collect(Collectors.toMap(k -> k, k -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(List.of("moduleInstanceId","locationId","parameterId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("forecastTime","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("committed").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			},
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)).stream().collect(Collectors.toMap(k -> k, k -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(List.of("moduleInstanceId","locationId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "endTime", "unique").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "unique").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId", "parameterId", "qualifierId", "encodedTimeStepId", "endTime", "unique").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId", "parameterId", "qualifierId", "encodedTimeStepId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId", "parameterId", "startTime", "endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("committed").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			},
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET)).stream().collect(Collectors.toMap(k -> k, k -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(List.of("moduleInstanceId","locationId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "endTime", "unique").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "unique").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId", "parameterId", "qualifierId", "encodedTimeStepId", "endTime", "unique").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId", "parameterId", "qualifierId", "encodedTimeStepId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId", "parameterId", "startTime", "endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("committed").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			},
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING)).stream().collect(Collectors.toMap(k -> k, k -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING)).stream().filter(s -> !s.equals("taskRunId")).collect(Collectors.toMap(k -> k, k -> 1))),
					new Document(List.of("moduleInstanceId","locationId","parameterId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("taskRunId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("taskRunId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("committed").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			},
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL)).stream().collect(Collectors.toMap(k -> k, k -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL)).stream().filter(s -> !s.equals("taskRunId")).collect(Collectors.toMap(k -> k, k -> 1))),
					new Document(List.of("moduleInstanceId","locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","parameterId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId","forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("taskRunId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("forecastTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("taskRunId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("committed").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			},
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)).stream().collect(Collectors.toMap(k -> k, k -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(List.of("moduleInstanceId","locationId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","locationId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("moduleInstanceId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("locationId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("parameterId","startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("startTime","endTime").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("committed").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			},
			Collection.MigrateMetaData.toString(), new Document[]{
					new Document(List.of("metaDataFileRelativePath").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(List.of("netcdfFiles.timeSeriesIds").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("committed").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			},
			Collection.MigrateLog.toString(), new Document[]{
					new Document(List.of("date").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(List.of("errorCategory").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			},
			Collection.BucketSize.toString(), new Document[]{
					new Document(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId").stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}
	);
}
