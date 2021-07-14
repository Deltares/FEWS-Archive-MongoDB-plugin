package nl.fews.archivedatabase.mongodb.shared.database;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 *
 */
@SuppressWarnings({"unused", "UnusedReturnValue"})
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
	 * NUM_RETRIES
	 */
	private static final int NUM_RETRIES = 5;

	/**
	 * RETRY_INTERVAL_MS
 	 */
	private static final int RETRY_INTERVAL_MS = 1000;

	/**
	 *
	 */
	private static MongoClient mongoClient = null;

	/**
	 *
	 */
	private static String database = null;

	/**
	 *
	 */
	private static String connectionString = null;

	/**
	 * Static Class
	 */
	private Database(){}

	/**
	 *
	 * @return MongoClient
	 */
	private static synchronized MongoClient create(){
		String connectionString = Settings.get("connectionString");
		if (Database.mongoClient == null || Database.connectionString == null || !Database.connectionString.equals(connectionString)) {
			Database.mongoClient = MongoClients.create(connectionString);
			Database.database = new ConnectionString(connectionString).getDatabase();
			if(Database.connectionString == null || !Database.connectionString.equals(connectionString)){
				ensureCollections();
			}
			Database.connectionString = connectionString;
		}
		return mongoClient;
	}

	/**
	 *
	 * @return MongoClient
	 */
	private static synchronized MongoClient createInternal(){
		String connectionString = Settings.get("connectionString");
		if (Database.mongoClient == null || Database.connectionString == null || !Database.connectionString.equals(connectionString)) {
			Database.mongoClient = MongoClients.create(connectionString);
			Database.database = new ConnectionString(connectionString).getDatabase();
			Database.connectionString = connectionString;
		}
		return mongoClient;
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
		MongoCollection<Document> mongoCollection = createInternal().getDatabase(database).getCollection(collection);
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
		Database.create().getDatabase(database).getCollection(collection).drop();
	}

	/**
	 *
	 * @param collection collection
	 * @param newName newName
	 */
	public static void renameCollection(String collection, String newName){
		Database.create().getDatabase(database).getCollection(collection).renameCollection(new MongoNamespace(database, newName));
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
	 *
	 * @param collection collection
	 * @param pipeline pipeline
	 * @return AggregateIterable<Document>
	 */
	public static AggregateIterable<Document> aggregate(String collection, List<Document> pipeline){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).aggregate(pipeline);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param projection projection
	 * @return Document
	 */
	public static Document findOne(String collection, Document query, Document projection){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).find(query).projection(projection).first();
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @return Document
	 */
	public static Document findOne(String collection, Document query){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).find(query).first();
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param projection projection
	 * @param limit limit
	 * @return FindIterable<Document>
	 */
	public static FindIterable<Document> find(String collection, Document query, Document projection, Integer limit){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).find(query).projection(projection).limit(limit);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param limit limit
	 * @return FindIterable<Document>
	 */
	public static FindIterable<Document> find(String collection, Document query, Integer limit){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).find(query).limit(limit);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param projection projection
	 * @return FindIterable<Document>
	 */
	public static FindIterable<Document> find(String collection, Document query, Document projection){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).find(query).projection(projection);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @return FindIterable<Document>
	 */
	public static FindIterable<Document> find(String collection, Document query){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).find(query);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param document document
	 * @return InsertOneResult
	 */
	public static InsertOneResult insertOne(String collection, Document document){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).insertOne(document);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES || (ex instanceof MongoWriteException && ((MongoWriteException)ex).getError().getCategory() == ErrorCategory.DUPLICATE_KEY))
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param documents documents
	 * @return InsertManyResult
	 */
	public static InsertManyResult insertMany(String collection, List<Document> documents){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).insertMany(documents);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES || (ex instanceof MongoBulkWriteException && ((MongoBulkWriteException)ex).getWriteErrors().stream().anyMatch(s -> s.getCategory() == ErrorCategory.DUPLICATE_KEY)))
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 * @return UpdateResult
	 */
	public static UpdateResult replaceOne(String collection, Document query, Document document){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).replaceOne(query, document);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 * @return UpdateResult
	 */
	public static UpdateResult updateOne(String collection, Document query, Document document){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).updateOne(query, document);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 * @param updateOptions updateOptions
	 * @return UpdateResult
	 */
	public static UpdateResult updateOne(String collection, Document query, Document document, UpdateOptions updateOptions){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).updateOne(query, document, updateOptions);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 * @return UpdateResult
	 */
	public static UpdateResult updateMany(String collection, Document query, Document document){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).updateMany(query, document);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 * @param updateOptions updateOptions
	 * @return UpdateResult
	 */
	public static UpdateResult updateMany(String collection, Document query, Document document, UpdateOptions updateOptions){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).updateMany(query, document, updateOptions);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @return DeleteResult
	 */
	public static DeleteResult deleteOne(String collection, Document query){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).deleteOne(query);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @return DeleteResult
	 */
	public static DeleteResult deleteMany(String collection, Document query){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).deleteMany(query);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
						Thread.currentThread().interrupt();
						throw new RuntimeException(iex);
					}
				}
			}
		}
		throw new MongoClientException("Failed Query");
	}

	/**
	 * lookup for string representation of each collection
	 */
	private static final Map<String, List<String>> collectionKeys = new ConcurrentHashMap<>(Map.of(
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucketSize", "bucket"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucketSize", "bucket"),
			TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "bucketSize", "bucket")
	));

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
