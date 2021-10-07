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
import java.util.stream.Stream;

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
		BucketSize,
		TimeSeriesIndex
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
		for (String collection: collectionIndex.keySet()) {
			if(getCollectionIndexes(collection).length > 0)
				ensureCollection(collection);
		}
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
			document = Document.parse(document.toJson());
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
	 */
	public static void updateTimeSeriesIndex(){
		Database.dropCollection(Database.Collection.TimeSeriesIndex.toString());
		Database.ensureCollection(Database.Collection.TimeSeriesIndex.toString());
		List.of(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, TimeSeriesType.SCALAR_SIMULATED_FORECASTING, TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED).forEach(timeSeriesType -> {
			String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
			List<Document> results = new ArrayList<>();
			Database.aggregate(collection, List.of(
					new Document("$sort", new Document("moduleInstanceId", 1).append("parameterId", 1).append("encodedTimeStepId", 1).append("metaData.areaId", 1).append("metaData.sourceId", 1)),
					new Document("$group", new Document("_id", new Document("moduleInstanceId", "$moduleInstanceId").append("parameterId", "$parameterId").append("encodedTimeStepId", "$encodedTimeStepId").append("areaId", "$metaData.areaId").append("sourceId", "$metaData.sourceId"))),
					new Document("$replaceRoot", new Document("newRoot", "$_id")),
					new Document("$addFields", new Document("collection", collection))
			)).forEach(results::add);
			if(!results.isEmpty())
				Database.insertMany(Collection.TimeSeriesIndex.toString(), results);
		});
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
	 * @param srcCollection srcCollection
	 * @param dstCollection dstCollection
	 */
	public static void replaceCollection(String srcCollection, String dstCollection){
		String tempCollectionName = String.format("TEMP_%s", dstCollection);
		Database.renameCollection(dstCollection, tempCollectionName);
		Database.renameCollection(srcCollection, dstCollection);
		Database.dropCollection(tempCollectionName);
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
	private static Document[] getCollectionIndexes(String collection) {
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
	 * @param field field
	 * @param clazz clazz
	 * @return DistinctIterable<String>
	 */
	public static <T> DistinctIterable<T> distinct(String collection, String field, Document query, Class<T> clazz){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create().getDatabase(database).getCollection(collection).distinct(field, query, clazz);
			}
			catch (Exception ex){
				if(i == NUM_RETRIES)
					throw ex;
				else{
					try{
						Thread.sleep(RETRY_INTERVAL_MS);
					}
					catch (InterruptedException iex){
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
	private static final Map<String, Document[]> collectionIndex = Map.ofEntries(
			Map.entry(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING)).stream().collect(Collectors.toMap(k -> k, k -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("forecastTime", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId", "metaData.areaId", "forecastTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "metaData.areaId", "forecastTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "metaData.areaId", "forecastTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("metaData.areaId", "forecastTime", "moduleInstanceId", "parameterId", "encodedTimeStepId","metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("committed").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			}),
			Map.entry(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)).stream().collect(Collectors.toMap(k -> k, k -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "endTime", "unique").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "unique").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "endTime", "unique").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "metaData.timeStepMinutes").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("metaData.areaId", "startTime", "endTime", "moduleInstanceId", "parameterId", "encodedTimeStepId","metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("committed").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			}),
			Map.entry(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET)).stream().collect(Collectors.toMap(k -> k, k -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "endTime", "unique").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "unique").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "endTime", "unique").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("metaData.areaId", "startTime", "endTime", "moduleInstanceId", "parameterId", "encodedTimeStepId","metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("committed").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			}),
			Map.entry(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING)).stream().collect(Collectors.toMap(k -> k, k -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING)).stream().filter(s -> !s.equals("taskRunId")).collect(Collectors.toMap(k -> k, k -> 1))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("taskRunId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("taskRunId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId", "metaData.areaId", "forecastTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "metaData.areaId", "forecastTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "metaData.areaId", "forecastTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("metaData.areaId", "forecastTime", "moduleInstanceId", "parameterId", "encodedTimeStepId","metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("committed").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			}),
			Map.entry(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL)).stream().collect(Collectors.toMap(k -> k, k -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL)).stream().filter(s -> !s.equals("taskRunId")).collect(Collectors.toMap(k -> k, k -> 1))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "metaData.timeStepMinutes").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("taskRunId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("taskRunId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId", "forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId", "metaData.areaId", "forecastTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "metaData.areaId", "forecastTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "metaData.areaId", "forecastTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("metaData.areaId", "forecastTime", "moduleInstanceId", "parameterId", "encodedTimeStepId","metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("committed").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			}),
			Map.entry(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED), new Document[]{
					new Document(collectionKeys.get(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)).stream().collect(Collectors.toMap(k -> k, k -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "startTime", "endTime", "unique").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "startTime", "unique").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "endTime", "unique").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "locationId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("locationId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId", "startTime", "endTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("encodedTimeStepId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("parameterId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("moduleInstanceId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("metaData.areaId", "startTime", "endTime", "moduleInstanceId", "parameterId", "encodedTimeStepId","metaData.sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("committed").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			}),
			Map.entry(Collection.MigrateMetaData.toString(), new Document[]{
					new Document(Stream.of("metaDataFileRelativePath").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(Stream.of("netcdfFiles.timeSeriesIds").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("committed").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			}),
			Map.entry(Collection.MigrateLog.toString(), new Document[]{
					new Document(Stream.of("date").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("errorCategory").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			}),
			Map.entry(Collection.BucketSize.toString(), new Document[]{
					new Document(Stream.of("bucketCollection", "bucketKey").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry(Collection.TimeSeriesIndex.toString(), new Document[]{
					new Document(Stream.of("collection", "moduleInstanceId", "parameterId", "encodedTimeStepId", "areaId", "sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(Stream.of("collection", "moduleInstanceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("collection", "parameterId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("collection", "encodedTimeStepId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("collection", "areaId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("collection", "sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("collection", "moduleInstanceId", "areaId", "sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("collection", "parameterId", "areaId", "sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("collection", "encodedTimeStepId", "areaId", "sourceId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			})
	);
}
