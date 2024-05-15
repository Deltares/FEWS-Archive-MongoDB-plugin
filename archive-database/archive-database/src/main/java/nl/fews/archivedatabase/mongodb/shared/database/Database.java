package nl.fews.archivedatabase.mongodb.shared.database;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.IndexModel;
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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 *
 */
@SuppressWarnings({"unused", "UnusedReturnValue"})
public final class Database {

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
	public static void close(){
		if(Database.mongoClient != null)
			Database.mongoClient.close();
		Database.mongoClient = null;
		Database.database = null;
		Database.connectionString = null;
	}

	/**
	 *
	 */
	private static void ensureCollections(){
		MongoDatabase mongoDatabase = createInternal().getDatabase(database);
		List<Document> indexOperations = Database.mongoClient.getDatabase("admin").runCommand(
				new Document("currentOp", 1).
				append("$ownOps", 1).
				append("$or", List.of(
						new Document("command.reIndex", new Document("$exists", true)),
						new Document("command.createIndexes", new Document("$exists", true))))).
				getList("inprog", Document.class);
		if(indexOperations.isEmpty()){
			Set<String> collections = new HashSet<>();
			mongoDatabase.listCollectionNames().forEach(collections::add);
			Index.collectionIndex.keySet().forEach(collection -> {
				if(collections.contains(collection))
					CompletableFuture.runAsync(() -> ensureCollection(collection));
				else
					Database.ensureCollection(collection);
			});
		}
	}

	/**
	 *
	 * @param collection collection
	 */
	public static void ensureCollection(String collection){
		MongoCollection<Document> mongoCollection = createInternal().getDatabase(database).getCollection(collection);
		Map<String, Object> existingIndexes = new HashMap<>();
		mongoCollection.listIndexes().forEach(index -> existingIndexes.put(index.get("key", Document.class).keySet().stream().sorted().collect(Collectors.joining("_")), index.get("key", Document.class)));

		List<IndexModel> indexes = new ArrayList<>();
		for (Document document: getCollectionIndexes(collection)) {
			document = Document.parse(document.toJson());
			Object o = document.remove("unique");
			boolean unique = o != null && (o.equals(true) || !o.equals(0));
			String key = document.keySet().stream().sorted().collect(Collectors.joining("_"));
			if(!existingIndexes.containsKey(key))
				indexes.add(new IndexModel(document, new IndexOptions().unique(unique)));
		}
		if(!indexes.isEmpty())
			mongoCollection.createIndexes(indexes);
	}

	/**
	 *
	 */
	public static void updateTimeSeriesIndex(){
		Database.ensureCollection(Collection.TimeSeriesIndex.toString());
		List.of(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, TimeSeriesType.SCALAR_SIMULATED_FORECASTING, TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED).forEach(timeSeriesType -> {
			String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
			List<Document> results = new ArrayList<>();
			Database.aggregate(collection, List.of(
					new Document("$sort", new Document("moduleInstanceId", 1).append("parameterId", 1).append("encodedTimeStepId", 1).append("metaData.areaId", 1).append("metaData.sourceId", 1)),
					new Document("$group", new Document("_id", new Document("moduleInstanceId", "$moduleInstanceId").append("parameterId", "$parameterId").append("encodedTimeStepId", "$encodedTimeStepId").append("areaId", "$metaData.areaId").append("sourceId", "$metaData.sourceId"))),
					new Document("$replaceRoot", new Document("newRoot", "$_id")),
					new Document("$addFields", new Document("collection", collection))
			)).forEach(results::add);
			Database.deleteMany(Collection.TimeSeriesIndex.toString(), new Document("collection", collection));
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
		return Index.collectionKeys.get(collection);
	}

	/**
	 *
	 * @param keys the durable key members matching the insert data type collection's unique key
	 * @return the key values for the passed timeseries
	 */
	public static Document getKeyDocument(List<String> keys, Document document) {
		return new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get, (k, v) -> v, LinkedHashMap::new)));
	}

	/**
	 *
	 * @param keyDocument keyDocument
	 * @return the key for the passed key document
	 */
	public static String getKey(Document keyDocument) {
		return keyDocument.toJson();
	}

	/**
	 *
	 * @param collection collection
	 * @return The array of document representing default indexes to be applied to the given collection.  The first entry is unique
	 */
	private static Document[] getCollectionIndexes(String collection) {
		return Index.collectionIndex.get(collection);
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
}
