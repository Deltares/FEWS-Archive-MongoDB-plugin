package nl.fews.archivedatabase.mongodb.database;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;

import nl.fews.archivedatabase.common.shared.interfaces.ClosableIterable;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 *
 */
@SuppressWarnings({"unused", "ConstantConditions", "unchecked"})
public final class Database implements nl.fews.archivedatabase.common.shared.interfaces.Database, AutoCloseable {
	/**
	 *
	 */
	private static volatile Database instance = null;

	/**
	 *
	 */
	private final MongoClient mongoClient;

	/**
	 *
	 */
	private final MongoDatabase db;

	/**
	 *
	 */
	private final String database;

	/**
	 *
	 */
	private static String connectionString = null;

	/**
	 *
	 */
	private Database(){
		var connectionString = Settings.get("connectionString", String.class);
		mongoClient = MongoClients.create(connectionString);
		database = new ConnectionString(connectionString).getDatabase();
		db = mongoClient.getDatabase(database);
		if(Database.connectionString == null || !Database.connectionString.equals(connectionString))
			ensureTables();
		Database.connectionString = connectionString;
	}

	/**
	 *
	 * @return Database
	 */
	public static Database instance(){
		if (instance == null)
			synchronized (Database.class) {
				if (instance == null)
					instance = new Database();
			}
		return instance;
	}

	/**
	 *
	 */
	@Override
	public void close(){
		mongoClient.close();
	}

	/**
	 *
	 * @return boolean
	 */
	private boolean hasIndexOperations(){
		return !mongoClient.getDatabase("admin").runCommand(
			new Document("currentOp", 1).
			append("$ownOps", 1).
			append("$or", List.of(
				new Document("command.reIndex", new Document("$exists", true)),
				new Document("command.createIndexes", new Document("$exists", true))))).
			getList("inprog", Document.class).isEmpty();
	}

	/**
	 *
	 */
	private void ensureTables(){
		if(!hasIndexOperations()){
			var collections = new HashSet<String>();
			db.listCollectionNames().forEach(collections::add);
			Schema.getCollections().forEach(collection -> {
				if(collections.contains(collection))
					CompletableFuture.runAsync(() -> ensureTable(collection));
				else
					ensureTable(collection);
			});
		}
	}

	/**
	 *
	 * @param collection collection
	 */
	@Override
	public void ensureTable(String collection){
		var mongoCollection = db.getCollection(collection);
		var existingIndexes = new LinkedHashMap<String, Object>();
		mongoCollection.listIndexes().forEach(index -> existingIndexes.put(index.get("key", Document.class).keySet().stream().sorted().collect(Collectors.joining("_")), index.get("key", Document.class)));

		var indexes = new ArrayList<IndexModel>();
		for (var d: Schema.getIndexes(collection)) {
			var unique = Integer.valueOf(1).equals(d.containsKey("options") && ((Map<?, ?>)d.get("options")).containsKey("unique") ? ((Map<?, ?>)d.get("options")).get("unique") : 0);
			var index = new Document(d).getList("keys", String.class).stream().collect(Collectors.toMap(k -> k, v -> 1, (a, b) -> a, Document::new));
			var key = String.join("_", new Document(d).getList("keys", String.class));
			if(!existingIndexes.containsKey(key))
				indexes.add(new IndexModel(index, new IndexOptions().unique(unique)));		}
		if(!indexes.isEmpty())
			mongoCollection.createIndexes(indexes);
	}

	/**
	 *
	 * @param collection collection
	 */
	@Override
	public void drop(String collection){
		db.getCollection(collection).drop();
	}

	/**
	 *
	 * @param collection collection
	 * @param newName newName
	 */
	@Override
	public void rename(String collection, String newName){
		db.getCollection(collection).renameCollection(new MongoNamespace(database, newName));
	}

	/**
	 *
	 * @param srcCollection srcCollection
	 * @param dstCollection dstCollection
	 */
	@Override
	public void replace(String srcCollection, String dstCollection){
		var tempCollectionName = String.format("TEMP_%s", dstCollection);
		rename(dstCollection, tempCollectionName);
		rename(srcCollection, dstCollection);
		drop(tempCollectionName);
	}

	/**
	 *
	 * @param collection collection
	 * @param pipeline pipeline
	 * @return ClosableIterable<Map<String, Object>>
	 */
	public ClosableIterable<Map<String, Object>> aggregate(String collection, List<Map<String, Object>> pipeline){
		return ClosableIterable.wrap(() -> new MongoDbClosableIterator(DatabaseUtil.retry(() -> db.getCollection(collection).aggregate(pipeline.stream().map(Document::new).toList()).allowDiskUse(true).iterator())));
	}

	/**
	 *
	 * @param collection collection
	 * @param pipeline pipeline
	 * @param hint hint
	 * @return ClosableIterable<Map<String, Object>>
	 */
	public ClosableIterable<Map<String, Object>> aggregate(String collection, List<Map<String, Object>> pipeline, Map<String, Object> hint){
		return ClosableIterable.wrap(() -> new MongoDbClosableIterator(DatabaseUtil.retry(() -> db.getCollection(collection).aggregate(pipeline.stream().map(Document::new).toList()).hint(new Document(hint)).allowDiskUse(true).iterator())));
	}

	/**
	 *
	 * @param collection collection
	 * @param pipeline pipeline
	 * @return Map<String, Object>
	 */
	public Map<String, Object> aggregateOne(String collection, List<Map<String, Object>> pipeline){
		return DatabaseUtil.retry(() -> db.getCollection(collection).aggregate(pipeline.stream().map(Document::new).toList()).first());
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param projection projection
	 * @return Map<String, Object>
	 */
	public Map<String, Object> findOne(String collection, Map<String, Object> query, Map<String, Object> projection){
		return DatabaseUtil.retry(() -> db.getCollection(collection).find(new Document(query)).projection(new Document(projection)).first());
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @return Map<String, Object>
	 */
	public Map<String, Object> findOne(String collection, Map<String, Object> query){
		return DatabaseUtil.retry(() -> db.getCollection(collection).find(new Document(query)).first());
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param field field
	 * @param clazz clazz
	 * @return Set<T>
	 */
	public <T> Set<T> distinct(String collection, String field, Map<String, Object> query, Class<T> clazz){
		return DatabaseUtil.retry(() -> db.getCollection(collection).distinct(field, new Document(query), clazz)).into(new HashSet<>());
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param projection projection
	 * @param limit limit
	 * @return ClosableIterable<Map<String, Object>>
	 */
	public ClosableIterable<Map<String, Object>> find(String collection, Map<String, Object> query, Map<String, Object> projection, Integer limit){
		return ClosableIterable.wrap(() -> new MongoDbClosableIterator(DatabaseUtil.retry(() -> db.getCollection(collection).find(new Document(query)).projection(new Document(projection)).limit(limit).allowDiskUse(true).iterator())));
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param limit limit
	 * @return ClosableIterable<Map<String, Object>>
	 */
	public ClosableIterable<Map<String, Object>> find(String collection, Map<String, Object> query, Integer limit){
		return ClosableIterable.wrap(() -> new MongoDbClosableIterator(DatabaseUtil.retry(() -> db.getCollection(collection).find(new Document(query)).limit(limit).allowDiskUse(true).iterator())));
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param projection projection
	 * @return ClosableIterable<Map<String, Object>>
	 */
	public ClosableIterable<Map<String, Object>> find(String collection, Map<String, Object> query, Map<String, Object> projection){
		return ClosableIterable.wrap(() -> new MongoDbClosableIterator(DatabaseUtil.retry(() -> db.getCollection(collection).find(new Document(query)).projection(new Document(projection)).allowDiskUse(true).iterator())));
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @return ClosableIterable<Map<String, Object>>
	 */
	public ClosableIterable<Map<String, Object>> find(String collection, Map<String, Object> query){
		return ClosableIterable.wrap(() -> new MongoDbClosableIterator(DatabaseUtil.retry(() -> db.getCollection(collection).find(new Document(query)).allowDiskUse(true).iterator())));
	}

	/**
	 *
	 * @param collection collection
	 * @param document document
	 */
	public ObjectId insertOne(String collection, Map<String, Object> document){
		return DatabaseUtil.retry(() -> db.getCollection(collection).insertOne(new Document(document))).getInsertedId().asObjectId().getValue();
	}

	/**
	 *
	 * @param collection collection
	 * @param documents documents
	 */
	public void insertMany(String collection, List<Map<String, Object>> documents){
		DatabaseUtil.retry(() -> db.getCollection(collection).insertMany(documents.stream().map(Document::new).toList()));
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 */
	public void replaceOne(String collection, Map<String, Object> query, Map<String, Object> document){
		DatabaseUtil.retry(() -> db.getCollection(collection).replaceOne(new Document(query), new Document(document)));
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 */
	public void updateOne(String collection, Map<String, Object> query, Map<String, Object> document){
		DatabaseUtil.retry(() -> db.getCollection(collection).updateOne(new Document(query), new Document(document)));
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 * @param upsert upsert
	 */
	public void updateOne(String collection, Map<String, Object> query, Map<String, Object> document, boolean upsert){
		DatabaseUtil.retry(() -> db.getCollection(collection).updateOne(new Document(query), new Document(document), new UpdateOptions().upsert(upsert)));
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 */
	public void updateMany(String collection, Map<String, Object> query, Map<String, Object> document){
		DatabaseUtil.retry(() -> db.getCollection(collection).updateMany(new Document(query), new Document(document)));
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 * @param upsert upsert
	 */
	public void updateMany(String collection, Map<String, Object> query, Map<String, Object> document, boolean upsert){
		DatabaseUtil.retry(() -> db.getCollection(collection).updateMany(new Document(query), new Document(document), new UpdateOptions().upsert(upsert)));
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 */
	public void deleteOne(String collection, Map<String, Object> query){
		DatabaseUtil.retry(() -> db.getCollection(collection).deleteOne(new Document(query)));
	}

	/**
	 *
	 * @param collection collection
	 * @param query query
	 */
	public void deleteMany(String collection, Map<String, Object> query){
		DatabaseUtil.retry(() -> db.getCollection(collection).deleteMany(new Document(query)));
	}
}
