package nl.fews.verification.mongodb.shared.database;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import nl.fews.verification.mongodb.shared.settings.Settings;

@SuppressWarnings({"unused", "UnusedReturnValue", "resource"})
public final class Mongo {
	private static final int NUM_RETRIES = 5;

	private static final int RETRY_INTERVAL_MS = 1000;

	private static final Map<String, MongoClient> mongoClient = new ConcurrentHashMap<>();

	private static final Map<String, String> connectionString = new ConcurrentHashMap<>();

	private static String database = null;

	private Mongo(){}

	static{
		Mongo.database = Settings.get("verificationDb");
	}

	private static synchronized MongoClient create(String database){
		String connectionString = database.equals(Settings.get("verificationDb")) ? Settings.get("mongoVerificationDbConnection") : database.equals(Settings.get("archiveDb")) ? Settings.get("mongoArchiveDbConnection") : "";
		if (!Mongo.mongoClient.containsKey(database) || !Mongo.connectionString.containsKey(database) || !Mongo.connectionString.get(database).equals(connectionString)) {
			Mongo.mongoClient.put(database, MongoClients.create(connectionString));
			if(!Mongo.connectionString.containsKey(database) || !Mongo.connectionString.get(database).equals(connectionString)){
				MongoIndex.ensureCollections();
			}
			Mongo.connectionString.put(database, connectionString);
		}
		return mongoClient.get(database);
	}

	public static void close(String database){
		if(Mongo.mongoClient.containsKey(database)) {
			Mongo.mongoClient.get(database).close();
			Mongo.mongoClient.remove(database);
			Mongo.connectionString.remove(database);
		}
	}

	public static ListCollectionsIterable<Document> listCollections(){
		return listCollections(database);
	}

	public static ListCollectionsIterable<Document> listCollections(String database){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(Settings.get("archiveDb")).getDatabase(database).listCollections();
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

	public static void dropCollection(String collection) {
		dropCollection(database, collection);
	}

	public static void dropCollection(String database, String collection){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				create(Settings.get("archiveDb")).getDatabase(database).getCollection(collection).drop(new DropCollectionOptions());
				return;
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

	public static void createView(String view, String collection, List<Document> document){
		createView(database, view, collection, document);
	}

	public static void createView(String database, String view, String collection, List<Document> document){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				create(Settings.get("archiveDb")).getDatabase(database).createView(view, collection, document);
				return;
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
	
	public static AggregateIterable<Document> aggregate(String collection, List<Document> pipeline){
		return aggregate(database, collection, pipeline);
	}

	public static AggregateIterable<Document> aggregate(String database, String collection, List<Document> pipeline){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).aggregate(pipeline);
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

	public static Document findOne(String collection, Document query, Document projection){
		return findOne(database, collection, query, projection);
	}

	public static Document findOne(String database, String collection, Document query, Document projection){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).find(query).projection(projection).first();
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

	public static Document findOne(String collection, Document query){
		return findOne(database, collection, query);
	}

	public static Document findOne(String database, String collection, Document query){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).find(query).first();
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

	public static FindIterable<Document> find(String collection, Document query, Document projection, Integer limit){
		return find(database, collection, query, projection, limit);
	}

	public static FindIterable<Document> find(String database, String collection, Document query, Document projection, Integer limit){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).find(query).projection(projection).limit(limit);
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

	public static FindIterable<Document> find(String collection, Document query, Integer limit){
		return find(database, collection, query, limit);
	}

	public static FindIterable<Document> find(String database, String collection, Document query, Integer limit){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).find(query).limit(limit);
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

	public static <T> DistinctIterable<T> distinct(String collection, String field, Document query, Class<T> clazz){
		return distinct(database, collection, field, query, clazz);
	}

	public static <T> DistinctIterable<T> distinct(String database, String collection, String field, Document query, Class<T> clazz){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).distinct(field, query, clazz);
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

	public static FindIterable<Document> find(String collection, Document query, Document projection){
		return find(database, collection, query, projection);
	}

	public static FindIterable<Document> find(String database, String collection, Document query, Document projection){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).find(query).projection(projection);
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

	public static FindIterable<Document> find(String collection, Document query){
		return find(database, collection, query);
	}

	public static FindIterable<Document> find(String database, String collection, Document query){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).find(query);
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

	public static InsertOneResult insertOne(String collection, Document document){
		return insertOne(database, collection, document);
	}

	public static InsertOneResult insertOne(String database, String collection, Document document){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).insertOne(document);
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

	public static InsertManyResult insertMany(String collection, List<Document> documents){
		return insertMany(database, collection, documents);
	}

	public static InsertManyResult insertMany(String database, String collection, List<Document> documents){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).insertMany(documents);
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

	public static UpdateResult replaceOne(String collection, Document query, Document document){
		return replaceOne(database, collection, query, document);
	}

	public static UpdateResult replaceOne(String database, String collection, Document query, Document document){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).replaceOne(query, document);
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

	public static UpdateResult replaceOne(String collection, Document query, Document document, ReplaceOptions replaceOptions){
		return replaceOne(database, collection, query, document, replaceOptions);
	}

	public static UpdateResult replaceOne(String database, String collection, Document query, Document document, ReplaceOptions replaceOptions){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).replaceOne(query, document, replaceOptions);
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

	public static UpdateResult updateOne(String collection, Document query, Document document){
		return updateOne(database, collection, query, document);
	}

	public static UpdateResult updateOne(String database, String collection, Document query, Document document){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).updateOne(query, document);
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

	public static UpdateResult updateOne(String collection, Document query, Document document, UpdateOptions updateOptions){
		return updateOne(database, collection, query, document, updateOptions);
	}

	public static UpdateResult updateOne(String database, String collection, Document query, Document document, UpdateOptions updateOptions){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).updateOne(query, document, updateOptions);
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

	public static UpdateResult updateMany(String collection, Document query, Document document){
		return updateMany(database, collection, query, document);
	}

	public static UpdateResult updateMany(String database, String collection, Document query, Document document){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).updateMany(query, document);
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

	public static UpdateResult updateMany(String collection, Document query, Document document, UpdateOptions updateOptions){
		return updateMany(database, collection, query, document, updateOptions);
	}

	public static UpdateResult updateMany(String database, String collection, Document query, Document document, UpdateOptions updateOptions){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).updateMany(query, document, updateOptions);
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

	public static DeleteResult deleteOne(String collection, Document query){
		return deleteOne(database, collection, query);
	}

	public static DeleteResult deleteOne(String database, String collection, Document query){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).deleteOne(query);
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

	public static DeleteResult deleteMany(String collection, Document query){
		return deleteMany(database, collection, query);
	}

	public static DeleteResult deleteMany(String database, String collection, Document query){
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try{
				return create(database).getDatabase(database).getCollection(collection).deleteMany(query);
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
