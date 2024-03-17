package nl.fews.verification.mongodb.shared.database;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class MongoIndex {

	private static MongoClient mongoClient = null;

	private static String connectionString = null;

	private static String database = null;

	private MongoIndex(){}

	public static void ensureCollections(){
		MongoDatabase mongoDatabase = MongoIndex.create().getDatabase(database);
		List<Document> indexOperations = MongoIndex.mongoClient.getDatabase("admin").runCommand(
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
					CompletableFuture.runAsync(() -> MongoIndex.ensureCollection(collection));
				else
					MongoIndex.ensureCollection(collection);
			});
		}
	}

	private static synchronized MongoClient create(){
		String connectionString = Settings.get("mongoVerificationDbConnection");
		if (MongoIndex.mongoClient == null || MongoIndex.connectionString == null || !MongoIndex.connectionString.equals(connectionString)) {
			MongoIndex.mongoClient = MongoClients.create(connectionString);
			MongoIndex.database = new ConnectionString(connectionString).getDatabase();
			MongoIndex.connectionString = connectionString;
		}
		return mongoClient;
	}

	public static void ensureCollection(String collection){
		MongoCollection<Document> mongoCollection = create().getDatabase(database).getCollection(collection);
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

	private static Document[] getCollectionIndexes(String collection) {
		return Index.collectionIndex.get(collection);
	}
}
