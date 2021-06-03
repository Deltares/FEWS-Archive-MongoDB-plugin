package nl.fews.archivedatabase.mongodb.export;

import nl.fews.archivedatabase.mongodb.export.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.export.interfaces.Synchronize;
import nl.fews.archivedatabase.mongodb.export.utils.DatabaseBucketUtil;
import nl.fews.archivedatabase.mongodb.export.utils.TimeSeriesTypeUtil;

import com.mongodb.client.*;

import org.bson.Document;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class MongoDbSynchronizeBuckets implements Synchronize {

    /**
     * database
     */
    private final String database;

    /**
     * connectionString
     */
    private final String connectionString;

    /**
     *
     * @param connectionString connectionString
     */
    public MongoDbSynchronizeBuckets(String connectionString) throws MalformedURLException {
        this.connectionString = connectionString;
        this.database = new URL(connectionString.replace("mongodb://", "https://")).getPath().substring(1);
    }

    /**
     * Inserts, updates or replaces data for bucketed (observed) timeseries
     * @param timeSeries the entire list of all documents passed to this instance
     * @param timeSeriesType timeSeriesType
     */
    public void synchronize(List<Document> timeSeries, TimeSeriesType timeSeriesType){
        String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
        List<String> keys = TimeSeriesTypeUtil.getTimeSeriesTypeKeys(timeSeriesType);

        try (MongoClient db = MongoClients.create(connectionString)) {
            for (Map.Entry<String, Map<Integer, List<Document>>> key : DatabaseBucketUtil.getDocumentsByKeyBucket(timeSeries, keys).entrySet()) {
                for (Map.Entry<Integer, List<Document>> bucket : key.getValue().entrySet()) {
                    Document document = new Document(bucket.getValue().get(bucket.getValue().size() - 1)).append("bucket", bucket.getKey());
                    Document existingQuery = new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get))).append("bucket", bucket.getKey());

                    Document existing = db.getDatabase(database).getCollection(collection).find(existingQuery).first();
                    if (existing == null) {
                        insert(db, bucket.getKey(), document.append("timeseries", new ArrayList<Document>()), bucket.getValue(), collection);
                    }
                    else {
                        document.get("metaData", Document.class).append("archiveTime", existing.get("metaData", Document.class).get("archiveTime"));
                        replaceOrRemove(db, bucket.getKey(), document.append("_id", existing.get("_id")).append("timeseries", existing.get("timeseries")), bucket.getValue(), collection);
                    }
                }
            }
        }
    }

    /**
     *
     * @param db db
     * @param bucket bucket
     * @param existingDocument existingDocument
     * @param documents documents to insert
     * @param collection collection
     */
    private void insert(MongoClient db, int bucket, Document existingDocument, List<Document> documents, String collection){
        Document document = DatabaseBucketUtil.mergeDocuments(bucket, existingDocument, documents);
        if(!document.getList("timeseries", Document.class).isEmpty())
            db.getDatabase(database).getCollection(collection).insertOne(document);
    }

    /**
     *
     * @param db db
     * @param bucket bucket
     * @param existingDocument existingDocument
     * @param documents documents to insert
     * @param collection collection
     */
    private void replaceOrRemove(MongoClient db, int bucket, Document existingDocument, List<Document> documents, String collection){
        Document document = DatabaseBucketUtil.mergeDocuments(bucket, existingDocument, documents);
        if(!document.getList("timeseries", Document.class).isEmpty())
            db.getDatabase(database).getCollection(collection).replaceOne(new Document("_id", document.get("_id")), document);
        else
            db.getDatabase(database).getCollection(collection).deleteOne(new Document("_id", document.get("_id")));
    }
}
