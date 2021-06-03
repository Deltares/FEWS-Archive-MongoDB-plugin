package nl.fews.archivedatabase.mongodb.export;

import nl.fews.archivedatabase.mongodb.export.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.export.interfaces.*;
import nl.fews.archivedatabase.mongodb.export.utils.DatabaseForecastUtil;
import nl.fews.archivedatabase.mongodb.export.utils.TimeSeriesTypeUtil;

import com.mongodb.client.*;

import org.bson.Document;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class MongoDbSynchronizeForecasts implements Synchronize {

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
    public MongoDbSynchronizeForecasts(String connectionString) throws MalformedURLException {
        this.connectionString = connectionString;
        this.database = new URL(connectionString.replace("mongodb://", "https://")).getPath().substring(1);
    }

    /**
     * Inserts, updates or replaces data for forecast timeseries
     * @param timeSeries the entire list of all documents passed to this instance
     * @param timeSeriesType timeSeriesType
     */
    public void synchronize(List<Document> timeSeries, TimeSeriesType timeSeriesType){
        String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
        List<String> keys = TimeSeriesTypeUtil.getTimeSeriesTypeKeys(timeSeriesType);

        try (MongoClient db = MongoClients.create(connectionString)) {
            for (Map.Entry<String, List<Document>> key : DatabaseForecastUtil.getDocumentsByKey(timeSeries, keys).entrySet()) {
                Document document = key.getValue().get(key.getValue().size() - 1);
                Document existingQuery = new Document(keys.stream().collect(Collectors.toMap(k -> k, document::get)));

                Document existing = db.getDatabase(database).getCollection(collection).find(existingQuery).first();
                if (existing == null) {
                    insert(db, document, collection);
                }
                else {
                    document.get("metaData", Document.class).append("archiveTime", existing.get("metaData", Document.class).get("archiveTime"));
                    replaceOrRemove(db, document.append("_id", existing.get("_id")).append("timeseries", existing.get("timeseries")), collection);
                }
            }
        }
    }

    /**
     *
     * @param db db
     * @param document document to insert
     * @param collection collection
     */
    private void insert(MongoClient db, Document document, String collection){
        if(!document.getList("timeseries", Document.class).isEmpty())
            db.getDatabase(database).getCollection(collection).insertOne(document);
    }

    /**
     *
     * @param db db
     * @param document document to insert
     * @param collection collection
     */
    private void replaceOrRemove(MongoClient db, Document document, String collection){
        if(!document.getList("timeseries", Document.class).isEmpty())
            db.getDatabase(database).getCollection(collection).replaceOne(new Document("_id", document.get("_id")), document);
        else
            db.getDatabase(database).getCollection(collection).deleteOne(new Document("_id", document.get("_id")));
    }
}
