package nl.fews.archivedatabase.mongodb.database;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import nl.fews.archivedatabase.common.shared.interfaces.ClosableIterator;

import java.util.Map;

public class MongoDbClosableIterator implements ClosableIterator<Map<String, Object>> {
    private final MongoCursor<Document> cursor;

    public MongoDbClosableIterator(MongoCursor<Document> cursor) {
        this.cursor = cursor;
    }

    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public Map<String, Object> next() {
        return cursor.next();
    }

    @Override
    public void close() {
        if (cursor != null) {
            cursor.close();
        }
    }
}