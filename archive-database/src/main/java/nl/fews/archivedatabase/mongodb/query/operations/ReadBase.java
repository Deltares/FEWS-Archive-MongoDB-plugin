package nl.fews.archivedatabase.mongodb.query.operations;

import com.mongodb.client.MongoCursor;
import nl.fews.archivedatabase.mongodb.query.interfaces.Read;
import org.bson.Document;

import java.util.*;

/**
 *
 */
public abstract class ReadBase implements Read {

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate startDate, inclusive
	 * @param endDate endDate, inclusive
	 * @return Map<String, List<Object>>
	 */
	@Override
	public abstract MongoCursor<Document> read(String collection, Map<String, List<Object>> query, Date startDate, Date endDate);
}
