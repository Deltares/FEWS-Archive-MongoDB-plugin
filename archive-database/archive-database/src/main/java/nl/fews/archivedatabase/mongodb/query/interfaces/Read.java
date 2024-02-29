package nl.fews.archivedatabase.mongodb.query.interfaces;

import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.util.*;

/**
 *
 */
public interface Read {
	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate startDate, inclusive
	 * @param endDate endDate, inclusive
	 * @return Map<String, List<Object>>
	 */
	MongoCursor<Document> read(String collection, Map<String, List<Object>> query, Date startDate, Date endDate);

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @return Map<String, List<Object>>
	 */
	MongoCursor<Document> read(String collection, Map<String, List<Object>> query);
}
