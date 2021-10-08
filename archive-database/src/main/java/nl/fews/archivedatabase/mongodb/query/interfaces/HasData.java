package nl.fews.archivedatabase.mongodb.query.interfaces;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 */
public interface HasData {
	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate startDate, inclusive
	 * @param endDate endDate, inclusive
	 * @return Map<String, List<Object>>
	 */
	boolean hasData(String collection, Map<String, List<Object>> query, Date startDate, Date endDate);
}
