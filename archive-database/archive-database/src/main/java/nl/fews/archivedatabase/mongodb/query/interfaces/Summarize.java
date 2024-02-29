package nl.fews.archivedatabase.mongodb.query.interfaces;

import java.util.*;

/**
 *
 */
public interface Summarize {
	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param distinctKeyFields the fields over which to group results and return a unique item count
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate startDate, inclusive
	 * @param endDate endDate, inclusive
	 * @return Map<String, List<Object>>
	 */
	Map<String, Integer> getSummary(String collection, Map<String, List<String>> distinctKeyFields, Map<String, List<Object>> query, Date startDate, Date endDate);
}
