package nl.fews.archivedatabase.mongodb.query.interfaces;

import java.util.List;
import java.util.Map;

/**
 *
 */
public interface Filter {
	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param fields the fields to return a distinct ordered list of available filter values for
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @return Map<String, List<Object>>
	 */
	Map<String, List<Object>> getFilters(String collection, Map<String, Class<?>> fields, Map<String, List<Object>> query);
}
