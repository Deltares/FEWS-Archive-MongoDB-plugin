package nl.fews.archivedatabase.common.query.interfaces;

import nl.fews.archivedatabase.common.shared.interfaces.ClosableIterable;

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
	 * @return ClosableIterable<Map<String, Object>>
	 */
	ClosableIterable<Map<String, Object>> read(String collection, Map<String, ?> query, Date startDate, Date endDate);

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @return ClosableIterable<Map<String, Object>>
	 */
	ClosableIterable<Map<String, Object>> read(String collection, Map<String, ?> query);

}
