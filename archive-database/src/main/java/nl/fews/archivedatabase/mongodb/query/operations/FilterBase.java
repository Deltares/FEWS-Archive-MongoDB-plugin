package nl.fews.archivedatabase.mongodb.query.operations;

import nl.fews.archivedatabase.mongodb.query.interfaces.Filter;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public abstract class FilterBase implements Filter {
	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param fields the fields to return a distinct ordered list of available filter values for
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate startDate, inclusive
	 * @param endDate endDate, inclusive
	 * @return Map<String, List<Object>>
	 */
	@Override
	public Map<String, List<Object>> getFilters(String collection, Map<String, Class<?>> fields, Map<String, List<Object>> query, Date startDate, Date endDate) {
		Map<String, List<Object>> filters = new ConcurrentHashMap<>();
		fields.entrySet().parallelStream().forEach(e -> {
			String field = e.getKey();
			Class<?> clazz = e.getValue();
			filters.put(field, new ArrayList<>());
			Database.distinct(collection, field, new Document(), clazz).forEach(f -> filters.get(field).add(f));
		});
		return filters;
	}
}
