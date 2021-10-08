package nl.fews.archivedatabase.mongodb.query.operations;

import nl.fews.archivedatabase.mongodb.shared.database.Database;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public final class Filter {
	/**
	 *
	 */
	private Filter() {
	}

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param fields the fields to return a distinct ordered list of available filter values for
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @return Map<String, List<Object>>
	 */
	public static Map<String, List<Object>> getFilters(String collection, Map<String, Class<?>> fields, Map<String, List<Object>> query) {
		Map<String, List<Object>> filters = new ConcurrentHashMap<>();
		fields.entrySet().parallelStream().forEach(e -> {
			String field = e.getKey();
			Class<?> clazz = e.getValue();
			filters.put(field, new ArrayList<>());
			Database.distinct(Database.Collection.TimeSeriesIndex.toString(), field, new Document("collection", collection), clazz).forEach(f -> filters.get(field).add(f));
		});
		filters.entrySet().parallelStream().forEach(e -> {
			String field = e.getKey();
			List<Object> members = e.getValue();

			List<Object> filtersFound = new ArrayList<>();
			members.parallelStream().forEach(f -> {
				Document document = new Document();
				document.append("collection", collection);
				document.append(field, f);
				query.forEach((k, v) -> {
					if(!v.isEmpty())
						document.append(k.replace("metaData.", ""), v.size() == 1 ? v.get(0) : new Document("$in", v));
				});
				Database.distinct(Database.Collection.TimeSeriesIndex.toString(), field, document, f.getClass()).forEach(filtersFound::add);
			});
			filters.put(field, filtersFound);
		});
		return filters;
	}
}
