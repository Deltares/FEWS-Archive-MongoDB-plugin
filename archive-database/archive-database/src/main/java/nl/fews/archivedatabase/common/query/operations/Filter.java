package nl.fews.archivedatabase.common.query.operations;
import nl.fews.archivedatabase.common.shared.interfaces.DatabaseBridge;
import nl.fews.archivedatabase.common.shared.database.Collection;

import nl.fews.archivedatabase.common.shared.settings.Settings;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public final class Filter {

	/**
	 *
	 */
	private static final DatabaseBridge database;

	static{
		try{
			database = (DatabaseBridge)Class.forName(String.format(Settings.DATABASE_NAMESPACE, Settings.get("databaseType", String.class).toLowerCase())).getConstructor().newInstance();
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 */
	private Filter() {}

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param fields the fields to return a distinct, ordered list of available filter values for
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @return Map<String, List<Object>>
	 */
	public static Map<String, Set<Object>> getFilters(String collection, Map<String, Class<String>> fields, Map<String, List<Object>> query) {
		var filters = new ConcurrentHashMap<String, Set<Object>>();
		fields.entrySet().parallelStream().forEach(e -> {
			var field = e.getKey();
			var clazz = e.getValue();
			filters.put(field, new HashSet<>());
			database.distinct(Collection.TimeSeriesIndex.toString(), field, Map.of("collection", collection), clazz).forEach(f -> filters.get(field).add(f));
		});
		filters.entrySet().parallelStream().forEach(e -> {
			var field = e.getKey();
			var members = e.getValue();

			var filtersFound = new HashSet<>();
			members.parallelStream().forEach(f -> {
				var document = new LinkedHashMap<String, Object>();
				document.put("collection", collection);
				document.put(field, f);
				query.forEach((k, v) -> {
					if(!v.isEmpty())
						document.put(k.replace("metaData.", ""), v.size() == 1 ? v.get(0) : Map.of("in", v));
				});
				filtersFound.addAll(database.distinct(Collection.TimeSeriesIndex.toString(), field, document, f.getClass()));
			});
			filters.put(field, filtersFound);
		});
		return filters;
	}
}
