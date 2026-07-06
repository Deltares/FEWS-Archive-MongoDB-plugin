package nl.fews.archivedatabase.common.query.operations;

import nl.fews.archivedatabase.common.query.interfaces.Read;
import nl.fews.archivedatabase.common.shared.interfaces.DatabaseBridge;
import nl.fews.archivedatabase.common.shared.interfaces.ClosableIterable;
import nl.fews.archivedatabase.common.shared.settings.Settings;

import java.util.*;

/**
 * Provides streaming capability for singleton timeseries
 */
public final class ReadSingletons implements Read {

	/**
	 *
	 */
	private final DatabaseBridge database;

	/**
	 *
	 */
	public ReadSingletons() {
		try{
			database = (DatabaseBridge)Class.forName(String.format(Settings.DATABASE_NAMESPACE, Settings.get("databaseType", String.class).toLowerCase())).getConstructor().newInstance();
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query      the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate  forecast startDate, inclusive
	 * @param endDate    forecast endDate, inclusive
	 * @return ClosableIterable<Map<String, Object>>
	 */
	@Override
	public ClosableIterable<Map<String, Object>> read(String collection, Map<String, ?> query, Date startDate, Date endDate) {
		var match = new LinkedHashMap<String, Object>();

		if(startDate != null && endDate != null)
			match.put("forecastTime", Map.of("gte", startDate, "lte", endDate));
		else if(startDate != null)
			match.put("forecastTime", Map.of("gte", startDate));
		else if(endDate != null)
			match.put("forecastTime", Map.of("lte", endDate));

		query.forEach((k, v) -> {
			if((v instanceof List<?> val) && !val.isEmpty())
				match.put(k, val.size() == 1 ? val.get(0) : Map.of("in", v));
			else
				match.put(k, v);
		});
		return database.find(collection, match);
	}

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @return ClosableIterable<Map<String, Object>>
	 */
	@Override
	public ClosableIterable<Map<String, Object>> read(String collection, Map<String, ?> query) {
		return read(collection, query, null, null);
	}
}
