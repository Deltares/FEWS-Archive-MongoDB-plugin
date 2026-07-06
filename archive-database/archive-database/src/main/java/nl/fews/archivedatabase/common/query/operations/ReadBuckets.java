package nl.fews.archivedatabase.common.query.operations;

import nl.fews.archivedatabase.common.query.interfaces.Read;
import nl.fews.archivedatabase.common.query.utils.BucketIterator;
import nl.fews.archivedatabase.common.shared.interfaces.DatabaseBridge;
import nl.fews.archivedatabase.common.shared.database.Index;
import nl.fews.archivedatabase.common.shared.interfaces.ClosableIterable;
import nl.fews.archivedatabase.common.shared.settings.Settings;

import java.util.*;

/**
 * Provides streaming capability for bucketed timeseries
 */
public final class ReadBuckets implements Read {

	/**
	 *
	 */
	private final DatabaseBridge database;

	/**
	 *
	 */
	public ReadBuckets() {
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
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate event startDate, inclusive
	 * @param endDate event endDate, inclusive
	 * @return ClosableIterable<Map<String, Object>>
	 */
	@Override
	public ClosableIterable<Map<String, Object>> read(String collection, Map<String, ?> query, Date startDate, Date endDate) {
		var match = new LinkedHashMap<String, Object>();

		if(startDate != null)
			match.put("endTime", Map.of("gte", startDate));
		if(endDate != null)
			match.put("startTime", Map.of("lte", endDate));

		query.forEach((k, v) -> {
			if((v instanceof List<?> val) && !val.isEmpty())
				match.put(k, val.size() == 1 ? val.get(0) : Map.of("in", v));
			else
				match.put(k, v);
		});

		var sort = new LinkedHashMap<String, Object>();
		Index.getKeys(collection).forEach(field -> sort.put(field, 1));

		return () -> new BucketIterator(database.aggregateReadBuckets(collection, match, sort).iterator(), collection, startDate, endDate);
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