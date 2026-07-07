package nl.fews.archivedatabase.common.query.operations;
import nl.fews.archivedatabase.common.shared.database.Database;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides streaming capability for singleton timeseries
 */
public final class HasDataSingletons {

	/**
	 * Static Class
	 */
	private HasDataSingletons(){}

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate forecast startDate, inclusive
	 * @param endDate forecast endDate, inclusive
	 * @return boolean
	 */
	public static boolean hasData(String collection, Map<String, List<Object>> query, Date startDate, Date endDate) {
		var document = new LinkedHashMap<String, Object>();

		if(startDate != null && endDate != null)
			document.put("forecastTime", Map.of("gte", startDate,"lte", endDate));
		else if(startDate != null)
			document.put("forecastTime", Map.of("gte", startDate));
		else if(endDate != null)
			document.put("forecastTime", Map.of("lte", endDate));

		query.forEach((k, v) -> {
			if(!v.isEmpty())
				document.put(k, v.size() == 1 ? v.get(0) : Map.of("in", v));
		});
		return Database.instance().findOne(collection, document, Map.of("_id", 1)) != null;
	}
}
