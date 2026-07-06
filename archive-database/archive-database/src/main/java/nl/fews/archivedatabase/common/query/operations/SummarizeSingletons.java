package nl.fews.archivedatabase.common.query.operations;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public final class SummarizeSingletons extends nl.fews.archivedatabase.common.query.operations.SummarizeBase {
	/**
	 * @param collection        the collection in the database from which the result was derived
	 * @param distinctKeyFields the fields over which to group results and return a unique item count
	 * @param query             the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate         event startDate, inclusive
	 * @param endDate           event endDate, inclusive
	 * @return Map<String, List < Object>>
	 */
	@Override
	public Map<String, Integer> getSummary(String collection, Map<String, List<String>> distinctKeyFields, Map<String, List<Object>> query, Date startDate, Date endDate) {
		var summarized = new ConcurrentHashMap<String, Integer>();
		var match = new LinkedHashMap<String, Object>(Map.of("forecastTime", Map.of("gte", startDate, "lte", endDate)));
		query.entrySet().stream().filter(e -> !e.getValue().isEmpty()).forEach(e ->
			match.put(e.getKey(), e.getValue().size() == 1 ?
				e.getValue().get(0) :
				Map.of("in", e.getValue())));

		distinctKeyFields.entrySet().parallelStream().forEach(e -> summarized.put(e.getKey(), e.getValue().size() == 1 ?
			getSingleFieldDistinctCount(collection, match, e.getValue().get(0)) :
			getMultiFieldGroupCount(collection, match, e.getValue())));

		return summarized;
	}
}
