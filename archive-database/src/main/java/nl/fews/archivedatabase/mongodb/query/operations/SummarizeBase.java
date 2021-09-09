package nl.fews.archivedatabase.mongodb.query.operations;

import nl.fews.archivedatabase.mongodb.query.interfaces.Summarize;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public abstract class SummarizeBase implements Summarize {
	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param distinctKeyFields the fields over which to group results and return a unique item count
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate event startDate, inclusive
	 * @param endDate event endDate, inclusive
	 * @return Map<String, List<Object>>
	 */
	@Override
	public Map<String, Integer> getSummary(String collection, Map<String, List<String>> distinctKeyFields, Map<String, List<Object>> query, Date startDate, Date endDate){
		Map<String, Integer> summarized = new ConcurrentHashMap<>();

		distinctKeyFields.entrySet().parallelStream().forEach(e -> {
			String keyField = e.getKey();
			List<String> fields = e.getValue();

			if(fields.size() == 1)
				summarized.put(keyField, getSingleFieldDistinctCount(collection, query, startDate, endDate, fields.get(0)));
			else
				summarized.put(keyField, getMultiFieldGroupCount(collection, query, startDate, endDate, fields));
		});

		return summarized;
	}

	public abstract int getMultiFieldGroupCount(String collection, Map<String, List<Object>> query, Date startDate, Date endDate, List<String> fields);

	public abstract int getSingleFieldDistinctCount(String collection, Map<String, List<Object>> query, Date startDate, Date endDate, String field);
}
