package nl.fews.archivedatabase.common.query.operations;

import nl.fews.archivedatabase.common.query.interfaces.Summarize;
import nl.fews.archivedatabase.common.shared.database.Database;

import java.util.stream.Collectors;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class SummarizeBase implements Summarize {

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param match the filter over which distinct counts are to be made
	 * @return int
	 */
	protected int getMultiFieldGroupCount(String collection, Map<String, Object> match, List<String> fields){
		var group = fields.stream().collect(Collectors.toMap(s -> s, s -> (Object)s, (a, b) -> b, LinkedHashMap::new));
		Map<String, Object> result = fields.isEmpty() ?
			Database.instance().aggregateOneMultiFieldGroupCount(collection, match, Map.of("count", 1)) :
			Database.instance().aggregateOneMultiFieldGroupCount(collection, match,  group, Map.of("count", 1));
		return result != null ? ((Number)result.get("count")).intValue() : 0;
	}

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param match the filter over which distinct counts are to be made
	 * @return int
	 */
	protected int getSingleFieldDistinctCount(String collection, Map<String, Object> match, String field){
		return Database.instance().distinct(collection, field, match, String.class).size();
	}
}
