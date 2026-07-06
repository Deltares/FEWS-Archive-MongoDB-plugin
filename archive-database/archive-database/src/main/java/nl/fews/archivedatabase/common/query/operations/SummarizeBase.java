package nl.fews.archivedatabase.common.query.operations;

import nl.fews.archivedatabase.common.query.interfaces.Summarize;
import nl.fews.archivedatabase.common.shared.interfaces.DatabaseBridge;
import nl.fews.archivedatabase.common.shared.settings.Settings;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public abstract class SummarizeBase implements Summarize {

	/**
	 *
	 */
	private final DatabaseBridge database;

	/**
	 *
	 */
	public SummarizeBase() {
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
	 * @param match the filter over which distinct counts are to be made
	 * @return int
	 */
	protected int getMultiFieldGroupCount(String collection, Map<String, Object> match, List<String> fields){
		var group = fields.stream().collect(Collectors.toMap(s -> s, s -> (Object)s, (a, b) -> b, LinkedHashMap::new));
		Map<String, Object> result = fields.isEmpty() ?
			database.aggregateOneMultiFieldGroupCount(collection, match, Map.of("count", 1)) :
			database.aggregateOneMultiFieldGroupCount(collection, match,  group, Map.of("count", 1));
		return result != null ? ((Number)result.get("count")).intValue() : 0;
	}

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param match the filter over which distinct counts are to be made
	 * @return int
	 */
	protected int getSingleFieldDistinctCount(String collection, Map<String, Object> match, String field){
		return database.distinct(collection, field, match, String.class).size();
	}
}
