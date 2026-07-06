package nl.fews.archivedatabase.common.shared.interfaces;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DatabaseBridge extends Database {
	/**
	 *
	 * @param collection collection
	 * @param query query
	 */
	void deleteOne(String collection, Map<String, Object> query);

	/**
	 *
	 * @param collection collection
	 * @param query query
	 */
	void deleteMany(String collection, Map<String, Object> query);

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 */
	void updateOne(String collection, Map<String, Object> query, Map<String, Object> document, boolean upsert);

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 */
	void updateOne(String collection, Map<String, Object> query, Map<String, Object> document);

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param document document
	 */
	void updateMany(String collection, Map<String, Object> query, Map<String, Object> document);

	/**
	 *
	 * @param collection collection
	 * @param document document
	 */
	Object insertOne(String collection, Map<String, Object> document);


	/**
	 *
	 * @param collection collection
	 * @param documents documents
	 */
	void insertMany(String collection, List<Map<String, Object>> documents);

	/**
	 *
	 * @param collection collection
	 * @param field field
	 * @param query query
	 * @param clazz clazz
	 * @return Set<T>
	 * @param <T> <T>
	 */
	<T> Set<T> distinct(String collection, String field, Map<String, Object> query, Class<T> clazz);

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param projection projection
	 * @return Map<String, Object>
	 */
	Map<String, Object> findOne(String collection, Map<String, Object> query, Map<String, Object> projection);
	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @return Map<String, Object>
	 */
	Map<String, Object> findOne(String collection, Map<String, Object> query);

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @param projection projection
	 * @return Map<String, Object>
	 */
	ClosableIterable<Map<String, Object>> find(String collection, Map<String, Object> query, Map<String, Object> projection);

	/**
	 *
	 * @param collection collection
	 * @param query query
	 * @return Map<String, Object>
	 */
	ClosableIterable<Map<String, Object>> find(String collection, Map<String, Object> query);

	/**
	 *
	 * @param collection collection
	 * @param match match
	 * @param sort sort
	 * @param projection projection
	 * @return ClosableIterable<Map<String, Object>>
	 */
	ClosableIterable<Map<String, Object>> aggregateStitchedTimeSeries(String collection, Map<String, Object> match, Map<String, Object> sort, Map<String, Object> projection);

	/**
	 *
	 * @param collection collection
	 * @param match match
	 * @param sort sort
	 * @return ClosableIterable<Map<String, Object>>
	 */
	ClosableIterable<Map<String, Object>> aggregateReadBuckets(String collection, Map<String, Object> match, Map<String, Object> sort);

	/**
	 *
	 * @param table table
	 * @param match match
	 * @param sort sort
	 * @param groupId groupId
	 * @return ClosableIterable<Map<String, Object>>
	 */
	ClosableIterable<Map<String, Object>> aggregateAvailableYears(String table, Map<String, Object> match, Map<String, Object> sort, Map<String, Object> groupId);

	/**
	 *
	 * @param table table
	 * @param match match
	 * @param sort sort
	 * @param groupId groupId
	 * @param limit limit
	 * @return ClosableIterable<Map<String, Object>>
	 */
	ClosableIterable<Map<String, Object>> aggregateExternalForecastTimes(String table, Map<String, Object> match, String groupId, Map<String, Object> sort, int limit);

	/**
	 *
	 * @param table table
	 * @param match match
	 * @param sort sort
	 * @param groupId groupId
	 * @param limit limit
	 * @return ClosableIterable<Map<String, Object>>
	 */
	ClosableIterable<Map<String, Object>> aggregateSimulatedTaskRunInfos(String table, Map<String, Object> match, Map<String, Object> groupId, Map<String, Object> sort, int limit);

	/**
	 *
	 * @param collection collection
	 * @param match match
	 * @param unwind unwind
	 * @return ClosableIterable<Map<String, Object>>
	 */
	ClosableIterable<Map<String, Object>> aggregateUnwoundTimeSeries(String collection, Map<String, Object> match, String unwind);

	/**
	 *
	 * @param collection collection
	 * @param sort sort
	 * @param groupId groupId
	 * @param first first
	 * @param hint hint
	 * @return ClosableIterable<Map<String, Object>>
	 */
	ClosableIterable<Map<String, Object>> aggregateTimeSeriesGroups(String collection, Map<String, Object> sort, Map<String, Object> groupId, Map<String, Object> first, Map<String, Object> hint);

	/**
	 *
	 * @param table table
	 * @param sort sort
	 * @param groupId groupId
	 * @param addFields addFields
	 * @return ClosableIterable<Map<String, Object>>
	 */
	ClosableIterable<Map<String, Object>> aggregateTimeSeriesIndex(String table, Map<String, Object> sort, Map<String, Object> groupId, Map<String, Object> addFields);

	/**
	 *
	 * @param table table
	 * @param match match
	 * @param sum sum
	 * @return Map<String, Object>
	 */
	Map<String, Object> aggregateOneMultiFieldGroupCount(String table, Map<String, Object> match, Map<String, Object> sum);

	/**
	 *
	 * @param table table
	 * @param match match
	 * @param groupId groupId
	 * @param sum sum
	 * @return Map<String, Object>
	 */
	Map<String, Object> aggregateOneMultiFieldGroupCount(String table, Map<String, Object> match, Map<String, Object> groupId, Map<String, Object> sum);
}

