package nl.fews.archivedatabase.common.export.operations;

import nl.fews.archivedatabase.common.export.interfaces.Synchronize;
import nl.fews.archivedatabase.common.shared.database.Table;
import nl.fews.archivedatabase.common.shared.database.Database;
import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.tuple.Triplet;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesTypeUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public abstract class SynchronizeBase implements Synchronize {

	/**
	 *
	 */
	private static final Map<Map<String, Object>, Object> timeSeriesIndex = new ConcurrentHashMap<>();

	/**
	 *
	 */
	private static final AtomicBoolean isTimeSeriesIndexInitialized = new AtomicBoolean(false);

	/**
	 * Inserts, updates or replaces data for bucketed (observed) timeseries
	 * @param timeSeries the entire list of all documents passed to this instance
	 * @param timeSeriesType timeSeriesType
	 */
	public void synchronize(Map<String, Object> timeSeries, TimeSeriesType timeSeriesType){
		Map<String, Triplet<List<Map<String, Object>>, List<Map<String, Object>>, List<Map<String, Object>>>> insertUpdateRemove = getInsertUpdateRemove(timeSeries, timeSeriesType);
		synchronize(
			timeSeriesType,
			insertUpdateRemove.values().stream().flatMap(s -> s.getValue0().stream()).toList(),
			insertUpdateRemove.values().stream().flatMap(s -> s.getValue1().stream()).toList(),
			insertUpdateRemove.values().stream().flatMap(s -> s.getValue2().stream()).toList());
	}

	/**
	 * Bulk operations
	 * @param timeSeriesType timeSeriesType
	 * @param insert insert documents
	 * @param replace replace documents
	 * @param remove remove documents
	 */
	private void synchronize(TimeSeriesType timeSeriesType, List<Map<String, Object>> insert, List<Map<String, Object>> replace, List<Map<String, Object>> remove){
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		Map<Map<String, Object>, Object> missingTimeSeriesIndexes = new ConcurrentHashMap<>();
		if(!insert.isEmpty())
		{
			insert.parallelStream().forEach(ts -> Database.instance().insertOne(collection, ts));
			missingTimeSeriesIndexes.putAll(insert.stream().map(s -> getTimeSeriesIndexKey(s, collection)).filter(key -> !timeSeriesIndex.containsKey(key)).distinct().collect(Collectors.toMap(s -> s, s -> s)));
		}

		if(!replace.isEmpty()) {
			Database.instance().deleteMany(collection, Map.of("_id", Map.of("in", replace.stream().map(s -> s.get("_id")).toList())));
			replace.parallelStream().forEach(ts -> Database.instance().insertOne(collection, ts));
			missingTimeSeriesIndexes.putAll(replace.stream().map(s -> getTimeSeriesIndexKey(s, collection)).filter(key -> !timeSeriesIndex.containsKey(key)).distinct().collect(Collectors.toMap(s -> s, s -> s)));
		}

		if (!remove.isEmpty())
			Database.instance().deleteMany(collection, Map.of("_id", Map.of("in", remove.stream().map(s -> s.get("_id")).toList())));

		addMissingTimeSeriesIndexes(missingTimeSeriesIndexes);
	}

	/**
	 *
	 * @param document document
	 * @param collection collection
	 * @return Map<String, Object>
	 */
	private Map<String, Object> getTimeSeriesIndexKey(Map<String, Object> document, String collection){
		String moduleInstanceId = document.containsKey("moduleInstanceId") ? (String)document.get("moduleInstanceId") : "";
		String parameterId = document.containsKey("parameterId") ? (String)document.get("parameterId") : "";
		String encodedTimeStepId = document.containsKey("encodedTimeStepId") ? (String)document.get("encodedTimeStepId") : "";
		String areaId = document.containsKey("metaData") && ((Map<?, ?>)document.get("metaData")).containsKey("areaId") ? (String)((Map<?, ?>)document.get("metaData")).get("areaId") : "";
		String sourceId = document.containsKey("metaData") && ((Map<?, ?>)document.get("metaData")).containsKey("sourceId") ? (String)((Map<?, ?>)document.get("metaData")).get("sourceId") : "";
		return Map.of("moduleInstanceId", moduleInstanceId, "parameterId", parameterId, "encodedTimeStepId", encodedTimeStepId, "areaId", areaId, "sourceId", sourceId, "collection", collection);
	}

	/**
	 *
	 * @param missingTimeSeriesIndexes missingTimeSeriesIndexes
	 */
	private synchronized void addMissingTimeSeriesIndexes(Map<Map<String, Object>, Object> missingTimeSeriesIndexes){
		missingTimeSeriesIndexes.forEach((k, v) -> {
			try{
				Database.instance().insertOne(Table.TimeSeriesIndex.toString(), k);
				timeSeriesIndex.put(k, "");
			}
			catch (RuntimeException ex){
				//IGNORE
			}
		});
	}

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param timeSeriesType timeSeriesType
	 * @return Map<String, Triplet<List<Map<String, Object>>, List<Map<String, Object>>, List<Map<String, Object>>>>
	 */
	protected abstract Map<String, Triplet<List<Map<String, Object>>, List<Map<String, Object>>, List<Map<String, Object>>>> getInsertUpdateRemove(Map<String, Object> timeSeries, TimeSeriesType timeSeriesType);
}
