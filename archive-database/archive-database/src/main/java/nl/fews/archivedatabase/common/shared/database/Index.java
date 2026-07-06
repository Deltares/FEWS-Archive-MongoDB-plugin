package nl.fews.archivedatabase.common.shared.database;

import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesTypeUtil;
import org.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 *
 */
public final class Index {

	/**
	 * Static Class
	 */
	private Index(){}

	/**
	 *
	 * @param collection collection
	 * @return the durable key members matching the insert data type collection's unique key
	 */
	public static List<String> getKeys(String collection) {
		return Index.keys.get(collection);
	}

	/**
	 *
	 * @param keys the durable key members matching the insert data type collection's unique key
	 * @return the key values for the passed timeseries
	 */
	public static Map<String, Object> getKeyDocument(List<String> keys, Map<String, Object> document) {
		return keys.stream().collect(Collectors.toMap(k -> k, document::get, (k, v) -> v, LinkedHashMap::new));
	}

	/**
	 *
	 * @param keyDocument keyDocument
	 * @return the string key for the passed document
	 */
	public static String getKey(Map<String, Object> keyDocument) {
		return new JSONObject(keyDocument).toString();
	}

	/**
	 * lookup for string representation of each collection
	 */
	private static final Map<String, List<String>> keys = new ConcurrentHashMap<>(Map.of(
		TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime"),
		TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucketSize", "bucket"),
		TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucketSize", "bucket"),
		TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"),
		TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"),
		TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED), List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "bucketSize", "bucket")
	));
}
