package nl.fews.archivedatabase.mongodb.database;

import nl.fews.archivedatabase.common.shared.database.Table;
import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesTypeUtil;
import nl.fews.archivedatabase.common.shared.database.Index;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class Schema {

	/**
	 * Static Class
	 */
	private Schema(){}

	/**
	 *
	 */
	private static final List<String> timeseriesFields = List.of("t", "lt", "v","dv", "f", "u", "c","fs");

	/**
	 *
	 */
	private static final Set<String> timeseriesValueFields = Set.of("v","dv");

	/**
	 *
	 * @return List<String>
	 */
	public static List<String> getTimeseriesFields(){
		return timeseriesFields;
	}

	/**
	 *
	 * @return Set<String>
	 */
	public static Set<String> getTimeseriesValueFields(){
		return timeseriesValueFields;
	}

	/**
	 *
	 * @param collection collection
	 * @return The array representing default indexes to be applied to the given collection.  The first entry is unique
	 */
	public static List<Map<String, Object>> getIndexes(String collection) {
		return Schema.index.get(collection);
	}

	/**
	 *
	 * @return all collections / tables
	 */
	public static Set<String> getCollections() {
		return Schema.index.keySet();
	}

	/**
	 * default indexes for each collection
	 */
	private static final Map<String, List<Map<String, Object>>> index = Map.of(
		TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING), List.of(
			Map.of("keys", Index.getKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING)), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "forecastTime")),
			Map.of("keys", List.of("locationId", "parameterId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "forecastTime")),
			Map.of("keys", List.of("locationId", "forecastTime")),
			Map.of("keys", List.of("parameterId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("locationId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "metaData.modifiedTime")),
			Map.of("keys", List.of("locationId", "metaData.modifiedTime")),
			Map.of("keys", List.of("parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "localForecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "localForecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "forecastTime" )),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "localForecastTime", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "localForecastTime", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "forecastTime", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "forecastTime", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId")),
			Map.of("keys", List.of("locationId", "parameterId")),
			Map.of("keys", List.of("moduleInstanceId")),
			Map.of("keys", List.of("locationId")),
			Map.of("keys", List.of("parameterId")),
			Map.of("keys", List.of("forecastTime")),
			Map.of("keys", List.of("localForecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("locationId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "startTime", "endTime")),
			Map.of("keys", List.of("locationId", "startTime", "endTime")),
			Map.of("keys", List.of("parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("forecastTime", "startTime", "endTime")),
			Map.of("keys", List.of("startTime", "endTime")),
			Map.of("keys", List.of("encodedTimeStepId")),
			Map.of("keys", List.of("metaData.sourceId")),
			Map.of("keys", List.of("encodedTimeStepId", "forecastTime")),
			Map.of("keys", List.of("encodedTimeStepId", "metaData.areaId", "forecastTime", "metaData.sourceId")),
			Map.of("keys", List.of("parameterId", "metaData.areaId", "forecastTime", "metaData.sourceId")),
			Map.of("keys", List.of("moduleInstanceId", "metaData.areaId", "forecastTime", "metaData.sourceId")),
			Map.of("keys", List.of("metaData.areaId", "forecastTime", "moduleInstanceId", "parameterId", "encodedTimeStepId", "metaData.sourceId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "encodedTimeStepId", "metaData.areaId", "metaData.sourceId")),
			Map.of("keys", List.of("committed"))
		),
		TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), List.of(
			Map.of("keys", Index.getKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "endTime"), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime"), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "endTime"), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "metaData.timeStepMinutes")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("locationId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "metaData.modifiedTime")),
			Map.of("keys", List.of("locationId", "metaData.modifiedTime")),
			Map.of("keys", List.of("parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("locationId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "startTime", "endTime")),
			Map.of("keys", List.of("locationId", "startTime", "endTime")),
			Map.of("keys", List.of("parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "localStartTime", "localEndTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "localStartTime", "localEndTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "startTime", "endTime" )),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId")),
			Map.of("keys", List.of("locationId", "parameterId")),
			Map.of("keys", List.of("moduleInstanceId")),
			Map.of("keys", List.of("locationId")),
			Map.of("keys", List.of("parameterId")),
			Map.of("keys", List.of("startTime", "endTime")),
			Map.of("keys", List.of("localStartTime", "localEndTime")),
			Map.of("keys", List.of("encodedTimeStepId")),
			Map.of("keys", List.of("metaData.sourceId")),
			Map.of("keys", List.of("encodedTimeStepId", "startTime", "endTime")),
			Map.of("keys", List.of("encodedTimeStepId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId")),
			Map.of("keys", List.of("parameterId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId")),
			Map.of("keys", List.of("moduleInstanceId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId")),
			Map.of("keys", List.of("metaData.areaId", "startTime", "endTime", "moduleInstanceId", "parameterId", "encodedTimeStepId", "metaData.sourceId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "encodedTimeStepId", "metaData.areaId", "metaData.sourceId")),
			Map.of("keys", List.of("committed"))
		),
		TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET), List.of(
			Map.of("keys", Index.getKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET)), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "endTime"), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime"), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "endTime"), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "metaData.timeStepMinutes")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("locationId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "metaData.modifiedTime")),
			Map.of("keys", List.of("locationId", "metaData.modifiedTime")),
			Map.of("keys", List.of("parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("locationId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "startTime", "endTime")),
			Map.of("keys", List.of("locationId", "startTime", "endTime")),
			Map.of("keys", List.of("parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "localStartTime", "localEndTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "localStartTime", "localEndTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "startTime", "endTime" )),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId")),
			Map.of("keys", List.of("locationId", "parameterId")),
			Map.of("keys", List.of("moduleInstanceId")),
			Map.of("keys", List.of("locationId")),
			Map.of("keys", List.of("parameterId")),
			Map.of("keys", List.of("startTime", "endTime")),
			Map.of("keys", List.of("localStartTime", "localEndTime")),
			Map.of("keys", List.of("encodedTimeStepId")),
			Map.of("keys", List.of("metaData.sourceId")),
			Map.of("keys", List.of("encodedTimeStepId", "startTime", "endTime")),
			Map.of("keys", List.of("encodedTimeStepId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId")),
			Map.of("keys", List.of("parameterId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId")),
			Map.of("keys", List.of("moduleInstanceId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId")),
			Map.of("keys", List.of("metaData.areaId", "startTime", "endTime", "moduleInstanceId", "parameterId", "encodedTimeStepId", "metaData.sourceId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "encodedTimeStepId", "metaData.areaId", "metaData.sourceId")),
			Map.of("keys", List.of("committed"))
		),
		TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), List.of(
			Map.of("keys", Index.getKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING)), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "forecastTime")),
			Map.of("keys", List.of("locationId", "parameterId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "forecastTime")),
			Map.of("keys", List.of("locationId", "forecastTime")),
			Map.of("keys", List.of("parameterId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("locationId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "metaData.modifiedTime")),
			Map.of("keys", List.of("locationId", "metaData.modifiedTime")),
			Map.of("keys", List.of("parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("metaData.modifiedTime")),
			Map.of("keys", List.of("taskRunId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "localForecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "localForecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "forecastTime" )),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "localForecastTime", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "localForecastTime", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "forecastTime", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "forecastTime", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId")),
			Map.of("keys", List.of("locationId", "parameterId")),
			Map.of("keys", List.of("moduleInstanceId")),
			Map.of("keys", List.of("locationId")),
			Map.of("keys", List.of("parameterId")),
			Map.of("keys", List.of("forecastTime")),
			Map.of("keys", List.of("localForecastTime")),
			Map.of("keys", List.of("taskRunId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("locationId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "startTime", "endTime")),
			Map.of("keys", List.of("locationId", "startTime", "endTime")),
			Map.of("keys", List.of("parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("startTime", "endTime")),
			Map.of("keys", List.of("encodedTimeStepId")),
			Map.of("keys", List.of("metaData.sourceId")),
			Map.of("keys", List.of("encodedTimeStepId", "forecastTime")),
			Map.of("keys", List.of("encodedTimeStepId", "metaData.areaId", "forecastTime", "metaData.sourceId")),
			Map.of("keys", List.of("parameterId", "metaData.areaId", "forecastTime", "metaData.sourceId")),
			Map.of("keys", List.of("moduleInstanceId", "metaData.areaId", "forecastTime", "metaData.sourceId")),
			Map.of("keys", List.of("metaData.areaId", "forecastTime", "moduleInstanceId", "parameterId", "encodedTimeStepId", "metaData.sourceId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "encodedTimeStepId", "metaData.areaId", "metaData.sourceId")),
			Map.of("keys", List.of("committed"))
		),
		TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), List.of(
			Map.of("keys", Index.getKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL)), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "metaData.timeStepMinutes")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "forecastTime")),
			Map.of("keys", List.of("locationId", "parameterId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "forecastTime")),
			Map.of("keys", List.of("locationId", "forecastTime")),
			Map.of("keys", List.of("parameterId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("locationId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "metaData.modifiedTime")),
			Map.of("keys", List.of("locationId", "metaData.modifiedTime")),
			Map.of("keys", List.of("parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("metaData.modifiedTime")),
			Map.of("keys", List.of("taskRunId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "localForecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "localForecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "forecastTime" )),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "forecastTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "localForecastTime", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "localForecastTime", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "forecastTime", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "forecastTime", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId")),
			Map.of("keys", List.of("locationId", "parameterId")),
			Map.of("keys", List.of("moduleInstanceId")),
			Map.of("keys", List.of("locationId")),
			Map.of("keys", List.of("parameterId")),
			Map.of("keys", List.of("forecastTime")),
			Map.of("keys", List.of("localForecastTime")),
			Map.of("keys", List.of("taskRunId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("locationId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "startTime", "endTime")),
			Map.of("keys", List.of("locationId", "startTime", "endTime")),
			Map.of("keys", List.of("parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("startTime", "endTime")),
			Map.of("keys", List.of("encodedTimeStepId")),
			Map.of("keys", List.of("metaData.sourceId")),
			Map.of("keys", List.of("encodedTimeStepId", "forecastTime")),
			Map.of("keys", List.of("encodedTimeStepId", "metaData.areaId", "forecastTime", "metaData.sourceId")),
			Map.of("keys", List.of("parameterId", "metaData.areaId", "forecastTime", "metaData.sourceId")),
			Map.of("keys", List.of("moduleInstanceId", "metaData.areaId", "forecastTime", "metaData.sourceId")),
			Map.of("keys", List.of("metaData.areaId", "forecastTime", "moduleInstanceId", "parameterId", "encodedTimeStepId", "metaData.sourceId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "encodedTimeStepId", "metaData.areaId", "metaData.sourceId")),
			Map.of("keys", List.of("committed"))
		),
		TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED), List.of(
			Map.of("keys", Index.getKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "startTime", "endTime"), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "startTime"), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "endTime"), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("locationId", "parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "metaData.modifiedTime")),
			Map.of("keys", List.of("locationId", "metaData.modifiedTime")),
			Map.of("keys", List.of("parameterId", "metaData.modifiedTime")),
			Map.of("keys", List.of("metaData.modifiedTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "localStartTime", "localEndTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "localStartTime", "localEndTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId", "startTime", "endTime" )),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId", "locationId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "qualifierId", "encodedTimeStepId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId")),
			Map.of("keys", List.of("locationId", "parameterId")),
			Map.of("keys", List.of("moduleInstanceId")),
			Map.of("keys", List.of("locationId")),
			Map.of("keys", List.of("parameterId")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "locationId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("locationId", "parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("moduleInstanceId", "startTime", "endTime")),
			Map.of("keys", List.of("locationId", "startTime", "endTime")),
			Map.of("keys", List.of("parameterId", "startTime", "endTime")),
			Map.of("keys", List.of("startTime", "endTime")),
			Map.of("keys", List.of("localStartTime", "localEndTime")),
			Map.of("keys", List.of("encodedTimeStepId")),
			Map.of("keys", List.of("encodedTimeStepId", "startTime", "endTime")),
			Map.of("keys", List.of("encodedTimeStepId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId")),
			Map.of("keys", List.of("parameterId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId")),
			Map.of("keys", List.of("moduleInstanceId", "metaData.areaId", "startTime", "endTime", "metaData.sourceId")),
			Map.of("keys", List.of("metaData.areaId", "startTime", "endTime", "moduleInstanceId", "parameterId", "encodedTimeStepId", "metaData.sourceId")),
			Map.of("keys", List.of("moduleInstanceId", "parameterId", "encodedTimeStepId", "metaData.areaId", "metaData.sourceId")),
			Map.of("keys", List.of("committed"))
		),
		Table.MigrateMetaData.toString(), List.of(
			Map.of("keys", List.of("metaDataFileRelativePath"), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("netcdfFiles.timeSeriesIds")),
			Map.of("keys", List.of("committed"))
		),
		Table.MigrateLog.toString(), List.of(
			Map.of("keys", List.of("date")),
			Map.of("keys", List.of("errorCategory"))
		),
		Table.BucketSize.toString(), List.of(
			Map.of("keys", List.of("bucketCollection", "bucketKey"), "options", Map.of("unique", 1))
		),
		Table.TimeSeriesIndex.toString(), List.of(
			Map.of("keys", List.of("collection", "moduleInstanceId", "parameterId", "encodedTimeStepId", "areaId", "sourceId"), "options", Map.of("unique", 1)),
			Map.of("keys", List.of("collection", "moduleInstanceId")),
			Map.of("keys", List.of("collection", "parameterId")),
			Map.of("keys", List.of("collection", "encodedTimeStepId")),
			Map.of("keys", List.of("collection", "areaId")),
			Map.of("keys", List.of("collection", "sourceId")),
			Map.of("keys", List.of("collection", "moduleInstanceId", "areaId", "sourceId")),
			Map.of("keys", List.of("collection", "parameterId", "areaId", "sourceId")),
			Map.of("keys", List.of("collection", "encodedTimeStepId", "areaId", "sourceId"))
		)
	);
}
