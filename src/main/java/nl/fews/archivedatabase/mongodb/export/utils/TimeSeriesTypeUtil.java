package nl.fews.archivedatabase.mongodb.export.utils;

import nl.fews.archivedatabase.mongodb.export.enums.TimeSeriesType;

import java.util.List;
import java.util.Map;

public class TimeSeriesTypeUtil {

	/**
	 * lookup for string representation of each timeseries type
	 */
	private static final Map<TimeSeriesType, String> timeSeriesTypeStrings = Map.of(
			TimeSeriesType.EXTERNAL_FORECASTING, "external forecasting",
			TimeSeriesType.EXTERNAL_HISTORICAL, "external historical",
			TimeSeriesType.SIMULATED_FORECASTING, "simulated forecasting",
			TimeSeriesType.SIMULATED_HISTORICAL, "simulated historical"
	);

	/**
	 * lookup for string representation of each timeseries type
	 */
	private static final Map<TimeSeriesType, String> timeSeriesTypeCollection = Map.of(
			TimeSeriesType.EXTERNAL_FORECASTING, "ExternalForecastingScalarTimeSeries",
			TimeSeriesType.EXTERNAL_HISTORICAL, "ExternalHistoricalScalarTimeSeries",
			TimeSeriesType.SIMULATED_FORECASTING, "SimulatedForecastingScalarTimeSeries",
			TimeSeriesType.SIMULATED_HISTORICAL, "SimulatedHistoricalScalarTimeSeries"
	);

	/**
	 * lookup for string representation of each timeseries type
	 */
	private static final Map<TimeSeriesType, String> timeSeriesTypeClassName = Map.of(
			TimeSeriesType.EXTERNAL_FORECASTING, "ExternalForecasting",
			TimeSeriesType.EXTERNAL_HISTORICAL, "ExternalHistorical",
			TimeSeriesType.SIMULATED_FORECASTING, "SimulatedForecasting",
			TimeSeriesType.SIMULATED_HISTORICAL, "SimulatedHistorical"
	);

	/**
	 * lookup for string representation of each timeseries type
	 */
	private static final Map<TimeSeriesType, String> timeSeriesTypeSyncType = Map.of(
			TimeSeriesType.EXTERNAL_FORECASTING, "Forecast",
			TimeSeriesType.EXTERNAL_HISTORICAL, "Bucket",
			TimeSeriesType.SIMULATED_FORECASTING, "Forecast",
			TimeSeriesType.SIMULATED_HISTORICAL, "Forecast"
	);

	/**
	 * lookup for string representation of each timeseries type
	 */
	private static final Map<TimeSeriesType, List<String>> timeSeriesTypeKeys = Map.of(
			TimeSeriesType.EXTERNAL_FORECASTING, List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime"),
			TimeSeriesType.EXTERNAL_HISTORICAL, List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucket"),
			TimeSeriesType.SIMULATED_FORECASTING, List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"),
			TimeSeriesType.SIMULATED_HISTORICAL, List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId")
	);

	private TimeSeriesTypeUtil(){

	}

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @return FEWS timeseries type string
	 */
	public static String getTimeSeriesTypeString(TimeSeriesType timeSeriesType) {
		return timeSeriesTypeStrings.get(timeSeriesType);
	}

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @return collection name
	 */
	public static String getTimeSeriesTypeCollection(TimeSeriesType timeSeriesType) {
		return timeSeriesTypeCollection.get(timeSeriesType);
	}

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @return collection name
	 */
	public static String getTimeSeriesTypeClassName(TimeSeriesType timeSeriesType) {
		return timeSeriesTypeClassName.get(timeSeriesType);
	}

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @return collection name
	 */
	public static String getTimeSeriesTypeSyncType(TimeSeriesType timeSeriesType) {
		return timeSeriesTypeSyncType.get(timeSeriesType);
	}

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @return the durable key members matching the insert data type collection's unique key
	 */
	public static List<String> getTimeSeriesTypeKeys(TimeSeriesType timeSeriesType) {
		return timeSeriesTypeKeys.get(timeSeriesType);
	}
}
