package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import org.javatuples.Pair;

import java.util.Map;

/**
 *
 */
public final class TimeSeriesTypeUtil {

	/**
	 *
	 */
	public static final String SIMULATED_HISTORICAL = "simulated historical";

	/**
	 *
	 */
	public enum TimeSeriesTypeDetermination{
		TimeSeriesType,
		MetaDataType
	}

	/**
	 * lookup for string representation of each timeseries type
	 */
	private static final Map<TimeSeriesType, String> timeSeriesTypeStrings = Map.of(
			TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, "external forecasting",
			TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, "external historical",
			TimeSeriesType.SCALAR_SIMULATED_FORECASTING, "simulated forecasting",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, "simulated historical",
			TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET, "external historical",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED, "simulated historical",
			TimeSeriesType.SCALAR_SIMULATED_FORECASTING_OVERWRITE, "simulated forecasting",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_OVERWRITE, "simulated historical"
	);

	/**
	 * lookup for string representation of each timeseries type
	 */
	private static final Map<TimeSeriesType, String> timeSeriesTypeCollection = Map.of(
			TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, "ExternalForecastingScalarTimeSeries",
			TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, "ExternalHistoricalScalarTimeSeries",
			TimeSeriesType.SCALAR_SIMULATED_FORECASTING, "SimulatedForecastingScalarTimeSeries",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, "SimulatedHistoricalScalarTimeSeries",
			TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET, "ExternalHistoricalScalarTimeSeriesBucket",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED, "SimulatedHistoricalScalarTimeSeriesStitched",
			TimeSeriesType.SCALAR_SIMULATED_FORECASTING_OVERWRITE, "SimulatedForecastingScalarTimeSeries",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_OVERWRITE, "SimulatedHistoricalScalarTimeSeries"
	);

	/**
	 * lookup for string representation of each timeseries type
	 */
	private static final Map<TimeSeriesType, String> timeSeriesTypeClassName = Map.of(
			TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, "ScalarExternalForecasting",
			TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, "ScalarExternalHistorical",
			TimeSeriesType.SCALAR_SIMULATED_FORECASTING, "ScalarSimulatedForecasting",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, "ScalarSimulatedHistorical",
			TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET, "ScalarSimulatedHistoricalBucket",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED, "ScalarSimulatedHistoricalStitched",
			TimeSeriesType.SCALAR_SIMULATED_FORECASTING_OVERWRITE, "ScalarSimulatedForecasting",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_OVERWRITE, "ScalarSimulatedHistorical"
	);

	/**
	 * lookup for string representation of each timeseries type
	 */
	private static final Map<String, String> classNameTimeSeriesTypeString = Map.of(
			"ScalarExternalForecasting", "external forecasting",
			"ScalarExternalHistorical", "external historical",
			"ScalarSimulatedForecasting", "simulated forecasting",
			"ScalarSimulatedHistorical", "simulated historical",
			"ScalarSimulatedHistoricalBucket", "external historical",
			"ScalarSimulatedHistoricalStitched", "simulated historical"
	);

	/**
	 * lookup for string representation of each timeseries type
	 */
	private static final Map<TimeSeriesType, String> timeSeriesTypeSyncType = Map.of(
			TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, "SynchronizeSingletons",
			TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, "SynchronizeBuckets",
			TimeSeriesType.SCALAR_SIMULATED_FORECASTING, "SynchronizeSingletons",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, "SynchronizeSingletons",
			TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET, "SynchronizeBuckets",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED, "SynchronizeBuckets",
			TimeSeriesType.SCALAR_SIMULATED_FORECASTING_OVERWRITE, "SynchronizeSingletons",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_OVERWRITE, "SynchronizeSingletons"
	);

	/**
	 * lookup by valueType + metaDataType
	 */
	private static final Map<Pair<String, String>, TimeSeriesType> timeSeriesTypes = Map.of(
			new Pair<>("scalar", "netcdfMetaData"), TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL,
			new Pair<>("scalar", "externalForecastMetaData"), TimeSeriesType.SCALAR_EXTERNAL_FORECASTING,
			new Pair<>("scalar", "simulationMetaData"), TimeSeriesType.SCALAR_SIMULATED_FORECASTING,
			new Pair<>("scalar", "historicalMetaData"), TimeSeriesType.SCALAR_SIMULATED_HISTORICAL,
			new Pair<>("gridded", "netcdfMetaData"), TimeSeriesType.GRID_EXTERNAL_HISTORICAL,
			new Pair<>("gridded", "externalForecastMetaData"), TimeSeriesType.GRID_EXTERNAL_FORECASTING,
			new Pair<>("", "ratingCurvesMetaData"), TimeSeriesType.RATING_CURVES,
			new Pair<>("", "configMetaData"), TimeSeriesType.CONFIGURATION,
			new Pair<>("", "productsMetaData"), TimeSeriesType.PRODUCTS
	);

	/**
	 * lookup by valueType + timeSeriesTypeString
	 */
	private static final Map<Pair<String, String>, TimeSeriesType> timeSeriesTypesByTypeString = Map.of(
			new Pair<>("scalar", "external historical"), TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL,
			new Pair<>("scalar", "external forecasting"), TimeSeriesType.SCALAR_EXTERNAL_FORECASTING,
			new Pair<>("scalar", "simulated forecasting"), TimeSeriesType.SCALAR_SIMULATED_FORECASTING,
			new Pair<>("scalar", "simulated historical"), TimeSeriesType.SCALAR_SIMULATED_HISTORICAL,
			new Pair<>("gridded", "external historical"), TimeSeriesType.GRID_EXTERNAL_HISTORICAL,
			new Pair<>("gridded", "external forecasting"), TimeSeriesType.GRID_EXTERNAL_FORECASTING
	);

	/**
	 * Static Class
	 */
	private TimeSeriesTypeUtil(){}

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
	 * @param className className
	 * @return collection name
	 */
	public static String getTimeSeriesTypeString(String className) {
		return classNameTimeSeriesTypeString.get(className);
	}

	/**
	 *
	 * @param valueTypeTimeSeriesType valueTypeMetaDataType
	 * @return TimeSeriesType
	 */
	public static TimeSeriesType getTimeSeriesType(Pair<String, String> valueTypeTimeSeriesType){
		return timeSeriesTypes.get(valueTypeTimeSeriesType);
	}

	/**
	 *
	 * @param valueTypeTimeSeriesType valueTypeTimeSeriesType
	 * @return TimeSeriesType
	 */
	public static TimeSeriesType getTimeSeriesTypeByTypeString(Pair<String, String> valueTypeTimeSeriesType){
		return timeSeriesTypesByTypeString.get(valueTypeTimeSeriesType);
	}
}
