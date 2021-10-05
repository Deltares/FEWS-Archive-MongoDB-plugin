package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import org.javatuples.Pair;

import java.util.Map;

/**
 *
 */
public final class TimeSeriesTypeUtil {

	/**
	 * timeSeriesTypeBucket
	 */
	private static final Map<TimeSeriesType, Boolean> timeSeriesTypeBucket = Map.of(
			TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, false,
			TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, true,
			TimeSeriesType.SCALAR_SIMULATED_FORECASTING, false,
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, false,
			TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET, true,
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED, true,
			TimeSeriesType.SCALAR_SIMULATED_FORECASTING_OVERWRITE, false,
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_OVERWRITE, false
	);

	/**
	 * lookup for string representation of each timeseries type
	 */
	private static final Map<TimeSeriesType, String> timeSeriesTypeString = Map.of(
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
	private static final Map<TimeSeriesType, String> timeSeriesTypeTypes = Map.of(
			TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, "Singletons",
			TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, "Buckets",
			TimeSeriesType.SCALAR_SIMULATED_FORECASTING, "Singletons",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, "Singletons",
			TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET, "Buckets",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED, "Buckets",
			TimeSeriesType.SCALAR_SIMULATED_FORECASTING_OVERWRITE, "Singletons",
			TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_OVERWRITE, "Singletons"
	);

	/**
	 * lookup for archive time series type representation of each timeseries type
	 */
	private static final Map<Pair<TimeSeriesValueType, nl.wldelft.fews.system.data.timeseries.TimeSeriesType>, TimeSeriesType> timeSeriesTypeByArchiveType = Map.of(
			new Pair<>(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.EXTERNAL_FORECASTING),TimeSeriesType.SCALAR_EXTERNAL_FORECASTING,
			new Pair<>(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.EXTERNAL_HISTORICAL),TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL,
			new Pair<>(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_FORECASTING),TimeSeriesType.SCALAR_SIMULATED_FORECASTING
			//nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_HISTORICAL,TimeSeriesType.SCALAR_SIMULATED_HISTORICAL,
			//nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_HISTORICAL,TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED
	);

	/**
	 * lookup by valueType + metaDataType
	 */
	private static final Map<Pair<String, String>, TimeSeriesType> timeSeriesType = Map.of(
			new Pair<>("scalar", "netcdfMetaData"), TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL,
			new Pair<>("scalar", "externalForecastMetaData"), TimeSeriesType.SCALAR_EXTERNAL_FORECASTING,
			new Pair<>("scalar", "simulationMetaData"), TimeSeriesType.SCALAR_SIMULATED_FORECASTING,
			new Pair<>("scalar", "historicalMetaData"), TimeSeriesType.SCALAR_SIMULATED_HISTORICAL,
			new Pair<>("grid", "netcdfMetaData"), TimeSeriesType.GRID_EXTERNAL_HISTORICAL,
			new Pair<>("grid", "externalForecastMetaData"), TimeSeriesType.GRID_EXTERNAL_FORECASTING,
			new Pair<>("", "ratingCurvesMetaData"), TimeSeriesType.RATING_CURVES,
			new Pair<>("", "configMetaData"), TimeSeriesType.CONFIGURATION,
			new Pair<>("", "productsMetaData"), TimeSeriesType.PRODUCTS
	);

	/**
	 * lookup by valueType + timeSeriesTypeString
	 */
	private static final Map<Pair<String, String>, TimeSeriesType> timeSeriesTypeByTypeString = Map.of(
			new Pair<>("scalar", "external historical"), TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL,
			new Pair<>("scalar", "external forecasting"), TimeSeriesType.SCALAR_EXTERNAL_FORECASTING,
			new Pair<>("scalar", "simulated forecasting"), TimeSeriesType.SCALAR_SIMULATED_FORECASTING,
			new Pair<>("scalar", "simulated historical"), TimeSeriesType.SCALAR_SIMULATED_HISTORICAL,
			new Pair<>("grid", "external historical"), TimeSeriesType.GRID_EXTERNAL_HISTORICAL,
			new Pair<>("grid", "external forecasting"), TimeSeriesType.GRID_EXTERNAL_FORECASTING
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
		return timeSeriesTypeString.get(timeSeriesType);
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
	public static String getTimeSeriesTypeTypes(TimeSeriesType timeSeriesType) {
		return timeSeriesTypeTypes.get(timeSeriesType);
	}

	/**
	 *
	 * @param valueTypeTimeSeriesType valueTypeMetaDataType
	 * @return TimeSeriesType
	 */
	public static TimeSeriesType getTimeSeriesType(Pair<String, String> valueTypeTimeSeriesType){
		return timeSeriesType.get(valueTypeTimeSeriesType);
	}

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @return TimeSeriesType
	 */
	public static boolean getTimeSeriesTypeBucket(TimeSeriesType timeSeriesType){
		return timeSeriesTypeBucket.get(timeSeriesType);
	}

	/**
	 *
	 * @param valueTypeTimeSeriesType valueTypeTimeSeriesType
	 * @return TimeSeriesType
	 */
	public static TimeSeriesType getTimeSeriesTypeByTypeString(Pair<String, String> valueTypeTimeSeriesType){
		return timeSeriesTypeByTypeString.get(valueTypeTimeSeriesType);
	}

	/**
	 *
	 * @param timeSeriesValueType timeSeriesValueType
	 * @param timeSeriesType timeSeriesType
	 * @return TimeSeriesType
	 */
	public static TimeSeriesType getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType timeSeriesValueType, nl.wldelft.fews.system.data.timeseries.TimeSeriesType timeSeriesType){
		return timeSeriesTypeByArchiveType.get(new Pair<>(timeSeriesValueType, timeSeriesType));
	}
}
