package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import org.javatuples.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class TimeSeriesTypeUtilTest {

	@Test
	void testGetTimeSeriesTypeString() {
		Assertions.assertEquals("external forecasting", TimeSeriesTypeUtil.getTimeSeriesTypeString(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING));
		Assertions.assertEquals("external historical", TimeSeriesTypeUtil.getTimeSeriesTypeString(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
		Assertions.assertEquals("simulated forecasting", TimeSeriesTypeUtil.getTimeSeriesTypeString(TimeSeriesType.SCALAR_SIMULATED_FORECASTING));
	}

	@Test
	void testGetTimeSeriesTypeCollection() {
		Assertions.assertEquals("ExternalForecastingScalarTimeSeries", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING));
		Assertions.assertEquals("ExternalHistoricalScalarTimeSeries", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
		Assertions.assertEquals("SimulatedForecastingScalarTimeSeries", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING));
	}

	@Test
	void getTimeSeriesTypeClassName() {
		Assertions.assertEquals("ScalarExternalForecasting", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING));
		Assertions.assertEquals("ScalarExternalHistorical", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
		Assertions.assertEquals("ScalarSimulatedForecasting", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(TimeSeriesType.SCALAR_SIMULATED_FORECASTING));
	}

	@Test
	void getTimeSeriesType() {
		Assertions.assertNull(TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("", "")));
		Assertions.assertEquals(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("scalar", "netcdfMetaData")));
		Assertions.assertEquals(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("scalar", "externalForecastMetaData")));
		Assertions.assertEquals(TimeSeriesType.SCALAR_SIMULATED_FORECASTING, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("scalar", "simulationMetaData")));
		Assertions.assertEquals(TimeSeriesType.GRID_EXTERNAL_HISTORICAL, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("gridded", "netcdfMetaData")));
		Assertions.assertEquals(TimeSeriesType.GRID_EXTERNAL_FORECASTING, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("gridded", "externalForecastMetaData")));
		Assertions.assertEquals(TimeSeriesType.RATING_CURVES, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("", "ratingCurvesMetaData")));
		Assertions.assertEquals(TimeSeriesType.CONFIGURATION, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("", "configMetaData")));
		Assertions.assertEquals(TimeSeriesType.PRODUCTS, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("", "productsMetaData")));
	}

	@Test
	void getTimeSeriesTypeByTypeString() {
		Assertions.assertNull(TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("", "")));
		Assertions.assertEquals(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>("scalar", "external historical")));
		Assertions.assertEquals(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>("scalar", "external forecasting")));
		Assertions.assertEquals(TimeSeriesType.SCALAR_SIMULATED_FORECASTING, TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>("scalar", "simulated forecasting")));
		Assertions.assertEquals(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>("scalar", "simulated historical")));
		Assertions.assertEquals(TimeSeriesType.GRID_EXTERNAL_HISTORICAL, TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>("gridded", "external historical")));
		Assertions.assertEquals(TimeSeriesType.GRID_EXTERNAL_FORECASTING, TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>("gridded", "external forecasting")));
	}
}