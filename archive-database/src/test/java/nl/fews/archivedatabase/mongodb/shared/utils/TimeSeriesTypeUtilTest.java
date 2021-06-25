package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import org.javatuples.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TimeSeriesTypeUtilTest {

	@Test
	void getTimeSeriesTypeString() {
		assertEquals("external forecasting", TimeSeriesTypeUtil.getTimeSeriesTypeString(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING));
		assertEquals("external historical", TimeSeriesTypeUtil.getTimeSeriesTypeString(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
		assertEquals("simulated forecasting", TimeSeriesTypeUtil.getTimeSeriesTypeString(TimeSeriesType.SCALAR_SIMULATED_FORECASTING));
	}

	@Test
	void getTimeSeriesTypeCollection() {
		assertEquals("ExternalForecastingScalarTimeSeries", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING));
		assertEquals("ExternalHistoricalScalarTimeSeries", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
		assertEquals("SimulatedForecastingScalarTimeSeries", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING));
	}

	@Test
	void getTimeSeriesTypeClassName() {
		assertEquals("ScalarExternalForecasting", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING));
		assertEquals("ScalarExternalHistorical", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
		assertEquals("ScalarSimulatedForecasting", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(TimeSeriesType.SCALAR_SIMULATED_FORECASTING));
	}

	@Test
	void getTimeSeriesType() {
		assertNull(TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("", "")));
		assertEquals(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("scalar", "netcdfMetaData")));
		assertEquals(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("scalar", "externalForecastMetaData")));
		assertEquals(TimeSeriesType.SCALAR_SIMULATED_FORECASTING, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("scalar", "simulationMetaData")));
		assertEquals(TimeSeriesType.GRID_EXTERNAL_HISTORICAL, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("gridded", "netcdfMetaData")));
		assertEquals(TimeSeriesType.GRID_EXTERNAL_FORECASTING, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("gridded", "externalForecastMetaData")));
		assertEquals(TimeSeriesType.RATING_CURVES, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("", "ratingCurvesMetaData")));
		assertEquals(TimeSeriesType.CONFIGURATION, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("", "configMetaData")));
		assertEquals(TimeSeriesType.PRODUCTS, TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("", "productsMetaData")));
	}

	@Test
	void getTimeSeriesTypeByTypeString() {
		assertNull(TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>("", "")));
		assertEquals(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>("scalar", "external historical")));
		assertEquals(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>("scalar", "external forecasting")));
		assertEquals(TimeSeriesType.SCALAR_SIMULATED_FORECASTING, TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>("scalar", "simulated forecasting")));
		assertEquals(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>("scalar", "simulated historical")));
		assertEquals(TimeSeriesType.GRID_EXTERNAL_HISTORICAL, TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>("gridded", "external historical")));
		assertEquals(TimeSeriesType.GRID_EXTERNAL_FORECASTING, TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>("gridded", "external forecasting")));
	}

	@Test
	void getTimeSeriesTypeSyncType() {
	}
}