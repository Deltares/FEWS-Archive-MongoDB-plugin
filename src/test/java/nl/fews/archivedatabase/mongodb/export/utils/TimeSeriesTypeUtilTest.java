package nl.fews.archivedatabase.mongodb.export.utils;

import junit.framework.TestCase;
import nl.fews.archivedatabase.mongodb.export.enums.TimeSeriesType;

import java.util.List;

public class TimeSeriesTypeUtilTest extends TestCase {

	public void testGetTimeSeriesTypeString() {
		assertEquals("external forecasting", TimeSeriesTypeUtil.getTimeSeriesTypeString(TimeSeriesType.EXTERNAL_FORECASTING));
		assertEquals("external historical", TimeSeriesTypeUtil.getTimeSeriesTypeString(TimeSeriesType.EXTERNAL_HISTORICAL));
		assertEquals("simulated forecasting", TimeSeriesTypeUtil.getTimeSeriesTypeString(TimeSeriesType.SIMULATED_FORECASTING));
	}

	public void testGetTimeSeriesTypeCollection() {
		assertEquals("ExternalForecastingScalarTimeSeries", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.EXTERNAL_FORECASTING));
		assertEquals("ExternalHistoricalScalarTimeSeries", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.EXTERNAL_HISTORICAL));
		assertEquals("SimulatedForecastingScalarTimeSeries", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SIMULATED_FORECASTING));
	}

	public void testGetTimeSeriesTypeKeys() {
		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime"), TimeSeriesTypeUtil.getTimeSeriesTypeKeys(TimeSeriesType.EXTERNAL_FORECASTING));
		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucket"), TimeSeriesTypeUtil.getTimeSeriesTypeKeys(TimeSeriesType.EXTERNAL_HISTORICAL));
		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"), TimeSeriesTypeUtil.getTimeSeriesTypeKeys(TimeSeriesType.SIMULATED_FORECASTING));
	}
}