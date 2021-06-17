package nl.fews.archivedatabase.mongodb.export.timeseries;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.export.TestSettings;
import nl.fews.archivedatabase.mongodb.export.interfaces.TimeSeries;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

class ScalarSimulatedHistoricalTest {

	private TimeSeriesHeader timeSeriesHeader;
	private TimeSeriesArray<TimeSeriesHeader> timeSeriesArray;

	@BeforeEach
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void setUp() {
		TestSettings.setTestSettings();
		TimeSeriesArrays timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();
		timeSeriesArray = timeSeriesArrays.get(0);
		timeSeriesHeader = timeSeriesArray.getHeader();
	}

	@Test
	void testGetRoot() {
		Document expected = Document.parse("{\"timeSeriesType\": \"simulated historical\", \"moduleInstanceId\": \"moduleInstanceId0\", \"locationId\": \"locationId0\", \"parameterId\": \"parameterId0\", \"qualifierIds\": [\"qualifierId0\", \"qualifierId0\"], \"qualifierId\": \"[\\\"qualifierId0\\\",\\\"qualifierId0\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"startTime\": {\"$date\": 1325376000000}, \"endTime\": {\"$date\": 1325570400000}, \"localStartTime\": {\"$date\": 1325376000000}, \"localEndTime\": {\"$date\": 1325570400000}, \"ensembleId\": \"ensembleId0\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": 1325376000000}, \"localForecastTime\": {\"$date\": 1325376000000}, \"taskRunId\": \"taskRunId\"}");

		TimeSeries timeSeries = new ScalarSimulatedHistorical();
		List<Document> timeSeriesDocuments = timeSeries.getEvents(timeSeriesArray);
		Document runInfoDocument = timeSeries.getRunInfo(timeSeriesHeader);
		Document document = timeSeries.getRoot(timeSeriesHeader, timeSeriesDocuments, runInfoDocument);
		Assertions.assertEquals(expected.toJson(), document.toJson());
	}

	@Test
	void testGetRunInfo() {
		Document expected = Document.parse("{\"dispatchTime\": \"dispatchTime\", \"taskRunId\": \"taskRunId\", \"mcId\": \"mcId\", \"userId\": \"userId\", \"time0\": \"time0\", \"workflowId\": \"workflowId\", \"configRevisionNumber\": \"configRevisionNumber\"}");
		TimeSeries timeSeries = new ScalarSimulatedHistorical();
		Document document = timeSeries.getRunInfo(timeSeriesHeader);
		Assertions.assertEquals(expected.toJson(), document.toJson());
	}
}