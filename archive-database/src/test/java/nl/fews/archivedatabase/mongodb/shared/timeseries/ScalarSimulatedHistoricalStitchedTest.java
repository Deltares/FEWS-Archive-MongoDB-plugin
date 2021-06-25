package nl.fews.archivedatabase.mongodb.shared.timeseries;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.shared.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.interfaces.TimeSeries;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ScalarSimulatedHistoricalStitchedTest {

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
	void getRoot() {
		Document expected = Document.parse("{\"timeSeriesType\": \"simulated historical\", \"moduleInstanceId\": \"moduleInstanceId0\", \"locationId\": \"locationId0\", \"parameterId\": \"parameterId0\", \"qualifierIds\": [\"qualifierId0\", \"qualifierId0\"], \"qualifierId\": \"[\\\"qualifierId0\\\",\\\"qualifierId0\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"startTime\": {\"$date\": 1325376000000}, \"endTime\": {\"$date\": 1325570400000}, \"localStartTime\": {\"$date\": 1325376000000}, \"localEndTime\": {\"$date\": 1325570400000}, \"ensembleId\": \"ensembleId0\", \"ensembleMemberId\": \"1\"}");

		TimeSeries timeSeries = new ScalarSimulatedHistoricalStitched();
		List<Document> timeSeriesDocuments = timeSeries.getEvents(timeSeriesArray);
		Document runInfoDocument = timeSeries.getRunInfo(timeSeriesHeader);
		Document document = timeSeries.getRoot(timeSeriesHeader, timeSeriesDocuments, runInfoDocument);
		assertEquals(expected.toJson(), document.toJson());
	}

	@Test
	void getMetaData() {
		Document expected = Document.parse("{\"sourceId\": \"sourceId\", \"areaId\": \"areaId\", \"unit\": \"unit\", \"displayUnit\": \"unit\", \"locationName\": \"locationId0\", \"parameterName\": \"parameterId0\", \"parameterType\": \"instantaneous\", \"timeStepLabel\": \"timeStepLabel\", \"timeStepMinutes\": 360, \"localTimeZone\": \"UTC\", \"ensembleMemberIndex\": 1}");
		TimeSeries timeSeries = new ScalarSimulatedHistoricalStitched();
		Document document = timeSeries.getMetaData(timeSeriesHeader, "areaId", "sourceId");
		document.remove("archiveTime");
		document.remove("modifiedTime");
		assertEquals(expected.toJson(), document.toJson());
	}
}