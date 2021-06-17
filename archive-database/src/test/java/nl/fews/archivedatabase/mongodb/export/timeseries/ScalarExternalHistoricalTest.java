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

class ScalarExternalHistoricalTest {

	private TimeSeriesHeader timeSeriesHeader;
	private TimeSeriesArray<TimeSeriesHeader> timeSeriesArray;

	@BeforeEach
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void setUp(){
		TestSettings.setTestSettings();
		TimeSeriesArrays timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();
		timeSeriesArray = timeSeriesArrays.get(0);
		timeSeriesHeader = timeSeriesArray.getHeader();
	}

	@Test
	void getRoot() {
		Document expected = Document.parse("{\"timeSeriesType\": \"external historical\", \"moduleInstanceId\": \"moduleInstanceId0\", \"locationId\": \"locationId0\", \"parameterId\": \"parameterId0\", \"qualifierIds\": [\"qualifierId0\", \"qualifierId0\"], \"qualifierId\": \"[\\\"qualifierId0\\\",\\\"qualifierId0\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"startTime\": {\"$date\": 1325376000000}, \"endTime\": {\"$date\": 1325570400000}, \"localStartTime\": {\"$date\": 1325376000000}, \"localEndTime\": {\"$date\": 1325570400000}}");

		TimeSeries timeSeries = new ScalarExternalHistorical();
		List<Document> timeSeriesDocuments = timeSeries.getEvents(timeSeriesArray);
		Document runInfoDocument = timeSeries.getRunInfo(timeSeriesHeader);
		Document document = timeSeries.getRoot(timeSeriesHeader, timeSeriesDocuments, runInfoDocument);
		Assertions.assertEquals(expected.toJson(), document.toJson());
	}

	@Test
	void getMetaData() {
		Document expected = Document.parse("{\"sourceId\": \"sourceId\", \"areaId\": \"areaId\", \"unit\": \"unit\", \"displayUnit\": \"unit\", \"locationName\": \"locationName0\", \"parameterName\": \"parameterName0\", \"parameterType\": \"instantaneous\", \"timeStepLabel\": \"timeStepLabel\", \"timeStepMinutes\": 360, \"localTimeZone\": \"UTC\"}");

		TimeSeries timeSeries = new ScalarExternalHistorical();
		Document document = timeSeries.getMetaData(timeSeriesHeader, "areaId", "sourceId");
		Assertions.assertEquals(document.getDate("archiveTime"), document.getDate("modifiedTime"));
		document.remove("archiveTime");
		document.remove("modifiedTime");
		Assertions.assertEquals(expected.toJson(), document.toJson());
	}

	@Test
	void getEvents() {
		Document expected = Document.parse("{\"result\": [{\"t\": {\"$date\": 1325376000000}, \"lt\": {\"$date\": 1325376000000}, \"v\": 0.0, \"dv\": 0.0, \"f\": 0, \"c\": \"comment0\"}, {\"t\": {\"$date\": 1325397600000}, \"lt\": {\"$date\": 1325397600000}, \"v\": 1.0, \"dv\": 1.0, \"f\": 0, \"c\": \"comment1\"}, {\"t\": {\"$date\": 1325419200000}, \"lt\": {\"$date\": 1325419200000}, \"v\": 2.0, \"dv\": 2.0, \"f\": 0, \"c\": \"comment2\"}, {\"t\": {\"$date\": 1325440800000}, \"lt\": {\"$date\": 1325440800000}, \"v\": 3.0, \"dv\": 3.0, \"f\": 0, \"c\": \"comment3\"}, {\"t\": {\"$date\": 1325462400000}, \"lt\": {\"$date\": 1325462400000}, \"v\": 4.0, \"dv\": 4.0, \"f\": 0, \"c\": \"comment4\"}, {\"t\": {\"$date\": 1325484000000}, \"lt\": {\"$date\": 1325484000000}, \"v\": 5.0, \"dv\": 5.0, \"f\": 0, \"c\": \"comment5\"}, {\"t\": {\"$date\": 1325505600000}, \"lt\": {\"$date\": 1325505600000}, \"v\": 6.0, \"dv\": 6.0, \"f\": 0, \"c\": \"comment6\"}, {\"t\": {\"$date\": 1325527200000}, \"lt\": {\"$date\": 1325527200000}, \"v\": 7.0, \"dv\": 7.0, \"f\": 0, \"c\": \"comment7\"}, {\"t\": {\"$date\": 1325548800000}, \"lt\": {\"$date\": 1325548800000}, \"v\": 8.0, \"dv\": 8.0, \"f\": 0, \"c\": \"comment8\"}, {\"t\": {\"$date\": 1325570400000}, \"lt\": {\"$date\": 1325570400000}, \"v\": 9.0, \"dv\": 9.0, \"f\": 0, \"c\": \"comment9\"}]}");

		TimeSeries timeseries = new ScalarExternalHistorical();
		List<Document> documents = timeseries.getEvents(timeSeriesArray);

		Assertions.assertEquals(expected.toJson(), new Document().append("result", documents).toJson());
	}

	@Test
	void getRunInfo() {
		Document expected = Document.parse("{}");

		TimeSeries timeSeries = new ScalarExternalHistorical();
		Document document = timeSeries.getRunInfo(timeSeriesHeader);
		Assertions.assertEquals(expected.toJson(), document.toJson());
	}
}