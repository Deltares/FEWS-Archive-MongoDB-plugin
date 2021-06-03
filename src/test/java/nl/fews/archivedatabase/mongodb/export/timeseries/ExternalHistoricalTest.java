package nl.fews.archivedatabase.mongodb.export.timeseries;

import junit.framework.TestCase;
import nl.fews.archivedatabase.util.TestUtil;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.List;

public class ExternalHistoricalTest extends TestCase {

	private TimeSeriesArray<TimeSeriesHeader> timeSeriesArray;

	@SuppressWarnings({"rawtypes", "unchecked"})
	public void setUp() {
		TimeSeriesArrays timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();
		timeSeriesArray = timeSeriesArrays.get(0);
	}

	public void testGetTimeSeries() {
		Document expected = Document.parse("{\"result\": [{\"t\": {\"$date\": 1325376000000}, \"lt\": {\"$date\": 1325376000000}, \"v\": 0.0, \"dv\": 0.0, \"f\": 0, \"c\": \"comment0\"}, {\"t\": {\"$date\": 1325397600000}, \"lt\": {\"$date\": 1325397600000}, \"v\": 1.0, \"dv\": 1.0, \"f\": 0, \"c\": \"comment1\"}, {\"t\": {\"$date\": 1325419200000}, \"lt\": {\"$date\": 1325419200000}, \"v\": 2.0, \"dv\": 2.0, \"f\": 0, \"c\": \"comment2\"}, {\"t\": {\"$date\": 1325440800000}, \"lt\": {\"$date\": 1325440800000}, \"v\": 3.0, \"dv\": 3.0, \"f\": 0, \"c\": \"comment3\"}, {\"t\": {\"$date\": 1325462400000}, \"lt\": {\"$date\": 1325462400000}, \"v\": 4.0, \"dv\": 4.0, \"f\": 0, \"c\": \"comment4\"}, {\"t\": {\"$date\": 1325484000000}, \"lt\": {\"$date\": 1325484000000}, \"v\": 5.0, \"dv\": 5.0, \"f\": 0, \"c\": \"comment5\"}, {\"t\": {\"$date\": 1325505600000}, \"lt\": {\"$date\": 1325505600000}, \"v\": 6.0, \"dv\": 6.0, \"f\": 0, \"c\": \"comment6\"}, {\"t\": {\"$date\": 1325527200000}, \"lt\": {\"$date\": 1325527200000}, \"v\": 7.0, \"dv\": 7.0, \"f\": 0, \"c\": \"comment7\"}, {\"t\": {\"$date\": 1325548800000}, \"lt\": {\"$date\": 1325548800000}, \"v\": 8.0, \"dv\": 8.0, \"f\": 0, \"c\": \"comment8\"}, {\"t\": {\"$date\": 1325570400000}, \"lt\": {\"$date\": 1325570400000}, \"v\": 9.0, \"dv\": 9.0, \"f\": 0, \"c\": \"comment9\"}]}");

		ExternalHistorical timeseries = new ExternalHistorical(new TestUtil.ArchiveDatabaseUnitConverterTestImplementation(), new TestUtil.ArchiveDatabaseTimeConverterTestImplementation());
		List<Document> documents = timeseries.getTimeSeries(timeSeriesArray);

		assertEquals(expected.toJson(), new Document().append("result", documents).toJson());
	}
}