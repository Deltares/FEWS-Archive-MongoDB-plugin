package nl.fews.archivedatabase.mongodb.export.root;

import junit.framework.TestCase;
import nl.fews.archivedatabase.util.TestUtil;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.List;

public class ExternalHistoricalTest extends TestCase {

	private TimeSeriesHeader timeSeriesHeader;
	private TimeSeriesArray<TimeSeriesHeader> timeSeriesArray;

	@SuppressWarnings({"rawtypes", "unchecked"})
	public void setUp(){
		TimeSeriesArrays timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();
		timeSeriesArray = timeSeriesArrays.get(0);
		timeSeriesHeader = timeSeriesArray.getHeader();
	}

	public void testGetRoot() {
		Document expected = Document.parse("{\"timeSeriesType\": \"external historical\", \"moduleInstanceId\": \"moduleInstanceId0\", \"locationId\": \"locationId0\", \"parameterId\": \"parameterId0\", \"qualifierIds\": [\"qualifierId0\", \"qualifierId0\"], \"qualifierId\": \"[\\\"qualifierId0\\\",\\\"qualifierId0\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"startTime\": {\"$date\": 1325376000000}, \"endTime\": {\"$date\": 1325570400000}, \"localStartTime\": {\"$date\": 1325376000000}, \"localEndTime\": {\"$date\": 1325570400000}}");

		nl.fews.archivedatabase.mongodb.export.timeseries.ExternalHistorical timeseries = new nl.fews.archivedatabase.mongodb.export.timeseries.ExternalHistorical(new TestUtil.ArchiveDatabaseUnitConverterTestImplementation(), new TestUtil.ArchiveDatabaseTimeConverterTestImplementation());
		List<Document> timeSeriesDocuments = timeseries.getTimeSeries(timeSeriesArray);
		Document runInfoDocument = new nl.fews.archivedatabase.mongodb.export.runinfo.ExternalHistorical().getRunInfo();

		ExternalHistorical externalHistorical = new ExternalHistorical(new TestUtil.ArchiveDatabaseTimeConverterTestImplementation());
		Document document = externalHistorical.getRoot(timeSeriesHeader, timeSeriesDocuments, runInfoDocument);
		assertEquals(expected.toJson(), document.toJson());
	}
}