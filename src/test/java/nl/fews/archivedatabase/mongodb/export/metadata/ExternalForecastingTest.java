package nl.fews.archivedatabase.mongodb.export.metadata;

import junit.framework.TestCase;
import nl.fews.archivedatabase.util.TestUtil;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

public class ExternalForecastingTest extends TestCase {

	private TimeSeriesHeader timeSeriesHeader;

	@SuppressWarnings("rawtypes")
	public void setUp() {
		TimeSeriesArrays timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();
		timeSeriesHeader = timeSeriesArrays.get(0).getHeader();
	}

	public void testGetMetaData() {
		Document expected = Document.parse("{\"sourceId\": \"sourceId\", \"areaId\": \"areaId\", \"unit\": \"unit\", \"displayUnit\": \"unit\", \"locationName\": \"locationName0\", \"parameterName\": \"parameterName0\", \"parameterType\": \"instantaneous\", \"timeStepLabel\": \"timeStepLabel\", \"timeStepMinutes\": 360, \"localTimeZone\": \"UTC\", \"ensembleMemberIndex\": 1}");

		ExternalForecasting externalForecasting = new ExternalForecasting("areaId", "sourceId", new TestUtil.ArchiveDatabaseUnitConverterTestImplementation(), new TestUtil.ArchiveDatabaseTimeConverterTestImplementation());
		Document document = externalForecasting.getMetaData(timeSeriesHeader);
		assertEquals(document.getDate("archiveTime"), document.getDate("modifiedTime"));
		document.remove("archiveTime");
		document.remove("modifiedTime");
		assertEquals(expected.toJson(), document.toJson());
	}
}