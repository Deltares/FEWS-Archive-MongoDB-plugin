package nl.fews.archivedatabase.mongodb.shared.timeseries;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.NetcdfUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.RunInfoUtil;
import nl.fews.archivedatabase.mongodb.shared.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.interfaces.TimeSeries;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfContent;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfMetaData;
import nl.wldelft.archive.util.metadata.timeseries.TimeSeriesRecord;
import nl.wldelft.archive.util.runinfo.ArchiveRunInfo;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ScalarExternalForecastingTest {

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
		Document expected = Document.parse("{\"timeSeriesType\": \"external forecasting\", \"moduleInstanceId\": \"moduleInstanceId0\", \"locationId\": \"locationId0\", \"parameterId\": \"parameterId0\", \"qualifierIds\": [\"qualifierId0\", \"qualifierId0\"], \"qualifierId\": \"[\\\"qualifierId0\\\",\\\"qualifierId0\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"startTime\": {\"$date\": 1325376000000}, \"endTime\": {\"$date\": 1325570400000}, \"localStartTime\": {\"$date\": 1325376000000}, \"localEndTime\": {\"$date\": 1325570400000}, \"ensembleId\": \"ensembleId0\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": 1325376000000}, \"localForecastTime\": {\"$date\": 1325376000000}}");

		TimeSeries timeSeries = new ScalarExternalForecasting();
		Document metadataDocument = timeSeries.getMetaData(timeSeriesHeader, "areaId", "sourceId");
		List<Document> timeSeriesDocuments = timeSeries.getEvents(timeSeriesArray, metadataDocument);
		Document runInfoDocument = timeSeries.getRunInfo(timeSeriesHeader);
		Document document = timeSeries.getRoot(timeSeriesHeader, timeSeriesDocuments, runInfoDocument);
		assertEquals(expected.toJson(), document.toJson());

		MetaDataUtil.getExistingMetaDataFilesFs().keySet().stream().filter(s -> s.toString().contains("external_forecasts") && s.toString().contains("scalar")).limit(1).forEach(metaDataFile ->
				NetcdfUtil.getExistingNetcdfFilesFs(metaDataFile, MetaDataUtil.getNetcdfMetaData(metaDataFile)).entrySet().stream().limit(1).forEach(netcdfFile -> {
					TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = NetcdfUtil.getTimeSeriesArrays(netcdfFile.getKey()).get(0);
					TimeSeries ts = new ScalarExternalForecasting();

					NetcdfMetaData netcdfMetaData = MetaDataUtil.getNetcdfMetaData(metaDataFile);
					Map<File, NetcdfContent> netcdfContentMap = MetaDataUtil.getNetcdfContentMap(metaDataFile, netcdfMetaData);
					NetcdfContent netcdfContent = netcdfContentMap.get(netcdfFile.getKey());
					Map<String, Map<String, TimeSeriesRecord>> timeSeriesRecordsMap = NetcdfUtil.getTimeSeriesRecordsMap(netcdfFile.getKey(), netcdfContent);
					TimeSeriesRecord timeSeriesRecord = timeSeriesRecordsMap.get(timeSeriesArray.getHeader().getLocationId()).get(timeSeriesArray.getHeader().getParameterId());
					timeSeriesArray = NetcdfUtil.getTimeSeriesArrayMerged(timeSeriesArray, timeSeriesRecord);
					assertNotNull(ts.getRoot(timeSeriesArray.getHeader(), ts.getEvents(timeSeriesArray, metadataDocument), new Document()));
				}));
	}

	@Test
	void getMetaData() {
		Document expected = Document.parse("{\"sourceId\": \"sourceId\", \"areaId\": \"areaId\", \"unit\": \"unit\", \"displayUnit\": \"unit\", \"locationName\": \"locationId0\", \"parameterName\": \"parameterId0\", \"parameterType\": \"instantaneous\", \"timeStepLabel\": \"timeStepLabel\", \"timeStepMinutes\": 360, \"localTimeZone\": \"UTC\", \"ensembleMemberIndex\": 1}");

		TimeSeries timeSeries = new ScalarExternalForecasting();
		Document document = timeSeries.getMetaData(timeSeriesHeader, "areaId", "sourceId");
		assertEquals(document.getDate("archiveTime"), document.getDate("modifiedTime"));
		document.remove("archiveTime");
		document.remove("modifiedTime");
		assertEquals(expected.toJson(), document.toJson());

		MetaDataUtil.getExistingMetaDataFilesFs().keySet().stream().filter(s -> s.toString().contains("external_forecasts") && s.toString().contains("scalar")).limit(1).forEach(metaDataFile ->
				NetcdfUtil.getExistingNetcdfFilesFs(metaDataFile, MetaDataUtil.getNetcdfMetaData(metaDataFile)).entrySet().stream().limit(1).forEach(netcdfFile -> {
					TimeSeries ts = new ScalarExternalForecasting();
					ArchiveRunInfo archiveRunInfo = RunInfoUtil.getRunInfo(MetaDataUtil.getNetcdfMetaData(metaDataFile));
					assertNotNull(ts.getRunInfo(archiveRunInfo));
				}));
	}
}