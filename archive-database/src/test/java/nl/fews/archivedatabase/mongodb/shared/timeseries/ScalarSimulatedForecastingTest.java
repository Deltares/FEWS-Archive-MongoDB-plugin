package nl.fews.archivedatabase.mongodb.shared.timeseries;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.NetcdfUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.RunInfoUtil;
import nl.fews.archivedatabase.mongodb.shared.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.interfaces.TimeSeries;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfContent;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfMetaData;
import nl.wldelft.archive.util.metadata.simulation.SimulationMetaData;
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

class ScalarSimulatedForecastingTest {

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
		Document expected = Document.parse("{\"timeSeriesType\": \"simulated forecasting\", \"moduleInstanceId\": \"moduleInstanceId0\", \"locationId\": \"locationId0\", \"parameterId\": \"parameterId0\", \"qualifierIds\": [\"qualifierId0\", \"qualifierId0\"], \"qualifierId\": \"[\\\"qualifierId0\\\",\\\"qualifierId0\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"startTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}, \"endTime\": {\"$date\": \"2012-01-03T06:00:00Z\"}, \"localStartTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}, \"localEndTime\": {\"$date\": \"2012-01-03T06:00:00Z\"}, \"ensembleId\": \"ensembleId0\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}, \"localForecastTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}, \"taskRunId\": \"taskRunId\"}");
		TimeSeries timeSeries = new ScalarSimulatedForecasting();
		List<Document> timeSeriesDocuments = timeSeries.getEvents(timeSeriesArray);
		Document runInfoDocument = timeSeries.getRunInfo(timeSeriesHeader);
		Document document = timeSeries.getRoot(timeSeriesHeader, timeSeriesDocuments, runInfoDocument);
		assertEquals(expected.toJson(), document.toJson());

		MetaDataUtil.getExistingMetaDataFilesFs().keySet().stream().filter(s -> s.toString().contains("simulated") && s.toString().contains("scalar")).limit(1).forEach(metaDataFile ->
				NetcdfUtil.getExistingNetcdfFilesFs(metaDataFile, MetaDataUtil.getNetcdfMetaData(metaDataFile)).entrySet().stream().limit(1).forEach(netcdfFile -> {
					TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = NetcdfUtil.getTimeSeriesArrays(netcdfFile.getKey()).get(0);
					TimeSeries ts = new ScalarExternalForecasting();

					NetcdfMetaData netcdfMetaData = MetaDataUtil.getNetcdfMetaData(metaDataFile);
					Map<File, NetcdfContent> netcdfContentMap = MetaDataUtil.getNetcdfContentMap(metaDataFile, netcdfMetaData);
					NetcdfContent netcdfContent = netcdfContentMap.get(netcdfFile.getKey());
					Map<String, Map<String, TimeSeriesRecord>> timeSeriesRecordsMap = NetcdfUtil.getTimeSeriesRecordsMap(netcdfFile.getKey(), netcdfContent);
					TimeSeriesRecord timeSeriesRecord = timeSeriesRecordsMap.get(timeSeriesArray.getHeader().getLocationId()).get(timeSeriesArray.getHeader().getParameterId());
					timeSeriesArray = NetcdfUtil.getTimeSeriesArrayMerged(timeSeriesArray, timeSeriesRecord);
					assertNotNull(ts.getRoot(timeSeriesArray.getHeader(), ts.getEvents(timeSeriesArray), new Document()));
				}));
	}

	@Test
	void getRunInfo() {
		Document expected = Document.parse("{\"dispatchTime\": \"dispatchTime\", \"taskRunId\": \"taskRunId\", \"mcId\": \"mcId\", \"userId\": \"userId\", \"time0\": \"time0\", \"workflowId\": \"workflowId\", \"configRevisionNumber\": \"configRevisionNumber\"}");
		TimeSeries timeSeries = new ScalarSimulatedForecasting();
		Document document = timeSeries.getRunInfo(timeSeriesHeader);
		assertEquals(expected.toJson(), document.toJson());

		MetaDataUtil.getExistingMetaDataFilesFs().keySet().stream().filter(s -> s.toString().contains("simulated") && s.toString().contains("scalar")).limit(1).forEach(metaDataFile ->
				NetcdfUtil.getExistingNetcdfFilesFs(metaDataFile, MetaDataUtil.getNetcdfMetaData(metaDataFile)).entrySet().stream().limit(1).forEach(netcdfFile -> {
					TimeSeries ts = new ScalarExternalForecasting();
					SimulationMetaData simulationMetaData = (SimulationMetaData) MetaDataUtil.getNetcdfMetaData(metaDataFile);
					ArchiveRunInfo archiveRunInfo = RunInfoUtil.getRunInfo(simulationMetaData);
					assertNotNull(ts.getRunInfo(archiveRunInfo));
				}));
	}
}