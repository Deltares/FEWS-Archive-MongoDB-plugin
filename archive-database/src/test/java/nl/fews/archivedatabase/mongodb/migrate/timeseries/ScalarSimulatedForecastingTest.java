package nl.fews.archivedatabase.mongodb.migrate.timeseries;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.NetcdfUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.RunInfoUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.TimeSeriesSetsUtil;
import org.bson.Document;
import org.javatuples.Pair;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

class ScalarSimulatedForecastingTest {

	@BeforeAll
	public static void setUpClass(){
		TestSettings.setTestSettings();
	}

	@Test
	void getRoot() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).findFirst().orElse(null);
		JSONObject metaData = MetaDataUtil.readMetaData(entry.getKey());
		JSONObject runInfo = RunInfoUtil.readRunInfo(RunInfoUtil.getRunInfoFile(metaData));
		Map<File, Pair<Date, JSONObject>> netcdfFiles = NetcdfUtil.getExistingNetcdfFilesFs(metaData);
		File netcdfFile = netcdfFiles.entrySet().stream().findFirst().orElse(null).getKey();
		Document document = NetcdfUtil.getTimeSeriesDocuments(netcdfFile).stream().findFirst().get();
		JSONObject timeSeriesSet = TimeSeriesSetsUtil.getDecomposedTimeSeriesSets(netcdfFile).getJSONObject(0);
		Assertions.assertNotNull(new ScalarSimulatedForecasting().getRoot(document, timeSeriesSet, new ArrayList<>(), new ScalarSimulatedForecasting().getRunInfo(runInfo)));
	}

	@Test
	void getMetaData() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).findFirst().orElse(null);
		JSONObject metaData = MetaDataUtil.readMetaData(entry.getKey());
		Map<File, Pair<Date, JSONObject>> netcdfFiles = NetcdfUtil.getExistingNetcdfFilesFs(metaData);
		Map.Entry<File, Pair<Date, JSONObject>> netcdfFile = netcdfFiles.entrySet().stream().findFirst().orElse(null);
		Document document = NetcdfUtil.getTimeSeriesDocuments(netcdfFile.getKey()).stream().findFirst().get();
		JSONObject timeSeriesSet = TimeSeriesSetsUtil.getDecomposedTimeSeriesSets(netcdfFile.getKey()).getJSONObject(0);
		Assertions.assertNotNull(new ScalarSimulatedForecasting().getMetaData(document, timeSeriesSet, netcdfFile.getValue().getValue1()));
	}

	@Test
	void getEvents() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).findFirst().orElse(null);
		JSONObject metaData = MetaDataUtil.readMetaData(entry.getKey());
		Map<File, Pair<Date, JSONObject>> netcdfFiles = NetcdfUtil.getExistingNetcdfFilesFs(metaData);
		Map.Entry<File, Pair<Date, JSONObject>> netcdfFile = netcdfFiles.entrySet().stream().findFirst().orElse(null);
		Document document = NetcdfUtil.getTimeSeriesDocuments(netcdfFile.getKey()).stream().findFirst().get();
		JSONObject timeSeriesSet = TimeSeriesSetsUtil.getDecomposedTimeSeriesSets(netcdfFile.getKey()).getJSONObject(0);
		Assertions.assertNotNull(new ScalarSimulatedForecasting().getEvents(document.getList("timeseries", Document.class), timeSeriesSet));
	}

	@Test
	void getRunInfo() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).findFirst().orElse(null);
		JSONObject metaData = MetaDataUtil.readMetaData(entry.getKey());
		JSONObject runInfo = RunInfoUtil.readRunInfo(RunInfoUtil.getRunInfoFile(metaData));
		Assertions.assertNotNull(new ScalarSimulatedForecasting().getRunInfo(runInfo));
	}
}