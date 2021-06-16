package nl.fews.archivedatabase.mongodb.migrate.timeseries;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.NetcdfUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.TimeSeriesSetsUtil;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import org.javatuples.Pair;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Date;
import java.util.Map;

class ScalarTimeSeriesExtractorTest {

	@BeforeAll
	public static void setUpClass(){
		TestSettings.setTestSettings();
	}

	@Test
	void extract() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).findFirst().orElse(null);
		JSONObject metaData = MetaDataUtil.readMetaData(entry.getKey());
		Map<File, Pair<Date, JSONObject>> netcdfFiles = NetcdfUtil.getExistingNetcdfFilesFs(metaData);
		Map.Entry<File, Pair<Date, JSONObject>> netcdfFile = netcdfFiles.entrySet().stream().findFirst().orElse(null);
		JSONArray timeSeriesSets = TimeSeriesSetsUtil
				.getDecomposedTimeSeriesSets(netcdfFile.getKey());
		Assertions.assertNotNull(new ScalarTimeSeriesExtractor().extract(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, netcdfFile.getKey(), timeSeriesSets, netcdfFile.getValue().getValue1(), new JSONObject()));
	}
}