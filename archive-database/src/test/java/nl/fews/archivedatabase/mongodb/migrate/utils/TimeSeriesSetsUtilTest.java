package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import org.javatuples.Pair;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Map;

class TimeSeriesSetsUtilTest {

	@BeforeAll
	public static void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("archiveRootDataFolder", Paths.get("src", "test", "resources").toAbsolutePath().toString());
	}

	@Test
	void getDecomposedTimeSeriesSets() {
		Map<File, Date> existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs();
		Map<File, Pair<Date, JSONObject>> existingNetcdfFilesFs = NetcdfUtil.getExistingNetcdfFilesFs(MetaDataUtil.readMetaData(existingMetaDataFilesFs.keySet().stream().findFirst().get()));
		Assertions.assertNotNull(TimeSeriesSetsUtil.getDecomposedTimeSeriesSets(existingNetcdfFilesFs.keySet().stream().findFirst().get()));
		//TimeSeriesSetsUtil.getDecomposedTimeSeriesSets(new File("C:\\_GIT\\FEWS\\archive-database\\src\\test\\resources\\2020\\11\\scalar\\observed\\externalhistorical_TW_MergePrep_TW_MINGPOND_1hour.nc"));
		//TimeSeriesSetsUtil.getDecomposedTimeSeriesSets(new File("C:\\_GIT\\FEWS\\archive-database\\src\\test\\resources\\2020\\11\\scalar\\observed\\externalhistorical_TA_ImportEDS_1hour.nc"));
		//TimeSeriesSetsUtil.getDecomposedTimeSeriesSets(new File("C:\\_GIT\\FEWS\\archive-database\\src\\test\\resources\\2020\\11\\scalar\\observed\\externalhistorical_TWT_ImportEDS_1hour.nc"));
		//TimeSeriesSetsUtil.getDecomposedTimeSeriesSets(new File("C:\\_GIT\\FEWS\\archive-database\\src\\test\\resources\\2020\\11\\scalar\\observed\\externalhistorical_QY_ImportEDMR_00CST.nc"));
		//TimeSeriesSetsUtil.getDecomposedTimeSeriesSets(new File("\\\\knxpwfewsmc1\\Archive\\data\\2019\\02\\scalar\\23\\external_forecasts\\RW_6Hour_To_FEWS_213900\\externalforecasting_QI_RW_6Hour_To_FEWS_6hour_CST.nc"));
		//TimeSeriesSetsUtil.getDecomposedTimeSeriesSets(new File("\\\\knxpwfewsmc1\\Archive\\data\\2020\\02\\scalar\\12\\external_forecasts\\Import_Gen_Forecast_205000\\externalforecasting_VT_Import_Gen_Forecast_nonequidistant.nc"));

	}
}