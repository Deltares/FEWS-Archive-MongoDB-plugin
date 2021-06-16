package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import org.javatuples.Pair;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Date;
import java.util.Map;

class NetcdfUtilTest {

	@BeforeAll
	public static void setUpClass(){
		TestSettings.setTestSettings();
	}

	@Test
	void getExistingNetcdfFilesFs(){
		Map<File, Date> existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs();
		Map<File, Pair<Date, JSONObject>> existingNetcdfFilesFs = NetcdfUtil.getExistingNetcdfFilesFs(MetaDataUtil.readMetaData(existingMetaDataFilesFs.keySet().stream().findFirst().get()));
		Assertions.assertNotNull(existingNetcdfFilesFs);
	}

	@Test
	void getTimeSeriesDocuments() {
		Map<File, Date> existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs();
		Map<File, Pair<Date, JSONObject>> existingNetcdfFilesFs = NetcdfUtil.getExistingNetcdfFilesFs(MetaDataUtil.readMetaData(existingMetaDataFilesFs.keySet().stream().findFirst().get()));
		Assertions.assertNotNull(NetcdfUtil.getTimeSeriesDocuments(existingNetcdfFilesFs.keySet().stream().findFirst().get()));
		//NetcdfUtil.getTimeSeriesDocuments(new File("C:\\_GIT\\FEWS\\archive-database\\src\\test\\resources\\2020\\11\\scalar\\observed\\externalhistorical_TWT_ImportEDS_1hour.nc"));
	}

	@Test
	void getTimeSeriesArraysAsDocuments() {
		Map<File, Date> existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs();
		Map<File, Pair<Date, JSONObject>> existingNetcdfFilesFs = NetcdfUtil.getExistingNetcdfFilesFs(MetaDataUtil.readMetaData(existingMetaDataFilesFs.keySet().stream().findFirst().get()));
		Assertions.assertNotNull(NetcdfUtil.getTimeSeriesArraysAsDocuments(existingNetcdfFilesFs.keySet().stream().findFirst().get()));
	}
}