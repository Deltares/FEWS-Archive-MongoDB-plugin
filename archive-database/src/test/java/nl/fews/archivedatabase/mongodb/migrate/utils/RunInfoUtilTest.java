package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Map;

class RunInfoUtilTest {

	@BeforeAll
	public static void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("archiveRootDataFolder", Paths.get("src", "test", "resources").toAbsolutePath().toString());
	}

	@Test
	void getRunInfoFile(){
		Map<File, Date> existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs();
		File runInfoFile = null;
		for (File file: existingMetaDataFilesFs.keySet()) {
			JSONObject metaData = MetaDataUtil.readMetaData(file);
			runInfoFile = RunInfoUtil.getRunInfoFile(metaData);
			if(runInfoFile != null){
				break;
			}
		}
		Assertions.assertNotNull(runInfoFile);
	}

	@Test
	void readRunInfo() {
		Map<File, Date> existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs();
		File runInfoFile = null;
		for (File file: existingMetaDataFilesFs.keySet()) {
			JSONObject metaData = MetaDataUtil.readMetaData(file);
			runInfoFile = RunInfoUtil.getRunInfoFile(metaData);
			if(runInfoFile != null){
				break;
			}
		}
		Assertions.assertNotNull(runInfoFile);
		JSONObject runInfo = RunInfoUtil.readRunInfo(runInfoFile);
		Assertions.assertNotNull(runInfo);
	}
}
