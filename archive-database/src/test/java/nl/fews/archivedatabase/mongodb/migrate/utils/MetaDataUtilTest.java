package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.util.Date;
import java.util.Map;

@Testcontainers
class MetaDataUtilTest {

	@Container
	public static MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo"));

	@BeforeAll
	public static void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));
	}

	@Test
	void getExistingMetaDataFilesFs() {
		Map<File, Date> existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs();
		Assertions.assertEquals(9, existingMetaDataFilesFs.size());
	}

	@Test
	void getExistingMetaDataFilesDb(){
		Map<File, Date> existingMetaDataFilesDb = MetaDataUtil.getExistingMetaDataFilesDb();
		Assertions.assertEquals(0, existingMetaDataFilesDb.size());
	}

	@Test
	void getMetaDataFilesInsert() {
		Map<File, Date> metaDataFilesInsert = MetaDataUtil.getMetaDataFilesInsert(MetaDataUtil.getExistingMetaDataFilesFs(), MetaDataUtil.getExistingMetaDataFilesDb());
		Assertions.assertEquals(9, metaDataFilesInsert.size());
	}

	@Test
	void getMetaDataFilesUpdate(){
		Map<File, Date> metaDataFilesUpdate = MetaDataUtil.getMetaDataFilesUpdate(MetaDataUtil.getExistingMetaDataFilesFs(), MetaDataUtil.getExistingMetaDataFilesDb());
		Assertions.assertEquals(0, metaDataFilesUpdate.size());
	}

	@Test
	void getMetaDataFilesDelete(){
		Map<File, Date> metaDataFilesDelete = MetaDataUtil.getMetaDataFilesDelete(MetaDataUtil.getExistingMetaDataFilesFs(), MetaDataUtil.getExistingMetaDataFilesDb());
		Assertions.assertEquals(0, metaDataFilesDelete.size());
	}

	@Test
	void readMetaData() {
		Map<File, Date> existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs();
		JSONObject metaData = MetaDataUtil.readMetaData(existingMetaDataFilesFs.keySet().stream().findFirst().orElse(null));
		Assertions.assertNotNull(metaData);
	}
}
