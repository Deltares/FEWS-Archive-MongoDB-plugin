package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class InsertTest {

	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	@Test
	void insertMetaDatas() throws ExecutionException, InterruptedException {
		Insert.insertMetaDatas(MetaDataUtil.getExistingMetaDataFilesFs(), MetaDataUtil.getExistingMetaDataFilesDb());
		assertEquals(7, MetaDataUtil.getExistingMetaDataFilesDb().size());
	}

	@Test
	void insertMetaData() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(f -> !f.getKey().toString().contains("gridded")).findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		assertEquals(1, MetaDataUtil.getExistingMetaDataFilesDb().size());
		assertEquals(8, MetaDataUtil.getMetaDataFilesInsert(MetaDataUtil.getExistingMetaDataFilesFs(), MetaDataUtil.getExistingMetaDataFilesDb()).size());
	}
}