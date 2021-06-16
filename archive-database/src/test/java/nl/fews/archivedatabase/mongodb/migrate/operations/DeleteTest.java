package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Testcontainers
class DeleteTest {
	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));
		Settings.put("archiveRootDataFolder", Paths.get("src", "test", "resources").toAbsolutePath().toString());
	}

	@Test
	void deleteMetaDatas() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		Assertions.assertEquals(1, MetaDataUtil.getExistingMetaDataFilesDb().size());
		Delete.deleteMetaDatas(new HashMap<>(), MetaDataUtil.getExistingMetaDataFilesDb());
		Assertions.assertEquals(0, MetaDataUtil.getExistingMetaDataFilesDb().size());
	}

	@Test
	void deleteMetaData() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		Assertions.assertEquals(1, MetaDataUtil.getExistingMetaDataFilesDb().size());
		Delete.deleteMetaData(entry.getKey());
		Assertions.assertEquals(0, MetaDataUtil.getExistingMetaDataFilesDb().size());
	}

	@Test
	void deleteUncommitted() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		Assertions.assertEquals(1, MetaDataUtil.getExistingMetaDataFilesDb().size());
		Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("metaDataCollection")).updateMany(new Document(), new Document("$set", new Document("committed", false)));
		Delete.deleteUncommitted();
		Assertions.assertEquals(0, MetaDataUtil.getExistingMetaDataFilesDb().size());
	}
}