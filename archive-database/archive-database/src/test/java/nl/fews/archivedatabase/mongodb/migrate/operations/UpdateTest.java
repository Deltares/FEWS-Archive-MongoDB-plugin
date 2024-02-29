package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import org.bson.Document;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@Testcontainers
class UpdateTest {
	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:6.0"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	@Test
	void updateMetaDatas() throws ExecutionException, InterruptedException {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(f -> !f.getKey().toString().contains("gridded")).findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		assertEquals(1, MetaDataUtil.getExistingMetaDataFilesDb().size());

		Document old = Database.findOne(Settings.get("metaDataCollection"), new Document());
		Database.updateMany(Settings.get("metaDataCollection"), new Document(), new Document("$set", new Document("metaDataFileTime", new Date(0))));

		Update.updateMetaDatas(MetaDataUtil.getExistingMetaDataFilesFs(), MetaDataUtil.getExistingMetaDataFilesDb());

		Document updated = Database.findOne(Settings.get("metaDataCollection"), new Document());
		assertNotEquals(updated.getObjectId("_id"), old.getObjectId("_id"));
	}
}