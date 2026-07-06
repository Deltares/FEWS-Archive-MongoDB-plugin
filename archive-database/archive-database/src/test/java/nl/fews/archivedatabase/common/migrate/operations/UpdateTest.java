package nl.fews.archivedatabase.common.migrate.operations;

import nl.fews.archivedatabase.common.migrate.TestSettings;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@Testcontainers
class UpdateTest {
	@Container
	final MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:8.3.2"));

	@BeforeEach
	public void setUp(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

//	@AfterEach
//	public void tearDown(){
//		Database.close();
//	}
//
//	@Test
//	void updateMetaDatas() throws ExecutionException, InterruptedException {
//		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(f -> !f.getKey().toString().contains("gridded")).min(Map.Entry.comparingByKey()).orElse(null);
//		Insert.insertMetaData(entry.getKey(), entry.getValue());
//		assertEquals(1, MetaDataUtil.getExistingMetaDataFilesDb().size());
//
//		Document old = Database.findOne(Collection.MigrateMetaData.toString(), new Document());
//		Database.updateMany(Collection.MigrateMetaData.toString(), new Document(), new Document("$set", new Document("metaDataFileTime", new Date(0))));
//
//		Update.updateMetaDatas(MetaDataUtil.getExistingMetaDataFilesFs(), MetaDataUtil.getExistingMetaDataFilesDb());
//
//		Document updated = Database.findOne(Collection.MigrateMetaData.toString(), new Document());
//		assertNotEquals(updated.getObjectId("_id"), old.getObjectId("_id"));
//	}
}