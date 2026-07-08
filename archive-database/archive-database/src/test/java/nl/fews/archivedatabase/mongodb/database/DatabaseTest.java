package nl.fews.archivedatabase.mongodb.database;

import nl.fews.archivedatabase.common.migrate.TestSettings;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class DatabaseTest {

	@Container
	public static MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:8.3.2"));

	@BeforeAll
	static void setUp() {
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

//	@Test
//	void instance() {
//		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime"),
//			IndexTest.getKeys((TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING))));
//		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucketSize", "bucket"),
//			IndexTest.getKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)));
//		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"),
//			IndexTest.getKeys((TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING))));
//	}
//
//	@Test
//	void rename() {
//		Database.insertOne("Test", new Document("Test", "Test"));
//		Database.rename("Test", String.format("TEST_%s", "Test"));
//		assertEquals("Test", Database.findOne(String.format("TEST_%s", "Test"), new Document()).getString("Test"));
//	}
//
//	@Test
//	void ensureTable() {
//		assertDoesNotThrow(() -> Database.ensureTable(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)));
//	}
//
//	@Test
//	void drop() {
//		Database.insertOne("dropCollection", new Document("Test", "Test"));
//		assertEquals("Test", Database.findOne("dropCollection", new Document()).getString("Test"));
//		Database.drop("dropCollection");
//		assertNull(Database.findOne("dropCollection", new Document()));
//	}
//
//	@Test
//	void replace() {
//		Database.insertOne("dropCollection", new Document("Test", "Test"));
//		Database.insertOne("dropCollectionNew", new Document("Test", "Test"));
//		assertEquals("Test", Database.findOne("dropCollection", new Document()).getString("Test"));
//		assertEquals("Test", Database.findOne("dropCollectionNew", new Document()).getString("Test"));
//		Database.replace("dropCollectionNew", "dropCollection");
//		assertNotNull(Database.findOne("dropCollection", new Document()));
//		assertNull(Database.findOne("dropCollectionNew", new Document()));
//	}
//
//	@Test
//	void aggregate() {
//		Database.insertOne("aggregate", new Document("aggregate", "aggregate"));
//		assertEquals("aggregate", Database.aggregate("aggregate", List.of(new Document("$match", new Document("aggregate", "aggregate")))).first().getString("aggregate"));
//	}
//
//	@Test
//	void findOne() {
//		Database.insertOne("findOne", new Document("findOne", "findOne"));
//		assertEquals("findOne", Database.findOne("findOne", new Document("findOne", "findOne")).getString("findOne"));
//	}
//
//	@Test
//	void find() {
//		Database.insertOne("find", new Document("find", "find"));
//		assertEquals("find", Database.find("find", new Document("find", "find")).first().getString("find"));
//	}
//
//	@Test
//	void insertOne() {
//		Database.insertOne("insertOne", new Document("insertOne", "insertOne"));
//		assertEquals("insertOne", Database.findOne("insertOne", new Document("insertOne", "insertOne")).getString("insertOne"));
//	}
//
//	@Test
//	void insertMany() {
//		Database.insertMany("insertMany", List.of(new Document("Test1", "Test"), new Document("Test2", "Test")));
//		assertEquals(2, Database.aggregate("insertMany", List.of(new Document("$group", new Document("_id", null).append("count", new Document("$sum", 1))))).first().getInteger("count"));
//	}
//
//	@Test
//	void replaceOne() {
//		Database.insertOne("replaceOne", new Document("Test", "Test"));
//		assertEquals("Test", Database.findOne("replaceOne", new Document("Test", "Test")).getString("Test"));
//		Database.replaceOne("replaceOne", new Document("Test", "Test"), new Document("Test", "Test2"));
//		assertEquals("Test2", Database.findOne("replaceOne", new Document("Test", "Test2")).getString("Test"));
//	}
//
//	@Test
//	void updateOne() {
//		Database.insertOne("updateOne", new Document("Test", "Test"));
//		assertEquals("Test", Database.findOne("updateOne", new Document("Test", "Test")).getString("Test"));
//		Database.updateOne("updateOne", new Document("Test", "Test"), new Document("$set", new Document("Test", "Test2")));
//		assertEquals("Test2", Database.findOne("updateOne", new Document("Test", "Test2")).getString("Test"));
//	}
//
//	@Test
//	void updateMany() {
//		Database.insertOne("updateOne", new Document("Test", "Test"));
//		assertEquals("Test", Database.findOne("updateOne", new Document("Test", "Test")).getString("Test"));
//		Database.updateMany("updateOne", new Document("Test", "Test"), new Document("$set", new Document("Test", "Test2")));
//		assertEquals("Test2", Database.findOne("updateOne", new Document("Test", "Test2")).getString("Test"));
//	}
//
//	@Test
//	void deleteOne() {
//		Database.insertMany("deleteOne", List.of(new Document("Test", "Test"), new Document("Test", "Test")));
//		assertEquals(2, Database.aggregate("deleteOne", List.of(new Document("$group", new Document("_id", null).append("count", new Document("$sum", 1))))).first().getInteger("count"));
//		Database.deleteOne("deleteOne", new Document("Test", "Test"));
//		assertEquals(1, Database.aggregate("deleteOne", List.of(new Document("$group", new Document("_id", null).append("count", new Document("$sum", 1))))).first().getInteger("count"));
//	}
//
//	@Test
//	void deleteMany() {
//		Database.insertMany("deleteMany", List.of(new Document("Test", "Test"), new Document("Test", "Test")));
//		assertEquals(2, Database.aggregate("deleteMany", List.of(new Document("$group", new Document("_id", null).append("count", new Document("$sum", 1))))).first().getInteger("count"));
//		Database.deleteMany("deleteMany", new Document("Test", "Test"));
//		assertNull(Database.aggregate("deleteMany", List.of(new Document("$group", new Document("_id", null).append("count", new Document("$sum", 1))))).first());
//	}

}