package nl.fews.archivedatabase.mongodb.shared.database;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class DatabaseTest {

	@Container
	public static MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo"));

	@BeforeAll
	static void setUp() {
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	@Test
	void getCollectionKeys() {
		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime"), Database.getCollectionKeys((TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING))));
		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucketSize", "bucket"), Database.getCollectionKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)));
		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"), Database.getCollectionKeys((TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING))));
	}

	@Test
	void renameCollection() {
		Database.insertOne("Test", new Document("Test", "Test"));
		Database.renameCollection("Test", String.format("TEST_%s", "Test"));
		assertEquals("Test", Database.findOne(String.format("TEST_%s", "Test"), new Document()).getString("Test"));
	}

	@Test
	void ensureCollection() {
		assertDoesNotThrow(() -> Database.ensureCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)));
	}

	@Test
	void dropCollection() {
		Database.insertOne("dropCollection", new Document("Test", "Test"));
		assertEquals("Test", Database.findOne("dropCollection", new Document()).getString("Test"));
		Database.dropCollection("dropCollection");
		assertNull(Database.findOne("dropCollection", new Document()));
	}

	@Test
	void replaceCollection() {
		Database.insertOne("dropCollection", new Document("Test", "Test"));
		Database.insertOne("dropCollectionNew", new Document("Test", "Test"));
		assertEquals("Test", Database.findOne("dropCollection", new Document()).getString("Test"));
		assertEquals("Test", Database.findOne("dropCollectionNew", new Document()).getString("Test"));
		Database.replaceCollection("dropCollectionNew", "dropCollection");
		assertNotNull(Database.findOne("dropCollection", new Document()));
		assertNull(Database.findOne("dropCollectionNew", new Document()));
	}

	@Test
	void aggregate() {
		Database.insertOne("aggregate", new Document("aggregate", "aggregate"));
		assertEquals("aggregate", Database.aggregate("aggregate", List.of(new Document("$match", new Document("aggregate", "aggregate")))).first().getString("aggregate"));
	}

	@Test
	void findOne() {
		Database.insertOne("findOne", new Document("findOne", "findOne"));
		assertEquals("findOne", Database.findOne("findOne", new Document("findOne", "findOne")).getString("findOne"));
	}

	@Test
	void find() {
		Database.insertOne("find", new Document("find", "find"));
		assertEquals("find", Database.find("find", new Document("find", "find")).first().getString("find"));
	}

	@Test
	void insertOne() {
		Database.insertOne("insertOne", new Document("insertOne", "insertOne"));
		assertEquals("insertOne", Database.findOne("insertOne", new Document("insertOne", "insertOne")).getString("insertOne"));
	}

	@Test
	void insertMany() {
		Database.insertMany("insertMany", List.of(new Document("Test1", "Test"), new Document("Test2", "Test")));
		assertEquals(2, Database.aggregate("insertMany", List.of(new Document("$group", new Document("_id", null).append("count", new Document("$sum", 1))))).first().getInteger("count"));
	}

	@Test
	void replaceOne() {
		Database.insertOne("replaceOne", new Document("Test", "Test"));
		assertEquals("Test", Database.findOne("replaceOne", new Document("Test", "Test")).getString("Test"));
		Database.replaceOne("replaceOne", new Document("Test", "Test"), new Document("Test", "Test2"));
		assertEquals("Test2", Database.findOne("replaceOne", new Document("Test", "Test2")).getString("Test"));
	}

	@Test
	void updateOne() {
		Database.insertOne("updateOne", new Document("Test", "Test"));
		assertEquals("Test", Database.findOne("updateOne", new Document("Test", "Test")).getString("Test"));
		Database.updateOne("updateOne", new Document("Test", "Test"), new Document("$set", new Document("Test", "Test2")));
		assertEquals("Test2", Database.findOne("updateOne", new Document("Test", "Test2")).getString("Test"));
	}

	@Test
	void updateMany() {
		Database.insertOne("updateOne", new Document("Test", "Test"));
		assertEquals("Test", Database.findOne("updateOne", new Document("Test", "Test")).getString("Test"));
		Database.updateMany("updateOne", new Document("Test", "Test"), new Document("$set", new Document("Test", "Test2")));
		assertEquals("Test2", Database.findOne("updateOne", new Document("Test", "Test2")).getString("Test"));
	}

	@Test
	void deleteOne() {
		Database.insertMany("deleteOne", List.of(new Document("Test", "Test"), new Document("Test", "Test")));
		assertEquals(2, Database.aggregate("deleteOne", List.of(new Document("$group", new Document("_id", null).append("count", new Document("$sum", 1))))).first().getInteger("count"));
		Database.deleteOne("deleteOne", new Document("Test", "Test"));
		assertEquals(1, Database.aggregate("deleteOne", List.of(new Document("$group", new Document("_id", null).append("count", new Document("$sum", 1))))).first().getInteger("count"));
	}

	@Test
	void deleteMany() {
		Database.insertMany("deleteMany", List.of(new Document("Test", "Test"), new Document("Test", "Test")));
		assertEquals(2, Database.aggregate("deleteMany", List.of(new Document("$group", new Document("_id", null).append("count", new Document("$sum", 1))))).first().getInteger("count"));
		Database.deleteMany("deleteMany", new Document("Test", "Test"));
		assertNull(Database.aggregate("deleteMany", List.of(new Document("$group", new Document("_id", null).append("count", new Document("$sum", 1))))).first());
	}

}