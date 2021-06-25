package nl.fews.archivedatabase.mongodb.shared.database;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;
import org.json.JSONArray;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class DatabaseTest {

	@Container
	public static MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo"));

	@BeforeAll
	static void setUp() {
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));
	}

	@Test
	void create() {
		assertNotNull(Database.create());
	}

	@Test
	void getCollectionKeys() {
		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime"), Database.getCollectionKeys((TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING))));
		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "bucketSize", "bucket"), Database.getCollectionKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)));
		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId", "ensembleId", "ensembleMemberId", "forecastTime", "taskRunId"), Database.getCollectionKeys((TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING))));
	}

	@Test
	void renameCollection() {
		Database.create().getDatabase(Database.getDatabaseName()).getCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)).insertOne(new Document("Test", "Test"));
		Database.renameCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), String.format("TEST_%s", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)));
		assertEquals("Test", Database.create().getDatabase(Database.getDatabaseName()).getCollection(String.format("TEST_%s", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL))).find().first().getString("Test"));
	}

	@Test
	void getDatabaseName() {
		assertEquals("FEWS_ARCHIVE_TEST", Database.getDatabaseName());
		assertEquals("FEWS_ARCHIVE_TEST", Database.getDatabaseName("mongodb://localhost/FEWS_ARCHIVE_TEST"));
	}

	@Test
	void ensureCollection() {
		Database.create();
		assertDoesNotThrow(() -> Database.ensureCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)));
	}

	@Test
	void dropCollection() {
		Database.create().getDatabase(Database.getDatabaseName()).getCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)).insertOne(new Document("Test", "Test"));
		assertEquals("Test", Database.create().getDatabase(Database.getDatabaseName()).getCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)).find().first().getString("Test"));
		Database.dropCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
		assertNull(Database.create().getDatabase(Database.getDatabaseName()).getCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)).find().first());
	}

	@Test
	void getCollectionIndexes() {
		Document document = Database.getCollectionIndexes(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL))[0];
		document.remove("unique");
		assertEquals(new JSONArray(Database.getCollectionKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL))).toString() , new JSONArray(document.keySet().stream().collect(Collectors.toList())).toString());

		assertEquals(new JSONArray(List.of("moduleInstanceId","locationId", "parameterId", "qualifierId", "encodedTimeStepId")).toString(),
				new JSONArray(Database.getCollectionIndexes(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL))[4].keySet().stream().collect(Collectors.toList())).toString());
	}
}