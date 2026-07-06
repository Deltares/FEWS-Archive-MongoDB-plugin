package nl.fews.archivedatabase.common.query.operations;

import nl.fews.archivedatabase.common.migrate.TestSettings;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class FilterTest {
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
//	void getFilters() throws Exception {
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//		Database.updateTimeSeriesIndex();
//		Map<String, Set<Object>> filters = Filter.getFilters(
//				TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL),
//				Map.of("parameterId", String.class, "moduleInstanceId", String.class, "encodedTimeStepId", String.class),
//				Map.of("metaData.areaId", List.of("scalar")));
//		assertEquals(3, filters.size());
//		assertEquals(1, filters.get("parameterId").size());
//		assertEquals(1, filters.get("moduleInstanceId").size());
//		assertEquals(1, filters.get("encodedTimeStepId").size());
//	}
}