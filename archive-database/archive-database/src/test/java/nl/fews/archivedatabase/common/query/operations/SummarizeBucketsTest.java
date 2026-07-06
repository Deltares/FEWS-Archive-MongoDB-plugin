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
class SummarizeBucketsTest {
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
//	void getSummary() throws Exception{
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//		Map<String, List<String>> distinctKeyFields = Map.of(
//				"parameterId", List.of("parameterId"),
//				"moduleInstanceId", List.of("moduleInstanceId"),
//				"numberOfTimeSeries", Index.getCollectionKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)).stream().filter(s -> !List.of("bucketSize", "bucket").contains(s)).collect(Collectors.toList()));
//		SummarizeBuckets summarizeBuckets = new SummarizeBuckets();
//		Map<String, Integer> summarized = summarizeBuckets.getSummary(
//				TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL),
//				distinctKeyFields,
//				Map.of("metaData.areaId", List.of("scalar")),
//				new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01"),
//				new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01"));
//		assertEquals(3, summarized.size());
//		assertEquals(1, summarized.get("parameterId"));
//		assertEquals(1, summarized.get("moduleInstanceId"));
//		assertEquals(184, summarized.get("numberOfTimeSeries"));
//	}
}