package nl.fews.archivedatabase.mongodb.query.operations;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.operations.Insert;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class SummarizeSingletonsTest {
	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	@Test
	void getSummary() throws Exception{
		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Map<String, List<String>> distinctKeyFields = Map.of(
				"parameterId", List.of("parameterId"),
				"moduleInstanceId", List.of("moduleInstanceId"),
				"numberOfTimeSeries", Database.getCollectionKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING)));
		SummarizeSingletons summarizeSingletons = new SummarizeSingletons();
		Map<String, Integer> summarized = summarizeSingletons.getSummary(
				TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING),
				distinctKeyFields,
				Map.of("metaData.areaId", List.of("scalar")),
				new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01"),
				new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01"));
		assertEquals(3, summarized.size());
		assertEquals(1, summarized.get("parameterId"));
		assertEquals(8, summarized.get("moduleInstanceId"));
		assertEquals(16, summarized.get("numberOfTimeSeries"));
	}
}