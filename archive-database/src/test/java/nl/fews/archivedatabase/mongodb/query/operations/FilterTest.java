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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class FilterTest {
	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));
	}

	@Test
	void getFilters() throws Exception {
		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Database.updateTimeSeriesIndex();
		Map<String, List<Object>> filters = Filter.getFilters(
				TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL),
				Map.of("parameterId", String.class, "moduleInstanceId", String.class, "encodedTimeStepId", String.class),
				Map.of("metaData.areaId", List.of("scalar")));
		assertEquals(3, filters.size());
		assertEquals(1, filters.get("parameterId").size());
		assertEquals(1, filters.get("moduleInstanceId").size());
		assertEquals(1, filters.get("encodedTimeStepId").size());
	}
}