package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.operations.Insert;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
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
import java.util.List;
import java.util.Map;

@Testcontainers
class TimeSeriesUtilTest {

	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));
		Settings.put("archiveRootDataFolder", Paths.get("src", "test", "resources").toAbsolutePath().toString());
	}

	@Test
	void getTimeSeriesGroups() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(List.of("moduleInstanceId"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Assertions.assertFalse(timeSeriesGroups.isEmpty());
	}

	@Test
	void removeTimeSeriesDocuments() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(List.of("moduleInstanceId"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Assertions.assertFalse(timeSeriesGroups.isEmpty());
		List<Document> timeSeries = TimeSeriesUtil.getTimeSeries(timeSeriesGroups.get(0), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Assertions.assertFalse(timeSeries.isEmpty());
		TimeSeriesUtil.removeTimeSeriesDocuments(timeSeriesGroups.get(0), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		List<Document> timeSeries2 = TimeSeriesUtil.getTimeSeries(timeSeriesGroups.get(0), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Assertions.assertTrue(timeSeries2.isEmpty());
	}

	@Test
	void getTimeSeriesDocuments() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(List.of("moduleInstanceId"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		List<Document> timeSeries = TimeSeriesUtil.getTimeSeries(timeSeriesGroups.get(0), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Map<Integer, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, BucketSize.YEARLY);
		List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroups.get(0), timeSeriesBuckets, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Assertions.assertFalse(timeSeriesDocuments.isEmpty());
	}

	@Test
	void saveTimeSeriesDocuments() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(List.of("moduleInstanceId"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		List<Document> timeSeries = TimeSeriesUtil.getTimeSeries(timeSeriesGroups.get(0), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Map<Integer, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, BucketSize.YEARLY);
		List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroups.get(0), timeSeriesBuckets, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		TimeSeriesUtil.saveTimeSeriesDocuments(timeSeriesDocuments, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED));
		timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(List.of("moduleInstanceId"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED));
		Assertions.assertFalse(timeSeriesGroups.isEmpty());
	}

	@Test
	void getTimeSeries() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(List.of("moduleInstanceId"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		List<Document> timeSeries = TimeSeriesUtil.getTimeSeries(timeSeriesGroups.get(0), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Assertions.assertFalse(timeSeries.isEmpty());
	}

	@Test
	void getCollapsedTimeSeries() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(List.of("moduleInstanceId"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		List<Document> timeSeries = TimeSeriesUtil.getTimeSeries(timeSeriesGroups.get(0), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		timeSeries = TimeSeriesUtil.getCollapsedTimeSeries(timeSeries);
		Assertions.assertFalse(timeSeries.isEmpty());
	}

	@Test
	void getTimeSeriesBuckets() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(List.of("moduleInstanceId"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		List<Document> timeSeries = TimeSeriesUtil.getTimeSeries(timeSeriesGroups.get(0), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Map<Integer, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, BucketSize.YEARLY);
		Assertions.assertFalse(timeSeriesBuckets.isEmpty());
	}

	@Test
	void getStitchedTimeSeries() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(List.of("moduleInstanceId"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		List<Document> timeSeries = TimeSeriesUtil.getStitchedTimeSeries(timeSeriesGroups.get(0), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Assertions.assertFalse(timeSeries.isEmpty());
	}
}