package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.operations.Insert;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Testcontainers
class TimeSeriesUtilTest {

	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	@Test
	void getTimeSeriesGroups() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)));
		assertFalse(timeSeriesGroups.isEmpty());
	}

	@Test
	void getTimeSeriesDocuments() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)));
		List<Document> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(timeSeriesGroups.get(0).get("timeSeriesGroup", Document.class), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Map<Long, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, BucketSize.YEAR);
		List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroups.get(0).get("timeSeriesGroup", Document.class), timeSeriesBuckets, BucketSize.YEAR, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		assertFalse(timeSeriesDocuments.isEmpty());
	}

	@Test
	void getTimeSeries() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)));
		List<Document> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(timeSeriesGroups.get(0).get("timeSeriesGroup", Document.class), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		assertFalse(timeSeries.isEmpty());
	}

	@Test
	void getTimeSeriesBuckets() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)));
		List<Document> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(timeSeriesGroups.get(0).get("timeSeriesGroup", Document.class), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Map<Long, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, BucketSize.YEAR);
		assertFalse(timeSeriesBuckets.isEmpty());
	}

	@Test
	void getStitchedTimeSeries() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)));
		List<Document> timeSeries = TimeSeriesUtil.getStitchedTimeSeries(timeSeriesGroups.get(0).get("timeSeriesGroup", Document.class), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		assertFalse(timeSeries.isEmpty());
	}

	@Test
	void getUnwoundTimeSeries() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)));
		List<Document> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(timeSeriesGroups.get(0).get("timeSeriesGroup", Document.class), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		assertFalse(timeSeries.isEmpty());
	}

	@Test
	void trimNullValues() {
		List<Document> d = new ArrayList<>();
		assertEquals(0, TimeSeriesUtil.trimNullValues(d).size());

		d.add(new Document("v", null));
		assertEquals(0, TimeSeriesUtil.trimNullValues(d).size());

		d.add(new Document("v", new Object()));
		assertEquals(1, TimeSeriesUtil.trimNullValues(d).size());

		d.add(new Document("v", null));
		assertEquals(1, TimeSeriesUtil.trimNullValues(d).size());

		d.add(new Document("v", new Object()));
		assertEquals(3, TimeSeriesUtil.trimNullValues(d).size());

		d.add(0, new Document("v", new Object()));
		assertEquals(5, TimeSeriesUtil.trimNullValues(d).size());
	}
}