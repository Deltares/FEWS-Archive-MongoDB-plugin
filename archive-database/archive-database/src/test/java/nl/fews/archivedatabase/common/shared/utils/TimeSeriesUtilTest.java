package nl.fews.archivedatabase.common.shared.utils;

import nl.fews.archivedatabase.common.migrate.TestSettings;
import nl.fews.archivedatabase.common.migrate.operations.Insert;
import nl.fews.archivedatabase.common.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.common.shared.enums.BucketSize;
import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.settings.Settings;
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

@SuppressWarnings("unchecked")
@Testcontainers
class TimeSeriesUtilTest {

	@Container
	final MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:8.3.2"));

	@BeforeEach
	public void setUp(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	@Test
	void getTimeSeriesGroups() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Map<String, Object>> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)));
		assertFalse(timeSeriesGroups.isEmpty());
	}

	@Test
	void getTimeSeriesDocuments() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Map<String, Object>> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)));
		List<Map<String, Object>> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries((Map<String, Object>)timeSeriesGroups.get(0).get("timeSeriesGroup"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Map<Long, List<Map<String, Object>>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, BucketSize.YEAR);
		List<Map<String, Object>> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments((Map<String, Object>)timeSeriesGroups.get(0).get("timeSeriesGroup"), timeSeriesBuckets, BucketSize.YEAR, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		assertFalse(timeSeriesDocuments.isEmpty());
	}

	@Test
	void getTimeSeries() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Map<String, Object>> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)));
		List<Map<String, Object>> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries((Map<String, Object>)timeSeriesGroups.get(0).get("timeSeriesGroup"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		assertFalse(timeSeries.isEmpty());
	}

	@Test
	void getTimeSeriesBuckets() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Map<String, Object>> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)));
		List<Map<String, Object>> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries((Map<String, Object>)timeSeriesGroups.get(0).get("timeSeriesGroup"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		Map<Long, List<Map<String, Object>>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, BucketSize.YEAR);
		assertFalse(timeSeriesBuckets.isEmpty());
	}

	@Test
	void getStitchedTimeSeries() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Map<String, Object>> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)));
		List<Map<String, Object>> timeSeries = TimeSeriesUtil.getStitchedTimeSeries((Map<String, Object>)timeSeriesGroups.get(0).get("timeSeriesGroup"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		assertFalse(timeSeries.isEmpty());
	}

	@Test
	void getUnwoundTimeSeries() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Map<String, Object>> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED)));
		List<Map<String, Object>> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries((Map<String, Object>)timeSeriesGroups.get(0).get("timeSeriesGroup"), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));
		assertFalse(timeSeries.isEmpty());
	}

	@Test
	void trimNullValues() {
		List<Map<String, Object>> d = new ArrayList<>();
		assertEquals(0, TimeSeriesUtil.trimNullValues(d).size());

		d.add(Map.of("v", null));
		assertEquals(0, TimeSeriesUtil.trimNullValues(d).size());

		d.add(Map.of("v", new Object()));
		assertEquals(1, TimeSeriesUtil.trimNullValues(d).size());

		d.add(Map.of("v", null));
		assertEquals(1, TimeSeriesUtil.trimNullValues(d).size());

		d.add(Map.of("v", new Object()));
		assertEquals(3, TimeSeriesUtil.trimNullValues(d).size());

		d.add(0, Map.of("v", new Object()));
		assertEquals(5, TimeSeriesUtil.trimNullValues(d).size());
	}
}