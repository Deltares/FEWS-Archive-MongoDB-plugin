package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesUtil;
import org.bson.Document;
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

import static org.junit.jupiter.api.Assertions.assertFalse;

@Testcontainers
class StitchScalarSimulatedHistoricalTest {

	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));
	}

	@Test
	void stitchGroups() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		StitchScalarSimulatedHistorical.stitchGroups();
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED);
		assertFalse(timeSeriesGroups.isEmpty());
	}

	@Test
	void stitchGroup() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).findFirst().orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL);
		StitchScalarSimulatedHistorical.stitchGroup(timeSeriesGroups.get(0).get("timeSeriesGroup", Document.class), BucketSize.valueOf(timeSeriesGroups.get(0).getString("bucketSize")));
		timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED);
		assertFalse(timeSeriesGroups.isEmpty());
	}
}