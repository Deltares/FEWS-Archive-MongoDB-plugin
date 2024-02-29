package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Testcontainers
class BucketScalarSimulatedHistoricalTest {

	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:5.0.12"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	@Test
	void bucketGroups() {
		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar") && s.getKey().toString().contains("TVA_UpdateStates")).min(Map.Entry.comparingByKey()).orElse(null);
		Insert.insertMetaData(entry.getKey(), entry.getValue());
		new BucketScalarSimulatedHistorical().bucketGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED));
		assertNotNull(Database.find(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED), new Document()).first());
	}
}