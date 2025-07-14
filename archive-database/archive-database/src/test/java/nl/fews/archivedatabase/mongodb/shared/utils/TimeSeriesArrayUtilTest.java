package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class TimeSeriesArrayUtilTest {

	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	@Test
	void getTimeSeriesArrayExistingPeriods() {
		TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();
		for(TimeSeriesArray<TimeSeriesHeader> timeSeriesArray: timeSeriesArrays.toArray()){
			Map<Boolean, List<Period>> existingPeriods = TimeSeriesArrayUtil.getTimeSeriesArrayExistingPeriods(timeSeriesArray);
			assertEquals(1, existingPeriods.get(false).size());
		}
	}

	@Test
	void getTimeSeriesHeader() {

		Document result = new Document().
			append("moduleInstanceId", "moduleInstanceId").
			append("locationId", "locationId").
			append("parameterId", "parameterId").
			append("qualifierIds", List.of("qualifierId1", "qualifierId2")).
			append("encodedTimeStepId", "encodedTimeStepId");

		var r = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, TimeSeriesType.SIMULATED_FORECASTING, result);
		assertNotNull(r);
	}
}