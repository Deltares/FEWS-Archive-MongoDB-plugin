package nl.fews.archivedatabase.common.shared.utils;

import nl.fews.archivedatabase.common.migrate.TestSettings;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class TimeSeriesArrayUtilTest {

	@Container
	final MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:8.3.2"));

	@BeforeEach
	public void setUp(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

//	@Test
//	void getTimeSeriesArrayExistingPeriods() {
//		TimeSeriesArrays<FewsTimeSeriesHeader> timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();
//		for(TimeSeriesArray<FewsTimeSeriesHeader> timeSeriesArray: timeSeriesArrays.toArray()){
//			Map<Boolean, List<Period>> existingPeriods = TimeSeriesArrayUtil.getTimeSeriesArrayExistingPeriods(timeSeriesArray);
//			assertEquals(1, existingPeriods.get(false).size());
//		}
//	}
//
//	@Test
//	void getTimeSeriesHeader() {
//
//		Document result = new Document().
//			append("moduleInstanceId", "moduleInstanceId").
//			append("locationId", "locationId").
//			append("parameterId", "parameterId").
//			append("qualifierIds", List.of("qualifierId1", "qualifierId2")).
//			append("encodedTimeStepId", "encodedTimeStepId");
//
//		var r = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, TimeSeriesType.SIMULATED_FORECASTING, result);
//		assertNotNull(r);
//	}
}