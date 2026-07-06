package nl.fews.archivedatabase.common.export.operations;

import nl.fews.archivedatabase.common.migrate.TestSettings;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Testcontainers
class SynchronizeBucketsTest {

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
//	void synchronize() {
//		TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();
//		TimeSeries timeSeries = new ScalarExternalHistorical();
//		for(TimeSeriesArray<TimeSeriesHeader> timeSeriesArray: timeSeriesArrays.toArray()){
//			TimeSeriesHeader header = timeSeriesArray.getHeader();
//
//			Document metadataDocument = timeSeries.getMetaData(header, "areaId", "sourceId");
//			List<Document> timeseriesDocuments = timeSeries.getEvents(timeSeriesArray, metadataDocument);
//			Document metaDataDocument = timeSeries.getMetaData(header, "areaId", "sourceId");
//			Document runInfoDocument = timeSeries.getRunInfo(header);
//			Document rootDocument = timeSeries.getRoot(header, timeseriesDocuments, runInfoDocument);
//
//			if(!metaDataDocument.isEmpty()) rootDocument.append("metaData", metaDataDocument);
//			if(!runInfoDocument.isEmpty()) rootDocument.append("runInfo", runInfoDocument);
//			if(!timeseriesDocuments.isEmpty()) rootDocument.append("timeseries", timeseriesDocuments);
//
//			if(!timeseriesDocuments.isEmpty()){
//				Synchronize synchronize = new SynchronizeBuckets();
//				synchronize.synchronize(rootDocument, TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL);
//			}
//		}
//		assertNotNull(Database.findOne(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), new Document()));
//	}
}