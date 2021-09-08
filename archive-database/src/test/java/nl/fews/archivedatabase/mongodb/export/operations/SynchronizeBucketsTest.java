package nl.fews.archivedatabase.mongodb.export.operations;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.export.interfaces.Synchronize;
import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.timeseries.ScalarExternalHistorical;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Testcontainers
class SynchronizeBucketsTest {

	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));
	}

	@Test
	void synchronize() {
		TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();
		TimeSeries timeSeries = new ScalarExternalHistorical();
		List<Document> ts = new ArrayList<>();
		for(TimeSeriesArray<TimeSeriesHeader> timeSeriesArray: timeSeriesArrays.toArray()){
			TimeSeriesHeader header = timeSeriesArray.getHeader();

			Document metadataDocument = timeSeries.getMetaData(header, "areaId", "sourceId");
			List<Document> timeseriesDocuments = timeSeries.getEvents(timeSeriesArray, metadataDocument);
			Document metaDataDocument = timeSeries.getMetaData(header, "areaId", "sourceId");
			Document runInfoDocument = timeSeries.getRunInfo(header);
			Document rootDocument = timeSeries.getRoot(header, timeseriesDocuments, runInfoDocument);

			if(!metaDataDocument.isEmpty()) rootDocument.append("metaData", metaDataDocument);
			if(!runInfoDocument.isEmpty()) rootDocument.append("runInfo", runInfoDocument);
			if(!timeseriesDocuments.isEmpty()) rootDocument.append("timeseries", timeseriesDocuments);

			if(!timeseriesDocuments.isEmpty()){
				ts.add(rootDocument);
			}
		}
		Synchronize synchronize = new SynchronizeBuckets();
		synchronize.synchronize(ts, TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL);

		assertNotNull(Database.findOne(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), new Document()));
	}
}