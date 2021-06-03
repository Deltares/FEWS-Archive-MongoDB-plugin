package nl.fews.archivedatabase.mongodb.export.utils;

import junit.framework.TestCase;
import nl.fews.archivedatabase.mongodb.export.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.export.interfaces.MetaData;
import nl.fews.archivedatabase.mongodb.export.interfaces.Root;
import nl.fews.archivedatabase.mongodb.export.interfaces.RunInfo;
import nl.fews.archivedatabase.mongodb.export.interfaces.TimeSeries;
import nl.fews.archivedatabase.util.TestUtil;
import nl.wldelft.fews.system.data.externaldatasource.opendatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.opendatabase.ArchiveDatabaseUnitConverter;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings({"rawtypes", "unchecked"})
public class DatabaseForecastUtilTest extends TestCase {

	public void testGetDocumentsByKey() {

		String[] expected = new String[]{
			"[\"moduleInstanceId0\",\"locationId0\",\"parameterId0\",\"[\\\"qualifierId0\\\",\\\"qualifierId0\\\"]\",\"SETS360\",\"ensembleId0\",\"1\",\"Sun Jan 01 00:00:00 UTC 2012\"]",
			"[\"moduleInstanceId0\",\"locationId0\",\"parameterId0\",\"[\\\"qualifierId0\\\",\\\"qualifierId0\\\"]\",\"SETS360\",\"ensembleId0\",\"1\",\"Tue Jan 01 00:00:00 UTC 2013\"]",
			"[\"moduleInstanceId1\",\"locationId1\",\"parameterId1\",\"[\\\"qualifierId1\\\",\\\"qualifierId1\\\"]\",\"SETS360\",\"ensembleId1\",\"1\",\"Sun Jan 01 00:00:00 UTC 2012\"]",
			"[\"moduleInstanceId1\",\"locationId1\",\"parameterId1\",\"[\\\"qualifierId1\\\",\\\"qualifierId1\\\"]\",\"SETS360\",\"ensembleId1\",\"1\",\"Tue Jan 01 00:00:00 UTC 2013\"]",
			"[\"moduleInstanceId2\",\"locationId2\",\"parameterId2\",\"[\\\"qualifierId2\\\",\\\"qualifierId2\\\"]\",\"SETS360\",\"ensembleId2\",\"1\",\"Sun Jan 01 00:00:00 UTC 2012\"]",
			"[\"moduleInstanceId2\",\"locationId2\",\"parameterId2\",\"[\\\"qualifierId2\\\",\\\"qualifierId2\\\"]\",\"SETS360\",\"ensembleId2\",\"1\",\"Tue Jan 01 00:00:00 UTC 2013\"]",
			"[\"moduleInstanceId3\",\"locationId3\",\"parameterId3\",\"[\\\"qualifierId3\\\",\\\"qualifierId3\\\"]\",\"SETS360\",\"ensembleId3\",\"1\",\"Sun Jan 01 00:00:00 UTC 2012\"]",
			"[\"moduleInstanceId3\",\"locationId3\",\"parameterId3\",\"[\\\"qualifierId3\\\",\\\"qualifierId3\\\"]\",\"SETS360\",\"ensembleId3\",\"1\",\"Tue Jan 01 00:00:00 UTC 2013\"]",
			"[\"moduleInstanceId4\",\"locationId4\",\"parameterId4\",\"[\\\"qualifierId4\\\",\\\"qualifierId4\\\"]\",\"SETS360\",\"ensembleId4\",\"1\",\"Sun Jan 01 00:00:00 UTC 2012\"]",
			"[\"moduleInstanceId4\",\"locationId4\",\"parameterId4\",\"[\\\"qualifierId4\\\",\\\"qualifierId4\\\"]\",\"SETS360\",\"ensembleId4\",\"1\",\"Tue Jan 01 00:00:00 UTC 2013\"]",
			"[\"moduleInstanceId5\",\"locationId5\",\"parameterId5\",\"[\\\"qualifierId5\\\",\\\"qualifierId5\\\"]\",\"SETS360\",\"ensembleId5\",\"1\",\"Sun Jan 01 00:00:00 UTC 2012\"]",
			"[\"moduleInstanceId5\",\"locationId5\",\"parameterId5\",\"[\\\"qualifierId5\\\",\\\"qualifierId5\\\"]\",\"SETS360\",\"ensembleId5\",\"1\",\"Tue Jan 01 00:00:00 UTC 2013\"]",
			"[\"moduleInstanceId6\",\"locationId6\",\"parameterId6\",\"[\\\"qualifierId6\\\",\\\"qualifierId6\\\"]\",\"SETS360\",\"ensembleId6\",\"1\",\"Sun Jan 01 00:00:00 UTC 2012\"]",
			"[\"moduleInstanceId6\",\"locationId6\",\"parameterId6\",\"[\\\"qualifierId6\\\",\\\"qualifierId6\\\"]\",\"SETS360\",\"ensembleId6\",\"1\",\"Tue Jan 01 00:00:00 UTC 2013\"]",
			"[\"moduleInstanceId7\",\"locationId7\",\"parameterId7\",\"[\\\"qualifierId7\\\",\\\"qualifierId7\\\"]\",\"SETS360\",\"ensembleId7\",\"1\",\"Sun Jan 01 00:00:00 UTC 2012\"]",
			"[\"moduleInstanceId7\",\"locationId7\",\"parameterId7\",\"[\\\"qualifierId7\\\",\\\"qualifierId7\\\"]\",\"SETS360\",\"ensembleId7\",\"1\",\"Tue Jan 01 00:00:00 UTC 2013\"]",
			"[\"moduleInstanceId8\",\"locationId8\",\"parameterId8\",\"[\\\"qualifierId8\\\",\\\"qualifierId8\\\"]\",\"SETS360\",\"ensembleId8\",\"1\",\"Sun Jan 01 00:00:00 UTC 2012\"]",
			"[\"moduleInstanceId8\",\"locationId8\",\"parameterId8\",\"[\\\"qualifierId8\\\",\\\"qualifierId8\\\"]\",\"SETS360\",\"ensembleId8\",\"1\",\"Tue Jan 01 00:00:00 UTC 2013\"]",
			"[\"moduleInstanceId9\",\"locationId9\",\"parameterId9\",\"[\\\"qualifierId9\\\",\\\"qualifierId9\\\"]\",\"SETS360\",\"ensembleId9\",\"1\",\"Sun Jan 01 00:00:00 UTC 2012\"]",
			"[\"moduleInstanceId9\",\"locationId9\",\"parameterId9\",\"[\\\"qualifierId9\\\",\\\"qualifierId9\\\"]\",\"SETS360\",\"ensembleId9\",\"1\",\"Tue Jan 01 00:00:00 UTC 2013\"]"
		};

		TimeSeriesArrays timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();

		ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter = new TestUtil.ArchiveDatabaseUnitConverterTestImplementation();
		ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter = new TestUtil.ArchiveDatabaseTimeConverterTestImplementation();

		TimeSeries timeSeries = new nl.fews.archivedatabase.mongodb.export.timeseries.ExternalForecasting(archiveDatabaseUnitConverter, archiveDatabaseTimeConverter);
		RunInfo runInfo = new nl.fews.archivedatabase.mongodb.export.runinfo.ExternalForecasting();
		MetaData metaData = new nl.fews.archivedatabase.mongodb.export.metadata.ExternalForecasting("areaId", "sourceId", archiveDatabaseUnitConverter, archiveDatabaseTimeConverter);
		Root root = new nl.fews.archivedatabase.mongodb.export.root.ExternalForecasting(archiveDatabaseTimeConverter);

		List<Document> ts = new ArrayList<>();

		for(TimeSeriesArray<TimeSeriesHeader> timeSeriesArray: timeSeriesArrays.toArray()){
			TimeSeriesHeader header = timeSeriesArray.getHeader();

			List<Document> timeseriesDocuments = timeSeries.getTimeSeries(timeSeriesArray);
			Document metaDataDocument = metaData.getMetaData(header);
			Document runInfoDocument = runInfo.getRunInfo();
			Document rootDocument = root.getRoot(header, timeseriesDocuments, runInfoDocument);

			if(!metaDataDocument.isEmpty()) rootDocument.append("metaData", metaDataDocument);
			if(!runInfoDocument.isEmpty()) rootDocument.append("runInfo", runInfoDocument);
			if(!timeseriesDocuments.isEmpty()) rootDocument.append("timeseries", timeseriesDocuments);

			if(!timeseriesDocuments.isEmpty()){
				ts.add(rootDocument);
			}
		}
		Map<String, List<Document>> documents = DatabaseForecastUtil.getDocumentsByKey(ts, TimeSeriesTypeUtil.getTimeSeriesTypeKeys(TimeSeriesType.EXTERNAL_FORECASTING));

		assertEquals(20, documents.size());

		for (List<Document> documentList:documents.values()) {
			assertEquals(1, documentList.size());
		}

		int index = 0;
		for (String key :documents.keySet().stream().sorted().collect(Collectors.toList())) {
			assertEquals(expected[index++], key);
		}
	}
}