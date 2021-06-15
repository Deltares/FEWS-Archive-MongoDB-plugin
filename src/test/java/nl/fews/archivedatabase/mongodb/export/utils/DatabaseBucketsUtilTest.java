package nl.fews.archivedatabase.mongodb.export.utils;

import junit.framework.TestCase;
import nl.fews.archivedatabase.mongodb.export.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.export.interfaces.MetaData;
import nl.fews.archivedatabase.mongodb.export.interfaces.Root;
import nl.fews.archivedatabase.mongodb.export.interfaces.RunInfo;
import nl.fews.archivedatabase.mongodb.export.interfaces.TimeSeries;
import nl.fews.archivedatabase.util.TestUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseUnitConverter;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings({"rawtypes", "unchecked"})
public class DatabaseBucketsUtilTest extends TestCase {

	private List<Document> ts;

	public void setUp() {
		TimeSeriesArrays timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();

		ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter = new TestUtil.ArchiveDatabaseUnitConverterTestImplementation();
		ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter = new TestUtil.ArchiveDatabaseTimeConverterTestImplementation();

		TimeSeries timeSeries = new nl.fews.archivedatabase.mongodb.export.timeseries.ExternalHistorical(archiveDatabaseUnitConverter, archiveDatabaseTimeConverter);
		RunInfo runInfo = new nl.fews.archivedatabase.mongodb.export.runinfo.ExternalHistorical();
		MetaData metaData = new nl.fews.archivedatabase.mongodb.export.metadata.ExternalHistorical("areaId", "sourceId", archiveDatabaseUnitConverter, archiveDatabaseTimeConverter);
		Root root = new nl.fews.archivedatabase.mongodb.export.root.ExternalHistorical(archiveDatabaseTimeConverter);

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
		this.ts = ts;
	}

	public void testGetDocumentsByKey() {

		String[] expected = new String[]{
			"[\"moduleInstanceId0\",\"locationId0\",\"parameterId0\",\"[\\\"qualifierId0\\\",\\\"qualifierId0\\\"]\",\"SETS360\"]",
			"[\"moduleInstanceId1\",\"locationId1\",\"parameterId1\",\"[\\\"qualifierId1\\\",\\\"qualifierId1\\\"]\",\"SETS360\"]",
			"[\"moduleInstanceId2\",\"locationId2\",\"parameterId2\",\"[\\\"qualifierId2\\\",\\\"qualifierId2\\\"]\",\"SETS360\"]",
			"[\"moduleInstanceId3\",\"locationId3\",\"parameterId3\",\"[\\\"qualifierId3\\\",\\\"qualifierId3\\\"]\",\"SETS360\"]",
			"[\"moduleInstanceId4\",\"locationId4\",\"parameterId4\",\"[\\\"qualifierId4\\\",\\\"qualifierId4\\\"]\",\"SETS360\"]",
			"[\"moduleInstanceId5\",\"locationId5\",\"parameterId5\",\"[\\\"qualifierId5\\\",\\\"qualifierId5\\\"]\",\"SETS360\"]",
			"[\"moduleInstanceId6\",\"locationId6\",\"parameterId6\",\"[\\\"qualifierId6\\\",\\\"qualifierId6\\\"]\",\"SETS360\"]",
			"[\"moduleInstanceId7\",\"locationId7\",\"parameterId7\",\"[\\\"qualifierId7\\\",\\\"qualifierId7\\\"]\",\"SETS360\"]",
			"[\"moduleInstanceId8\",\"locationId8\",\"parameterId8\",\"[\\\"qualifierId8\\\",\\\"qualifierId8\\\"]\",\"SETS360\"]",
			"[\"moduleInstanceId9\",\"locationId9\",\"parameterId9\",\"[\\\"qualifierId9\\\",\\\"qualifierId9\\\"]\",\"SETS360\"]"
		};

		Map<String, Map<Integer, List<Document>>> documents = DatabaseBucketUtil.getDocumentsByKeyBucket(ts, TimeSeriesTypeUtil.getTimeSeriesTypeKeys(TimeSeriesType.EXTERNAL_HISTORICAL));

		assertEquals(10, documents.size());

		for (Map<Integer, List<Document>> documentList:documents.values()) {
			assertEquals(2, documentList.size());
		}

		int index = 0;
		for (String key :documents.keySet().stream().sorted().collect(Collectors.toList())) {
			assertEquals(expected[index++], key);
		}
	}

	public void testRemoveExistingTimeseries() {
		List<Document> timeseries = new ArrayList<>(ts.get(0).getList("timeseries", Document.class));
		timeseries.add(new Document("t", Date.from(Instant.parse("1900-01-01T00:00:00Z"))));
		Document existingDocument = new Document("timeseries", timeseries);

		assertEquals(11, existingDocument.getList("timeseries", Document.class).size());

		DatabaseBucketUtil.removeExistingTimeseries(existingDocument, List.of(ts.get(0)));

		assertEquals(1, existingDocument.getList("timeseries", Document.class).size());
	}

	public void testMergeDocuments() {

		assertEquals(10, ts.get(0).getList("timeseries", Document.class).size());

		Document d = DatabaseBucketUtil.mergeDocuments(2012, new Document(ts.get(0)).append("timeseries", new ArrayList<Document>()), ts);
		assertEquals(10, d.getList("timeseries", Document.class).size());

		d = DatabaseBucketUtil.mergeDocuments(2013, new Document(ts.get(10)).append("timeseries", new ArrayList<Document>()), ts);
		assertEquals(10, d.getList("timeseries", Document.class).size());

		d = DatabaseBucketUtil.mergeDocuments(2014, new Document(ts.get(0)).append("timeseries", new ArrayList<Document>()), ts);
		assertEquals(0, d.getList("timeseries", Document.class).size());
	}
}