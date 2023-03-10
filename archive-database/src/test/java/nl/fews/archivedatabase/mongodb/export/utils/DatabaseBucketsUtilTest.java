package nl.fews.archivedatabase.mongodb.export.utils;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.export.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.shared.timeseries.ScalarExternalHistorical;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;
import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings({"rawtypes", "unchecked"})
class DatabaseBucketsUtilTest {

	private List<Document> ts;

	@BeforeEach
	public void setUp() {
		TestSettings.setTestSettings();
		TimeSeriesArrays timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();

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
		this.ts = ts;
	}

	@Test
	void getDocumentsByKey() {

		String[] expected = new String[]{
			"{\"moduleInstanceId\": \"moduleInstanceId0\", \"locationId\": \"locationId0\", \"parameterId\": \"parameterId0\", \"qualifierId\": \"[\\\"qualifierId0\\\",\\\"qualifierId0\\\"]\", \"encodedTimeStepId\": \"SETS360\"}",
			"{\"moduleInstanceId\": \"moduleInstanceId1\", \"locationId\": \"locationId1\", \"parameterId\": \"parameterId1\", \"qualifierId\": \"[\\\"qualifierId1\\\",\\\"qualifierId1\\\"]\", \"encodedTimeStepId\": \"SETS360\"}",
			"{\"moduleInstanceId\": \"moduleInstanceId2\", \"locationId\": \"locationId2\", \"parameterId\": \"parameterId2\", \"qualifierId\": \"[\\\"qualifierId2\\\",\\\"qualifierId2\\\"]\", \"encodedTimeStepId\": \"SETS360\"}",
			"{\"moduleInstanceId\": \"moduleInstanceId3\", \"locationId\": \"locationId3\", \"parameterId\": \"parameterId3\", \"qualifierId\": \"[\\\"qualifierId3\\\",\\\"qualifierId3\\\"]\", \"encodedTimeStepId\": \"SETS360\"}",
			"{\"moduleInstanceId\": \"moduleInstanceId4\", \"locationId\": \"locationId4\", \"parameterId\": \"parameterId4\", \"qualifierId\": \"[\\\"qualifierId4\\\",\\\"qualifierId4\\\"]\", \"encodedTimeStepId\": \"SETS360\"}",
			"{\"moduleInstanceId\": \"moduleInstanceId5\", \"locationId\": \"locationId5\", \"parameterId\": \"parameterId5\", \"qualifierId\": \"[\\\"qualifierId5\\\",\\\"qualifierId5\\\"]\", \"encodedTimeStepId\": \"SETS360\"}",
			"{\"moduleInstanceId\": \"moduleInstanceId6\", \"locationId\": \"locationId6\", \"parameterId\": \"parameterId6\", \"qualifierId\": \"[\\\"qualifierId6\\\",\\\"qualifierId6\\\"]\", \"encodedTimeStepId\": \"SETS360\"}",
			"{\"moduleInstanceId\": \"moduleInstanceId7\", \"locationId\": \"locationId7\", \"parameterId\": \"parameterId7\", \"qualifierId\": \"[\\\"qualifierId7\\\",\\\"qualifierId7\\\"]\", \"encodedTimeStepId\": \"SETS360\"}",
			"{\"moduleInstanceId\": \"moduleInstanceId8\", \"locationId\": \"locationId8\", \"parameterId\": \"parameterId8\", \"qualifierId\": \"[\\\"qualifierId8\\\",\\\"qualifierId8\\\"]\", \"encodedTimeStepId\": \"SETS360\"}",
			"{\"moduleInstanceId\": \"moduleInstanceId9\", \"locationId\": \"locationId9\", \"parameterId\": \"parameterId9\", \"qualifierId\": \"[\\\"qualifierId9\\\",\\\"qualifierId9\\\"]\", \"encodedTimeStepId\": \"SETS360\"}"
		};

		Map<String, Map<Pair<BucketSize, Long>, Document>> documents = new HashMap<>();
		for(Document document: ts){
			for (Map.Entry<String, Map<Pair<BucketSize, Long>, Document>> x: DatabaseBucketUtil.getDocumentsByKeyBucket(document, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)).entrySet()){
				documents.putIfAbsent(x.getKey(), new HashMap<>());
				for (Map.Entry<Pair<BucketSize, Long>, Document> y: x.getValue().entrySet()){
					documents.get(x.getKey()).putIfAbsent(y.getKey(), new Document(document));
				}
			}
		}

		assertEquals(10, documents.size());

		for (Map<Pair<BucketSize, Long>, Document> documentList:documents.values()) {
			assertEquals(2, documentList.size());
		}

		int index = 0;
		for (String key :documents.keySet().stream().sorted().collect(Collectors.toList())) {
			assertEquals(expected[index++], key);
		}
	}

	@Test
	void removeExistingTimeseries() {
		List<Document> timeseries = new ArrayList<>(ts.get(0).getList("timeseries", Document.class));
		timeseries.add(new Document("t", Date.from(Instant.parse("1900-01-01T00:00:00Z"))));
		Document existingDocument = new Document("timeseries", timeseries);

		assertEquals(11, existingDocument.getList("timeseries", Document.class).size());

		DatabaseBucketUtil.removeExistingTimeseries(existingDocument, ts.get(0));

		assertEquals(1, existingDocument.getList("timeseries", Document.class).size());
	}

	@Test
	void mergeExistingDocument() {

		assertEquals(10, ts.get(0).getList("timeseries", Document.class).size());

		Document d = DatabaseBucketUtil.mergeExistingDocument( new Document(ts.get(0)).append("timeseries", new ArrayList<Document>()), ts.get(0));
		assertEquals(10, d.getList("timeseries", Document.class).size());

		d = DatabaseBucketUtil.mergeExistingDocument( new Document(ts.get(10)).append("timeseries", new ArrayList<Document>()), ts.get(0));
		assertEquals(10, d.getList("timeseries", Document.class).size());

		d = DatabaseBucketUtil.mergeExistingDocument( new Document(ts.get(0)).append("timeseries", new ArrayList<Document>()), ts.get(0));
		assertEquals(0, d.getList("timeseries", Document.class).size());
	}
}