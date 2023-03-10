package nl.fews.archivedatabase.mongodb.export.utils;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.export.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.shared.timeseries.ScalarExternalForecasting;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings({"rawtypes", "unchecked"})
class DatabaseSingletonUtilTest {

	@BeforeEach
	public void setUp() {
		TestSettings.setTestSettings();
	}

	@Test
	void getDocumentsByKey() {

		String[] expected = new String[]{
			"{\"moduleInstanceId\": \"moduleInstanceId0\", \"locationId\": \"locationId0\", \"parameterId\": \"parameterId0\", \"qualifierId\": \"[\\\"qualifierId0\\\",\\\"qualifierId0\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"ensembleId\": \"ensembleId0\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}}",
			"{\"moduleInstanceId\": \"moduleInstanceId1\", \"locationId\": \"locationId1\", \"parameterId\": \"parameterId1\", \"qualifierId\": \"[\\\"qualifierId1\\\",\\\"qualifierId1\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"ensembleId\": \"ensembleId1\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}}",
			"{\"moduleInstanceId\": \"moduleInstanceId2\", \"locationId\": \"locationId2\", \"parameterId\": \"parameterId2\", \"qualifierId\": \"[\\\"qualifierId2\\\",\\\"qualifierId2\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"ensembleId\": \"ensembleId2\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}}",
			"{\"moduleInstanceId\": \"moduleInstanceId3\", \"locationId\": \"locationId3\", \"parameterId\": \"parameterId3\", \"qualifierId\": \"[\\\"qualifierId3\\\",\\\"qualifierId3\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"ensembleId\": \"ensembleId3\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}}",
			"{\"moduleInstanceId\": \"moduleInstanceId4\", \"locationId\": \"locationId4\", \"parameterId\": \"parameterId4\", \"qualifierId\": \"[\\\"qualifierId4\\\",\\\"qualifierId4\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"ensembleId\": \"ensembleId4\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}}",
			"{\"moduleInstanceId\": \"moduleInstanceId5\", \"locationId\": \"locationId5\", \"parameterId\": \"parameterId5\", \"qualifierId\": \"[\\\"qualifierId5\\\",\\\"qualifierId5\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"ensembleId\": \"ensembleId5\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}}",
			"{\"moduleInstanceId\": \"moduleInstanceId6\", \"locationId\": \"locationId6\", \"parameterId\": \"parameterId6\", \"qualifierId\": \"[\\\"qualifierId6\\\",\\\"qualifierId6\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"ensembleId\": \"ensembleId6\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}}",
			"{\"moduleInstanceId\": \"moduleInstanceId7\", \"locationId\": \"locationId7\", \"parameterId\": \"parameterId7\", \"qualifierId\": \"[\\\"qualifierId7\\\",\\\"qualifierId7\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"ensembleId\": \"ensembleId7\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}}",
			"{\"moduleInstanceId\": \"moduleInstanceId8\", \"locationId\": \"locationId8\", \"parameterId\": \"parameterId8\", \"qualifierId\": \"[\\\"qualifierId8\\\",\\\"qualifierId8\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"ensembleId\": \"ensembleId8\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}}",
			"{\"moduleInstanceId\": \"moduleInstanceId9\", \"locationId\": \"locationId9\", \"parameterId\": \"parameterId9\", \"qualifierId\": \"[\\\"qualifierId9\\\",\\\"qualifierId9\\\"]\", \"encodedTimeStepId\": \"SETS360\", \"ensembleId\": \"ensembleId9\", \"ensembleMemberId\": \"1\", \"forecastTime\": {\"$date\": \"2012-01-01T00:00:00Z\"}}"
		};

		TimeSeriesArrays timeSeriesArrays = TestUtil.getDefaultTimeSeriesArrays();

		TimeSeries timeSeries = new ScalarExternalForecasting();

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
		Map<String, List<Document>> documents = new HashMap<>();
		for(Document document: ts){
			for (Map.Entry<String, List<Document>> x: DatabaseSingletonUtil.getDocumentsByKey(document, TimeSeriesType.SCALAR_EXTERNAL_FORECASTING).entrySet()){
				documents.putIfAbsent(x.getKey(), new ArrayList<>());
				documents.get(x.getKey()).addAll(x.getValue());
			}
		}

		assertEquals(10, documents.size());

		for (List<Document> documentList:documents.values()) {
			assertEquals(1, documentList.size());
		}

		int index = 0;
		for (String key :documents.keySet().stream().sorted().collect(Collectors.toList())) {
			assertEquals(expected[index++], key);
		}
	}
}