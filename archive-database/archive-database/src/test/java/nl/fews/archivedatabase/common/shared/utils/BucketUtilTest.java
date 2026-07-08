package nl.fews.archivedatabase.common.shared.utils;

import nl.fews.archivedatabase.common.migrate.TestSettings;
import nl.fews.archivedatabase.common.migrate.operations.Insert;
import nl.fews.archivedatabase.common.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.common.shared.database.Index;
import nl.fews.archivedatabase.common.shared.database.IndexTest;
import nl.fews.archivedatabase.common.shared.enums.BucketSize;
import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Testcontainers
class BucketUtilTest {

	@Container
	final MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:8.3.2"));

	@BeforeEach
	public void setUp(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	@Test
	void getBucketSize() {
		assertEquals(BucketSize.YEAR, BucketUtil.getBucketSize(60));
		assertEquals(BucketSize.YEAR, BucketUtil.getBucketSize(61));
		assertEquals(BucketSize.MONTH, BucketUtil.getBucketSize(59));
	}

	@Test
	void getBucketValue() {
		Date date = new Date();
		LocalDateTime l = DateUtil.getLocalDateTime(date);
		assertEquals(l.getYear() / 10 * 10, BucketUtil.getBucketValue(date, BucketSize.DECADE));
		assertEquals(l.getYear(), BucketUtil.getBucketValue(date, BucketSize.YEAR));
		assertEquals(l.getYear() * 100L + l.getMonthValue(), BucketUtil.getBucketValue(date, BucketSize.MONTH));
		assertEquals(l.getYear() * 10000L + l.getMonthValue() * 100 + l.getDayOfMonth(), BucketUtil.getBucketValue(date, BucketSize.DAY));
	}

	@Test
	void getEstimatedBucketSize() throws Exception {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		assertEquals(BucketSize.YEAR, BucketUtil.getEstimatedBucketSize(simpleDateFormat.parse("2020-01-01"), simpleDateFormat.parse("2020-01-02"), 23));
		assertEquals(BucketSize.YEAR, BucketUtil.getEstimatedBucketSize(simpleDateFormat.parse("2020-01-01"), simpleDateFormat.parse("2020-01-02"), 24));
		assertEquals(BucketSize.MONTH, BucketUtil.getEstimatedBucketSize(simpleDateFormat.parse("2020-01-01"), simpleDateFormat.parse("2020-01-02"), 25));
	}

	@Test
	void getBucketKeyFields() {
		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId"), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)));
	}

//	@Test
//	void ensureBucketSize() {
//		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).min(Map.Entry.comparingByKey()).orElse(null);
//		Insert.insertMetaData(entry.getKey(), entry.getValue());
//		List<Map<String, Object>> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET)));
//
//		Map<String, Object> timeSeriesGroup = (Map<String, Object>)timeSeriesGroups.get(0).get("timeSeriesGroup");
//		BucketSize bucketSize = BucketSize.valueOf((String)timeSeriesGroups.get(0).get("bucketSize"));
//		List<Map<String, Object>> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//		Map<Long, List<Map<String, Object>>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
//		List<Map<String, Object>> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, bucketSize, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//
//		for (Map<String, Object> timesSeriesDocument: timeSeriesDocuments) {
//			timesSeriesDocument.put("encodedTimeStepId", "NETS");
//			Date startTime = (Date)timesSeriesDocument.get("startTime");
//			Date endTime = (Date)timesSeriesDocument.get("endTime");
//			long seconds = Duration.between(DateUtil.getLocalDateTime(startTime), DateUtil.getLocalDateTime(endTime)).getSeconds()/100000;
//			Map<String, Object> timeseries = ((List<Map<String, Object>>)timesSeriesDocument.get("timeseries")).stream().findFirst().get();
//			timesSeriesDocument.put("timeseries", IntStream.range(0, 100000).boxed().map(s -> Map<String, Object>.parse(timeseries.toJson()).append("t", new Date(startTime.getTime()+seconds*s*1000))).collect(Collectors.toList()));
//		}
//		Database.instance().insertMany(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), timeSeriesDocuments);
//		BucketUtil.ensureBucketSize(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), timeSeriesDocuments);
//
//		Map<String, Object> result = Database.instance().findOne(Collection.BucketSize.toString(), new Map<String, Object>("bucketCollection", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)).append("bucketKey", Index.getKey(Index.getKeyDocument(BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)), timeSeriesDocuments.get(0)))));
//		assertNotNull(result);
//		assertEquals("DAY", result.get("bucketSize"));
//	}
//
//	@Test
//	void getNetsBucketSize() {
//		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).min(Map.Entry.comparingByKey()).orElse(null);
//		Insert.insertMetaData(entry.getKey(), entry.getValue());
//		List<Map<String, Object>> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET)));
//
//		List<Map<String, Object>> timeSeriesGroup = timeSeriesGroups.get(0).get("timeSeriesGroup", Map<String, Object>.class);
//		BucketSize bucketSize = BucketSize.valueOf((String)timeSeriesGroups.get(0).get("bucketSize"));
//		List<Map<String, Object>> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//		Map<Long, List<Map<String, Object>>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
//		List<Map<String, Object>> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, bucketSize, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//
//		assertEquals(BucketSize.YEAR, BucketUtil.getNetsBucketSize(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), Index.getKey(Index.getKeyDocument(BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)), timeSeriesDocuments.get(0)))));
//	}

//		@Test
//		void getBucketKey() {
//			Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).min(Map.Entry.comparingByKey()).orElse(null);
//			Insert.insertMetaData(entry.getKey(), entry.getValue());
//			List<Map<String, Object>> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET)));
//
//			Map<String, Object> timeSeriesGroup = (Map<String, Object>)timeSeriesGroups.get(0).get("timeSeriesGroup");
//			BucketSize bucketSize = BucketSize.valueOf((String)timeSeriesGroups.get(0).get("bucketSize"));
//			List<Map<String, Object>> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//			Map<Long, List<Map<String, Object>>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
//			List<Map<String, Object>> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, bucketSize, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//
//			assertNotNull(Index.getKey(Index.getKeyDocument(BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)), timeSeriesDocuments.get(0))));
//			assertNotNull(Index.getKey(Index.getKeyDocument(BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)), timeSeriesDocuments.get(0))));
//		}
//
//		@Test
//		void getBucketKeyDocument() {
//			Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).min(Map.Entry.comparingByKey()).orElse(null);
//			Insert.insertMetaData(entry.getKey(), entry.getValue());
//			List<Map<String, Object>> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET)));
//
//			Map<String, Object> timeSeriesGroup = (Map<String, Object>)timeSeriesGroups.get(0).get("timeSeriesGroup");
//			BucketSize bucketSize = BucketSize.valueOf((String)timeSeriesGroups.get(0).get("bucketSize"));
//			List<Map<String, Object>> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//			Map<Long, List<Map<String, Object>>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
//			List<Map<String, Object>> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, bucketSize, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//
//		assertNotNull(Index.getKeyDocument(BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)), timeSeriesDocuments.get(0)));
//	}
}