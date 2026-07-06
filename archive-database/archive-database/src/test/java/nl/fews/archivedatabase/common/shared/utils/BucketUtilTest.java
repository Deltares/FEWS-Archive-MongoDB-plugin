package nl.fews.archivedatabase.common.shared.utils;

import nl.fews.archivedatabase.common.migrate.TestSettings;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

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

//	@AfterEach
//	public void tearDown(){
//		Database.close();
//	}
//
//	@Test
//	void getBucketSize() {
//		assertEquals(BucketSize.YEAR, BucketUtil.getBucketSize(60));
//		assertEquals(BucketSize.YEAR, BucketUtil.getBucketSize(61));
//		assertEquals(BucketSize.MONTH, BucketUtil.getBucketSize(59));
//	}
//
//	@Test
//	void getBucketValue() {
//		Date date = new Date();
//		LocalDateTime l = DateUtil.getLocalDateTime(date);
//		assertEquals(l.getYear() / 10 * 10, BucketUtil.getBucketValue(date, BucketSize.DECADE));
//		assertEquals(l.getYear(), BucketUtil.getBucketValue(date, BucketSize.YEAR));
//		assertEquals(l.getYear() * 100L + l.getMonthValue(), BucketUtil.getBucketValue(date, BucketSize.MONTH));
//		assertEquals(l.getYear() * 10000L + l.getMonthValue() * 100 + l.getDayOfMonth(), BucketUtil.getBucketValue(date, BucketSize.DAY));
//	}
//
//	@Test
//	void getEstimatedBucketSize() throws Exception {
//		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
//		simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
//		assertEquals(BucketSize.YEAR, BucketUtil.getEstimatedBucketSize(simpleDateFormat.parse("2020-01-01"), simpleDateFormat.parse("2020-01-02"), 23));
//		assertEquals(BucketSize.YEAR, BucketUtil.getEstimatedBucketSize(simpleDateFormat.parse("2020-01-01"), simpleDateFormat.parse("2020-01-02"), 24));
//		assertEquals(BucketSize.MONTH, BucketUtil.getEstimatedBucketSize(simpleDateFormat.parse("2020-01-01"), simpleDateFormat.parse("2020-01-02"), 25));
//	}
//
//	@Test
//	void ensureBucketSize() {
//		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).min(Map.Entry.comparingByKey()).orElse(null);
//		Insert.insertMetaData(entry.getKey(), entry.getValue());
//		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET)));
//
//		Document timeSeriesGroup = timeSeriesGroups.get(0).get("timeSeriesGroup", Document.class);
//		BucketSize bucketSize = BucketSize.valueOf(timeSeriesGroups.get(0).getString("bucketSize"));
//		List<Document> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//		Map<Long, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
//		List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, bucketSize, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//
//		for (Document timesSeriesDocument: timeSeriesDocuments) {
//			timesSeriesDocument.append("encodedTimeStepId", "NETS");
//			Date startTime = timesSeriesDocument.getDate("startTime");
//			Date endTime = timesSeriesDocument.getDate("endTime");
//			long seconds = Duration.between(DateUtil.getLocalDateTime(startTime), DateUtil.getLocalDateTime(endTime)).getSeconds()/100000;
//			Document timeseries = timesSeriesDocument.getList("timeseries", Document.class).stream().findFirst().get();
//			timesSeriesDocument.append("timeseries", IntStream.range(0, 100000).boxed().map(s -> Document.parse(timeseries.toJson()).append("t", new Date(startTime.getTime()+seconds*s*1000))).collect(Collectors.toList()));
//		}
//		Database.insertMany(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), timeSeriesDocuments);
//		BucketUtil.ensureBucketSize(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), timeSeriesDocuments);
//
//		Document result = Database.findOne(Collection.BucketSize.toString(), new Document("bucketCollection", TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)).append("bucketKey", Index.getKey(Index.getKeyDocument(BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)), timeSeriesDocuments.get(0)))));
//		assertNotNull(result);
//		assertEquals("DAY", result.getString("bucketSize"));
//	}
//
//	@Test
//	void getNetsBucketSize() {
//		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).min(Map.Entry.comparingByKey()).orElse(null);
//		Insert.insertMetaData(entry.getKey(), entry.getValue());
//		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET)));
//
//		Document timeSeriesGroup = timeSeriesGroups.get(0).get("timeSeriesGroup", Document.class);
//		BucketSize bucketSize = BucketSize.valueOf(timeSeriesGroups.get(0).getString("bucketSize"));
//		List<Document> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//		Map<Long, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
//		List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, bucketSize, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//
//		assertEquals(BucketSize.YEAR, BucketUtil.getNetsBucketSize(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), Index.getKey(Index.getKeyDocument(BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)), timeSeriesDocuments.get(0)))));
//	}
//
//	@Test
//	void getBucketKey() {
//		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).min(Map.Entry.comparingByKey()).orElse(null);
//		Insert.insertMetaData(entry.getKey(), entry.getValue());
//		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET)));
//
//		Document timeSeriesGroup = timeSeriesGroups.get(0).get("timeSeriesGroup", Document.class);
//		BucketSize bucketSize = BucketSize.valueOf(timeSeriesGroups.get(0).getString("bucketSize"));
//		List<Document> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//		Map<Long, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
//		List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, bucketSize, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//
//		assertNotNull(Index.getKey(Index.getKeyDocument(BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)), timeSeriesDocuments.get(0))));
//		assertNotNull(Index.getKey(Index.getKeyDocument(BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)), timeSeriesDocuments.get(0))));
//	}
//
//	@Test
//	void getBucketKeyDocument() {
//		Map.Entry<File, Date> entry = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).min(Map.Entry.comparingByKey()).orElse(null);
//		Insert.insertMetaData(entry.getKey(), entry.getValue());
//		List<Document> timeSeriesGroups = TimeSeriesUtil.getTimeSeriesGroups(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET)));
//
//		Document timeSeriesGroup = timeSeriesGroups.get(0).get("timeSeriesGroup", Document.class);
//		BucketSize bucketSize = BucketSize.valueOf(timeSeriesGroups.get(0).getString("bucketSize"));
//		List<Document> timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//		Map<Long, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
//		List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, bucketSize, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
//
//		assertNotNull(Index.getKeyDocument(BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)), timeSeriesDocuments.get(0)));
//	}
//
//	@Test
//	void getBucketKeyFields() {
//		assertEquals(List.of("moduleInstanceId", "locationId", "parameterId", "qualifierId", "encodedTimeStepId"), BucketUtil.getBucketKeyFields(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL)));
//	}
}