package nl.fews.archivedatabase.common.query;

import nl.fews.archivedatabase.common.migrate.TestSettings;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ArchiveDatabaseBridgeTimeSeriesReaderTest {
	@Container
	final MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:8.3.2"));

	@BeforeEach
	public void setUp(){
		TestSettings.setTestSettings();
	}

//	@AfterEach
//	public void tearDown(){
//		Database.close();
//	}
//
//	@Test
//	void importExternalHistorical() throws Exception {
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseTimeSeriesReader archiveDatabaseTimeSeriesReader = (ArchiveDatabaseTimeSeriesReader) archiveDatabase.getArchiveDataBaseTimeSeriesReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//		Database.updateTimeSeriesIndex();
//
//		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();
//		timeSeriesHeader.setModuleInstanceId("moduleInstanceId");
//		timeSeriesHeader.setLocationId("locationId");
//		timeSeriesHeader.setParameterId("parameterId");
//		timeSeriesHeader.setQualifierIds("qualifierId");
//		timeSeriesHeader.setTimeStep(TimeStepUtils.decode("SETS60"));
//		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader);
//		timeSeriesArray.put(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), 1);
//		TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = new TimeSeriesArrays<>(timeSeriesArray);
//		Period period = new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime());
//
//		TimeSeriesArrays<TimeSeriesHeader> timeSeriesArraysResult = archiveDatabaseTimeSeriesReader.importExternalHistorical(period, timeSeriesArrays);
//		assertFalse(timeSeriesArrays.isEmpty());
//		assertEquals(0, timeSeriesArraysResult.size());
//	}
//
//	@Test
//	void importExternalForecasting() throws Exception {
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseTimeSeriesReader archiveDatabaseTimeSeriesReader = (ArchiveDatabaseTimeSeriesReader) archiveDatabase.getArchiveDataBaseTimeSeriesReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("external_forecasts") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//		Database.updateTimeSeriesIndex();
//
//		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();
//		timeSeriesHeader.setModuleInstanceId("moduleInstanceId");
//		timeSeriesHeader.setLocationId("locationId");
//		timeSeriesHeader.setParameterId("parameterId");
//		timeSeriesHeader.setQualifierIds("qualifierId");
//		timeSeriesHeader.setTimeStep(TimeStepUtils.decode("SETS60"));
//		timeSeriesHeader.setEnsembleId("ensembleId");
//		timeSeriesHeader.setEnsembleMemberId("ensembleMemberId");
//		timeSeriesHeader.setForecastTime(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime());
//
////		List<TimeSeriesArrays<TimeSeriesHeader>> timeSeriesArrays = mongoDbArchiveDatabaseTimeSeriesReader.importExternalForecasting(Set.of(new ArchiveDatabaseForecastImportRequest(new FewsTimeSeriesHeaders((FewsTimeSeriesHeader[]) List.of(timeSeriesHeader).toArray()))));
////		assertEquals(0, timeSeriesArrays.size());
//	}
//
//	@Test
//	void getTimeSeriesForTaskRun() throws Exception{
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseTimeSeriesReader archiveDatabaseTimeSeriesReader = (ArchiveDatabaseTimeSeriesReader) archiveDatabase.getArchiveDataBaseTimeSeriesReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//
//		Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor> timeSeriesArrays = archiveDatabaseTimeSeriesReader.getTimeSeriesForTaskRun("SA11024320_1", TimeSeriesType.SIMULATED_FORECASTING);
//		assertEquals(4309, timeSeriesArrays.getObject0().size());
//	}
//
//	@Test
//	void getEnsembleMembers() throws Exception{
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseTimeSeriesReader archiveDatabaseTimeSeriesReader = (ArchiveDatabaseTimeSeriesReader) archiveDatabase.getArchiveDataBaseTimeSeriesReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//
//		Set<String> ensembleMembers = archiveDatabaseTimeSeriesReader.getEnsembleMembers("ALCT1", "ADIMF", Set.of("SACSMA_ALCT1_Forecast"), "GEFS.ENS", new String[]{}, TimeSeriesType.SIMULATED_FORECASTING);
//		assertEquals(31, ensembleMembers.size());
//	}
//
//	@Test
//	void getModuleInstanceIds() throws Exception{
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseTimeSeriesReader archiveDatabaseTimeSeriesReader = (ArchiveDatabaseTimeSeriesReader) archiveDatabase.getArchiveDataBaseTimeSeriesReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//
//		Set<String> moduleInstanceIds = archiveDatabaseTimeSeriesReader.getModuleInstanceIds("ALCT1", "ADIMF", "GEFS.ENS", new String[]{}, TimeSeriesType.SIMULATED_FORECASTING);
//		assertEquals(1, moduleInstanceIds.size());
//	}
//
//	@Test
//	void getSimulatedTaskRunInfos() throws Exception{
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseTimeSeriesReader archiveDatabaseTimeSeriesReader = (ArchiveDatabaseTimeSeriesReader) archiveDatabase.getArchiveDataBaseTimeSeriesReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//
//		List<SimulatedTaskRunInfo> simulatedTaskRunInfos = archiveDatabaseTimeSeriesReader.getSimulatedTaskRunInfos("ALCT1", "ADIMF", "SACSMA_ALCT1_Forecast", "GEFS.ENS", new String[]{}, "DTOD_0_6_12_18TZ_CST", new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2022-01-01").getTime()),1000);
//		assertEquals(1, simulatedTaskRunInfos.size());
//	}
//
//	@Test
//	void searchForExternalForecastTimes() throws Exception{
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseTimeSeriesReader archiveDatabaseTimeSeriesReader = (ArchiveDatabaseTimeSeriesReader) archiveDatabase.getArchiveDataBaseTimeSeriesReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("external_forecasts") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//
//		LongUnmodifiableList forecastTimes = archiveDatabaseTimeSeriesReader.searchForExternalForecastTimes("BARK2E", "MAP", "QPF_to_MAP", "", new String[]{"Extended", "HRRR"}, TimeSeriesType.EXTERNAL_FORECASTING, new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2022-01-01").getTime()),1000);
//		assertEquals(1, forecastTimes.size());
//	}
//
//	@Test
//	void getAvailableYears() throws Exception {
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseTimeSeriesReader archiveDatabaseTimeSeriesReader = (ArchiveDatabaseTimeSeriesReader) archiveDatabase.getArchiveDataBaseTimeSeriesReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//
//		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();
//		timeSeriesHeader.setModuleInstanceId("Preprocess_AV");
//		timeSeriesHeader.setLocationId("BOH-01");
//		timeSeriesHeader.setParameterId("AV");
//		timeSeriesHeader.setTimeStep(TimeStepUtils.decode("SETS60"));
//		Period period = new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2020-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2020-12-01").getTime());
//
//		Set<Integer> years = archiveDatabaseTimeSeriesReader.getAvailableYears(List.of(timeSeriesHeader), period);
//		assertEquals(1, years.size());
//	}
}
