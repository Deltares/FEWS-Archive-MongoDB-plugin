package nl.fews.archivedatabase.common.query;

import nl.fews.archivedatabase.common.migrate.TestSettings;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Testcontainers
class ArchiveDatabaseGuiDataReaderTest {
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
//	void read() throws Exception{
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseGuiDataReader archiveDatabaseGuiDataReader = (ArchiveDatabaseGuiDataReader) archiveDatabase.getArchiveDatabaseGuiDataReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//
//		ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters = new ArchiveDatabaseResultSearchParameters(
//				"scalar",
//				TimeSeriesType.EXTERNAL_HISTORICAL,
//				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()),
//				Set.of(new String[0]),
//				Set.of(new TimeStep[0]),
//				Set.of(new String[0]),
//				Set.of(new String[0]));
//
//		ArchiveDatabaseReadResult archiveDatabaseReadResult = archiveDatabaseGuiDataReader.read(archiveDatabaseResultSearchParameters);
//		int count = 0;
//		while(archiveDatabaseReadResult.hasNext()){
//			archiveDatabaseReadResult.next();
//			count++;
//		}
//		assertEquals(184, count);
//	}
//
//	@Test
//	void read2() throws Exception{
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseGuiDataReader archiveDatabaseGuiDataReader = (ArchiveDatabaseGuiDataReader) archiveDatabase.getArchiveDatabaseGuiDataReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//
//		ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters = new ArchiveDatabaseResultSearchParameters(
//				"scalar",
//				TimeSeriesType.SIMULATED_FORECASTING,
//				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()),
//				Set.of(new String[0]),
//				Set.of(new TimeStep[0]),
//				Set.of(new String[0]),
//				Set.of(new String[0]));
//
//		ArchiveDatabaseReadResult archiveDatabaseReadResult = archiveDatabaseGuiDataReader.read(archiveDatabaseResultSearchParameters);
//		int count = 0;
//		while(archiveDatabaseReadResult.hasNext()){
//			archiveDatabaseReadResult.next();
//			count++;
//		}
//		assertEquals(16, count);
//	}
//
//	@Test
//	void getSummary()throws Exception{
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseGuiDataReader archiveDatabaseGuiDataReader = (ArchiveDatabaseGuiDataReader) archiveDatabase.getArchiveDatabaseGuiDataReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//
//		ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters = new ArchiveDatabaseResultSearchParameters(
//				"scalar",
//				TimeSeriesType.EXTERNAL_HISTORICAL,
//				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()),
//				Set.of(new String[0]),
//				Set.of(new TimeStep[0]),
//				Set.of(new String[0]),
//				Set.of(new String[0]));
//
//		ArchiveDatabaseSummary archiveDatabaseSummary = archiveDatabaseGuiDataReader.getSummary(archiveDatabaseResultSearchParameters);
//		assertEquals(1, archiveDatabaseSummary.numberOfParameters());
//		assertEquals(1, archiveDatabaseSummary.numberOfModuleInstanceIds());
//		assertEquals(184, archiveDatabaseSummary.numberOfTimeSeries());
//	}
//
//	@Test
//	void getSummary2()throws Exception{
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseGuiDataReader archiveDatabaseGuiDataReader = (ArchiveDatabaseGuiDataReader) archiveDatabase.getArchiveDatabaseGuiDataReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//
//		ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters = new ArchiveDatabaseResultSearchParameters(
//				"scalar",
//				TimeSeriesType.SIMULATED_FORECASTING,
//				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()),
//				Set.of(new String[0]),
//				Set.of(new TimeStep[0]),
//				Set.of(new String[0]),
//				Set.of(new String[0]));
//
//		ArchiveDatabaseSummary archiveDatabaseSummary = archiveDatabaseGuiDataReader.getSummary(archiveDatabaseResultSearchParameters);
//		assertEquals(1, archiveDatabaseSummary.numberOfParameters());
//		assertEquals(8, archiveDatabaseSummary.numberOfModuleInstanceIds());
//		assertEquals(16, archiveDatabaseSummary.numberOfTimeSeries());
//	}
//
//	@Test
//	void getFilterOptions() throws Exception{
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseGuiDataReader archiveDatabaseGuiDataReader = (ArchiveDatabaseGuiDataReader) archiveDatabase.getArchiveDatabaseGuiDataReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//		Database.updateTimeSeriesIndex();
//
//		ArchiveDatabaseFilterOptions archiveDatabaseFilterOptions = archiveDatabaseGuiDataReader.getFilterOptions(
//				"scalar",
//				TimeSeriesType.EXTERNAL_HISTORICAL,
//				Set.of());
//		assertEquals(1, archiveDatabaseFilterOptions.getParameterIds().size());
//		assertEquals(1, archiveDatabaseFilterOptions.getModuleInstanceIds().size());
//		assertEquals(1, archiveDatabaseFilterOptions.getTimeSteps().size());
//	}
//
//	@Test
//	void getFilterOptions2() throws Exception{
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseGuiDataReader archiveDatabaseGuiDataReader = (ArchiveDatabaseGuiDataReader) archiveDatabase.getArchiveDatabaseGuiDataReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//		Database.updateTimeSeriesIndex();
//
//		ArchiveDatabaseFilterOptions archiveDatabaseFilterOptions = archiveDatabaseGuiDataReader.getFilterOptions(
//				"scalar",
//				TimeSeriesType.SIMULATED_FORECASTING,
//				Set.of());
//		assertEquals(2, archiveDatabaseFilterOptions.getParameterIds().size());
//		//assertEquals(139, archiveDatabaseFilterOptions.getModuleInstanceIds().size());
//		assertEquals(1, archiveDatabaseFilterOptions.getTimeSteps().size());
//	}
//
//	@Test
//	void getSourceIds() throws Exception {
//		ArchiveDatabase archiveDatabase = ArchiveDatabase.create();
//		archiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
//
//		ArchiveDatabaseGuiDataReader archiveDatabaseGuiDataReader = (ArchiveDatabaseGuiDataReader) archiveDatabase.getArchiveDatabaseGuiDataReader();
//
//		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//		Insert.insertMetaDatas(entries, Map.of());
//		Database.updateTimeSeriesIndex();
//
//		Set<String> sourceIds = archiveDatabaseGuiDataReader.getSourceIds(TimeSeriesType.EXTERNAL_HISTORICAL);
//		assertEquals(1, sourceIds.size());
//	}
}
