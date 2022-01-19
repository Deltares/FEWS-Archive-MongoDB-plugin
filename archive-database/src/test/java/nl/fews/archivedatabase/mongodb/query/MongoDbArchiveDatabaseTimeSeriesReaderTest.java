package nl.fews.archivedatabase.mongodb.query;

import nl.fews.archivedatabase.mongodb.MongoDbArchiveDatabase;
import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.operations.Insert;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseFilterOptions;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseReadResult;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseResultSearchParameters;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseSummary;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.SimulatedTaskRunInfo;
import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.Box;
import nl.wldelft.util.LongUnmodifiableList;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class MongoDbArchiveDatabaseTimeSeriesReaderTest {
	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
	}

	@Test
	void read() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();
		mongoDbArchiveDatabaseTimeSeriesReader.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters = new ArchiveDatabaseResultSearchParameters(
				"scalar",
				TimeSeriesType.EXTERNAL_HISTORICAL,
				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()),
				Set.of(new String[0]),
				Set.of(new TimeStep[0]),
				Set.of(new String[0]),
				Set.of(new String[0]));

		ArchiveDatabaseReadResult archiveDatabaseReadResult = mongoDbArchiveDatabaseTimeSeriesReader.read(archiveDatabaseResultSearchParameters);
		int count = 0;
		while(archiveDatabaseReadResult.hasNext()){
			archiveDatabaseReadResult.next();
			count++;
		}
		assertEquals(184, count);
	}

	@Test
	void read2() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();
		mongoDbArchiveDatabaseTimeSeriesReader.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters = new ArchiveDatabaseResultSearchParameters(
				"scalar",
				TimeSeriesType.SIMULATED_FORECASTING,
				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()),
				Set.of(new String[0]),
				Set.of(new TimeStep[0]),
				Set.of(new String[0]),
				Set.of(new String[0]));

		ArchiveDatabaseReadResult archiveDatabaseReadResult = mongoDbArchiveDatabaseTimeSeriesReader.read(archiveDatabaseResultSearchParameters);
		int count = 0;
		while(archiveDatabaseReadResult.hasNext()){
			archiveDatabaseReadResult.next();
			count++;
		}
		assertEquals(16, count);
	}

	@Test
	void getSummary()throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters = new ArchiveDatabaseResultSearchParameters(
				"scalar",
				TimeSeriesType.EXTERNAL_HISTORICAL,
				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()),
				Set.of(new String[0]),
				Set.of(new TimeStep[0]),
				Set.of(new String[0]),
				Set.of(new String[0]));

		ArchiveDatabaseSummary archiveDatabaseSummary = mongoDbArchiveDatabaseTimeSeriesReader.getSummary(archiveDatabaseResultSearchParameters);
		assertEquals(1, archiveDatabaseSummary.numberOfParameters());
		assertEquals(1, archiveDatabaseSummary.numberOfModuleInstanceIds());
		assertEquals(184, archiveDatabaseSummary.numberOfTimeSeries());
	}

	@Test
	void getSummary2()throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters = new ArchiveDatabaseResultSearchParameters(
				"scalar",
				TimeSeriesType.SIMULATED_FORECASTING,
				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()),
				Set.of(new String[0]),
				Set.of(new TimeStep[0]),
				Set.of(new String[0]),
				Set.of(new String[0]));

		ArchiveDatabaseSummary archiveDatabaseSummary = mongoDbArchiveDatabaseTimeSeriesReader.getSummary(archiveDatabaseResultSearchParameters);
		assertEquals(1, archiveDatabaseSummary.numberOfParameters());
		assertEquals(8, archiveDatabaseSummary.numberOfModuleInstanceIds());
		assertEquals(16, archiveDatabaseSummary.numberOfTimeSeries());
	}

	@Test
	void getFilterOptions() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Database.updateTimeSeriesIndex();

		ArchiveDatabaseFilterOptions archiveDatabaseFilterOptions = mongoDbArchiveDatabaseTimeSeriesReader.getFilterOptions(
				"scalar",
				TimeSeriesType.EXTERNAL_HISTORICAL,
				Set.of());
		assertEquals(1, archiveDatabaseFilterOptions.getParameterIds().size());
		assertEquals(1, archiveDatabaseFilterOptions.getModuleInstanceIds().size());
		assertEquals(1, archiveDatabaseFilterOptions.getTimeSteps().size());
	}

	@Test
	void getFilterOptions2() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Database.updateTimeSeriesIndex();

		ArchiveDatabaseFilterOptions archiveDatabaseFilterOptions = mongoDbArchiveDatabaseTimeSeriesReader.getFilterOptions(
				"scalar",
				TimeSeriesType.SIMULATED_FORECASTING,
				Set.of());
		assertEquals(2, archiveDatabaseFilterOptions.getParameterIds().size());
		assertEquals(139, archiveDatabaseFilterOptions.getModuleInstanceIds().size());
		assertEquals(1, archiveDatabaseFilterOptions.getTimeSteps().size());
	}

	@Test
	void getSourceIds() throws Exception {
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Database.updateTimeSeriesIndex();

		Set<String> sourceIds = mongoDbArchiveDatabaseTimeSeriesReader.getSourceIds(TimeSeriesType.EXTERNAL_HISTORICAL);
		assertEquals(1, sourceIds.size());
	}

	@Test
	void getObservedDataImportRequest() throws Exception {
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Database.updateTimeSeriesIndex();

		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();
		timeSeriesHeader.setModuleInstanceId("moduleInstanceId");
		timeSeriesHeader.setLocationId("locationId");
		timeSeriesHeader.setParameterId("parameterId");
		timeSeriesHeader.setQualifierIds("qualifierId");
		timeSeriesHeader.setTimeStep(IrregularTimeStep.INSTANCE);
		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader);
		timeSeriesArray.put(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), 1);
		TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = new TimeSeriesArrays<>(timeSeriesArray);
		Period period = new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime());

		List<SingleExternalDataImportRequest> singleExternalDataImportRequests = mongoDbArchiveDatabaseTimeSeriesReader.getObservedDataImportRequest(period, timeSeriesArrays);
		assertEquals(0, singleExternalDataImportRequests.size());
	}

	@Test
	void getExternalForecastImportRequests() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("external_forecasts") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Database.updateTimeSeriesIndex();

		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();
		timeSeriesHeader.setModuleInstanceId("moduleInstanceId");
		timeSeriesHeader.setLocationId("locationId");
		timeSeriesHeader.setParameterId("parameterId");
		timeSeriesHeader.setQualifierIds("qualifierId");
		timeSeriesHeader.setForecastTime(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime());
		timeSeriesHeader.setTimeStep(IrregularTimeStep.INSTANCE);

//		List<SingleExternalDataImportRequest> singleExternalDataImportRequests = mongoDbArchiveDatabaseTimeSeriesReader.getExternalForecastImportRequests(new FewsTimeSeriesHeaders(timeSeriesHeader));
//		assertEquals(0, singleExternalDataImportRequests.size());
	}

	@Test
	void importSingleDataImportRequest() throws Exception {
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Database.updateTimeSeriesIndex();

		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();
		timeSeriesHeader.setModuleInstanceId("moduleInstanceId");
		timeSeriesHeader.setLocationId("locationId");
		timeSeriesHeader.setParameterId("parameterId");
		timeSeriesHeader.setQualifierIds("qualifierId");
		timeSeriesHeader.setTimeStep(IrregularTimeStep.INSTANCE);
		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader);
		timeSeriesArray.put(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), 1);
		TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = new TimeSeriesArrays<>(timeSeriesArray);
		Period period = new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime());

		List<SingleExternalDataImportRequest> singleExternalDataImportRequests = mongoDbArchiveDatabaseTimeSeriesReader.getObservedDataImportRequest(period, timeSeriesArrays);
		//TimeSeriesArrays<TimeSeriesHeader> results = mongoDbArchiveDatabaseTimeSeriesReader.importSingleDataImportRequest(singleExternalDataImportRequests.get(0));
		assertEquals(0, singleExternalDataImportRequests.size());
	}

	@Test
	void getTimeSeriesForTaskRun() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();
		mongoDbArchiveDatabaseTimeSeriesReader.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor> timeSeriesArrays = mongoDbArchiveDatabaseTimeSeriesReader.getTimeSeriesForTaskRun("SA11024320_1", TimeSeriesType.SIMULATED_FORECASTING);
		assertEquals(4309, timeSeriesArrays.getObject0().size());
	}

	@Test
	void getEnsembleMembers() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		Set<String> ensembleMembers = mongoDbArchiveDatabaseTimeSeriesReader.getEnsembleMembers("ALCT1", "ADIMF", Set.of("SACSMA_ALCT1_Forecast"), "GEFS.ENS", new String[]{}, TimeSeriesType.SIMULATED_FORECASTING);
		assertEquals(31, ensembleMembers.size());
	}

	@Test
	void getSimulatedTaskRunInfos() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		List<SimulatedTaskRunInfo> simulatedTaskRunInfos = mongoDbArchiveDatabaseTimeSeriesReader.getSimulatedTaskRunInfos("ALCT1", "ADIMF", "SACSMA_ALCT1_Forecast", "GEFS.ENS", new String[]{}, new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2022-01-01").getTime()),1000);
		assertEquals(1, simulatedTaskRunInfos.size());
	}

	@Test
	void searchForExternalForecastTimes() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getFirstMappedPort()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("external_forecasts") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		LongUnmodifiableList forecastTimes = mongoDbArchiveDatabaseTimeSeriesReader.searchForExternalForecastTimes("BARK2E", "MAP", "QPF_to_MAP", "", new String[]{"Extended", "HRRR"}, TimeSeriesType.EXTERNAL_FORECASTING, new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2022-01-01").getTime()),1000);
		assertEquals(1, forecastTimes.size());
	}
}
