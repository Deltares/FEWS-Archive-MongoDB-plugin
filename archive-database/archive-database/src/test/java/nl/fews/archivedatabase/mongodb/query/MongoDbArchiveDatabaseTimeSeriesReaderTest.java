package nl.fews.archivedatabase.mongodb.query;

import nl.fews.archivedatabase.mongodb.MongoDbArchiveDatabase;
import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.operations.Insert;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.ArchiveDatabaseForecastImportRequest;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.SimulatedTaskRunInfo;
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

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class MongoDbArchiveDatabaseTimeSeriesReaderTest {
	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
	}

	@Test
	void importExternalHistorical() throws Exception {
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Database.updateTimeSeriesIndex();

		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();
		timeSeriesHeader.setModuleInstanceId("moduleInstanceId");
		timeSeriesHeader.setLocationId("locationId");
		timeSeriesHeader.setParameterId("parameterId");
		timeSeriesHeader.setQualifierIds("qualifierId");
		timeSeriesHeader.setTimeStep(TimeStepUtils.decode("SETS60"));
		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader);
		timeSeriesArray.put(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), 1);
		TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = new TimeSeriesArrays<>(timeSeriesArray);
		Period period = new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime());

		TimeSeriesArrays<TimeSeriesHeader> timeSeriesArraysResult = mongoDbArchiveDatabaseTimeSeriesReader.importExternalHistorical(period, timeSeriesArrays);
		assertFalse(timeSeriesArrays.isEmpty());
		assertEquals(0, timeSeriesArraysResult.size());
	}

	@Test
	void importExternalForecasting() throws Exception {
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("external_forecasts") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Database.updateTimeSeriesIndex();

		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();
		timeSeriesHeader.setModuleInstanceId("moduleInstanceId");
		timeSeriesHeader.setLocationId("locationId");
		timeSeriesHeader.setParameterId("parameterId");
		timeSeriesHeader.setQualifierIds("qualifierId");
		timeSeriesHeader.setTimeStep(TimeStepUtils.decode("SETS60"));
		timeSeriesHeader.setEnsembleId("ensembleId");
		timeSeriesHeader.setEnsembleMemberId("ensembleMemberId");
		timeSeriesHeader.setForecastTime(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime());

//		List<TimeSeriesArrays<TimeSeriesHeader>> timeSeriesArrays = mongoDbArchiveDatabaseTimeSeriesReader.importExternalForecasting(Set.of(new ArchiveDatabaseForecastImportRequest(List.of(timeSeriesHeader), List.of(""))));
//		assertEquals(0, timeSeriesArrays.size());
	}

	@Test
	void getTimeSeriesForTaskRun() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor> timeSeriesArrays = mongoDbArchiveDatabaseTimeSeriesReader.getTimeSeriesForTaskRun("SA11024320_1", TimeSeriesType.SIMULATED_FORECASTING);
		assertEquals(4309, timeSeriesArrays.getObject0().size());
	}

	@Test
	void getEnsembleMembers() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		Set<String> ensembleMembers = mongoDbArchiveDatabaseTimeSeriesReader.getEnsembleMembers("ALCT1", "ADIMF", Set.of("SACSMA_ALCT1_Forecast"), "GEFS.ENS", new String[]{}, TimeSeriesType.SIMULATED_FORECASTING);
		assertEquals(31, ensembleMembers.size());
	}

	@Test
	void getModuleInstanceIds() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		Set<String> moduleInstanceIds = mongoDbArchiveDatabaseTimeSeriesReader.getModuleInstanceIds("ALCT1", "ADIMF", "GEFS.ENS", new String[]{}, TimeSeriesType.SIMULATED_FORECASTING);
		assertEquals(1, moduleInstanceIds.size());
	}

	@Test
	void getSimulatedTaskRunInfos() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		List<SimulatedTaskRunInfo> simulatedTaskRunInfos = mongoDbArchiveDatabaseTimeSeriesReader.getSimulatedTaskRunInfos("ALCT1", "ADIMF", "SACSMA_ALCT1_Forecast", "GEFS.ENS", new String[]{}, "DTOD_0_6_12_18TZ_CST", new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2022-01-01").getTime()),1000);
		assertEquals(1, simulatedTaskRunInfos.size());
	}

	@Test
	void searchForExternalForecastTimes() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("external_forecasts") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		LongUnmodifiableList forecastTimes = mongoDbArchiveDatabaseTimeSeriesReader.searchForExternalForecastTimes("BARK2E", "MAP", "QPF_to_MAP", "", new String[]{"Extended", "HRRR"}, TimeSeriesType.EXTERNAL_FORECASTING, new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2022-01-01").getTime()),1000);
		assertEquals(1, forecastTimes.size());
	}

	@Test
	void getAvailableYears() throws Exception {
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();
		timeSeriesHeader.setModuleInstanceId("Preprocess_AV");
		timeSeriesHeader.setLocationId("BOH-01");
		timeSeriesHeader.setParameterId("AV");
		timeSeriesHeader.setTimeStep(TimeStepUtils.decode("SETS60"));
		Period period = new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2020-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2020-12-01").getTime());

		Set<Integer> years = mongoDbArchiveDatabaseTimeSeriesReader.getAvailableYears(List.of(timeSeriesHeader), period);
		assertEquals(1, years.size());
	}
}
