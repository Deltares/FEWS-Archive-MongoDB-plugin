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
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
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
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Testcontainers
class MongoDbArchiveDatabaseGuiDataReaderTest {
	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:6.0"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
	}

	@Test
	void read() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseGuiDataReader mongoDbArchiveDatabaseGuiDataReader = (MongoDbArchiveDatabaseGuiDataReader)mongoDbArchiveDatabase.getArchiveDatabaseGuiDataReader();
		mongoDbArchiveDatabaseGuiDataReader.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());

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

		ArchiveDatabaseReadResult archiveDatabaseReadResult = mongoDbArchiveDatabaseGuiDataReader.read(archiveDatabaseResultSearchParameters);
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
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseGuiDataReader mongoDbArchiveDatabaseGuiDataReader = (MongoDbArchiveDatabaseGuiDataReader)mongoDbArchiveDatabase.getArchiveDatabaseGuiDataReader();
		mongoDbArchiveDatabaseGuiDataReader.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());

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

		ArchiveDatabaseReadResult archiveDatabaseReadResult = mongoDbArchiveDatabaseGuiDataReader.read(archiveDatabaseResultSearchParameters);
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
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseGuiDataReader mongoDbArchiveDatabaseGuiDataReader = (MongoDbArchiveDatabaseGuiDataReader)mongoDbArchiveDatabase.getArchiveDatabaseGuiDataReader();

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

		ArchiveDatabaseSummary archiveDatabaseSummary = mongoDbArchiveDatabaseGuiDataReader.getSummary(archiveDatabaseResultSearchParameters);
		assertEquals(1, archiveDatabaseSummary.numberOfParameters());
		assertEquals(1, archiveDatabaseSummary.numberOfModuleInstanceIds());
		assertEquals(184, archiveDatabaseSummary.numberOfTimeSeries());
	}

	@Test
	void getSummary2()throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseGuiDataReader mongoDbArchiveDatabaseGuiDataReader = (MongoDbArchiveDatabaseGuiDataReader)mongoDbArchiveDatabase.getArchiveDatabaseGuiDataReader();

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

		ArchiveDatabaseSummary archiveDatabaseSummary = mongoDbArchiveDatabaseGuiDataReader.getSummary(archiveDatabaseResultSearchParameters);
		assertEquals(1, archiveDatabaseSummary.numberOfParameters());
		assertEquals(8, archiveDatabaseSummary.numberOfModuleInstanceIds());
		assertEquals(16, archiveDatabaseSummary.numberOfTimeSeries());
	}

	@Test
	void getFilterOptions() throws Exception{
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseGuiDataReader mongoDbArchiveDatabaseGuiDataReader = (MongoDbArchiveDatabaseGuiDataReader)mongoDbArchiveDatabase.getArchiveDatabaseGuiDataReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Database.updateTimeSeriesIndex();

		ArchiveDatabaseFilterOptions archiveDatabaseFilterOptions = mongoDbArchiveDatabaseGuiDataReader.getFilterOptions(
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
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseGuiDataReader mongoDbArchiveDatabaseGuiDataReader = (MongoDbArchiveDatabaseGuiDataReader)mongoDbArchiveDatabase.getArchiveDatabaseGuiDataReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Database.updateTimeSeriesIndex();

		ArchiveDatabaseFilterOptions archiveDatabaseFilterOptions = mongoDbArchiveDatabaseGuiDataReader.getFilterOptions(
				"scalar",
				TimeSeriesType.SIMULATED_FORECASTING,
				Set.of());
		assertEquals(2, archiveDatabaseFilterOptions.getParameterIds().size());
		//assertEquals(139, archiveDatabaseFilterOptions.getModuleInstanceIds().size());
		assertEquals(1, archiveDatabaseFilterOptions.getTimeSteps().size());
	}

	@Test
	void getSourceIds() throws Exception {
		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
		mongoDbArchiveDatabase.setArchiveDatabaseUrl(String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));

		MongoDbArchiveDatabaseGuiDataReader mongoDbArchiveDatabaseGuiDataReader = (MongoDbArchiveDatabaseGuiDataReader)mongoDbArchiveDatabase.getArchiveDatabaseGuiDataReader();

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());
		Database.updateTimeSeriesIndex();

		Set<String> sourceIds = mongoDbArchiveDatabaseGuiDataReader.getSourceIds(TimeSeriesType.EXTERNAL_HISTORICAL);
		assertEquals(1, sourceIds.size());
	}
}
