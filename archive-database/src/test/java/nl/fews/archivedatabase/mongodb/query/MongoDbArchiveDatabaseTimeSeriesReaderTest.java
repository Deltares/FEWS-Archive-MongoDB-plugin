package nl.fews.archivedatabase.mongodb.query;

import nl.fews.archivedatabase.mongodb.MongoDbArchiveDatabase;
import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.migrate.operations.Insert;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.castor.archive.types.ArchiveTimeSeriesType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseFilterOptions;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseReadResult;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseResultSearchParameters;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseSummary;
import nl.wldelft.util.Period;
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

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("observed") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters = new ArchiveDatabaseResultSearchParameters(
				"scalar",
				ArchiveTimeSeriesType.OBSERVED,
				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()));
		archiveDatabaseResultSearchParameters.setParameterIds(Set.of());
		archiveDatabaseResultSearchParameters.setSourceIds(Set.of());
		archiveDatabaseResultSearchParameters.setModuleInstanceIds(Set.of());

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

		Map<File, Date> entries = MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.getKey().toString().contains("simulated") && s.getKey().toString().contains("scalar")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Insert.insertMetaDatas(entries, Map.of());

		ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters = new ArchiveDatabaseResultSearchParameters(
				"scalar",
				ArchiveTimeSeriesType.SIMULATED,
				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()));
		archiveDatabaseResultSearchParameters.setParameterIds(Set.of());
		archiveDatabaseResultSearchParameters.setSourceIds(Set.of());
		archiveDatabaseResultSearchParameters.setModuleInstanceIds(Set.of());

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
				ArchiveTimeSeriesType.OBSERVED,
				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()));
		archiveDatabaseResultSearchParameters.setParameterIds(Set.of());
		archiveDatabaseResultSearchParameters.setSourceIds(Set.of());
		archiveDatabaseResultSearchParameters.setModuleInstanceIds(Set.of());

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
				ArchiveTimeSeriesType.SIMULATED,
				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()));
		archiveDatabaseResultSearchParameters.setParameterIds(Set.of());
		archiveDatabaseResultSearchParameters.setSourceIds(Set.of());
		archiveDatabaseResultSearchParameters.setModuleInstanceIds(Set.of());

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

		ArchiveDatabaseFilterOptions archiveDatabaseFilterOptions = mongoDbArchiveDatabaseTimeSeriesReader.getFilterOptions(
				"scalar",
				ArchiveTimeSeriesType.OBSERVED,
				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()),
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

		ArchiveDatabaseFilterOptions archiveDatabaseFilterOptions = mongoDbArchiveDatabaseTimeSeriesReader.getFilterOptions(
				"scalar",
				ArchiveTimeSeriesType.SIMULATED,
				new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01").getTime()),
				Set.of());
		assertEquals(1, archiveDatabaseFilterOptions.getParameterIds().size());
		assertEquals(8, archiveDatabaseFilterOptions.getModuleInstanceIds().size());
		assertEquals(1, archiveDatabaseFilterOptions.getTimeSteps().size());
	}
}
