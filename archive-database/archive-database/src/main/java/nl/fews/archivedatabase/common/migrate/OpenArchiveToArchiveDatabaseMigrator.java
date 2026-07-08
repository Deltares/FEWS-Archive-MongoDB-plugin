package nl.fews.archivedatabase.common.migrate;

import nl.fews.archivedatabase.common.migrate.operations.*;
import nl.fews.archivedatabase.common.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.common.shared.database.Table;
import nl.fews.archivedatabase.common.shared.database.Database;
import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesTypeUtil;
import nl.fews.archivedatabase.mongodb.logging.DbAppender;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.util.LogUtils;
import nl.wldelft.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;

import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.*;

/**
 *
 */
public final class OpenArchiveToArchiveDatabaseMigrator implements nl.wldelft.fews.system.data.externaldatasource.archivedatabase.OpenArchiveToArchiveDatabaseMigrator, AutoCloseable {

	//DEFAULTS THAT MAY BE ADDED TO INTERFACE AND OPTIONALLY OVERRIDDEN LATER
	static{
		Settings.put("metadataFileName", "metaData.xml");
		Settings.put("runInfoFileName", "runInfo.xml");
	}

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(OpenArchiveToArchiveDatabaseMigrator.class);

	/**
	 *
	 */
	private static OpenArchiveToArchiveDatabaseMigrator openArchiveToArchiveDatabaseMigrator = null;

	/**
	 *
	 */
	private static final Object mutex = new Object();

	/**
	 *
	 */
	private static final String DATABASE_LOGGING_NAMESPACE = "nl.fews.archivedatabase.%s.shared.logging.DbAppender";

	/**
	 * Creates a new instance of this interface implementation
	 */
	public static OpenArchiveToArchiveDatabaseMigrator create() {
		if(openArchiveToArchiveDatabaseMigrator == null)
			openArchiveToArchiveDatabaseMigrator = new OpenArchiveToArchiveDatabaseMigrator();
		return openArchiveToArchiveDatabaseMigrator;
	}

	public void close() {
		synchronized (mutex){
			OpenArchiveToArchiveDatabaseMigrator.openArchiveToArchiveDatabaseMigrator = null;
		}
	}

	/**
	 * block direct instantiation; use static create() method
	 */
	private OpenArchiveToArchiveDatabaseMigrator(){
		try{
			var appenderClass = Class.forName(String.format(DATABASE_LOGGING_NAMESPACE, Settings.get("databaseType", String.class).toLowerCase()));
			var method = appenderClass.getMethod("createAppender", String.class, String.class, Filter.class);
			var appender = (Appender) method.invoke(null, "migrateLogAppender", Settings.get("connectionString", String.class), null);
			LogUtils.addAppender(appender);
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param archiveDatabaseUnitConverter archiveDatabaseUnitConverter
	 */
	@Override
	public void setUnitConverter(ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter) {
		Settings.put("archiveDatabaseUnitConverter", archiveDatabaseUnitConverter);
	}

	/**
	 *
	 * @param  archiveDatabaseTimeConverter archiveDatabaseTimeConverter
	 */
	@Override
	public void setTimeConverter(ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter) {
		Settings.put("archiveDatabaseTimeConverter", archiveDatabaseTimeConverter);
	}

	/**
	 *
	 * @param archiveDatabaseRegionConfigInfoProvider archiveDatabaseRegionConfigInfoProvider
	 */
	@Override
	public void setRegionConfigInfoProvider(ArchiveDatabaseRegionConfigInfoProvider archiveDatabaseRegionConfigInfoProvider) {
		Settings.put("archiveDatabaseRegionConfigInfoProvider", archiveDatabaseRegionConfigInfoProvider);
	}

	/**
	 * The key-value pair properties passed to this implementation
	 * @param properties properties
	 */
	@Override
	public void setProperties(Properties properties) {
		Settings.put("folderMaxDepth", properties.indexOf("folderMaxDepth") == -1 ? 4 : properties.getObject(properties.indexOf("folderMaxDepth")) instanceof Integer ? properties.getInt("folderMaxDepth", 4) : Integer.parseInt(properties.getString("folderMaxDepth", "4")));
		Settings.put("valueTypes", properties.indexOf("valueTypes") == -1 ? "scalar" : Arrays.stream(properties.getString("valueTypes", "scalar").split(",")).map(String::trim).toList());
		Settings.put("useBulkInsert", properties.indexOf("useBulkInsert") != -1 && (properties.getObject(properties.indexOf("useBulkInsert")) instanceof Boolean ? properties.getBool("useBulkInsert", false) : Boolean.parseBoolean(properties.getString("useBulkInsert", "false"))));
		Settings.put("renameFinalizedCollection", properties.indexOf("renameFinalizedCollection") != -1 && (properties.getObject(properties.indexOf("renameFinalizedCollection")) instanceof Boolean ? properties.getBool("renameFinalizedCollection", true) : Boolean.parseBoolean(properties.getString("renameFinalizedCollection", "true"))));
	}

	/**
	 *
	 * @param configRevision configRevision
	 */
	@Override
	public void setConfigRevision(String configRevision) {
		Settings.put("configRevision", configRevision == null ? "" : configRevision);
	}

	/**
	 *
	 * @param areaId areaId
	 * @param sourceId sourceId
	 */
	@Override
	public void migrate(String[] areaId, String sourceId) {

		try{
			logger.info("Settings: {}", Settings.toJsonString(1));
			logger.info("Start: deleteUncommitted");
			Delete.deleteUncommitted();
			logger.info("End: deleteUncommitted");

			logger.info("Start: getExistingMetaDataFilesFs");
			var existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs(areaId == null ? null : Arrays.asList(areaId));
			if(existingMetaDataFilesFs.isEmpty()){
				throw new FileNotFoundException(String.format("No meta data files found @[Settings.baseDirectoryArchive]: %s", Paths.get(Settings.get("baseDirectoryArchive", String.class))));
			}
			logger.info("End: getExistingMetaDataFilesFs");

			logger.info("Start: existingMetaDataFilesDb");
			var existingMetaDataFilesDb = MetaDataUtil.getExistingMetaDataFilesDb();
			logger.info("End: existingMetaDataFilesDb");

			logger.info("Start: insertMetaDatas");
			Insert.insertMetaDatas(existingMetaDataFilesFs, existingMetaDataFilesDb);
			logger.info("End: insertMetaDatas");

			logger.info("Start: updateMetaDatas");
			Update.updateMetaDatas(existingMetaDataFilesFs, existingMetaDataFilesDb);
			logger.info("End: updateMetaDatas");

			logger.info("Start: deleteMetaDatas");
			Delete.deleteMetaDatas(existingMetaDataFilesFs, existingMetaDataFilesDb);
			logger.info("End: deleteMetaDatas");
		}
		catch (Exception ex){
			logger.error("migrate error", ex);
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 */
	public void replaceScalarExternalHistoricalWithBucketedCollection(){
		Database.instance().replace(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
		Database.instance().updateMany(Table.BucketSize.toString(), Map.of("bucketCollection", "ExternalHistoricalScalarTimeSeriesBucket"), Map.of("set", Map.of("bucketCollection", "ExternalHistoricalScalarTimeSeries")));
	}

	/**
	 *
	 */
	public void bucketScalarExternalHistorical(){
		var singletonCollection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL);
		var bucketCollection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET);

		Database.instance().drop(bucketCollection);
		Database.instance().ensureTable(bucketCollection);
		var bucketScalarExternalHistorical = new BucketScalarExternalHistorical();
		bucketScalarExternalHistorical.bucketGroups(singletonCollection, bucketCollection);
	}

	/**
	 *
	 */
	public void bucketScalarSimulatedHistorical(){
		var singletonCollection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL);
		var bucketCollection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED);

		Database.instance().drop(bucketCollection);
		Database.instance().ensureTable(bucketCollection);
		var bucketScalarSimulatedHistorical = new BucketScalarSimulatedHistorical();
		bucketScalarSimulatedHistorical.bucketGroups(singletonCollection, bucketCollection);
	}

	/**
	 *
	 * @param openArchiveToArchiveDatabaseMigrationSettings openArchiveToArchiveDatabaseMigrationSettings
	 */
	@Override
	public void setOpenArchiveToDatabaseSettings(OpenArchiveToArchiveDatabaseMigrationSettings openArchiveToArchiveDatabaseMigrationSettings) {
		Settings.put("baseDirectoryArchive", Paths.get(openArchiveToArchiveDatabaseMigrationSettings.getBaseDirectoryArchive()).toString());
		Settings.put("databaseBaseThreads", openArchiveToArchiveDatabaseMigrationSettings.getDatabaseBaseThreads());
		Settings.put("netcdfReadThreads", openArchiveToArchiveDatabaseMigrationSettings.getNetcdfReadThreads());
	}

	/**
	 *
	 * @param finalize finalize
	 */
	@Override
	public void finalizeMigration(boolean finalize) {
		if(finalize) {

			logger.info("Start: bucketScalarExternalHistorical");
			bucketScalarExternalHistorical();
			logger.info("End: bucketScalarExternalHistorical");

			logger.info("Start: bucketScalarSimulatedHistorical");
			bucketScalarSimulatedHistorical();
			logger.info("End: bucketScalarSimulatedHistorical");

			if (Settings.get("renameFinalizedCollection")) {

				logger.info("Start: replaceScalarExternalHistoricalWithBucketedCollection");
				replaceScalarExternalHistoricalWithBucketedCollection();
				logger.info("End: replaceScalarExternalHistoricalWithBucketedCollection");

				logger.info("Start: updateTimeSeriesIndex");
				updateTimeSeriesIndex();
				logger.info("End: updateTimeSeriesIndex");
			}
		}
	}

	/**
	 *
	 */
	public void updateTimeSeriesIndex(){
		Database.instance().ensureTable(Table.TimeSeriesIndex.toString());
		List.of(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, TimeSeriesType.SCALAR_SIMULATED_FORECASTING, TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED).forEach(timeSeriesType -> {
			var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
			var results = new ArrayList<Map<String, Object>>();
			Database.instance().aggregateTimeSeriesIndex(collection,
				Map.of("moduleInstanceId", 1, "parameterId", 1, "encodedTimeStepId", 1, "metaData.areaId", 1, "metaData.sourceId", 1),
				Map.of("moduleInstanceId", "moduleInstanceId", "parameterId", "parameterId", "encodedTimeStepId", "encodedTimeStepId", "areaId", "metaData.areaId", "sourceId", "metaData.sourceId"),
				Map.of("collection", collection)
			).forEach(results::add);
			Database.instance().deleteMany(Table.TimeSeriesIndex.toString(), Map.of("collection", collection));
			if(!results.isEmpty())
				Database.instance().insertMany(Table.TimeSeriesIndex.toString(), results);
		});
	}
}
