package nl.fews.archivedatabase.mongodb.migrate;

import nl.fews.archivedatabase.mongodb.migrate.interfaces.BucketHistorical;
import nl.fews.archivedatabase.mongodb.migrate.operations.*;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.LogUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public final class MongoDbOpenArchiveToArchiveDatabaseMigrator implements OpenArchiveToArchiveDatabaseMigrator {

	//DEFAULTS THAT MAY BE ADDED TO INTERFACE AND OPTIONALLY OVERRIDDEN LATER
	static{
		Settings.put("metaDataCollection", Database.Collection.MigrateMetaData.toString());
		Settings.put("logCollection", Database.Collection.MigrateLog.toString());
		Settings.put("bucketSizeCollection", Database.Collection.BucketSize.toString());
		Settings.put("metadataFileName", "metaData.xml");
		Settings.put("runInfoFileName", "runInfo.xml");
	}

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(MongoDbOpenArchiveToArchiveDatabaseMigrator.class);

	/**
	 *
	 */
	private static MongoDbOpenArchiveToArchiveDatabaseMigrator mongoDbOpenArchiveToArchiveDatabaseMigrator = null;

	/**
	 * Creates a new instance of this interface implementation
	 */
	public static MongoDbOpenArchiveToArchiveDatabaseMigrator create() {
		if(mongoDbOpenArchiveToArchiveDatabaseMigrator == null)
			mongoDbOpenArchiveToArchiveDatabaseMigrator = new MongoDbOpenArchiveToArchiveDatabaseMigrator();
		return mongoDbOpenArchiveToArchiveDatabaseMigrator;
	}

	/**
	 * block direct instantiation; use static create() method
	 */
	private MongoDbOpenArchiveToArchiveDatabaseMigrator(){}

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
		Settings.put("valueTypes", properties.indexOf("valueTypes") == -1 ? "scalar" : Arrays.stream(properties.getString("valueTypes", "scalar").split(",")).map(String::trim).collect(Collectors.toList()));
		Settings.put("useBulkInsert", properties.indexOf("useBulkInsert") != -1 && (properties.getObject(properties.indexOf("useBulkInsert")) instanceof Boolean ? properties.getBool("useBulkInsert", false) : Boolean.parseBoolean(properties.getString("useBulkInsert", "false"))));
		Settings.put("renameFinalizedCollection", properties.indexOf("renameFinalizedCollection") != -1 && (properties.getObject(properties.indexOf("renameFinalizedCollection")) instanceof Boolean ? properties.getBool("renameFinalizedCollection", true) : Boolean.parseBoolean(properties.getString("renameFinalizedCollection", "true"))));
	}

	/**
	 *
	 * @param configRevision configRevision
	 */
	@Override
	public void setConfigRevision(String configRevision) {
		Settings.put("configRevision", configRevision);
	}

	/**
	 *
	 * @param areaId areaId
	 * @param sourceId sourceId
	 */
	@Override
	public void migrate(String areaId, String sourceId) {
		try{
			logger.info("Settings: {}", Settings.toJsonString(1));
			logger.info("Start: deleteUncommitted");
			Delete.deleteUncommitted();
			logger.info("End: deleteUncommitted");

			logger.info("Start: getExistingMetaDataFilesFs");
			Map<File, Date> existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs(areaId);
			logger.info("End: getExistingMetaDataFilesFs");

			logger.info("Start: existingMetaDataFilesDb");
			Map<File, Date> existingMetaDataFilesDb = MetaDataUtil.getExistingMetaDataFilesDb();
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
		Database.replaceCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
	}

	/**
	 *
	 */
	public void bucketScalarExternalHistorical(){
		String singletonCollection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL);
		String bucketCollection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET);

		Database.dropCollection(bucketCollection);
		Database.ensureCollection(bucketCollection);
		BucketHistorical bucketHistorical = new BucketScalarExternalHistorical();
		bucketHistorical.bucketGroups(singletonCollection, bucketCollection);
	}

	/**
	 *
	 */
	public void bucketScalarSimulatedHistorical(){
		String singletonCollection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL);
		String bucketCollection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED);

		Database.dropCollection(bucketCollection);
		Database.ensureCollection(bucketCollection);
		BucketHistorical bucketHistorical = new BucketScalarSimulatedHistorical();
		bucketHistorical.bucketGroups(singletonCollection, bucketCollection);
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
				Database.updateTimeSeriesIndex();
				logger.info("End: updateTimeSeriesIndex");
			}
		}
	}
}
