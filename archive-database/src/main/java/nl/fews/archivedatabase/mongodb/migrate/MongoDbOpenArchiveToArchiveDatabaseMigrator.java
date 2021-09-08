package nl.fews.archivedatabase.mongodb.migrate;

import nl.fews.archivedatabase.mongodb.migrate.interfaces.BucketHistorical;
import nl.fews.archivedatabase.mongodb.migrate.operations.*;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Map;

/**
 *
 */
public final class MongoDbOpenArchiveToArchiveDatabaseMigrator implements OpenArchiveToArchiveDatabaseMigrator {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(BucketHistoricalBase.class);

	//DEFAULTS THAT MAY BE ADDED TO INTERFACE AND OPTIONALLY OVERRIDDEN LATER
	static{
		Settings.put("metaDataCollection", Database.Collection.MigrateMetaData.toString());
		Settings.put("logCollection", Database.Collection.MigrateLog.toString());
		Settings.put("bucketSizeCollection", Database.Collection.BucketSize.toString());
		Settings.put("folderMaxDepth", 4);
		Settings.put("metadataFileName", "metaData.xml");
		Settings.put("runInfoFileName", "runInfo.xml");
	}

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
		logger.info("setUnitConverter");
		Settings.put("archiveDatabaseUnitConverter", archiveDatabaseUnitConverter);
	}

	/**
	 *
	 * @param  archiveDatabaseTimeConverter archiveDatabaseTimeConverter
	 */
	@Override
	public void setTimeConverter(ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter) {
		logger.info("setTimeConverter");
		Settings.put("archiveDatabaseTimeConverter", archiveDatabaseTimeConverter);
	}

	/**
	 *
	 * @param archiveDatabaseRegionConfigInfoProvider archiveDatabaseRegionConfigInfoProvider
	 */
	@Override
	public void setRegionConfigInfoProvider(ArchiveDatabaseRegionConfigInfoProvider archiveDatabaseRegionConfigInfoProvider) {
		logger.info("setRegionConfigInfoProvider");
		Settings.put("archiveDatabaseRegionConfigInfoProvider", archiveDatabaseRegionConfigInfoProvider);
	}

	/**
	 * The key-value pair properties passed to this implementation
	 * @param properties properties
	 */
	@Override
	public void setProperties(Properties properties) {
		logger.info("setProperties");
		Settings.put("properties", properties);
	}

	/**
	 * The base url format string template for connecting to a mongo db instance
	 * @param archiveDatabaseUrl mongodb://%s:%s@mongo.infisys.net:27018/admin?tls=true => mongodb://username:password@[server|dns|ip]:port/authDB?connectionSettings
	 */
	@Override
	public void setArchiveDatabaseUrl(String archiveDatabaseUrl) {
		logger.info("setArchiveDatabaseUrl");
		Settings.put("archiveDatabaseUrl", archiveDatabaseUrl);
	}

	/**
	 * The user / pass to use for mongo db connections
	 * @param archiveDatabaseUserName The password to apply to the archiveDatabaseUrl
	 * @param archiveDatabasePassword The username to apply to the archiveDatabaseUrl
	 */
	@Override
	public void setUserNamePassword(String archiveDatabaseUserName, String archiveDatabasePassword) {
		logger.info("setUserNamePassword");
		Settings.put("archiveDatabaseUserName", archiveDatabaseUserName);
		Settings.put("archiveDatabasePassword", archiveDatabasePassword);
	}

	/**
	 *
	 * @param configRevision configRevision
	 */
	@Override
	public void setConfigRevision(String configRevision) {
		logger.info("setConfigRevision");
		Settings.put("configRevision", configRevision);
	}

	/**
	 *
	 * @param areaId areaId
	 * @param sourceId sourceId
	 */
	@Override
	public void migrate(String areaId, String sourceId) {
		logger.info("migrate");
		String connectionString = Settings.get("archiveDatabaseUserName")==null || Settings.get("archiveDatabaseUserName").equals("") || Settings.get("archiveDatabasePassword")==null || Settings.get("archiveDatabasePassword").equals("") || Settings.get("archiveDatabaseUrl", String.class).contains("@") ?
				Settings.get("archiveDatabaseUrl") :
				Settings.get("archiveDatabaseUrl", String.class).replace("mongodb://", String.format("mongodb://%s:%s@", Settings.get("archiveDatabaseUserName"), Settings.get("archiveDatabasePassword")));
		Settings.put("connectionString", connectionString);

		Delete.deleteUncommitted();

		Map<File, Date> existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs(areaId);
		Map<File, Date> existingMetaDataFilesDb = MetaDataUtil.getExistingMetaDataFilesDb(areaId);
		Insert.insertMetaDatas(existingMetaDataFilesFs, existingMetaDataFilesDb);
		Update.updateMetaDatas(existingMetaDataFilesFs, existingMetaDataFilesDb);
		Delete.deleteMetaDatas(existingMetaDataFilesFs, existingMetaDataFilesDb);
	}

	/**
	 *
	 */
	public void replaceScalarExternalHistoricalWithBucketedCollection(){
		logger.info("replaceScalarExternalHistoricalWithBucketedCollection");
		Database.replaceCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET), TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL));
	}

	/**
	 *
	 */
	public void bucketScalarExternalHistorical(){
		logger.info("bucketScalarExternalHistorical");
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
		logger.info("bucketScalarSimulatedHistorical");
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
		logger.info("setOpenArchiveToDatabaseSettings");
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
		logger.info("finalizeMigration");
		if(finalize) {
			String connectionString = Settings.get("archiveDatabaseUserName")==null || Settings.get("archiveDatabaseUserName").equals("") || Settings.get("archiveDatabasePassword")==null || Settings.get("archiveDatabasePassword").equals("") || Settings.get("archiveDatabaseUrl", String.class).contains("@") ?
					Settings.get("archiveDatabaseUrl") :
					Settings.get("archiveDatabaseUrl", String.class).replace("mongodb://", String.format("mongodb://%s:%s@", Settings.get("archiveDatabaseUserName"), Settings.get("archiveDatabasePassword")));
			Settings.put("connectionString", connectionString);

			bucketScalarExternalHistorical();
			bucketScalarSimulatedHistorical();
			replaceScalarExternalHistoricalWithBucketedCollection();
		}
	}
}
