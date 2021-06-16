package nl.fews.archivedatabase.mongodb.migrate;

import nl.fews.archivedatabase.mongodb.migrate.operations.Delete;
import nl.fews.archivedatabase.mongodb.migrate.operations.Insert;
import nl.fews.archivedatabase.mongodb.migrate.operations.Update;
import nl.fews.archivedatabase.mongodb.migrate.timeseries.ScalarExternalHistoricalBucket;
import nl.fews.archivedatabase.mongodb.migrate.timeseries.ScalarSimulatedHistoricalStitched;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseUnitConverter;
import nl.wldelft.util.Properties;

import java.io.File;
import java.util.Date;
import java.util.Map;

/**
 *
 */
public final class MongoDbMigrateDatabasePlugin implements MigrateDatabase {

	//DEFAULTS THAT MAY BE ADDED TO INTERFACE AND OPTIONALLY OVERRIDDEN LATER
	static{
		Settings.put("metaDataCollection", Database.Collection.MigrateMetaData.toString());
		Settings.put("logCollection", Database.Collection.MigrateLog.toString());
		Settings.put("folderMaxDepth", 4);
		Settings.put("metadataFileName", "metaData.xml");
	}

	/**
	 * Creates a new instance of this interface implementation
	 */
	public static MongoDbMigrateDatabasePlugin create() {
		return new MongoDbMigrateDatabasePlugin();
	}


	/**
	 * block direct instantiation; use static create() method
	 */
	private MongoDbMigrateDatabasePlugin(){}

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
	 * The key-value pair properties passed to this implementation
	 * @param properties properties
	 */
	@Override
	public void setProperties(Properties properties) {
		Settings.put("properties", properties);
	}

	/**
	 * The base url format string template for connecting to a mongo db instance
	 * @param archiveDatabaseUrl mongodb://%s:%s@mongo.infisys.net:27018/admin?tls=true => mongodb://username:password@[server|dns|ip]:port/authDB?connectionSettings
	 */
	@Override
	public void setArchiveDatabaseUrl(String archiveDatabaseUrl) {
		Settings.put("archiveDatabaseUrl", archiveDatabaseUrl);
	}

	/**
	 * The user / pass to use for mongo db connections
	 * @param archiveDatabaseUserName The password to apply to the archiveDatabaseUrl
	 * @param archiveDatabasePassword The username to apply to the archiveDatabaseUrl
	 */
	@Override
	public void setUserNamePassword(String archiveDatabaseUserName, String archiveDatabasePassword) {
		Settings.put("archiveDatabaseUserName", archiveDatabaseUserName);
		Settings.put("archiveDatabasePassword", archiveDatabasePassword);
	}

	/**
	 *
	 * @param dbThreads dbThreads
	 * @param fsThreads fsThreads
	 */
	@Override
	public void setNumThreads(int dbThreads, int fsThreads) {
		Settings.put("dbThreads", dbThreads);
		Settings.put("fsThreads", fsThreads);
	}

	/**
	 *
	 * @param archiveRootDataFolder archiveRootDataFolder
	 */
	@Override
	public void setArchiveRootDataFolder(String archiveRootDataFolder){
		Settings.put("archiveRootDataFolder", archiveRootDataFolder);
	}

	/**
	 *
	 */
	@Override
	public void migrateTimeSeries() {
		String connectionString = Settings.get("archiveDatabaseUserName")==null || Settings.get("archiveDatabaseUserName").equals("") || Settings.get("archiveDatabasePassword")==null || Settings.get("archiveDatabasePassword").equals("") || Settings.get("archiveDatabaseUrl", String.class).contains("@") ?
				Settings.get("archiveDatabaseUrl") :
				Settings.get("archiveDatabaseUrl", String.class).replace("mongodb://", String.format("mongodb://%s:%s@", Settings.get("archiveDatabaseUserName"), Settings.get("archiveDatabasePassword")));
		Settings.put("connectionString", connectionString);

		Delete.deleteUncommitted();

		Map<File, Date> existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs();
		Map<File, Date> existingMetaDataFilesDb = MetaDataUtil.getExistingMetaDataFilesDb();
		Insert.insertMetaDatas(existingMetaDataFilesFs, existingMetaDataFilesDb);
		Update.updateMetaDatas(existingMetaDataFilesFs, existingMetaDataFilesDb);
		Delete.deleteMetaDatas(existingMetaDataFilesFs, existingMetaDataFilesDb);

		Database.dropCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET));
		Database.ensureCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL_BUCKET));
		ScalarExternalHistoricalBucket.bucketGroups();

		Database.dropCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED));
		Database.ensureCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED));
		ScalarSimulatedHistoricalStitched.stitchGroups();
	}
}
