package nl.fews.archivedatabase.mongodb;

import nl.fews.archivedatabase.mongodb.export.MongoDbArchiveDatabaseTimeSeriesExporter;
import nl.fews.archivedatabase.mongodb.migrate.MongoDbOpenArchiveToArchiveDatabaseMigrator;
import nl.fews.archivedatabase.mongodb.query.MongoDbArchiveDatabaseTimeSeriesReader;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabase;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeSeriesExporter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.OpenArchiveToArchiveDatabaseMigrator;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeSeriesReader;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MongoDbArchiveDatabase implements ArchiveDatabase<TimeSeriesHeader> {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(MongoDbArchiveDatabase.class);

	/**
	 *
	 */
	private static MongoDbArchiveDatabase mongoDbArchiveDatabase = null;

	/**
	 *
	 */
	private static MongoDbArchiveDatabaseTimeSeriesExporter mongoDbArchiveDatabaseTimeSeriesExporter = null;

	/**
	 *
	 */
	private static MongoDbOpenArchiveToArchiveDatabaseMigrator mongoDbOpenArchiveToArchiveDatabaseMigrator = null;

	/**
	 *
	 */
	private static MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = null;

	/**
	 *
	 */
	private static boolean connectionLogged = false;

	/**
	 *
	 */
	private MongoDbArchiveDatabase(){}

	static{
		logger.info("{} Version: {}", MongoDbArchiveDatabase.class.getSimpleName(), MongoDbArchiveDatabase.class.getPackage().getImplementationVersion());
	}

	/**
	 *
	 * @return MongoDbArchiveDatabase
	 */
	public static MongoDbArchiveDatabase create(){
		if(mongoDbArchiveDatabase == null)
			mongoDbArchiveDatabase = new MongoDbArchiveDatabase();
		return mongoDbArchiveDatabase;
	}

	/**
	 *
	 * @return MongoDbArchiveDatabaseTimeSeriesExporter
	 */
	@Override
	public ArchiveDatabaseTimeSeriesExporter<TimeSeriesHeader> getArchiveTimeSeriesExporter() {
		String connectionString = Settings.get("archiveDatabaseUserName")==null || Settings.get("archiveDatabaseUserName").equals("") || Settings.get("archiveDatabasePassword")==null || Settings.get("archiveDatabasePassword").equals("") || Settings.get("archiveDatabaseUrl", String.class).contains("@") ?
				Settings.get("archiveDatabaseUrl") :
				Settings.get("archiveDatabaseUrl", String.class).replace("mongodb://", String.format("mongodb://%s:%s@", Settings.get("archiveDatabaseUserName"), Settings.get("archiveDatabasePassword")));
		Settings.put("connectionString", connectionString);

		if(mongoDbArchiveDatabaseTimeSeriesExporter == null)
			mongoDbArchiveDatabaseTimeSeriesExporter = MongoDbArchiveDatabaseTimeSeriesExporter.create();
		return mongoDbArchiveDatabaseTimeSeriesExporter;
	}

	/**
	 *
	 * @return MongoDbOpenArchiveToArchiveDatabaseMigrator
	 */
	@Override
	public OpenArchiveToArchiveDatabaseMigrator getOpenArchiveToArchiveDatabaseMigrator() {
		String connectionString = Settings.get("archiveDatabaseUserName")==null || Settings.get("archiveDatabaseUserName").equals("") || Settings.get("archiveDatabasePassword")==null || Settings.get("archiveDatabasePassword").equals("") || Settings.get("archiveDatabaseUrl", String.class).contains("@") ?
				Settings.get("archiveDatabaseUrl") :
				Settings.get("archiveDatabaseUrl", String.class).replace("mongodb://", String.format("mongodb://%s:%s@", Settings.get("archiveDatabaseUserName"), Settings.get("archiveDatabasePassword")));
		Settings.put("connectionString", connectionString);

		if(mongoDbOpenArchiveToArchiveDatabaseMigrator == null)
			mongoDbOpenArchiveToArchiveDatabaseMigrator = MongoDbOpenArchiveToArchiveDatabaseMigrator.create();
		return mongoDbOpenArchiveToArchiveDatabaseMigrator;
	}

	/**
	 * 
	 */
	@Override
	public ArchiveDatabaseTimeSeriesReader getArchiveDataBaseTimeSeriesReader() {
		String connectionString = Settings.get("archiveDatabaseUserName")==null || Settings.get("archiveDatabaseUserName").equals("") || Settings.get("archiveDatabasePassword")==null || Settings.get("archiveDatabasePassword").equals("") || Settings.get("archiveDatabaseUrl", String.class).contains("@") ?
				Settings.get("archiveDatabaseUrl") :
				Settings.get("archiveDatabaseUrl", String.class).replace("mongodb://", String.format("mongodb://%s:%s@", Settings.get("archiveDatabaseUserName"), Settings.get("archiveDatabasePassword")));
		Settings.put("connectionString", connectionString);

		if(mongoDbArchiveDatabaseTimeSeriesReader == null)
			mongoDbArchiveDatabaseTimeSeriesReader = MongoDbArchiveDatabaseTimeSeriesReader.create();
		return mongoDbArchiveDatabaseTimeSeriesReader;
	}

	/**
	 * The base url format string template for connecting to a mongo db instance
	 * @param archiveDatabaseUrl mongodb://%s:%s@mongo.infisys.net:27018/admin?tls=true => mongodb://username:password@[server|dns|ip]:port/authDB?connectionSettings
	 */
	@Override
	public void setArchiveDatabaseUrl(String archiveDatabaseUrl) {
		Settings.put("archiveDatabaseUrl", archiveDatabaseUrl);
		if(!connectionLogged){
			logger.info("{} Version: {} Database: {}", MongoDbArchiveDatabase.class.getSimpleName(), MongoDbArchiveDatabase.class.getPackage().getImplementationVersion(), archiveDatabaseUrl);
			connectionLogged = true;
		}
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
}
