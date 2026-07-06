package nl.fews.archivedatabase.common;

import nl.fews.archivedatabase.common.export.ArchiveDatabaseTimeSeriesExporter;
import nl.fews.archivedatabase.common.migrate.OpenArchiveToArchiveDatabaseMigrator;
import nl.fews.archivedatabase.common.query.ArchiveDatabaseGuiDataReader;
import nl.fews.archivedatabase.common.query.ArchiveDatabaseTimeSeriesReader;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.interfaces.FewsTimeSeriesHeaderProvider;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ArchiveDatabase implements nl.fews.archivedatabase.interfaces.ArchiveDatabase<FewsTimeSeriesHeader> {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(ArchiveDatabase.class);

	/**
	 *
	 */
	private static ArchiveDatabase archiveDatabase = null;

	/**
	 *
	 */
	private static ArchiveDatabaseTimeSeriesExporter archiveDatabaseTimeSeriesExporter = null;

	/**
	 *
	 */
	private static OpenArchiveToArchiveDatabaseMigrator openArchiveToArchiveDatabaseMigrator = null;

	/**
	 *
	 */
	private static ArchiveDatabaseTimeSeriesReader archiveDatabaseTimeSeriesReader = null;

	/**
	 *
	 */
	private static ArchiveDatabaseGuiDataReader archiveDatabaseGuiDataReader = null;

	/**
	 *
	 */
	private static boolean connectionLogged = false;

	/**
	 *
	 */
	protected ArchiveDatabase(){}

	static{
		logger.info("{} Version: {}", ArchiveDatabase.class.getSimpleName(), ArchiveDatabase.class.getPackage().getImplementationVersion());
	}

	/**
	 *
	 * @return ArchiveDatabase
	 */
	public static ArchiveDatabase create(){
		if(archiveDatabase == null)
			archiveDatabase = new ArchiveDatabase();
		return archiveDatabase;
	}

	/**
	 *
	 * @return ArchiveDatabaseTimeSeriesExporter
	 */
	@Override
	public ArchiveDatabaseTimeSeriesExporter getArchiveTimeSeriesExporter() {
		var connectionString = Settings.get("archiveDatabaseUserName")==null || Settings.get("archiveDatabaseUserName").equals("") || Settings.get("archiveDatabasePassword")==null || Settings.get("archiveDatabasePassword").equals("") || Settings.get("archiveDatabaseConnectionString", String.class).contains("@") ?
			Settings.get("archiveDatabaseConnectionString") :
			String.format(Settings.get("archiveDatabaseConnectionString", String.class), Settings.get("archiveDatabaseUserName"), Settings.get("archiveDatabasePassword"));
		Settings.put("connectionString", connectionString);

		if(archiveDatabaseTimeSeriesExporter == null)
			archiveDatabaseTimeSeriesExporter = ArchiveDatabaseTimeSeriesExporter.create();
		return archiveDatabaseTimeSeriesExporter;
	}

	/**
	 *
	 * @return OpenArchiveToArchiveDatabaseMigrator
	 */
	@Override
	public OpenArchiveToArchiveDatabaseMigrator getOpenArchiveToArchiveDatabaseMigrator() {
		var connectionString = Settings.get("archiveDatabaseUserName")==null || Settings.get("archiveDatabaseUserName").equals("") || Settings.get("archiveDatabasePassword")==null || Settings.get("archiveDatabasePassword").equals("") || Settings.get("archiveDatabaseConnectionString", String.class).contains("@") ?
			Settings.get("archiveDatabaseConnectionString") :
			String.format(Settings.get("archiveDatabaseConnectionString", String.class), Settings.get("archiveDatabaseUserName"), Settings.get("archiveDatabasePassword"));
		Settings.put("connectionString", connectionString);

		if(openArchiveToArchiveDatabaseMigrator == null)
			openArchiveToArchiveDatabaseMigrator = OpenArchiveToArchiveDatabaseMigrator.create();
		return openArchiveToArchiveDatabaseMigrator;
	}

	/**
	 * 
	 */
	@Override
	public ArchiveDatabaseTimeSeriesReader getArchiveDataBaseTimeSeriesReader() {
		var connectionString =
				Settings.get("archiveDatabaseUserName") == null || Settings.get("archiveDatabaseUserName").equals("") ||
				Settings.get("archiveDatabasePassword") == null || Settings.get("archiveDatabasePassword").equals("") ?
			Settings.get("archiveDatabaseConnectionString") :
			String.format(Settings.get("archiveDatabaseConnectionString"), Settings.get("archiveDatabaseUserName"), Settings.get("archiveDatabasePassword"));
		Settings.put("connectionString", connectionString);

		if(archiveDatabaseTimeSeriesReader == null)
			archiveDatabaseTimeSeriesReader = ArchiveDatabaseTimeSeriesReader.create();
		return archiveDatabaseTimeSeriesReader;
	}

	@Override
	public ArchiveDatabaseGuiDataReader getArchiveDatabaseGuiDataReader() {
		String connectionString = Settings.get("archiveDatabaseUserName")==null || Settings.get("archiveDatabaseUserName").equals("") || Settings.get("archiveDatabasePassword")==null || Settings.get("archiveDatabasePassword").equals("") || Settings.get("archiveDatabaseConnectionString", String.class).contains("@") ?
			Settings.get("archiveDatabaseConnectionString") :
			String.format(Settings.get("archiveDatabaseConnectionString", String.class), Settings.get("archiveDatabaseUserName"), Settings.get("archiveDatabasePassword"));
		Settings.put("connectionString", connectionString);

		if(archiveDatabaseGuiDataReader == null)
			archiveDatabaseGuiDataReader = ArchiveDatabaseGuiDataReader.create();
		return archiveDatabaseGuiDataReader;
	}

	/**
	 * The base url format string template for connecting to a db instance
	 * @param archiveDatabaseUrl archiveDatabaseUrl
	 */
	@Override
	public void setArchiveDatabaseUrl(String archiveDatabaseUrl) {
		Settings.put("archiveDatabaseUrl", archiveDatabaseUrl);
		if(!connectionLogged){
			logger.info("{} Version: {} Database: {}", ArchiveDatabase.class.getSimpleName(), ArchiveDatabase.class.getPackage().getImplementationVersion(), archiveDatabaseUrl);
			connectionLogged = true;
		}
	}

	/**
	 * The user / pass to use for db connections
	 * @param archiveDatabaseUserName The password to apply to the archiveDatabaseConnectionString
	 * @param archiveDatabasePassword The username to apply to the archiveDatabaseConnectionString
	 */
	@Override
	public void setUserNamePassword(String archiveDatabaseUserName, String archiveDatabasePassword) {
		Settings.put("archiveDatabaseUserName", archiveDatabaseUserName);
		Settings.put("archiveDatabasePassword", archiveDatabasePassword);
	}

	/**
	 *
	 * @param  fewsTimeSeriesHeaderProvider fewsTimeSeriesHeaderProvider
	 */
	@Override
	public void setHeaderProvider(FewsTimeSeriesHeaderProvider fewsTimeSeriesHeaderProvider) {
		Settings.put("headerProvider", fewsTimeSeriesHeaderProvider);
	}
}
