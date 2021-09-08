package nl.fews.archivedatabase.mongodb;

import nl.fews.archivedatabase.mongodb.export.MongoDbArchiveDatabaseTimeSeriesExporter;
import nl.fews.archivedatabase.mongodb.migrate.MongoDbOpenArchiveToArchiveDatabaseMigrator;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabase;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeSeriesExporter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.OpenArchiveToArchiveDatabaseMigrator;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeSeriesReader;
import nl.wldelft.util.timeseries.TimeSeriesHeader;

public class MongoDbArchiveDatabase implements ArchiveDatabase<TimeSeriesHeader> {

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
	private MongoDbArchiveDatabase(){}

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
		if(mongoDbOpenArchiveToArchiveDatabaseMigrator == null)
			mongoDbOpenArchiveToArchiveDatabaseMigrator = MongoDbOpenArchiveToArchiveDatabaseMigrator.create();
		return mongoDbOpenArchiveToArchiveDatabaseMigrator;
	}


	/**
	 * 
	 */
	@Override
	public ArchiveDatabaseTimeSeriesReader getArchiveDataBaseTimeSeriesReader() {
		return null;
	}
}
