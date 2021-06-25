package nl.fews.archivedatabase.mongodb;

import nl.fews.archivedatabase.mongodb.export.MongoDbArchiveDatabaseTimeSeriesExporter;
import nl.fews.archivedatabase.mongodb.migrate.MongoDbOpenArchiveToArchiveDatabaseMigrator;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabase;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeSeriesExporter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.OpenArchiveToArchiveDatabaseMigrator;
import nl.wldelft.util.timeseries.TimeSeriesHeader;

public class MongoDbArchiveDatabase implements ArchiveDatabase<TimeSeriesHeader> {
	/**
	 *
	 * @return MongoDbArchiveDatabaseTimeSeriesExporter
	 */
	@Override
	public ArchiveDatabaseTimeSeriesExporter<TimeSeriesHeader> getArchiveTimeSeriesExporter() {
		return MongoDbArchiveDatabaseTimeSeriesExporter.create();
	}

	/**
	 *
	 * @return MongoDbOpenArchiveToArchiveDatabaseMigrator
	 */
	@Override
	public OpenArchiveToArchiveDatabaseMigrator getOpenArchiveToArchiveDatabaseMigrator() {
		return MongoDbOpenArchiveToArchiveDatabaseMigrator.create();
	}
}
