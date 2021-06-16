package nl.fews.archivedatabase.mongodb.migrate;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseUnitConverter;
import nl.wldelft.util.Properties;

public interface MigrateDatabase {
	void migrateTimeSeries();

	void setUnitConverter(ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter);

	void setTimeConverter(ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter);

	void setProperties(Properties properties);

	void setArchiveDatabaseUrl(String archiveDatabaseUrl);

	void setUserNamePassword(String archiveDatabaseUserName, String archiveDatabasePassword);

	void setArchiveRootDataFolder(String archiveRootDataFolder);

	void setNumThreads(int dbThreads, int fsThreads);
}