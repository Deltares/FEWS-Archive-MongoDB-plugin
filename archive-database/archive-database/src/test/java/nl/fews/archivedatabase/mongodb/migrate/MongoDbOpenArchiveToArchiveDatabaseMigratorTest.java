package nl.fews.archivedatabase.mongodb.migrate;

import nl.fews.archivedatabase.mongodb.MongoDbArchiveDatabase;
import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.shared.logging.MongoDbAppender;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.OpenArchiveToArchiveDatabaseMigrationSettings;
import nl.wldelft.util.LogUtils;
import nl.wldelft.util.Properties;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class MongoDbOpenArchiveToArchiveDatabaseMigratorTest {

	private static JSONObject testSettings = null;

	static{
		LogUtils.initConsole();
	}

	@BeforeAll
	static void setUpClass() throws IOException {
		Path path = Paths.get("src", "test", "resources", "TestSettings.json").toAbsolutePath();
		if (path.toFile().exists()) {
			testSettings = new JSONObject(Files.readString(path));
		}
	}

	@Test
	void migrate() throws IOException {
		String hostName = InetAddress.getLocalHost().getHostName();
		if(testSettings != null && hostName.equalsIgnoreCase(testSettings.getString("hostName"))) {
			MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
			mongoDbArchiveDatabase.setArchiveDatabaseUrl(testSettings.getString("archiveDatabaseUrl"));
			mongoDbArchiveDatabase.setUserNamePassword(testSettings.isNull("userName") ? "" : testSettings.getString("userName"), testSettings.isNull("password") ? "" : testSettings.getString("password"));

			MongoDbOpenArchiveToArchiveDatabaseMigrator migrateDatabase = (MongoDbOpenArchiveToArchiveDatabaseMigrator)mongoDbArchiveDatabase.getOpenArchiveToArchiveDatabaseMigrator();
			OpenArchiveToArchiveDatabaseMigrationSettings openArchiveToArchiveDatabaseMigrationSettings = new OpenArchiveToArchiveDatabaseMigrationSettings(
					testSettings.getInt("databaseBaseThreads"),
					testSettings.getInt("netcdfReadThreads"),
					testSettings.getString("baseDirectoryArchive")
			);
			migrateDatabase.setOpenArchiveToDatabaseSettings(openArchiveToArchiveDatabaseMigrationSettings);
			migrateDatabase.setTimeConverter(new TestUtil.ArchiveDatabaseTimeConverterTestImplementation());
			migrateDatabase.setUnitConverter(new TestUtil.ArchiveDatabaseUnitConverterTestImplementation());
			migrateDatabase.setRegionConfigInfoProvider(new TestUtil.ArchiveDatabaseRegionConfigInfoProviderTestImplementation());
			if (testSettings.get("properties") != JSONObject.NULL){
				Properties.Builder builder = new Properties.Builder();
				testSettings.getJSONObject("properties").toMap().forEach((k, v) -> builder.addObject(k, v.toString()));
				migrateDatabase.setProperties(builder.build());
			}

			LogUtils.addAppender(MongoDbAppender.createAppender("databaseLogAppender", Settings.get("connectionString"), null));
			migrateDatabase.migrate(
					testSettings.get("areaId") != JSONObject.NULL ? new String[]{testSettings.getString("areaId")} : null,
					testSettings.get("sourceId") != JSONObject.NULL ? testSettings.getString("sourceId") : null);

		}
	}

	@Test
	void finalizeMigration() throws IOException {
		String hostName = InetAddress.getLocalHost().getHostName();
		if(testSettings != null && hostName.equalsIgnoreCase(testSettings.getString("hostName"))) {
			MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
			mongoDbArchiveDatabase.setArchiveDatabaseUrl(testSettings.getString("archiveDatabaseUrl"));
			mongoDbArchiveDatabase.setUserNamePassword(testSettings.isNull("userName") ? "" : testSettings.getString("userName"), testSettings.isNull("password") ? "" : testSettings.getString("password"));

			MongoDbOpenArchiveToArchiveDatabaseMigrator migrateDatabase = (MongoDbOpenArchiveToArchiveDatabaseMigrator)mongoDbArchiveDatabase.getOpenArchiveToArchiveDatabaseMigrator();
			OpenArchiveToArchiveDatabaseMigrationSettings openArchiveToArchiveDatabaseMigrationSettings = new OpenArchiveToArchiveDatabaseMigrationSettings(
					testSettings.getInt("databaseBaseThreads"),
					testSettings.getInt("netcdfReadThreads"),
					testSettings.getString("baseDirectoryArchive")
			);
			migrateDatabase.setOpenArchiveToDatabaseSettings(openArchiveToArchiveDatabaseMigrationSettings);
			migrateDatabase.setTimeConverter(new TestUtil.ArchiveDatabaseTimeConverterTestImplementation());
			migrateDatabase.setUnitConverter(new TestUtil.ArchiveDatabaseUnitConverterTestImplementation());
			migrateDatabase.setRegionConfigInfoProvider(new TestUtil.ArchiveDatabaseRegionConfigInfoProviderTestImplementation());

			migrateDatabase.finalizeMigration(true);
		}
	}
}
