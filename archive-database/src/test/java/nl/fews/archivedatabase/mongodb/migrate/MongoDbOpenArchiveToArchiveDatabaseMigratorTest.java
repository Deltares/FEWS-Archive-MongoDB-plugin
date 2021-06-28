package nl.fews.archivedatabase.mongodb.migrate;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.shared.logging.MongoDbAppender;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.OpenArchiveToArchiveDatabaseMigrationSettings;
import nl.wldelft.util.LogUtils;
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

	@BeforeAll
	static void setUpClass() throws IOException {
		Path path = Paths.get("src", "test", "resources", "TestSettings.json").toAbsolutePath();
		if (path.toFile().exists()) {
			testSettings = new JSONObject(Files.readString(path));
		}

		if(testSettings != null && InetAddress.getLocalHost().getHostName().equalsIgnoreCase(testSettings.getString("hostName"))) {
			LogUtils.initConsole();
			LogUtils.addAppender(MongoDbAppender.createAppender("databaseLogAppender",
					!testSettings.isNull("userName") && !testSettings.getString("userName").equals("") ?
					testSettings.getString("archiveDatabaseUrl").replace("mongodb://", String.format("mongodb://%s:%s@", testSettings.getString("userName"), testSettings.getString("password"))) :
					testSettings.getString("archiveDatabaseUrl"), null));
		}
	}

	@Test
	void migrate() throws IOException {
		String hostName = InetAddress.getLocalHost().getHostName();
		if(testSettings != null && hostName.equalsIgnoreCase(testSettings.getString("hostName")) && hostName.equalsIgnoreCase("CHADWHH01")) {
			MongoDbOpenArchiveToArchiveDatabaseMigrator migrateDatabase = MongoDbOpenArchiveToArchiveDatabaseMigrator.create();
			migrateDatabase.setArchiveDatabaseUrl(testSettings.getString("archiveDatabaseUrl"));
			OpenArchiveToArchiveDatabaseMigrationSettings openArchiveToArchiveDatabaseMigrationSettings = new OpenArchiveToArchiveDatabaseMigrationSettings(
					testSettings.getInt("databaseBaseThreads"),
					testSettings.getInt("netcdfReadThreads"),
					testSettings.getString("baseDirectoryArchive")
			);
			migrateDatabase.setOpenArchiveToDatabaseSettings(openArchiveToArchiveDatabaseMigrationSettings);
			migrateDatabase.setTimeConverter(new TestUtil.ArchiveDatabaseTimeConverterTestImplementation());
			migrateDatabase.setUnitConverter(new TestUtil.ArchiveDatabaseUnitConverterTestImplementation());
			migrateDatabase.setRegionConfigInfoProvider(new TestUtil.ArchiveDatabaseRegionConfigInfoProviderTestImplementation());
			migrateDatabase.setUserNamePassword(testSettings.isNull("userName") ? "" : testSettings.getString("userName"), testSettings.isNull("password") ? "" : testSettings.getString("password"));
			migrateDatabase.migrate(
					testSettings.get("areaId") != JSONObject.NULL ? testSettings.getString("areaId") : null,
					testSettings.get("sourceId") != JSONObject.NULL ? testSettings.getString("sourceId") : null);
		}
	}
}
