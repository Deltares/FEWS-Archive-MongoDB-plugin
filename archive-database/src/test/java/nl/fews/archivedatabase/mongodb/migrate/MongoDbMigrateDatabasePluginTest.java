package nl.fews.archivedatabase.mongodb.migrate;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.shared.logging.MongoDbAppender;
import nl.wldelft.util.LogUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class MongoDbMigrateDatabasePluginTest {

	private static JSONObject testSettings = null;

	@BeforeAll
	static void setUpClass() throws IOException {
		Path path = Paths.get("src", "test", "resources", "TestSettings.json").toAbsolutePath();
		if (path.toFile().exists()) {
			testSettings = new JSONObject(Files.readString(path));
		}

		if(testSettings != null && InetAddress.getLocalHost().getHostName().equalsIgnoreCase(testSettings.getString("HostName"))) {
			LogUtils.initConsole();
			LogUtils.addAppender(MongoDbAppender.createAppender("databaseLogAppender",
				testSettings.getString("ArchiveDatabaseUrl").replace("mongodb://", String.format("mongodb://%s:%s@", testSettings.getString("UserName"), testSettings.getString("Password"))), null));
		}
	}

	@Test
	void migrateTimeSeries() throws IOException {
		if(testSettings != null && InetAddress.getLocalHost().getHostName().equalsIgnoreCase(testSettings.getString("HostName"))) {
			MigrateDatabase migrateDatabase = MongoDbMigrateDatabasePlugin.create();
			migrateDatabase.setArchiveDatabaseUrl(testSettings.getString("ArchiveDatabaseUrl"));
			migrateDatabase.setArchiveRootDataFolder(testSettings.getString("ArchiveRootDataFolder"));
			migrateDatabase.setNumThreads(testSettings.getInt("DbNumThreads"), testSettings.getInt("FsNumThreads"));
			migrateDatabase.setTimeConverter(new TestUtil.ArchiveDatabaseTimeConverterTestImplementation());
			migrateDatabase.setUnitConverter(new TestUtil.ArchiveDatabaseUnitConverterTestImplementation());
			migrateDatabase.setUserNamePassword(testSettings.getString("UserName"), testSettings.getString("Password"));
			migrateDatabase.migrateTimeSeries();
		}
	}
}
