package nl.fews.archivedatabase.common.shared.database;

import nl.fews.archivedatabase.common.migrate.TestSettings;
import nl.fews.archivedatabase.common.shared.interfaces.DatabaseBridge;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class DatabaseTest {

	@Container
	public static MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:8.3.2"));

	@BeforeAll
	static void setUp() {
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	@Test
	void instance() {
		var result = Database.instance();
		assertInstanceOf(DatabaseBridge.class, result);
	}
}