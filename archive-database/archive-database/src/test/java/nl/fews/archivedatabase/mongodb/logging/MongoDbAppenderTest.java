package nl.fews.archivedatabase.mongodb.logging;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import nl.fews.archivedatabase.common.migrate.TestSettings;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.wldelft.util.LogUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.bson.Document;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class MongoDbAppenderTest {

	private static final Logger logger;

	@Container
	final MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:8.3.2"));

	@BeforeEach
	public void setUp(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	static {
		LogUtils.initConsole();
		logger = LogManager.getLogger(MongoDbAppenderTest.class);
	}

	@Test
	void appendWritesLogEntry() {
		DbAppender dbAppender = DbAppender.createAppender("databaseLogAppender", Settings.get("connectionString"), null);
		LogUtils.addAppender(dbAppender);

		String message = "Test";
		logger.warn(message);

		var connectionString = Settings.get("connectionString", String.class);
		try (var client = MongoClients.create(connectionString)) {
			var database = client.getDatabase(new ConnectionString(connectionString).getDatabase());
			var document = database.getCollection("MigrateLog").find(new Document("message", message)).first();

			Assertions.assertEquals(message, document.getString("message"));
			Assertions.assertEquals("WARN", document.getString("level"));
			Assertions.assertEquals(MongoDbAppenderTest.class.getName(), document.getString("loggerName"));
			Assertions.assertNotNull(document.getString("machineName"));
			Assertions.assertNotNull(document.getDate("date"));
		}
	}
}
