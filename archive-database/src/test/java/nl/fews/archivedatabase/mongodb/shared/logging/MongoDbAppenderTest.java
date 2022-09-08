package nl.fews.archivedatabase.mongodb.shared.logging;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.util.LogUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class MongoDbAppenderTest {
	@Container
	public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo"));

	@BeforeEach
	public void setUpClass(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	@Test
	void append() {
		LogUtils.initConsole();
		LogUtils.addAppender(MongoDbAppender.createAppender("databaseLogAppender", Settings.get("connectionString"), null));
		LoggerFactory.getLogger(MongoDbAppenderTest.class).warn("Test");
	}
}