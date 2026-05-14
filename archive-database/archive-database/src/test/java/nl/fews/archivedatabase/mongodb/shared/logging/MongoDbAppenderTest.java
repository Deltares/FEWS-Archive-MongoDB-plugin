package nl.fews.archivedatabase.mongodb.shared.logging;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.util.LogUtils;
import org.junit.jupiter.api.AfterEach;
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
	final MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:8.3.2"));

	@BeforeEach
	public void setUp(){
		TestSettings.setTestSettings();
		Settings.put("connectionString", String.format(Settings.get("databaseUrl", String.class), mongoDBContainer.getConnectionString()));
	}

	@AfterEach
	public void tearDown(){
		Database.close();
	}

	@AfterEach
	public void tearDownClass(){

	}

	@Test
	void append() {
		LogUtils.initConsole();
		try(MongoDbAppender mongoDbAppender = MongoDbAppender.createAppender("databaseLogAppender", Settings.get("connectionString"), null)){
			LogUtils.addAppender(mongoDbAppender);
			LoggerFactory.getLogger(MongoDbAppenderTest.class).warn("Test");
		}
	}
}