package nl.fews.archivedatabase.common.shared.logging;

import nl.fews.archivedatabase.common.migrate.TestSettings;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import org.junit.jupiter.api.BeforeEach;
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

//	@AfterEach
//	public void tearDown(){
//		Database.close();
//	}
//
//	@AfterEach
//	public void tearDownClass(){
//
//	}
//
//	@Test
//	void append() {
//		LogUtils.initConsole();
//		try(DbAppender dbAppender = DbAppender.createAppender("databaseLogAppender", Settings.get("connectionString"), null)){
//			LogUtils.addAppender(dbAppender);
//			LoggerFactory.getLogger(MongoDbAppenderTest.class).warn("Test");
//		}
//	}
}