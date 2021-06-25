package nl.fews.archivedatabase.mongodb.shared.utils;

import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LogUtilTest {

	@Test
	void getLogMessageJson() {
		assertTrue(LogUtil.getLogMessageJson(new Exception("Test"), Map.of("Extra", "Extra")).toJson().contains("Extra"));
		assertTrue(LogUtil.getLogMessageJson(new MongoWriteException(new WriteError(1, "Test", new BsonDocument("Details", new BsonString("Details"))), new ServerAddress()), Map.of("Extra", "Extra")).toJson().contains("Extra"));
	}
}