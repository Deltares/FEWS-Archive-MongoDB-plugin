package nl.fews.archivedatabase.mongodb.shared.utils;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LogUtilTest {

	@Test
	void getLogMessageJson() {
		assertTrue(LogUtil.getLogMessageJson(new Exception("Test"), Map.of("Extra", "Extra")).toJson().contains("Extra"));
	}
}