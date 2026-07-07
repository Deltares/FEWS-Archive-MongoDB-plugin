package nl.fews.archivedatabase.common.shared.utils;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LogUtilTest {

	@Test
	void getLogMessageJson() {
		assertTrue(new JSONObject(LogUtil.getLogMessageJson(new Exception("Test"), Map.of("Extra", "Extra"))).toString().contains("Extra"));
	}
}