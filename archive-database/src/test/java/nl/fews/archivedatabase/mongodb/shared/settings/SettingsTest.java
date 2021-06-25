package nl.fews.archivedatabase.mongodb.shared.settings;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SettingsTest {

	@Test
	void put() {
		assertDoesNotThrow(() -> Settings.put("key", "value"));
	}

	@Test
	void get() {
		Settings.put("intKey", 1);
		Settings.put("stringKey", "1");

		assertEquals("1", Settings.get("stringKey"));
		assertEquals((Integer)1, Settings.get("intKey"));

		assertEquals("1", Settings.get("stringKey", String.class));
		assertEquals((Integer)1, Settings.get("intKey", Integer.class));
	}
}