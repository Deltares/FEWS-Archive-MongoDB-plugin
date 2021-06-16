package nl.fews.archivedatabase.mongodb.shared.settings;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SettingsTest {

	@Test
	void put() {
		Assertions.assertDoesNotThrow(() -> Settings.put("key", "value"));
	}

	@Test
	void get() {
		Settings.put("intKey", 1);
		Settings.put("stringKey", "1");

		Assertions.assertEquals("1", Settings.get("stringKey"));
		Assertions.assertEquals((Integer)1, Settings.get("intKey"));

		Assertions.assertEquals("1", Settings.get("stringKey", String.class));
		Assertions.assertEquals((Integer)1, Settings.get("intKey", Integer.class));
	}
}