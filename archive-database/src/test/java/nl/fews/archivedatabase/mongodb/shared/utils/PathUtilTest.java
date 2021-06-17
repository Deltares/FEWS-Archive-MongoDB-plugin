package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;

class PathUtilTest {

	@BeforeAll
	public static void setUpClass(){
		TestSettings.setTestSettings();
	}

	@Test
	void toRelativePathString() {
		Assertions.assertEquals("/test", PathUtil.toRelativePathString(new File(Settings.get("archiveRootDataFolder", String.class), "test"), Settings.get("archiveRootDataFolder", String.class)));
	}

	@Test
	void fromRelativePathString() {
		Assertions.assertEquals(new File(Settings.get("archiveRootDataFolder", String.class), "test"), PathUtil.fromRelativePathString("/test", Settings.get("archiveRootDataFolder", String.class)));
	}

	@Test
	void normalize() {
		Assertions.assertEquals(String.format("a%1$sa%1$sa", File.separatorChar), PathUtil.normalize(new File(String.format("a%1$sa%1$sa", File.separatorChar == '/' ? '\\' : '/'))).toString());
	}
}