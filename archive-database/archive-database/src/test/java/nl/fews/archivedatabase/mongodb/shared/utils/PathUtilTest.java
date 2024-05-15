package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class PathUtilTest {

	@BeforeAll
	public static void setUpClass(){
		TestSettings.setTestSettings();
	}

	@Test
	void toRelativePathString() {
		assertEquals("/test", PathUtil.toRelativePathString(new File(Settings.get("baseDirectoryArchive", String.class), "test"), Settings.get("baseDirectoryArchive", String.class)));
	}

	@Test
	void fromRelativePathString() {
		assertEquals(new File(Settings.get("baseDirectoryArchive", String.class), "test"), PathUtil.fromRelativePathString("/test", Settings.get("baseDirectoryArchive", String.class)));
	}

	@Test
	void normalize() {
		assertEquals(String.format("a%1$sa%1$sa", File.separatorChar), PathUtil.normalize(new File(String.format("a%1$sa%1$sa", File.separatorChar == '/' ? '\\' : '/'))).toString());
	}

	@Test
	void containsSegment() {
		assertTrue(PathUtil.containsSegment(new File("./path/to/scalar/data"), "scalar"));
		assertFalse(PathUtil.containsSegment(new File("./path/to/gridded/data"), "scalar"));
		assertFalse(PathUtil.containsSegment(new File("./path/to/scalar/data"), "Scalar"));
		assertTrue(PathUtil.containsSegment(new File("./path/to/scalar/data"), "Scalar", true));
	}
}