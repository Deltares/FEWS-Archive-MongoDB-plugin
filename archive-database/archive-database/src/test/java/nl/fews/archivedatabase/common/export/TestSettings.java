package nl.fews.archivedatabase.common.export;

import nl.fews.archivedatabase.common.TestUtil;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.wldelft.util.LogUtils;

public class TestSettings {

	static {
		LogUtils.initConsole();
	}

	private TestSettings(){}

	public static void setTestSettings(){
		Settings.put("configRevision", "configRevision");
		Settings.put("archiveDatabaseUnitConverter", new TestUtil.ArchiveDatabaseUnitConverterTestImplementation());
		Settings.put("archiveDatabaseTimeConverter", new TestUtil.ArchiveDatabaseTimeConverterTestImplementation());
		Settings.put("archiveDatabaseRegionConfigInfoProvider", new TestUtil.ArchiveDatabaseRegionConfigInfoProviderTestImplementation());
		Settings.put("databaseUrl", "%s/FEWS_ARCHIVE_TEST");
	}
}
