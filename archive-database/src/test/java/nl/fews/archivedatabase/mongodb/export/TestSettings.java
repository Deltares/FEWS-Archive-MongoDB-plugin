package nl.fews.archivedatabase.mongodb.export;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.util.LogUtils;

public class TestSettings {

	static {
		LogUtils.initConsole();
	}

	private TestSettings(){}

	public static void setTestSettings(){
		Settings.put("configRevision", "configRevision");
		Settings.put("logCollection", Database.Collection.MigrateLog.toString());
		Settings.put("bucketSizeCollection", Database.Collection.BucketSize.toString());
		Settings.put("archiveDatabaseUnitConverter", new TestUtil.ArchiveDatabaseUnitConverterTestImplementation());
		Settings.put("archiveDatabaseTimeConverter", new TestUtil.ArchiveDatabaseTimeConverterTestImplementation());
		Settings.put("archiveDatabaseRegionConfigInfoProvider", new TestUtil.ArchiveDatabaseRegionConfigInfoProviderTestImplementation());
		Settings.put("databaseUrl", "%s/FEWS_ARCHIVE_TEST");
	}
}
