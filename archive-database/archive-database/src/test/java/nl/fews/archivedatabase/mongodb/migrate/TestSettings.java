package nl.fews.archivedatabase.mongodb.migrate;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Collection;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.util.LogUtils;
import org.apache.logging.log4j.LogManager;
import org.json.JSONObject;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class TestSettings {

	static {
		LogUtils.initConsole();
		LogManager.getLogger("Test");
	}

	private TestSettings(){}

	public static void setTestSettings(){
		try {
			JSONObject testSettings = null;
			Path path = Paths.get("src", "test", "resources", "TestSettings.json").toAbsolutePath();
			if (path.toFile().exists()) {
				testSettings = new JSONObject(Files.readString(path));
			}

			Settings.put("metaDataCollection", Collection.MigrateMetaData.toString());
			Settings.put("logCollection", Collection.MigrateLog.toString());
			Settings.put("bucketSizeCollection", Collection.BucketSize.toString());
			Settings.put("databaseBaseThreads", 16);
			Settings.put("netcdfReadThreads", 32);
			Settings.put("folderMaxDepth", 4);
			Settings.put("metadataFileName", "metaData.xml");
			Settings.put("runInfoFileName", "runInfo.xml");
			Settings.put("valueTypes", List.of("scalar"));
			Settings.put("useBulkInsert", false);
			Settings.put("renameFinalizedCollection", false);
			Settings.put("archiveDatabaseUserName", "fews_admin");
			Settings.put("baseDirectoryArchive", Paths.get("src", "test", "resources").toAbsolutePath().toString());

			Settings.put("archiveDatabaseUnitConverter", new TestUtil.ArchiveDatabaseUnitConverterTestImplementation());
			Settings.put("archiveDatabaseTimeConverter", new TestUtil.ArchiveDatabaseTimeConverterTestImplementation());
			Settings.put("archiveDatabaseRegionConfigInfoProvider", new TestUtil.ArchiveDatabaseRegionConfigInfoProviderTestImplementation());
			Settings.put("databaseUrl", "%s/FEWS_ARCHIVE_TEST");
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}
