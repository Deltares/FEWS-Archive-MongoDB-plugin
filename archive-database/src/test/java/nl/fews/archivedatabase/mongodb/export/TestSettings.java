package nl.fews.archivedatabase.mongodb.export;

import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
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
		Settings.put("databaseUrl", "mongodb://%s:%s/FEWS_ARCHIVE_TEST");
		Settings.put("bucketDefinitions", "[{\"definition\": {\"moduleInstanceId\": [\"SpillGates\"], \"qualifierId\": [], \"locationId\": [\"NJH-01\",\"NJH-02\",\"NJH-03\",\"NJH-04\",\"O3H-01\",\"NJH-OG-01\",\"NJH-SP-01\",\"NJH-SP-02\",\"NJH-SP-03\",\"NJH-SP-04\",\"NJH-SP-05\",\"NJH-SP-06\",\"NJH-SP-07\",\"NJH-SP-08\",\"NJH-SP-09\",\"NJH-SP-10\",\"O3H-SL-01\",\"O3H-SL-02\",\"O3H-SP-01\",\"O3H-SP-02\",\"O3H-SP-03\",\"O3H-SP-04\",\"O3H-SP-05\",\"O3H-SP-06\",\"O3H-SP-07\",\"NJH-01-TD\",\"NJH-02-TD\",\"NJH-03-TD\",\"NJH-04-TD\",\"NJH-OG-02\",\"NJH-OG-03\",\"NJH-OG-04\",\"NJH-OG-05\",\"NJH-OG-06\",\"NJH-OG-07\",\"NJH-OG-08\",\"NJH-OG-09\",\"NJH-OG-10\",\"NJH-OG-11\",\"O3H-OG-01\",\"O3H-OG-02\",\"O3H-OG-03\",\"O3H-OG-04\"], \"parameterId\": [\"NS\"], \"encodedTimeStepId\": [\"NETS\"]}, \"bucketSize\": \"YEARLY\", \"collapse\": true}]");
	}
}
