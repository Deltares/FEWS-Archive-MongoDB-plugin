package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfMetaData;
import nl.wldelft.archive.util.runinfo.ArchiveRunInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class RunInfoUtilTest {

	@BeforeAll
	public static void setUpClass(){
		TestSettings.setTestSettings();
	}

	@Test
	void getRunInfo() {
		Map<File, Date> existingMetaDataFilesFs = MetaDataUtil.getExistingMetaDataFilesFs();
		ArchiveRunInfo archiveRunInfo = null;
		for (File metaDataFile: existingMetaDataFilesFs.keySet()) {
			NetcdfMetaData netcdfMetaData =MetaDataUtil.getNetcdfMetaData(metaDataFile);
			archiveRunInfo = RunInfoUtil.getRunInfo(netcdfMetaData);
			if(archiveRunInfo != null){
				break;
			}
		}
		assertNotNull(archiveRunInfo);
	}
}
