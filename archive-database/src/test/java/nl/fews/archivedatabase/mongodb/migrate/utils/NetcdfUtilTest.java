package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.migrate.TestSettings;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfContent;
import nl.wldelft.archive.util.metadata.timeseries.TimeSeriesRecord;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class NetcdfUtilTest {

	@BeforeAll
	public static void setUpClass(){
		TestSettings.setTestSettings();
	}

	@Test
	void getExistingNetcdfFilesFs(){
		MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().limit(1).forEach(entry ->
				assertNotNull(NetcdfUtil.getExistingNetcdfFilesFs(entry.getKey(), MetaDataUtil.getNetcdfMetaData(entry.getKey()))));
	}

	@Test
	void getTimeSeriesArrayMerged() {
		MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.toString().contains("scalar")).limit(1).forEach(metaDataFile ->
				NetcdfUtil.getExistingNetcdfFilesFs(metaDataFile.getKey(), MetaDataUtil.getNetcdfMetaData(metaDataFile.getKey())).entrySet().stream().limit(1).forEach(netcdfFile -> {
					NetcdfContent netcdfContent = netcdfFile.getValue().getValue1();
					TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = NetcdfUtil.getTimeSeriesArrays(netcdfFile.getKey());
					Map<String, Map<String, TimeSeriesRecord>> timeSeriesRecordsMap = NetcdfUtil.getTimeSeriesRecordsMap(netcdfFile.getKey(), netcdfContent);
					TimeSeriesRecord timeSeriesRecord = timeSeriesRecordsMap.get(timeSeriesArrays.get(0).getHeader().getLocationId()).get(timeSeriesArrays.get(0).getHeader().getParameterId());
					assertNotNull(NetcdfUtil.getTimeSeriesArrayMerged(timeSeriesArrays.get(0), timeSeriesRecord));
		}));
	}

	@Test
	void getTimeSeriesRecordsMap() {
		MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.toString().contains("scalar")).limit(1).forEach(metaDataFile ->
				NetcdfUtil.getExistingNetcdfFilesFs(metaDataFile.getKey(), MetaDataUtil.getNetcdfMetaData(metaDataFile.getKey())).entrySet().stream().limit(1).forEach(netcdfFile -> {
					NetcdfContent netcdfContent = netcdfFile.getValue().getValue1();
					assertNotNull(NetcdfUtil.getTimeSeriesRecordsMap(netcdfFile.getKey(), netcdfContent));
		}));
	}

	@Test
	void getTimeSeriesArrays() {
		MetaDataUtil.getExistingMetaDataFilesFs().entrySet().stream().filter(s -> s.toString().contains("scalar")).limit(1).forEach(metaDataFile ->
				NetcdfUtil.getExistingNetcdfFilesFs(metaDataFile.getKey(), MetaDataUtil.getNetcdfMetaData(metaDataFile.getKey())).entrySet().stream().limit(1).forEach(netcdfFile ->
						assertNotNull(NetcdfUtil.getTimeSeriesArrays(netcdfFile.getKey()))));
	}
}