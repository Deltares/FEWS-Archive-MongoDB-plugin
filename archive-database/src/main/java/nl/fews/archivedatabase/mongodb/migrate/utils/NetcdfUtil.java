package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.PathUtil;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfContent;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfMetaData;
import nl.wldelft.archive.util.metadata.timeseries.TimeSeriesRecord;
import nl.wldelft.archiveserver.catalogue.indeces.ElasticSearchIndexUtil;
import nl.wldelft.netcdf.NetcdfTimeSeriesParser;
import nl.wldelft.util.timeseries.*;
import org.javatuples.Pair;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class NetcdfUtil {

	/**
	 *
	 */
	private static final Pattern qualifierSplit = Pattern.compile(";;;");

	/**
	 * Static Class
	 */
	private NetcdfUtil(){}

	/**
	 *
	 * @param metaDataFile metaDataFile
	 * @param netcdfMetaData netcdfMetaData
	 * @return Map<File, Date>
	 */
	public static Map<File, Pair<Date, NetcdfContent>> getExistingNetcdfFilesFs(File metaDataFile, NetcdfMetaData netcdfMetaData) {
		Map<File, Pair<Date, NetcdfContent>> existingNetcdfFilesFs = new HashMap<>();
		List<String> valueTypes = Settings.get("valueTypes");
		for (int i = 0; i < netcdfMetaData.netcdfFileCount(); i++) {
			if(!valueTypes.contains(netcdfMetaData.getNetcdf(i).getValueType().toString()))
				continue;
			File netcdfFile = PathUtil.normalize(new File(metaDataFile.getParentFile(), netcdfMetaData.getNetcdf(i).getUrl()));
			if (netcdfFile.exists())
				existingNetcdfFilesFs.put(netcdfFile, new Pair<>(new Date(netcdfFile.lastModified()), netcdfMetaData.getNetcdf(i)));
		}
		return existingNetcdfFilesFs;
	}

	/**
	 *
	 * @param timeSeriesArray timeSeriesArray
	 * @param timeSeriesRecord timeSeriesRecord
	 * @return TimeSeriesArray<TimeSeriesHeader>
	 */
	public static TimeSeriesArray<TimeSeriesHeader> getTimeSeriesArrayMerged(TimeSeriesArray<TimeSeriesHeader> timeSeriesArray, TimeSeriesRecord timeSeriesRecord){
		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();

		timeSeriesHeader.setUnit(timeSeriesArray.getHeader().getUnit());
		timeSeriesHeader.setEnsembleMemberId(timeSeriesArray.getHeader().getEnsembleMemberId());
		timeSeriesHeader.setEnsembleMemberIndex(timeSeriesArray.getHeader().getEnsembleMemberIndex());
		timeSeriesHeader.setForecastTime(timeSeriesArray.getHeader().getForecastTime());

		timeSeriesHeader.setLocationId(timeSeriesRecord.getLocationId());
		timeSeriesHeader.setParameterId(timeSeriesRecord.getParameterId());
		timeSeriesHeader.setTimeStep(TimeStepUtils.decode(timeSeriesRecord.getTimeStepId()));
		timeSeriesHeader.setQualifierIds(timeSeriesRecord.getQualifierIds() != null && !timeSeriesRecord.getQualifierIds().equals("none") && !timeSeriesRecord.getQualifierIds().equals("") ?  qualifierSplit.split(timeSeriesRecord.getQualifierIds()) : new String[]{});
		timeSeriesHeader.setEnsembleId(timeSeriesRecord.getEnsembleId());
		timeSeriesHeader.setModuleInstanceId(timeSeriesRecord.getModuleInstanceId());

		TimeSeriesArray<TimeSeriesHeader> mergedTimeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader);
		long[] times = timeSeriesArray.toTimesArray();
		mergedTimeSeriesArray.ensureTimes(times);
		for (int i = 0; i < times.length; i++) {
			mergedTimeSeriesArray.setValue(i,  timeSeriesArray.getValue(i));
			mergedTimeSeriesArray.setFlag(i, timeSeriesArray.getFlag(i));
			mergedTimeSeriesArray.setComment(i, timeSeriesArray.getComment(i));
		}

		return mergedTimeSeriesArray;
	}

	/**
	 *
	 * @param netcdfFile netcdfFile
	 * @param netcdfContent netcdfContent
	 * @return Map<String, Map<String, TimeSeriesRecord>>
	 */
	public static Map<String, Map<String, TimeSeriesRecord>> getTimeSeriesRecordsMap(File netcdfFile, NetcdfContent netcdfContent){
		try {
			Map<String, Map<String, TimeSeriesRecord>> timeSeriesRecordsMap = new HashMap<>();
			try (NetcdfFile dataSet = NetcdfFiles.open(netcdfFile.getAbsolutePath())) {
				for (TimeSeriesRecord timeSeriesRecord : ElasticSearchIndexUtil.createTimeSeries(dataSet, netcdfContent, false).getObject0()) {
					timeSeriesRecordsMap.putIfAbsent(timeSeriesRecord.getArchiveLocationId(), new HashMap<>());
					timeSeriesRecordsMap.get(timeSeriesRecord.getArchiveLocationId()).putIfAbsent(timeSeriesRecord.getArchiveParameterId(), timeSeriesRecord);
				}
			}
			return timeSeriesRecordsMap;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param netcdfFile netcdfFile
	 * @return TimeSeriesArrays
	 */
	@SuppressWarnings("unchecked")
	public static TimeSeriesArrays<TimeSeriesHeader> getTimeSeriesArrays(File netcdfFile){
		try {
			SimpleTimeSeriesContentHandler timeSeriesContentHandler = new SimpleTimeSeriesContentHandler();
			timeSeriesContentHandler.setTimeSeriesType(TimeSeriesArray.Type.SCALAR);
			new NetcdfTimeSeriesParser(NetcdfTimeSeriesParser.NETCDF_SCALAR).parse(netcdfFile, timeSeriesContentHandler);
			TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = (TimeSeriesArrays<TimeSeriesHeader>)timeSeriesContentHandler.getTimeSeriesArrays();
			timeSeriesArrays.removeCompletelyMissing();
			return timeSeriesArrays;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}
