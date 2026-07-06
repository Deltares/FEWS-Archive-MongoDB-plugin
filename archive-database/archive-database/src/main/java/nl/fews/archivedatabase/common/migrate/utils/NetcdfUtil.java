package nl.fews.archivedatabase.common.migrate.utils;

import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.common.shared.tuple.Pair;
import nl.fews.archivedatabase.common.shared.utils.PathUtil;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfContent;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfMetaData;
import nl.wldelft.archive.util.metadata.timeseries.ArchiveTimeSeriesRecord;
import nl.wldelft.archive.util.metadata.timeseries.TimeSeriesRecord;
import nl.wldelft.fews.system.data.externaldatasource.util.ArchiveIntegrationUtil;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.netcdf.NetcdfTimeSeriesParser;
import nl.wldelft.util.timeseries.*;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

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
		var existingNetcdfFilesFs = new LinkedHashMap<File, Pair<Date, NetcdfContent>>();
		var valueTypes = Settings.get("valueTypes", List.class);
		for (int i = 0; i < netcdfMetaData.netcdfFileCount(); i++) {
			if(!valueTypes.contains(netcdfMetaData.getNetcdf(i).getValueType().toString()))
				continue;
			var netcdfFile = PathUtil.normalize(new File(metaDataFile.getParentFile(), netcdfMetaData.getNetcdf(i).getUrl()));
			if (netcdfFile.exists())
				existingNetcdfFilesFs.put(netcdfFile, new Pair<>(new Date(netcdfFile.lastModified()), netcdfMetaData.getNetcdf(i)));
		}
		return existingNetcdfFilesFs;
	}

	/**
	 *
	 * @param timeSeriesArray timeSeriesArray
	 * @param timeSeriesRecord timeSeriesRecord
	 * @return TimeSeriesArray<FewsTimeSeriesHeader>
	 */
	public static TimeSeriesArray<FewsTimeSeriesHeader> getTimeSeriesArrayMerged(TimeSeriesArray<FewsTimeSeriesHeader> timeSeriesArray, TimeSeriesRecord timeSeriesRecord){
		var timeSeriesHeader = new DefaultTimeSeriesHeader();

		timeSeriesHeader.setUnit(timeSeriesArray.getHeader().getUnit());
		timeSeriesHeader.setEnsembleMemberId(timeSeriesArray.getHeader().getEnsembleMemberId() == null || timeSeriesArray.getHeader().getEnsembleMemberId().equals("none") || timeSeriesArray.getHeader().getEnsembleMemberId().equals("0") ? null : timeSeriesArray.getHeader().getEnsembleMemberId());
		timeSeriesHeader.setEnsembleMemberIndex(timeSeriesArray.getHeader().getEnsembleMemberIndex());
		timeSeriesHeader.setForecastTime(timeSeriesArray.getHeader().getForecastTime());

		timeSeriesHeader.setLocationId(timeSeriesRecord.getLocationId());
		timeSeriesHeader.setParameterId(timeSeriesRecord.getParameterId());
		timeSeriesHeader.setTimeStep(TimeStepUtils.decode(timeSeriesRecord.getTimeStepId()));
		timeSeriesHeader.setQualifierIds(timeSeriesRecord.getQualifierIds() == null || timeSeriesRecord.getQualifierIds().equals("none") || timeSeriesRecord.getQualifierIds().isEmpty() ?  new String[]{} : qualifierSplit.split(timeSeriesRecord.getQualifierIds()));
		timeSeriesHeader.setEnsembleId(timeSeriesRecord.getEnsembleId() == null || timeSeriesRecord.getEnsembleId().equals("none") || timeSeriesRecord.getEnsembleId().equals("main") ? null : timeSeriesRecord.getEnsembleId());
		timeSeriesHeader.setModuleInstanceId(timeSeriesRecord.getModuleInstanceId());

		//TODO: ANDRE!!!
		//var mergedTimeSeriesArray = new TimeSeriesArray<FewsTimeSeriesHeader>(timeSeriesHeader);
		var mergedTimeSeriesArray = new TimeSeriesArray<FewsTimeSeriesHeader>((FewsTimeSeriesHeader)new Object());
		var times = timeSeriesArray.toTimesArray();
		mergedTimeSeriesArray.ensureTimes(times);
		for (int i = 0; i < times.length; i++) {
			mergedTimeSeriesArray.setValue(i,  timeSeriesArray.getValue(i));
			mergedTimeSeriesArray.setFlag(i, timeSeriesArray.getFlag(i));
			mergedTimeSeriesArray.setComment(i, timeSeriesArray.getComment(i));
			mergedTimeSeriesArray.setFlagSource(i, timeSeriesArray.getFlagSource(i));
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
			var baseDirectoryArchive = Settings.get("baseDirectoryArchive", String.class);
			var relativePathNetcdFile = netcdfFile.getAbsolutePath().substring(baseDirectoryArchive.length());
			var timeSeriesRecordsMap = new LinkedHashMap<String, Map<String, TimeSeriesRecord>>();
			try (NetcdfFile dataSet = NetcdfFiles.open(netcdfFile.getAbsolutePath())) {
				for (ArchiveTimeSeriesRecord archiveTimeSeriesRecord : ArchiveIntegrationUtil.createTimeSeries(dataSet, relativePathNetcdFile, false, netcdfContent)) {
					if(archiveTimeSeriesRecord.getArchiveLocationId().isEmpty())
						archiveTimeSeriesRecord.getLocationId().forEach(locationId -> archiveTimeSeriesRecord.getArchiveLocationId().add(locationId));
					IntStream.range(0, archiveTimeSeriesRecord.getArchiveLocationId().size()).forEach(i -> {
						timeSeriesRecordsMap.putIfAbsent(archiveTimeSeriesRecord.getArchiveLocationId().get(i), new LinkedHashMap<>());
						var timeSeriesRecord = new TimeSeriesRecord(
								archiveTimeSeriesRecord.getLocationId().get(i),
								archiveTimeSeriesRecord.getArchiveLocationId().get(i),
								archiveTimeSeriesRecord.getParameterId(),
								archiveTimeSeriesRecord.getArchiveParameterId(),
								archiveTimeSeriesRecord.getModuleInstanceId().get(archiveTimeSeriesRecord.getModuleInstanceId().size() == 1 ? 0 : i),
								archiveTimeSeriesRecord.getTimeStepId(),
								archiveTimeSeriesRecord.getQualifierIds().get(archiveTimeSeriesRecord.getQualifierIds().size() == 1 ? 0 : i),
								archiveTimeSeriesRecord.getEnsembleId(),
								String.join(",", archiveTimeSeriesRecord.getEnsembleMemberIds()),
								archiveTimeSeriesRecord.getTimeSeriesType()
						);
						timeSeriesRecordsMap.get(archiveTimeSeriesRecord.getArchiveLocationId().get(i)).putIfAbsent(archiveTimeSeriesRecord.getArchiveParameterId(), timeSeriesRecord);
					});
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
	public static TimeSeriesArrays<FewsTimeSeriesHeader> getTimeSeriesArrays(File netcdfFile){
		try {
			var timeSeriesContentHandler = new SimpleTimeSeriesContentHandler();
			timeSeriesContentHandler.setTimeSeriesType(TimeSeriesArray.Type.SCALAR);
			new NetcdfTimeSeriesParser(NetcdfTimeSeriesParser.NETCDF_SCALAR).parse(netcdfFile, timeSeriesContentHandler);
			var timeSeriesArrays = (TimeSeriesArrays<FewsTimeSeriesHeader>)timeSeriesContentHandler.getTimeSeriesArrays();
			timeSeriesArrays.removeCompletelyMissing();
			return timeSeriesArrays;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}
