package nl.fews.archivedatabase.common.migrate.operations;

import nl.fews.archivedatabase.common.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.common.migrate.utils.NetcdfUtil;
import nl.fews.archivedatabase.common.migrate.utils.RunInfoUtil;
import nl.fews.archivedatabase.common.shared.database.Database;
import nl.fews.archivedatabase.common.shared.database.Collection;
import nl.fews.archivedatabase.common.shared.enums.BucketSize;
import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.interfaces.TimeSeries;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.common.shared.tuple.Pair;
import nl.fews.archivedatabase.common.shared.utils.BucketUtil;
import nl.fews.archivedatabase.common.shared.utils.LogUtil;
import nl.fews.archivedatabase.common.shared.utils.PathUtil;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfContent;
import nl.wldelft.archive.util.metadata.simulation.SimulationMetaData;
import nl.wldelft.archive.util.metadata.timeseries.TimeSeriesRecord;
import nl.wldelft.archive.util.runinfo.ArchiveRunInfo;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
@SuppressWarnings("unchecked")
public final class Insert {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(Insert.class);

	/**
	 *
	 */
	private static final String BASE_NAMESPACE = "nl.fews.archivedatabase.common";

	/**
	 *
	 */
	private static int progressCurrent = 0;

	/**
	 *
	 */
	private static int progressExpected = 0;

	/**
	 *
	 */
	private static final Object mutex = new Object();


	/**
	 * Static Class
	 */
	private Insert(){}

	/**
	 *
	 * @param existingMetaDataFilesFs existingMetaDataFilesFs
	 * @param existingMetaDataFilesDb existingMetaDataFilesDb
	 */
	public static void insertMetaDatas(Map<File, Date> existingMetaDataFilesFs, Map<File, Date> existingMetaDataFilesDb) throws ExecutionException, InterruptedException {
		var pool = Executors.newFixedThreadPool(Settings.get("databaseBaseThreads"));
		var metaDataFiles = MetaDataUtil.getMetaDataFilesInsert(existingMetaDataFilesFs, existingMetaDataFilesDb);
		var tasks = new ArrayList<Callable<Void>>();
		progressExpected = metaDataFiles.size();
		progressCurrent = 0;

		class InsertMetaData implements Callable<Void> {
			private final File file;
			private final Date date;
			public InsertMetaData(File file, Date date) {
				this.file = file;
				this.date = date;
			}
			@Override
			public Void call() {
				insertMetaData(file, date);
				synchronized (mutex) {
					if (++progressCurrent % 100 == 0)
						logger.info("Insert Progress: {}/{} {}", progressCurrent, progressExpected, String.format("%,.2f%%", ((double) progressCurrent / progressExpected * 100)));
				}
				return null;
			}
		}

		metaDataFiles.forEach((file, date) -> tasks.add(new InsertMetaData(file, date)));
		for (Future<Void> x : pool.invokeAll(tasks))
			x.get();
		pool.shutdown();

		logger.info("Insert Progress: {}/{} {}, Pool Size: {}", progressCurrent, progressExpected, String.format("%,.2f%%", ((double)progressCurrent/progressExpected*100)), 0);
	}

	/**
	 *
	 * @param metaDataFile metaDataFile
	 * @param metaDataValue metaDataValue
	 */
	public static void insertMetaData(File metaDataFile, Date metaDataValue){
		try{
			var netcdfMetaData = MetaDataUtil.getNetcdfMetaData(metaDataFile);
			if (netcdfMetaData == null)
				return;
			var areaId = netcdfMetaData instanceof SimulationMetaData simulationMetaData ? simulationMetaData.getAreaId() : null;
			var archiveRunInfo = RunInfoUtil.getRunInfo(netcdfMetaData);
			var netcdfFiles = NetcdfUtil.getExistingNetcdfFilesFs(metaDataFile, netcdfMetaData);
			var allInsertedIds = new LinkedHashMap<String, List<Object>>();

			var metaDataDocument = new LinkedHashMap<String, Object>(Map.of(
			"metaDataFileRelativePath", PathUtil.toRelativePathString(metaDataFile, Settings.get("baseDirectoryArchive", String.class)),
			"metaDataFileTime", metaDataValue,
			"netcdfFiles", new ArrayList<Map<String, Object>>(),
			"committed", false));

			netcdfFiles.forEach((netcdfFile, dateNetcdf) -> {
				try {
					var insertedIds = Insert.insertNetcdfs(netcdfFile, dateNetcdf.getValue1(), archiveRunInfo, areaId);
					if (insertedIds.getValue1() != null && !insertedIds.getValue1().isEmpty()) {
						allInsertedIds.putIfAbsent(insertedIds.getValue0(), new ArrayList<>());
						allInsertedIds.get(insertedIds.getValue0()).addAll(insertedIds.getValue1());
						var netcdfFileEntry = new LinkedHashMap<String, Object>();
						netcdfFileEntry.put("netcdfFileRelativePath", PathUtil.toRelativePathString(netcdfFile, Settings.get("baseDirectoryArchive", String.class)));
						netcdfFileEntry.put("netcdfFileTime", dateNetcdf.getValue0());
						netcdfFileEntry.put("timeSeriesIds", insertedIds.getValue1());
						netcdfFileEntry.put("collection", insertedIds.getValue0());
						((List<Map<String, Object>>)metaDataDocument.get("netcdfFiles")).add(netcdfFileEntry);
					}
				}
				catch (Exception ex){
					logger.warn(new JSONObject(LogUtil.getLogMessageJson(ex, Map.of("netcdfFile", netcdfFile.toString()))).toString(), ex);
				}
			});
			if(!((List<Map<String, Object>>)metaDataDocument.get("netcdfFiles")).isEmpty()) {
				var insertedId = Database.instance().insertOne(Collection.MigrateMetaData.toString(), metaDataDocument);
				commitInserted(insertedId, allInsertedIds);
			}
		}
		catch (Exception ex){
			logger.warn(new JSONObject(LogUtil.getLogMessageJson(ex, Map.of("metaDataFile", metaDataFile.toString()))).toString(), ex);
		}
	}

	/**
	 *
	 * @param metaDataInsertedId metaDataInsertedId
	 * @param allInsertedIds allInsertedIds
	 */
	private static void commitInserted(Object metaDataInsertedId, Map<String, List<Object>> allInsertedIds){
		allInsertedIds.forEach((collection, insertedIds) -> {
			if(collection != null && !insertedIds.isEmpty())
				Database.instance().updateMany(collection, Map.of("_id", Map.of("in", insertedIds)), Map.of("set", Map.of("committed", true)));
		});
		Database.instance().updateOne(Collection.MigrateMetaData.toString(), Map.of("_id", metaDataInsertedId), Map.of("set", Map.of("committed", true)));
	}

	/**
	 *
	 * @param netcdfFile netcdfFile
	 * @param netcdfContent netcdfContent
	 * @param archiveRunInfo archiveRunInfo
	 * @param areaId areaId
	 * @return List<ObjectId>
	 */
	private static Pair<String, List<Object>> insertNetcdfs(File netcdfFile, NetcdfContent netcdfContent, ArchiveRunInfo archiveRunInfo, String areaId) {

		if(netcdfFile == null || !netcdfFile.exists())
			return new Pair<>(null, new ArrayList<>());

		var timeSeriesRecordsMap = NetcdfUtil.getTimeSeriesRecordsMap(netcdfFile, netcdfContent);
		var timeSeriesRecord = timeSeriesRecordsMap.values().stream().findFirst().orElse(new LinkedHashMap<>()).values().stream().findFirst().orElse(null);
		if(timeSeriesRecord == null)
			return new Pair<>(null, new ArrayList<>());

		var timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>(netcdfContent.getValueType().toString(), nl.wldelft.fews.system.data.timeseries.TimeSeriesType.getByIntId(timeSeriesRecord.getTimeSeriesType()).getName()));
		if(timeSeriesType == null)
			return new Pair<>(null, new ArrayList<>());

		var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		if(TimeSeriesTypeUtil.getTimeSeriesTypeClassName(timeSeriesType) == null)
			return new Pair<>(null, new ArrayList<>());

		if(Settings.get("useBulkInsert")){
			var timeSeries = Insert.bulkExtractTimeSeries(timeSeriesType, timeSeriesRecordsMap, netcdfFile, netcdfContent, archiveRunInfo, areaId);
			if (timeSeries.isEmpty())
				return new Pair<>(collection, new ArrayList<>());

			return Insert.bulkInsertTimeseries(collection, timeSeries, netcdfFile);
		}
		else{
			return Insert.insertTimeSeries(collection, timeSeriesType, timeSeriesRecordsMap, netcdfFile, netcdfContent, archiveRunInfo, areaId);
		}
	}

	/**
	 *
	 * @param collection collection
	 * @param timeSeriesType timeSeriesType
	 * @param timeSeriesRecordsMap timeSeriesRecordsMap
	 * @param netcdfFile netcdfFile
	 * @param netcdfContent netcdfContent
	 * @param archiveRunInfo archiveRunInfo
	 * @param areaId areaId
	 * @return Pair<String,List<ObjectId>>
	 */
	private static Pair<String,List<Object>> insertTimeSeries(String collection, TimeSeriesType timeSeriesType, Map<String, Map<String, TimeSeriesRecord>> timeSeriesRecordsMap, File netcdfFile, NetcdfContent netcdfContent, ArchiveRunInfo archiveRunInfo, String areaId){
		var insertedIds = new ArrayList<>();

		var timeSeriesArrays = NetcdfUtil.getTimeSeriesArrays(netcdfFile);
		for (int i = 0; i < timeSeriesArrays.size(); i++) {
			var timeSeriesArray = timeSeriesArrays.get(i);
			var timeSeriesRecord = timeSeriesRecordsMap.get(timeSeriesArray.getHeader().getLocationId()).get(timeSeriesArray.getHeader().getParameterId());

			timeSeriesArray = NetcdfUtil.getTimeSeriesArrayMerged(timeSeriesArray, timeSeriesRecord);

			var ts = Insert.extractTimeSeries(timeSeriesType, timeSeriesArray, netcdfContent, archiveRunInfo, areaId);
			if (ts.containsKey("timeseries"))
				insertedIds.addAll(singleInsertTimeseries(collection, ts, netcdfFile));
		}
		return new Pair<>(collection, insertedIds);
	}

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @param timeSeriesRecordsMap timeSeriesRecordsMap
	 * @param netcdfFile netcdfFile
	 * @param netcdfContent netcdfContent
	 * @param archiveRunInfo archiveRunInfo
	 * @param areaId areaId
	 * @return List<Map<String, Object>>
	 */
	private static List<Map<String, Object>> bulkExtractTimeSeries(TimeSeriesType timeSeriesType, Map<String, Map<String, TimeSeriesRecord>> timeSeriesRecordsMap, File netcdfFile, NetcdfContent netcdfContent, ArchiveRunInfo archiveRunInfo, String areaId){
		var timeSeries = new ArrayList<Map<String, Object>>();

		var timeSeriesArrays = NetcdfUtil.getTimeSeriesArrays(netcdfFile);
		for (int i = 0; i < timeSeriesArrays.size(); i++) {
			var timeSeriesArray = timeSeriesArrays.get(i);
			var timeSeriesRecord = timeSeriesRecordsMap.get(timeSeriesArray.getHeader().getLocationId()).get(timeSeriesArray.getHeader().getParameterId());

			timeSeriesArray = NetcdfUtil.getTimeSeriesArrayMerged(timeSeriesArray, timeSeriesRecord);

			var  ts = Insert.extractTimeSeries(timeSeriesType, timeSeriesArray, netcdfContent, archiveRunInfo, areaId);
			if (ts.containsKey("timeseries"))
				timeSeries.add(ts);
		}
		return timeSeries;
	}

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @param timeSeriesArray timeSeriesArray
	 * @param netcdfContent netcdfContent
	 * @param archiveRunInfo archiveRunInfo
	 * @param areaId areaId
	 * @return Map<String, Object>
	 */
	private static Map<String, Object> extractTimeSeries(TimeSeriesType timeSeriesType, TimeSeriesArray<FewsTimeSeriesHeader> timeSeriesArray, NetcdfContent netcdfContent, ArchiveRunInfo archiveRunInfo, String areaId){
		try {
			var timeSeries = (TimeSeries) Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "shared.timeseries", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(timeSeriesType))).getConstructor().newInstance();
			var header = timeSeriesArray.getHeader();

			areaId = netcdfContent.getAreaId() != null && !netcdfContent.getAreaId().isEmpty() ? netcdfContent.getAreaId() : areaId;

			var metaDataDocument = timeSeries.getMetaData(header, areaId, netcdfContent.getSourceId());
			var eventDocuments = timeSeries.getEvents(timeSeriesArray, metaDataDocument);
			var runInfoDocument = timeSeries.getRunInfo(archiveRunInfo);
			var rootDocument = timeSeries.getRoot(header, eventDocuments, runInfoDocument);
			rootDocument.put("committed", false);

			if(!eventDocuments.isEmpty() && TimeSeriesTypeUtil.getTimeSeriesTypeBucket(timeSeriesType)){
				var startTime = (Date)rootDocument.get("startTime");
				rootDocument.put("bucketSize", BucketSize.VARIABLE.toString());
				rootDocument.put("bucket", BucketUtil.getBucketValue(startTime, BucketSize.SECOND));
			}

			if(!metaDataDocument.isEmpty()) rootDocument.put("metaData", metaDataDocument);
			if(!runInfoDocument.isEmpty()) rootDocument.put("runInfo", runInfoDocument);
			if(!eventDocuments.isEmpty()) rootDocument.put("timeseries", eventDocuments);

			return rootDocument;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param collection collection
	 * @param timeSeries timeSeries
	 * @param netcdfFile netcdfFile
	 * @return Pair<String,List<ObjectId>>
	 */
	private static Pair<String,List<Object>> bulkInsertTimeseries(String collection, List<Map<String, Object>> timeSeries, File netcdfFile){
		var insertedIds = new ArrayList<>();
		try {
			Database.instance().insertMany(collection, timeSeries);
			insertedIds.addAll(timeSeries.stream().map(s -> s.get("_id")).toList());
		}
		catch (RuntimeException ex) {
			Database.instance().deleteMany(collection, Map.of("_id", Map.of("in", timeSeries.stream().filter(s -> s.containsKey("_id") && s.get("_id") != null).map(s -> s.get("_id")).toList())));
			for (var ts : timeSeries)
				insertedIds.addAll(Insert.singleInsertTimeseries(collection, ts, netcdfFile));
		}
		return new Pair<>(collection, insertedIds);
	}

	/**
	 *
	 * @param collection collection
	 * @param timeSeries timeSeries
	 * @param netcdfFile netcdfFile
	 */
	private static List<Object> singleInsertTimeseries(String collection, Map<String, Object> timeSeries, File netcdfFile){
		var insertedIds = new ArrayList<>();
		try {
			Database.instance().insertOne(collection, timeSeries);
			insertedIds.add(timeSeries.get("_id"));
		}
		catch (RuntimeException wex) {
			timeSeries.remove("timeseries");

			var dupKey = wex.getMessage().isEmpty() ? new LinkedHashMap<String, Object>() : new JSONObject(wex.getMessage()).toMap();
			var existingTimeseries = !dupKey.isEmpty() ? Database.instance().findOne(collection, dupKey, Map.of("timeseries", 0)) : null;
			existingTimeseries = existingTimeseries != null ? existingTimeseries : Map.of();

			var existingMetaData = !existingTimeseries.isEmpty() ? Database.instance().findOne(Collection.MigrateMetaData.toString(), Map.of("netcdfFiles.timeSeriesIds", existingTimeseries.get("_id"))) : null;
			existingMetaData = existingMetaData != null ? existingMetaData : Map.of();

			var message = new JSONObject(Map.of(
					"dupKey", dupKey,
					"existingMetaData", existingMetaData,
					"existingTimeseries", existingTimeseries,
					"duplicatedTimeseries", timeSeries,
					"netcdfFile", netcdfFile.toString())).toString();
			logger.warn(String.format("Duplicate: %s", message));
		}
		return insertedIds;
	}
}