package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.utils.DatabaseUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.NetcdfUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.RunInfoUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.BucketUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.LogUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.PathUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfContent;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfMetaData;
import nl.wldelft.archive.util.metadata.timeseries.TimeSeriesRecord;
import nl.wldelft.archive.util.runinfo.ArchiveRunInfo;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.javatuples.Pair;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;

/**
 *
 */
@SuppressWarnings({"ConstantConditions"})
public final class Insert {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(Insert.class);

	/**
	 *
	 */
	private static final String BASE_NAMESPACE = "nl.fews.archivedatabase.mongodb";

	/**
	 * Static Class
	 */
	private Insert(){}

	/**
	 *
	 * @param existingMetaDataFilesFs existingMetaDataFilesFs
	 * @param existingMetaDataFilesDb existingMetaDataFilesDb
	 */
	public static void insertMetaDatas(Map<File, Date> existingMetaDataFilesFs, Map<File, Date> existingMetaDataFilesDb){
		ForkJoinPool pool = new ForkJoinPool(Settings.get("databaseBaseThreads"));
		ArrayList<Callable<Void>> tasks = new ArrayList<>();
		MetaDataUtil.getMetaDataFilesInsert(existingMetaDataFilesFs, existingMetaDataFilesDb).forEach((file, date) -> tasks.add(() -> {
			insertMetaData(file, date);
			return null;
		}));
		pool.invokeAll(tasks);
		pool.shutdown();
	}

	/**
	 *
	 * @param metaDataFile metaDataFile
	 * @param metaDataValue metaDataValue
	 */
	public static void insertMetaData(File metaDataFile, Date metaDataValue){
		try{
			NetcdfMetaData netcdfMetaData = MetaDataUtil.getNetcdfMetaData(metaDataFile);
			if (netcdfMetaData == null)
				return;
			ArchiveRunInfo archiveRunInfo = RunInfoUtil.getRunInfo(netcdfMetaData);
			Map<File, NetcdfContent> netcdfContentMap = MetaDataUtil.getNetcdfContentMap(metaDataFile, netcdfMetaData);
			Map<File, Pair<Date, NetcdfContent>> netcdfFiles = NetcdfUtil.getExistingNetcdfFilesFs(metaDataFile, netcdfMetaData);
			Map<String, List<ObjectId>> allInsertedIds = new HashMap<>();

			Document metaDataDocument = new Document();
			metaDataDocument.append("metaDataFileRelativePath", PathUtil.toRelativePathString(metaDataFile, Settings.get("baseDirectoryArchive", String.class)));
			metaDataDocument.append("metaDataFileTime", metaDataValue);
			metaDataDocument.append("netcdfFiles", new ArrayList<Document>());
			metaDataDocument.append("committed", false);

			netcdfFiles.forEach((netcdfFile, dateNetcdf) -> {
				NetcdfContent netcdfContent = netcdfContentMap.get(netcdfFile);
				Pair<String, List<ObjectId>> insertedIds = Insert.insertNetcdfs(netcdfFile, netcdfContent, archiveRunInfo);
				if (insertedIds.getValue1() != null && !insertedIds.getValue1().isEmpty()) {
					allInsertedIds.putIfAbsent(insertedIds.getValue0(), new ArrayList<>());
					allInsertedIds.get(insertedIds.getValue0()).addAll(insertedIds.getValue1());
					Document netcdfFileEntry = new Document();
					netcdfFileEntry.append("netcdfFileRelativePath", PathUtil.toRelativePathString(netcdfFile, Settings.get("baseDirectoryArchive", String.class)));
					netcdfFileEntry.append("netcdfFileTime", dateNetcdf.getValue0());
					netcdfFileEntry.append("timeSeriesIds", insertedIds.getValue1());
					netcdfFileEntry.append("collection", insertedIds.getValue0());
					metaDataDocument.getList("netcdfFiles", Document.class).add(netcdfFileEntry);
				}
			});
			ObjectId insertedId = Database.insertOne(Settings.get("metaDataCollection"), metaDataDocument).getInsertedId().asObjectId().getValue();
			commitInserted(insertedId, allInsertedIds);
		}
		catch (Exception ex){
			logger.warn(LogUtil.getLogMessageJson(ex, Map.of("metaDataFile", metaDataFile.toString())).toJson(), ex);
		}
	}

	/**
	 *
	 * @param metaDataInsertedId metaDataInsertedId
	 * @param allInsertedIds allInsertedIds
	 */
	private static void commitInserted(ObjectId metaDataInsertedId, Map<String, List<ObjectId>> allInsertedIds){
		allInsertedIds.forEach((collection, insertedIds) -> {
			if(collection != null && !insertedIds.isEmpty())
				Database.updateMany(collection, new Document("_id", new Document("$in", insertedIds)), new Document("$set", new Document("committed", true)));
		});
		Database.updateOne(Settings.get("metaDataCollection"), new Document("_id", metaDataInsertedId), new Document("$set", new Document("committed", true)));
	}

	/**
	 *
	 * @param netcdfFile netcdfFile
	 * @param netcdfContent netcdfContent
	 * @param archiveRunInfo archiveRunInfo
	 * @return List<ObjectId>
	 */
	private static Pair<String, List<ObjectId>> insertNetcdfs(File netcdfFile, NetcdfContent netcdfContent, ArchiveRunInfo archiveRunInfo) {

		if(netcdfFile == null || !netcdfFile.exists())
			return new Pair<>(null, new ArrayList<>());

		Map<String, Map<String, TimeSeriesRecord>> timeSeriesRecordsMap = NetcdfUtil.getTimeSeriesRecordsMap(netcdfFile, netcdfContent);
		TimeSeriesRecord timeSeriesRecord = timeSeriesRecordsMap.values().stream().findFirst().orElse(new HashMap<>()).values().stream().findFirst().orElse(null);
		if(timeSeriesRecord == null)
			return new Pair<>(null, new ArrayList<>());

		TimeSeriesType timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>(netcdfContent.getValueType().toString(), nl.wldelft.fews.system.data.timeseries.TimeSeriesType.getByIntId(timeSeriesRecord.getTimeSeriesType()).getName()));
		if(timeSeriesType == null)
			return new Pair<>(null, new ArrayList<>());

		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		if(TimeSeriesTypeUtil.getTimeSeriesTypeClassName(timeSeriesType) == null)
			return new Pair<>(null, new ArrayList<>());

		List<Document> timeSeries = extract(timeSeriesType, timeSeriesRecordsMap, netcdfFile, netcdfContent, archiveRunInfo);
		if (timeSeries.isEmpty())
			return new Pair<>(collection, new ArrayList<>());

		if(timeSeries.stream().filter(s -> s.getString("encodedTimeStepId").equals("nonequidistant")).anyMatch(s -> s.getList("timeseries", Document.class).size() > BucketUtil.TIME_SERIES_MAX_ENTRY_COUNT)){
			timeSeries.forEach(ts -> BucketUtil.ensureBucketSize(timeSeriesType, ts));
			timeSeries = extract(timeSeriesType, timeSeriesRecordsMap, netcdfFile, netcdfContent, archiveRunInfo);
		}

		if(timeSeries.stream().filter(s -> s.getString("encodedTimeStepId").equals("nonequidistant")).anyMatch(s -> s.getList("timeseries", Document.class).size() > BucketUtil.TIME_SERIES_MAX_ENTRY_COUNT))
			throw new IllegalStateException("Cannot resolve nonequidistant bucket size.");

		return DatabaseUtil.synchronize(collection, timeSeries, netcdfFile);
	}

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @param timeSeriesRecordsMap timeSeriesRecordsMap
	 * @param netcdfFile netcdfFile
	 * @param netcdfContent netcdfContent
	 * @param archiveRunInfo archiveRunInfo
	 * @return List<Document>
	 */
	private static List<Document> extract(TimeSeriesType timeSeriesType, Map<String, Map<String, TimeSeriesRecord>> timeSeriesRecordsMap, File netcdfFile, NetcdfContent netcdfContent, ArchiveRunInfo archiveRunInfo){
		List<Document> timeSeries = new ArrayList<>();

		TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = NetcdfUtil.getTimeSeriesArrays(netcdfFile);
		for (int i = 0; i < timeSeriesArrays.size(); i++) {
			TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = timeSeriesArrays.get(i);
			TimeSeriesRecord timeSeriesRecord = timeSeriesRecordsMap.get(timeSeriesArray.getHeader().getLocationId()).get(timeSeriesArray.getHeader().getParameterId());

			timeSeriesArray = NetcdfUtil.getTimeSeriesArrayMerged(timeSeriesArray, timeSeriesRecord);

			Document ts = getTimeSeries(timeSeriesType, timeSeriesArray, netcdfContent, archiveRunInfo);
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
	 * @return Document
	 */
	private static Document getTimeSeries(TimeSeriesType timeSeriesType, TimeSeriesArray<TimeSeriesHeader> timeSeriesArray, NetcdfContent netcdfContent, ArchiveRunInfo archiveRunInfo){
		try {
			TimeSeries timeSeries = (TimeSeries) Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "shared.timeseries", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(timeSeriesType))).getConstructor().newInstance();
			TimeSeriesHeader header = timeSeriesArray.getHeader();

			List<Document> eventDocuments = timeSeries.getEvents(timeSeriesArray);
			Document metaDataDocument = timeSeries.getMetaData(header, netcdfContent.getAreaId(), netcdfContent.getSourceId());
			Document runInfoDocument = timeSeries.getRunInfo(archiveRunInfo);
			Document rootDocument = timeSeries.getRoot(header, eventDocuments, runInfoDocument).append("committed", false);

			if(!eventDocuments.isEmpty() && TimeSeriesTypeUtil.getTimeSeriesTypeBucket(timeSeriesType)){
				Date startTime = rootDocument.getDate("startTime");
				Date endTime = rootDocument.getDate("endTime");

				BucketSize bucketSize = BucketUtil.getArchiveInferredBucketSize(startTime, endTime);
				long bucketValue = BucketUtil.getBucketValue(startTime, bucketSize);
				rootDocument.append("bucketSize", bucketSize.toString());
				rootDocument.append("bucket", bucketValue);
			}

			if(!metaDataDocument.isEmpty()) rootDocument.append("metaData", metaDataDocument);
			if(!runInfoDocument.isEmpty()) rootDocument.append("runInfo", runInfoDocument);
			if(!eventDocuments.isEmpty()) rootDocument.append("timeseries", eventDocuments);

			return rootDocument;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}