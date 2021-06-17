package nl.fews.archivedatabase.mongodb.migrate.operations;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoWriteException;
import nl.fews.archivedatabase.mongodb.migrate.interfaces.TimeSeriesExtractor;
import nl.fews.archivedatabase.mongodb.migrate.timeseries.ScalarTimeSeriesExtractor;
import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.NetcdfUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.RunInfoUtil;
import nl.fews.archivedatabase.mongodb.migrate.utils.TimeSeriesSetsUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.LogUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.PathUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.javatuples.Pair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

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
	 * Static Class
	 */
	private Insert(){}

	/**
	 *
	 * @param existingMetaDataFilesFs existingMetaDataFilesFs
	 * @param existingMetaDataFilesDb existingMetaDataFilesDb
	 */
	public static void insertMetaDatas(Map<File, Date> existingMetaDataFilesFs, Map<File, Date> existingMetaDataFilesDb){
		try {
			ForkJoinPool pool = new ForkJoinPool(Settings.get("dbThreads"));
			ArrayList<Callable<Void>> tasks = new ArrayList<>();
			MetaDataUtil.getMetaDataFilesInsert(existingMetaDataFilesFs, existingMetaDataFilesDb).forEach((file, date) -> tasks.add(() -> {
				insertMetaData(file, date);
				return null;
			}));
			List<Future<Void>> results = pool.invokeAll(tasks);

			for (Future<Void> x : results) {
				x.get();
			}
			pool.shutdown();
		}
		catch (Exception ex){
			Thread.currentThread().interrupt();
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param metaDataFile metaDataFile
	 * @param metaDataValue metaDataValue
	 */
	public static void insertMetaData(File metaDataFile, Date metaDataValue){
		JSONObject metaData = MetaDataUtil.readMetaData(metaDataFile);
		JSONObject runInfo = RunInfoUtil.readRunInfo(RunInfoUtil.getRunInfoFile(metaData));
		Map<File, Pair<Date, JSONObject>> netcdfFiles = NetcdfUtil.getExistingNetcdfFilesFs(metaData);

		Map<String, List<ObjectId>> allInsertedIds = new HashMap<>();

		Document metaDataDocument = new Document();
		metaDataDocument.append("metaDataFileRelativePath", PathUtil.toRelativePathString(metaDataFile, Settings.get("archiveRootDataFolder", String.class)));
		metaDataDocument.append("metaDataFileTime", metaDataValue);
		metaDataDocument.append("netcdfFiles", new ArrayList<Document>());
		metaDataDocument.append("committed", false);

		netcdfFiles.forEach((netcdfFile, dateNetcdf) -> {
			try {
				Pair<String, List<ObjectId>> insertedIds = Insert.insertNetcdf(netcdfFile, dateNetcdf.getValue1(), metaData, runInfo);
				if (insertedIds.getValue1() != null && !insertedIds.getValue1().isEmpty()) {
					allInsertedIds.putIfAbsent(insertedIds.getValue0(), new ArrayList<>());
					allInsertedIds.get(insertedIds.getValue0()).addAll(insertedIds.getValue1());
					Document netcdfFileEntry = new Document();
					netcdfFileEntry.append("netcdfFileRelativePath", PathUtil.toRelativePathString(netcdfFile, Settings.get("archiveRootDataFolder", String.class)));
					netcdfFileEntry.append("netcdfFileTime", dateNetcdf.getValue0());
					netcdfFileEntry.append("timeSeriesIds", insertedIds.getValue1());
					netcdfFileEntry.append("collection", insertedIds.getValue0());
					metaDataDocument.getList("netcdfFiles", Document.class).add(netcdfFileEntry);
				}
			}
			catch (Exception ex){
				throw ex;
			}
		});
		try {
			ObjectId insertedId = Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("metaDataCollection")).insertOne(metaDataDocument).getInsertedId().asObjectId().getValue();
			commitInserted(insertedId, allInsertedIds);
		}
		catch (Exception ex){
			throw ex;
		}
	}

	/**
	 *
	 * @param metaDataInsertedId metaDataInsertedId
	 * @param allInsertedIds allInsertedIds
	 */
	private static void commitInserted(ObjectId metaDataInsertedId, Map<String, List<ObjectId>> allInsertedIds){
		allInsertedIds.forEach((collection, insertedIds) -> {
			if(collection != null && !insertedIds.isEmpty()) {
				try{
					Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).updateMany(new Document("_id", new Document("$in", insertedIds)), new Document("$set", new Document("committed", true)));
				}
				catch (Exception ex){
					throw ex;
				}
			}
			else
				System.out.println();
		});
		Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("metaDataCollection")).updateOne(new Document("_id", metaDataInsertedId), new Document("$set", new Document("committed", true)));
	}

	/**
	 *
	 * @param netcdfFile netcdfFile
	 * @param netcdfMetaData netcdfMetaData
	 * @param metaData metaData
	 * @param runInfo runInfo
	 * @return List<ObjectId>
	 */
	private static Pair<String, List<ObjectId>> insertNetcdf(File netcdfFile, JSONObject netcdfMetaData, JSONObject metaData, JSONObject runInfo) {
		List<ObjectId> insertedIds = new ArrayList<>();
		if(netcdfFile == null)
			return new Pair<>(null, insertedIds);

		JSONArray timeSeriesSets = TimeSeriesSetsUtil.getDecomposedTimeSeriesSets(netcdfFile);

		TimeSeriesType timeSeriesType = getTimeSeriesType(metaData, timeSeriesSets);
		if(timeSeriesType == null)
			return new Pair<>(null, insertedIds);

		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		if(TimeSeriesTypeUtil.getTimeSeriesTypeClassName(timeSeriesType) == null)
			return new Pair<>(null, insertedIds);

		TimeSeriesExtractor timeSeriesExtractor = new ScalarTimeSeriesExtractor();
		List<Document> timeSeries = timeSeriesExtractor.extract(timeSeriesType, netcdfFile, timeSeriesSets, netcdfMetaData, runInfo);
		if(!timeSeries.isEmpty()) {
			try {
				Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).insertMany(timeSeries);
				insertedIds.addAll(timeSeries.stream().map(s -> s.getObjectId("_id")).collect(Collectors.toList()));
			}
			catch (MongoBulkWriteException ex) {
				insertedIds.addAll(ex.getWriteResult().getInserts().stream().map(s -> s.getId().asObjectId().getValue()).collect(Collectors.toList()));
				for (Document ts : timeSeries) {
					try {
						Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).insertOne(ts);
						insertedIds.add(ts.getObjectId("_id"));
					}
					catch (MongoWriteException wex) {
						JSONObject message = LogUtil.getLogMessageJson(wex, Map.of("netcdfFile", netcdfFile.toString(), "netcdfMetaData", netcdfMetaData, "metaData", metaData, "runInfo", runInfo));
						logger.warn(message.toString(), wex);
					}
				}
			}
			catch(Exception ex){
				throw ex;
			}
		}
		return new Pair<>(collection, insertedIds);
	}

	/**
	 *
	 * @param metaData metaData
	 * @param timeSeriesSets timeSeriesSets
	 * @return TimeSeriesType
	 */
	private static TimeSeriesType getTimeSeriesType(JSONObject metaData, JSONArray timeSeriesSets){
		try {
			TimeSeriesType timeSeriesType = null;

			String timeSeriesSetTypeString = !timeSeriesSets.isEmpty() && timeSeriesSets.getJSONObject(0).has("timeSeriesType") ? timeSeriesSets.getJSONObject(0).getString("timeSeriesType") : null;
			TimeSeriesTypeUtil.TimeSeriesTypeDetermination timeSeriesTypeDetermination = Settings.get("properties") != null ? TimeSeriesTypeUtil.TimeSeriesTypeDetermination.valueOf(Settings.get("properties", Properties.class).getString("TimeSeriesTypeDetermination", "TimeSeriesType")) : TimeSeriesTypeUtil.TimeSeriesTypeDetermination.TimeSeriesType;

			if (timeSeriesTypeDetermination == TimeSeriesTypeUtil.TimeSeriesTypeDetermination.TimeSeriesType) {
				timeSeriesType = timeSeriesSetTypeString != null ? TimeSeriesTypeUtil.getTimeSeriesTypeByTypeString(new Pair<>(metaData.getString("valueType"), timeSeriesSetTypeString)) : null;
				timeSeriesType = timeSeriesType == null ? TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>(metaData.getString("valueType"), metaData.getString("metaDataType"))) : timeSeriesType;
			}
			else if (timeSeriesTypeDetermination == TimeSeriesTypeUtil.TimeSeriesTypeDetermination.MetaDataType) {
				timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesType(new Pair<>(metaData.getString("valueType"), metaData.getString("metaDataType")));
				timeSeriesType = timeSeriesType == TimeSeriesType.SCALAR_SIMULATED_FORECASTING && TimeSeriesTypeUtil.SIMULATED_HISTORICAL.equals(timeSeriesSetTypeString) ? TimeSeriesType.SCALAR_SIMULATED_HISTORICAL : timeSeriesType;
			}

			return timeSeriesType;
		}
		catch (Exception ex){
			throw ex;
		}
	}
}