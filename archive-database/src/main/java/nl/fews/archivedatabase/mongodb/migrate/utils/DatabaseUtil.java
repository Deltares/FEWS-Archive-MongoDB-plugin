package nl.fews.archivedatabase.mongodb.migrate.utils;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoWriteException;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.LogUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.javatuples.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class DatabaseUtil {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(DatabaseUtil.class);

	/**
	 *
	 */
	private static final Pattern dupKeyPattern = Pattern.compile("(\\{.*})");

	/**
	 *
	 */
	private DatabaseUtil(){}

	/**
	 *
	 * @param collection collection
	 * @param timeSeries timeSeries
	 * @param netcdfFile netcdfFile
	 * @return Pair<String,List<ObjectId>>
	 */
	public static Pair<String,List<ObjectId>> synchronize(String collection, List<Document> timeSeries, File netcdfFile){
		List<ObjectId> insertedIds = new ArrayList<>();
		try {
			Database.insertMany(collection, timeSeries);
			insertedIds.addAll(timeSeries.stream().map(s -> s.getObjectId("_id")).collect(Collectors.toList()));
		}
		catch (MongoBulkWriteException ex) {
			Database.deleteMany(collection, new Document("_id", new Document("$in", timeSeries.stream().filter(s -> s.containsKey("_id") && s.getObjectId("_id") != null).map(s -> s.getObjectId("_id")).collect(Collectors.toList()))));
			for (Document ts : timeSeries)
				insertNetcdf(collection, ts, insertedIds, netcdfFile);
		}
		return new Pair<>(collection, insertedIds);
	}

	/**
	 *
	 * @param collection collection
	 * @param timeSeries timeSeries
	 * @param insertedIds insertedIds
	 * @param netcdfFile netcdfFile
	 */
	private static void insertNetcdf(String collection, Document timeSeries, List<ObjectId> insertedIds, File netcdfFile){
		try {
			Database.insertOne(collection, timeSeries);
			insertedIds.add(timeSeries.getObjectId("_id"));
		}
		catch (MongoWriteException wex) {
			timeSeries.remove("timeseries");

			Matcher matcher = dupKeyPattern.matcher(wex.getError().getMessage());
			Document dupKey = matcher.find() ? Document.parse(matcher.group(1).replace("\",\"", "\\\",\\\"").replace("\"[\"", "\"[\\\"").replace("\"]\"", "\\\"]\"")) : new Document();

			Document existingTimeseries = !dupKey.isEmpty() ? Database.findOne(collection, dupKey, new Document("timeseries", 0)) : null;
			existingTimeseries = existingTimeseries != null ? existingTimeseries : new Document();

			Document existingMetaData = !existingTimeseries.isEmpty() ? Database.findOne(Settings.get("metaDataCollection"), new Document("netcdfFiles.timeSeriesIds", existingTimeseries.getObjectId("_id"))) : null;
			existingMetaData = existingMetaData != null ? existingMetaData : new Document();

			Document message = LogUtil.getLogMessageJson(wex, Map.of(
					"dupKey", dupKey,
					"existingMetaData", existingMetaData,
					"existingTimeseries", existingTimeseries,
					"duplicatedTimeseries", timeSeries,
					"netcdfFile", netcdfFile.toString()));
			logger.warn(message.toJson(), wex);
		}
	}
}
