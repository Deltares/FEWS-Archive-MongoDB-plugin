package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.PathUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

public final class Delete {

	/**
	 * Static Class
	 */
	private Delete(){}

	/**
	 *
	 * @param existingMetaDataFilesFs existingMetaDataFilesFs
	 * @param existingMetaDataFilesDb existingMetaDataFilesDb
	 */
	public static void deleteMetaDatas(Map<File, Date> existingMetaDataFilesFs, Map<File, Date> existingMetaDataFilesDb){
		try {
			ForkJoinPool pool = new ForkJoinPool(Settings.get("dbThreads"));
			ArrayList<Callable<Void>> tasks = new ArrayList<>();
			MetaDataUtil.getMetaDataFilesDelete(existingMetaDataFilesFs, existingMetaDataFilesDb).forEach((file, date) -> tasks.add(() -> {
				deleteMetaData(file);
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
	 */
	public static void deleteMetaData(File metaDataFile){
		Document metaData = Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("metaDataCollection")).find(new Document("metaDataFileRelativePath", PathUtil.toRelativePathString(metaDataFile, Settings.get("archiveRootDataFolder", String.class)))).first();
		if (metaData != null){
			Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("metaDataCollection")).updateOne(new Document("_id", metaData.getObjectId("_id")), new Document("$set", new Document("committed", false)));
			metaData.getList("netcdfFiles", Document.class).parallelStream().forEach(Delete::deleteNetcdf);
			Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("metaDataCollection")).deleteOne(new Document("_id", metaData.getObjectId("_id")));
		}
	}

	/**
	 *
	 * @param netcdfFile netcdfFile
	 */
	private static void deleteNetcdf(Document netcdfFile){
		if(netcdfFile != null && netcdfFile.getString("collection") != null && !netcdfFile.getList("timeSeriesIds", ObjectId.class).isEmpty()){
			Database.create().getDatabase(Database.getDatabaseName()).getCollection(netcdfFile.getString("collection")).deleteMany(new Document("_id", new Document("$in", netcdfFile.getList("timeSeriesIds", ObjectId.class))));
		}
	}

	/**
	 *
	 */
	public static void deleteUncommitted(){
		Arrays.stream(TimeSeriesType.values()).filter(s -> TimeSeriesTypeUtil.getTimeSeriesTypeCollection(s) != null).parallel().forEach(
				s -> Database.create().getDatabase(Database.getDatabaseName()).getCollection(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(s)).deleteMany(new Document("committed", false)));

		Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("metaDataCollection")).find(new Document("committed", false)).projection(new Document("_id", 0).append("metaDataFileRelativePath", 1)).forEach(
				s -> Delete.deleteMetaData(PathUtil.fromRelativePathString(s.getString("metaDataFileRelativePath"), Settings.get("archiveRootDataFolder", String.class))));

		Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("metaDataCollection")).deleteMany(new Document("committed", false));
	}
}
