package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.LogUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.PathUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

public final class Delete {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(Delete.class);

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
	private Delete(){}

	/**
	 *
	 * @param existingMetaDataFilesFs existingMetaDataFilesFs
	 * @param existingMetaDataFilesDb existingMetaDataFilesDb
	 */
	public static void deleteMetaDatas(Map<File, Date> existingMetaDataFilesFs, Map<File, Date> existingMetaDataFilesDb) throws ExecutionException, InterruptedException {
		ExecutorService pool = Executors.newFixedThreadPool(Settings.get("databaseBaseThreads"));
		Map<File, Date> metaDataFiles = MetaDataUtil.getMetaDataFilesDelete(existingMetaDataFilesFs, existingMetaDataFilesDb);
		ArrayList<Callable<Void>> tasks = new ArrayList<>();
		progressExpected = metaDataFiles.size();
		progressCurrent = 0;

		class DeleteMetaData implements Callable<Void> {
			private final File file;
			public DeleteMetaData(File file) {
				this.file = file;
			}
			@Override
			public Void call() {
				deleteMetaData(file);
				synchronized (mutex){
					if (++progressCurrent % 100 == 0)
						logger.info("Delete Progress: {}/{} {}", progressCurrent, progressExpected, String.format("%,.2f%%", ((double)progressCurrent/progressExpected*100)));
				}
				return null;
			}
		}

		metaDataFiles.forEach((file, date) -> tasks.add(new DeleteMetaData(file)));
		List<Future<Void>> results = pool.invokeAll(tasks);
		for (Future<Void> x : results) {
			x.get();
		}
		pool.shutdown();

		logger.info("Delete Progress: {}/{} {}", progressCurrent, progressExpected, String.format("%,.2f%%", ((double)progressCurrent/progressExpected*100)));
	}

	/**
	 *
	 * @param metaDataFile metaDataFile
	 */
	public static void deleteMetaData(File metaDataFile){
		try {
			Document metaData = Database.findOne(Settings.get("metaDataCollection"), new Document("metaDataFileRelativePath", PathUtil.toRelativePathString(metaDataFile, Settings.get("baseDirectoryArchive", String.class))));
			if (metaData != null) {
				Database.updateOne(Settings.get("metaDataCollection"), new Document("_id", metaData.getObjectId("_id")), new Document("$set", new Document("committed", false)));
				metaData.getList("netcdfFiles", Document.class).parallelStream().forEach(Delete::deleteNetcdf);
				Database.deleteOne(Settings.get("metaDataCollection"), new Document("_id", metaData.getObjectId("_id")));
			}
		}
		catch (Exception ex){
			logger.warn(LogUtil.getLogMessageJson(ex, Map.of("metaDataFile", metaDataFile.toString())).toJson(), ex);
		}
	}

	/**
	 *
	 * @param netcdfFile netcdfFile
	 */
	private static void deleteNetcdf(Document netcdfFile){
		if(netcdfFile != null && netcdfFile.getString("collection") != null && !netcdfFile.getList("timeSeriesIds", ObjectId.class).isEmpty()){
			Database.deleteMany(netcdfFile.getString("collection"), new Document("_id", new Document("$in", netcdfFile.getList("timeSeriesIds", ObjectId.class))));
		}
	}

	/**
	 *
	 */
	public static void deleteUncommitted(){
		Arrays.stream(TimeSeriesType.values()).filter(s -> TimeSeriesTypeUtil.getTimeSeriesTypeCollection(s) != null).parallel().forEach(
				s -> Database.deleteMany(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(s), new Document("committed", false)));

		Database.find(Settings.get("metaDataCollection"), new Document("committed", false), new Document("_id", 0).append("metaDataFileRelativePath", 1)).forEach(
				s -> Delete.deleteMetaData(PathUtil.fromRelativePathString(s.getString("metaDataFileRelativePath"), Settings.get("baseDirectoryArchive", String.class))));

		Database.deleteMany(Settings.get("metaDataCollection"), new Document("committed", false));
	}
}
