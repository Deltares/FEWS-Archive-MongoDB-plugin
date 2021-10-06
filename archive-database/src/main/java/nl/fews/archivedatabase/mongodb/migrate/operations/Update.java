package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.LogUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.PathUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

/**
 *
 */
public final class Update {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(Update.class);

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
	private Update(){}

	/**
	 *
	 * @param existingMetaDataFilesFs existingMetaDataFilesFs
	 * @param existingMetaDataFilesDb existingMetaDataFilesDb
	 */
	public static void updateMetaDatas(Map<File, Date> existingMetaDataFilesFs, Map<File, Date> existingMetaDataFilesDb) throws ExecutionException, InterruptedException {
		ForkJoinPool pool = new ForkJoinPool(Settings.get("databaseBaseThreads"));
		ArrayList<Callable<Void>> tasks = new ArrayList<>();
		Map<File, Date> metaDataFiles = MetaDataUtil.getMetaDataFilesUpdate(existingMetaDataFilesFs, existingMetaDataFilesDb);
		progressExpected = metaDataFiles.size();
		progressCurrent = 0;
		metaDataFiles.forEach((file, date) -> tasks.add(() -> {
			updateMetaData(file, date);
			synchronized (mutex){
				if (++progressCurrent % 100 == 0)
					logger.info("Update Progress: {}/{} {}%", progressCurrent, progressExpected, String.format("%,.2f", ((double)progressCurrent/progressExpected*100)));
			}
			return null;
		}));
		List<Future<Void>> results = pool.invokeAll(tasks);
		for (Future<Void> x : results) {
			x.get();
		}
		pool.shutdown();
		logger.info("Update Progress: {}/{} {}%", progressCurrent, progressExpected, String.format("%,.2f", ((double)progressCurrent/progressExpected*100)));
	}

	/**
	 *
	 * @param metaDataFile metaDataFile
	 * @param metaDataDate metaDataDate
	 */
	private static void updateMetaData(File metaDataFile, Date metaDataDate) {
		try {
			Document dbMetaData = Database.findOne(Settings.get("metaDataCollection"), new Document("metaDataFileRelativePath", PathUtil.toRelativePathString(metaDataFile, Settings.get("baseDirectoryArchive", String.class))));
			if (dbMetaData != null) {
				Delete.deleteMetaData(metaDataFile);
				Insert.insertMetaData(metaDataFile, metaDataDate);
			}
		}
		catch (Exception ex){
			logger.warn(LogUtil.getLogMessageJson(ex, Map.of("metaDataFile", metaDataFile.toString())).toJson(), ex);
		}
	}
}
