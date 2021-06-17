package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.PathUtil;
import org.bson.Document;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

/**
 *
 */
public final class Update {

	/**
	 * Static Class
	 */
	private Update(){}

	/**
	 *
	 * @param existingMetaDataFilesFs existingMetaDataFilesFs
	 * @param existingMetaDataFilesDb existingMetaDataFilesDb
	 */
	public static void updateMetaDatas(Map<File, Date> existingMetaDataFilesFs, Map<File, Date> existingMetaDataFilesDb){
		try {
			ForkJoinPool pool = new ForkJoinPool(Settings.get("dbThreads"));
			ArrayList<Callable<Void>> tasks = new ArrayList<>();
			MetaDataUtil.getMetaDataFilesUpdate(existingMetaDataFilesFs, existingMetaDataFilesDb).forEach((file, date) -> tasks.add(() -> {
				updateMetaData(file, date);
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
	 * @param metaDataDate metaDataDate
	 */
	private static void updateMetaData(File metaDataFile, Date metaDataDate) {
		Document dbMetaData = Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("metaDataCollection")).find(new Document("metaDataFileRelativePath", PathUtil.toRelativePathString(metaDataFile, Settings.get("archiveRootDataFolder", String.class)))).first();
		if(dbMetaData != null){
			Delete.deleteMetaData(metaDataFile);
			Insert.insertMetaData(metaDataFile, metaDataDate);
		}
	}
}
