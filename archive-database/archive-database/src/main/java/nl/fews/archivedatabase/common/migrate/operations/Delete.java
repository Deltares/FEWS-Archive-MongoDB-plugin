package nl.fews.archivedatabase.common.migrate.operations;

import nl.fews.archivedatabase.common.migrate.utils.MetaDataUtil;
import nl.fews.archivedatabase.common.shared.interfaces.DatabaseBridge;
import nl.fews.archivedatabase.common.shared.database.Collection;
import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.common.shared.utils.LogUtil;
import nl.fews.archivedatabase.common.shared.utils.PathUtil;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesTypeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

@SuppressWarnings("unchecked")
public final class Delete {

	/**
	 *
	 */
	private static final DatabaseBridge database;

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

	static{
		try{
			database = (DatabaseBridge)Class.forName(String.format(Settings.DATABASE_NAMESPACE, Settings.get("databaseType", String.class).toLowerCase())).getConstructor().newInstance();
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

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
		var pool = Executors.newFixedThreadPool(Settings.get("databaseBaseThreads"));
		var metaDataFiles = MetaDataUtil.getMetaDataFilesDelete(existingMetaDataFilesFs, existingMetaDataFilesDb);
		var tasks = new ArrayList<Callable<Void>>();
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
		for (Future<Void> x : pool.invokeAll(tasks)) 
			x.get();
		pool.shutdown();

		logger.info("Delete Progress: {}/{} {}", progressCurrent, progressExpected, String.format("%,.2f%%", ((double)progressCurrent/progressExpected*100)));
	}

	/**
	 *
	 * @param metaDataFile metaDataFile
	 */
	public static void deleteMetaData(File metaDataFile){
		try {
			var metaData = database.findOne(Collection.MigrateMetaData.toString(), Map.of("metaDataFileRelativePath", PathUtil.toRelativePathString(metaDataFile, Settings.get("baseDirectoryArchive", String.class))));
			if (metaData != null) {
				database.updateOne(Collection.MigrateMetaData.toString(), Map.of("_id", metaData.get("_id")), Map.of("set", Map.of("committed", false)));
				((List<Map<String, Object>>)metaData.get("netcdfFiles")).parallelStream().forEach(Delete::deleteNetcdf);
				database.deleteOne(Collection.MigrateMetaData.toString(), Map.of("_id", metaData.get("_id")));
			}
		}
		catch (Exception ex){
			logger.warn(new JSONObject(LogUtil.getLogMessageJson(ex, Map.of("metaDataFile", metaDataFile.toString()))).toString(), ex);
		}
	}

	/**
	 *
	 * @param netcdfFile netcdfFile
	 */
	private static void deleteNetcdf(Map<String, Object> netcdfFile){
		if(netcdfFile != null && netcdfFile.get("collection") != null && !((List<Object>)netcdfFile.get("timeSeriesIds")).isEmpty()){
			database.deleteMany((String)netcdfFile.get("collection"), Map.of("_id", Map.of("in", (List<Object>)netcdfFile.get("timeSeriesIds"))));
		}
	}

	/**
	 *
	 */
	public static void deleteUncommitted(){
		Arrays.stream(TimeSeriesType.values()).filter(s -> TimeSeriesTypeUtil.getTimeSeriesTypeCollection(s) != null).parallel().forEach(
				s -> database.deleteMany(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(s), Map.of("committed", false)));

		database.find(Collection.MigrateMetaData.toString(), Map.of("committed", false), Map.of("_id", 0, "metaDataFileRelativePath", 1)).forEach(
			s -> Delete.deleteMetaData(PathUtil.fromRelativePathString((String)s.get("metaDataFileRelativePath"), Settings.get("baseDirectoryArchive", String.class))));

		database.deleteMany(Collection.MigrateMetaData.toString(), Map.of("committed", false));
	}
}
