package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.PathUtil;
import nl.wldelft.archive.util.metadata.externalforecast.ExternalForecastMetaDataReader;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfMetaData;
import nl.wldelft.archive.util.metadata.observed.MetaDataReader;
import nl.wldelft.archive.util.metadata.simulation.SimulationMetaDataReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.json.JSONObject;
import org.json.XML;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public final class MetaDataUtil {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(MetaDataUtil.class);

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
	private MetaDataUtil(){}

	/**
	 *
	 * @param existingMetaDataFilesFs existingMetaDataFs
	 * @param existingMetaDataFilesDb existingMetaDataDb
	 * @return Map<File, Date>
	 */
	public static Map<File, Date> getMetaDataFilesDelete(Map<File, Date> existingMetaDataFilesFs, Map<File, Date> existingMetaDataFilesDb){
		return existingMetaDataFilesDb.entrySet().stream().filter(m -> !existingMetaDataFilesFs.containsKey(m.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	/**
	 *
	 * @param existingMetaDataFilesFs existingMetaDataFs
	 * @param existingMetaDataFilesDb existingMetaDataDb
	 * @return Map<File, Date>
	 */
	public static Map<File, Date> getMetaDataFilesUpdate(Map<File, Date> existingMetaDataFilesFs, Map<File, Date> existingMetaDataFilesDb){
		return existingMetaDataFilesFs.entrySet().stream().filter(key -> existingMetaDataFilesDb.containsKey(key.getKey()) && !existingMetaDataFilesDb.get(key.getKey()).equals(key.getValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	/**
	 *
	 * @param existingMetaDataFilesFs existingMetaDataFs
	 * @param existingMetaDataFilesDb existingMetaDataDb
	 * @return Map<File, Date>
	 */
	public static Map<File, Date> getMetaDataFilesInsert(Map<File, Date> existingMetaDataFilesFs, Map<File, Date> existingMetaDataFilesDb){
		return existingMetaDataFilesFs.entrySet().stream().filter(m -> !existingMetaDataFilesDb.containsKey(m.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	/**
	 *
	 * @return Map<File, Date>
	 */
	public static Map<File, Date> getExistingMetaDataFilesFs (){
		return MetaDataUtil.getExistingMetaDataFilesFs(null);
	}

	/**
	 * @param areaId areaId
	 * @return Map<File, Date>
	 */
	public static Map<File, Date> getExistingMetaDataFilesFs (String areaId) {
		Map<File, Date> existingMetaDataFilesFs = new HashMap<>();
		Path start = Paths.get(Settings.get("baseDirectoryArchive", String.class));
		int rootDepth = start.getNameCount();
		int folderMaxDepth = Settings.get("folderMaxDepth");
		String metadataFileName = Settings.get("metadataFileName");

		try {
			ForkJoinPool pool = new ForkJoinPool(Settings.get("netcdfReadThreads"));
			ArrayList<Callable<Map<File, Date>>> tasks = new ArrayList<>();
			List<Path> folders = Files.find(start, folderMaxDepth, (p, a) -> a.isDirectory() && p.getNameCount()-rootDepth == folderMaxDepth).filter(f -> areaId == null || areaId.isEmpty() || PathUtil.containsSegment(f, areaId)).collect(Collectors.toList());
			progressExpected = folders.size();
			progressCurrent = 0;
			folders.forEach(folder -> tasks.add(() -> {
				synchronized (mutex){
					if (++progressCurrent % 100 == 0)
						logger.info("getExistingMetaDataFilesFs - Progress: {}/{} {}%", progressCurrent, progressExpected, String.format("%,.2f", ((double)progressCurrent/progressExpected*100)));
				}
				return Files.find(folder, Integer.MAX_VALUE, (p, a) -> a.isRegularFile() && p.endsWith(metadataFileName)).collect(Collectors.toMap(Path::toFile, s -> new Date(s.toFile().lastModified())));
			}));
			List<Future<Map<File, Date>>> results = pool.invokeAll(tasks);

			for (Future<Map<File, Date>> x : results) {
				existingMetaDataFilesFs.putAll(x.get());
			}
			pool.shutdown();
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
		return existingMetaDataFilesFs;
	}

	/**
	 *
	 * @return Map<File, Date>
	 */
	public static Map<File, Date> getExistingMetaDataFilesDb () {
		Map<File, Date> existingMetaDataDb = new HashMap<>();
		Database.aggregate(Settings.get("metaDataCollection"), List.of(
				new Document("$project", new Document("_id", 0).append("metaDataFileRelativePath", 1).append("metaDataFileTime", 1)))).
				forEach(e -> {
					File file = PathUtil.normalize(new File(Settings.get("baseDirectoryArchive", String.class), e.getString("metaDataFileRelativePath")));
					existingMetaDataDb.put(file, e.getDate("metaDataFileTime"));
				});
		return existingMetaDataDb;
	}

	/**
	 *
	 * @param metaDataFile metaDataFile
	 * @return NetcdfMetaData
	 */
	public static NetcdfMetaData getNetcdfMetaData(File metaDataFile){
		try {
			JSONObject document = XML.toJSONObject(new InputStreamReader(new FileInputStream(metaDataFile), StandardCharsets.UTF_8));
			String metaDataType = document.keySet().toArray()[0].toString();
			if(!document.getJSONObject(metaDataType).has("netcdf"))
				return null;
			switch (metaDataType) {
				case "externalForecastMetaData":
					return ExternalForecastMetaDataReader.readMetaData(metaDataFile);
				case "netcdfMetaData":
					return MetaDataReader.readMetaData(metaDataFile);
				case "simulationMetaData":
					return SimulationMetaDataReader.readMetaData(metaDataFile);
				default:
					return null;
			}
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}