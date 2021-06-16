package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.PathUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.ThreadingUtil;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import java.util.stream.Stream;

public final class MetaDataUtil {

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

	public static Map<File, Date> getExistingMetaDataFilesFs () {
		Map<File, Date> existingMetaDataFilesFs = new HashMap<>();
		int rootDepth = Paths.get(Settings.get("archiveRootDataFolder", String.class)).getNameCount();
		try(Stream<Path> basePaths = Files.find(Paths.get(Settings.get("archiveRootDataFolder", String.class)), Settings.get("folderMaxDepth"), (p, a) -> a.isDirectory() && p.getNameCount()-rootDepth == (Integer) Settings.get("folderMaxDepth"))) {
			ForkJoinPool pool = new ForkJoinPool(Settings.get("fsThreads"));
			ArrayList<Callable<Map<File, Date>>> tasks = new ArrayList<>();
			basePaths.forEach(folder -> tasks.add(() -> {
				Map<File, Date> existing = new HashMap<>();
				Stream<Path> fs = Files.find(folder, Integer.MAX_VALUE, (p, a) -> a.isRegularFile() && p.endsWith(Settings.get("metadataFileName", String.class)));
				fs.forEach(ThreadingUtil.throwing(metaDataFile -> existing.put(metaDataFile.toFile(), new Date(metaDataFile.toFile().lastModified()))));
				return existing;
			}));
			List<Future<Map<File, Date>>> results = pool.invokeAll(tasks);

			for (Future<Map<File, Date>> x : results) {
				existingMetaDataFilesFs.putAll(x.get());
			}
			pool.shutdown();
		}
		catch (Exception ex){
			Thread.currentThread().interrupt();
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
		Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("metaDataCollection")).aggregate(List.of(
			new Document("$project", new Document("_id", 0).append("metaDataFileRelativePath", 1).append("metaDataFileTime", 1)))).
			forEach(e -> existingMetaDataDb.put(PathUtil.normalize(new File(Settings.get("archiveRootDataFolder", String.class), e.getString("metaDataFileRelativePath"))), e.getDate("metaDataFileTime")));
		return existingMetaDataDb;
	}

	/**
	 * @param metaDataFile metaDataFile
	 * @return JSONObject
	 */
	public static JSONObject readMetaData(File metaDataFile) {
		try{
			JSONObject document = XML.toJSONObject(new InputStreamReader(new FileInputStream(metaDataFile), StandardCharsets.UTF_8));

			String metaDataType = document.keySet().toArray()[0].toString();
			Object netcdf = document.getJSONObject(metaDataType).has("netcdf") ? document.getJSONObject(metaDataType).get("netcdf") : null;
			JSONObject firstNetcdfNode = netcdf != null ? netcdf instanceof JSONObject ? (JSONObject) netcdf : ((JSONArray) netcdf).getJSONObject(0) : null;
			String valueType = firstNetcdfNode != null ? firstNetcdfNode.getString("valueType") : "";

			document.put("parentFilePath", metaDataFile.getParent());
			document.put("metaDataType", metaDataType);
			document.put("valueType", valueType);

			return document;
		}
		catch (FileNotFoundException ex){
			throw new RuntimeException(ex);
		}
	}
}