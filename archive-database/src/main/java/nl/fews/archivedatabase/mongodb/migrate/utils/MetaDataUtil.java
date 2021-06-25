package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.PathUtil;
import nl.wldelft.archive.util.metadata.externalforecast.ExternalForecastMetaDataReader;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfContent;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfMetaData;
import nl.wldelft.archive.util.metadata.observed.MetaDataReader;
import nl.wldelft.archive.util.metadata.simulation.SimulationMetaDataReader;
import org.bson.Document;
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
import java.util.stream.IntStream;

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

	/**
	 *
	 * @return Map<File, Date>
	 */
	public static Map<File, Date> getExistingMetaDataFilesFs () {
		return getExistingMetaDataFilesFs(null);
	}

	/**
	 *
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
			Files.find(start, folderMaxDepth, (p, a) -> a.isDirectory() && p.getNameCount()-rootDepth == folderMaxDepth).filter(f -> areaId == null || PathUtil.containsSegment(f, areaId)).forEach(folder -> tasks.add(() ->
				Files.find(folder, Integer.MAX_VALUE, (p, a) -> a.isRegularFile() && p.endsWith(metadataFileName)).collect(Collectors.toMap(Path::toFile, s -> new Date(s.toFile().lastModified())))
			));
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
		return getExistingMetaDataFilesDb(null);
	}

	/**
	 *
	 * @param areaId areaId
	 * @return Map<File, Date>
	 */
	public static Map<File, Date> getExistingMetaDataFilesDb (String areaId) {
		Map<File, Date> existingMetaDataDb = new HashMap<>();
		Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("metaDataCollection")).aggregate(List.of(
				new Document("$project", new Document("_id", 0).append("metaDataFileRelativePath", 1).append("metaDataFileTime", 1)))).
				forEach(e -> {
					File file = PathUtil.normalize(new File(Settings.get("baseDirectoryArchive", String.class), e.getString("metaDataFileRelativePath")));
					if(areaId == null || PathUtil.containsSegment(file, areaId))
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
			String metaDataType = XML.toJSONObject(new InputStreamReader(new FileInputStream(metaDataFile), StandardCharsets.UTF_8)).keySet().toArray()[0].toString();
			switch (metaDataType) {
				case "externalForecastMetaData":
					return ExternalForecastMetaDataReader.readMetaData(metaDataFile);
				case "netcdfMetaData":
					return MetaDataReader.readMetaData(metaDataFile);
				case "simulationMetaData":
					return SimulationMetaDataReader.readMetaData(metaDataFile);
				default:
					throw new IllegalArgumentException(metaDataType);
			}
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param metaDataFile metaDataFile
	 * @param netcdfMetaData netcdfMetaData
	 * @return Map<File, NetcdfContent>
	 */
	public static Map<File, NetcdfContent> getNetcdfContentMap(File metaDataFile, NetcdfMetaData netcdfMetaData){
		return IntStream.range(0, netcdfMetaData.netcdfFileCount()).boxed().collect(Collectors.toMap(
				i -> PathUtil.normalize(new File(metaDataFile.getParentFile(), netcdfMetaData.getNetcdf(i).getUrl())), netcdfMetaData::getNetcdf));
	}
}