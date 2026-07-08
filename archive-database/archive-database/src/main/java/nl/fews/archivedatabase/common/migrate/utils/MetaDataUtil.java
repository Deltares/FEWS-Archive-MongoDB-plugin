package nl.fews.archivedatabase.common.migrate.utils;

import nl.fews.archivedatabase.common.shared.database.Database;
import nl.fews.archivedatabase.common.shared.database.Table;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.common.shared.utils.PathUtil;
import nl.wldelft.archive.util.metadata.externalforecast.ExternalForecastMetaDataReader;
import nl.wldelft.archive.util.metadata.netcdf.NetcdfMetaData;
import nl.wldelft.archive.util.metadata.observed.MetaDataReader;
import nl.wldelft.archive.util.metadata.simulation.SimulationMetaDataReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.XML;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
	 * @param areaIds areaIds
	 * @return Map<File, Date>
	 */
	public static Map<File, Date> getExistingMetaDataFilesFs (List<String> areaIds) {
		try {
			var existingMetaDataFilesFs = new LinkedHashMap<File, Date>();
			var start = Paths.get(Settings.get("baseDirectoryArchive", String.class));
			var rootDepth = start.getNameCount();
			var folderMaxDepth = Settings.get("folderMaxDepth", Integer.class);
			var metadataFileName = Settings.get("metadataFileName", String.class);

			var pool = Executors.newFixedThreadPool(Settings.get("netcdfReadThreads"));
			var tasks = new ArrayList<Callable<Map<File, Date>>>();
			List<Path> folders;
			try(Stream<Path> files = Files.find(start, folderMaxDepth, (p, a) -> a.isDirectory() && p.getNameCount()-rootDepth == folderMaxDepth)) {
				folders = files.filter(f -> areaIds == null || areaIds.isEmpty() || areaIds.stream().anyMatch(areaId -> PathUtil.containsSegment(f, areaId))).toList();
			}
			progressExpected = folders.size();
			progressCurrent = 0;

			class Find implements Callable<Map<File, Date>> {
				private final Path folder;
				private final String metadataFileName;
				public Find(Path folder, String metadataFileName) {
					this.folder = folder;
					this.metadataFileName = metadataFileName;
				}
				@Override
				public Map<File, Date> call() throws Exception {
					synchronized (mutex){
						if (++progressCurrent % 100 == 0)
							logger.info("getExistingMetaDataFilesFs - Progress: {}/{} {}", progressCurrent, progressExpected, String.format("%,.2f%%", ((double)progressCurrent/progressExpected*100)));
					}
					try(Stream<Path> files = Files.find(folder, Integer.MAX_VALUE, (p, a) -> a.isRegularFile() && p.endsWith(metadataFileName))){
						return files.collect(Collectors.toMap(Path::toFile, s -> new Date(s.toFile().lastModified())));
					}
				}
			}

			folders.forEach(folder -> tasks.add(new Find(folder, metadataFileName)));
			for (var x : pool.invokeAll(tasks))
				existingMetaDataFilesFs.putAll(x.get());
			pool.shutdown();

			return existingMetaDataFilesFs;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @return Map<File, Date>
	 */
	public static Map<File, Date> getExistingMetaDataFilesDb () {
		var existingMetaDataDb = new LinkedHashMap<File, Date>();
		Database.instance().find(Table.MigrateMetaData.toString(), Map.of(), Map.of("_id", 0, "metaDataFileRelativePath", 1, "metaDataFileTime", 1)).forEach(e -> {
			var file = PathUtil.normalize(new File(Settings.get("baseDirectoryArchive", String.class), (String)e.get("metaDataFileRelativePath")));
			existingMetaDataDb.put(file, (Date)e.get("metaDataFileTime"));
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
			var document = XML.toJSONObject(new InputStreamReader(new FileInputStream(metaDataFile), StandardCharsets.UTF_8));
			var metaDataType = document.keySet().toArray()[0].toString();
			if(!document.getJSONObject(metaDataType).has("netcdf"))
				return null;
			return switch (metaDataType) {
				case "externalForecastMetaData" -> ExternalForecastMetaDataReader.readMetaData(metaDataFile);
				case "netcdfMetaData" -> MetaDataReader.readMetaData(metaDataFile);
				case "simulationMetaData" -> SimulationMetaDataReader.readMetaData(metaDataFile);
				default -> null;
			};
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}