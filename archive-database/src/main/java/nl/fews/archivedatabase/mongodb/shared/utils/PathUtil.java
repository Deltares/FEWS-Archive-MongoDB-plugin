package nl.fews.archivedatabase.mongodb.shared.utils;

import java.io.File;

/**
 *
 */
public final class PathUtil {

	/**
	 *
	 */
	private static final char PATH_FROM_CHAR = File.separatorChar == '/' ? '\\' : '/';

	/**
	 *
	 */
	private static final char PATH_TO_CHAR = File.separatorChar == '/' ? '/' : '\\';

	/**
	 * Static Class
	 */
	private PathUtil(){}

	/**
	 *
	 * @param file file
	 * @return String
	 */
	public static String toRelativePathString(File file, String archiveRootDataFolder){
		String relativePath = file.toString().replace(archiveRootDataFolder, "").replace("\\", "/");
		if(!relativePath.startsWith("/")){
			relativePath = String.format("/%s", relativePath);
		}
		return relativePath;
	}

	/**
	 *
	 * @param relativePath relativePath
	 * @return File
	 */
	public static File fromRelativePathString(String relativePath, String archiveRootDataFolder){
		return PathUtil.normalize(new File(archiveRootDataFolder, relativePath));
	}

	/**
	 *
	 * @param file file
	 * @return File
	 */
	public static File normalize(File file){
		return new File(file.toString().replace(PATH_FROM_CHAR, PATH_TO_CHAR));
	}
}
