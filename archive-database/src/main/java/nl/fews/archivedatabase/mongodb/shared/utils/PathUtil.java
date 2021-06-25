package nl.fews.archivedatabase.mongodb.shared.utils;

import java.io.File;
import java.nio.file.Path;

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
	public static String toRelativePathString(File file, String baseDirectoryArchive){
		String relativePath = file.toString().replace(baseDirectoryArchive, "").replace("\\", "/");
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
	public static File fromRelativePathString(String relativePath, String baseDirectoryArchive){
		return PathUtil.normalize(new File(baseDirectoryArchive, relativePath));
	}

	/**
	 *
	 * @param file file
	 * @return File
	 */
	public static File normalize(File file){
		return new File(file.toString().replace(PATH_FROM_CHAR, PATH_TO_CHAR));
	}

	/**
	 *
	 * @param segment segment
	 * @param file file
	 * @return File
	 */
	public static boolean containsSegment(File file, String segment){
		return containsSegment(file, segment, false);
	}

	/**
	 *
	 * @param segment segment
	 * @param path path
	 * @return File
	 */
	public static boolean containsSegment(Path path, String segment){
		return containsSegment(path, segment, false);
	}

	/**
	 *
	 * @param segment segment
	 * @param file file
	 * @param ignoreCase ignoreCase
	 * @return File
	 */
	public static boolean containsSegment(File file, String segment, boolean ignoreCase){
		return containsSegment(file.toPath(), segment, ignoreCase);
	}

	/**
	 *
	 * @param segment segment
	 * @param path path
	 * @param ignoreCase ignoreCase
	 * @return File
	 */
	public static boolean containsSegment(Path path, String segment, boolean ignoreCase){
		for (int i = 0; i < path.getNameCount(); i++)
			if(ignoreCase && path.getName(i).toString().equalsIgnoreCase(segment) || path.getName(i).toString().equals(segment))
				return true;
		return false;
	}
}
