package nl.fews.verification.mongodb.shared.io;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IO {
	private IO(){}

	/**
	 * Retrieves the content of a resource file as a string.
	 *
	 * @param path the path of the resource file
	 * @return the content of the resource file
	 * @throws RuntimeException if an error occurs while reading the resource file
	 */
	public static String getResourceString(String path){
		try(InputStream stream = IO.class.getClassLoader().getResourceAsStream(path)) {
			return new String(stream.readAllBytes());
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Writes a string to a file specified by the given path.
	 *
	 * @param path the path of the file to write the string to
	 * @param s the string to be written to the file
	 * @throws RuntimeException if an error occurs while writing the string to the file
	 */
	public static void writeString(Path path, String s){
		try{
			Files.writeString(path, s);
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Writes a string to a file specified by the given path.
	 *
	 * @param path the path of the file to read
	 * @throws RuntimeException if an error occurs while reading
	 */
	public static String readString(Path path){
		try{
			return Files.readString(path);
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Deletes all files located within the specified path.
	 *
	 * @param path the path to the directory containing the files to be deleted
	 * @throws RuntimeException if an error occurs while deleting the files
	 */
	public static void deleteFiles(Path path){
		try (Stream<Path> list = Files.list(path)){
			list.collect(Collectors.toList()).parallelStream().forEach(f -> f.toFile().delete());
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Executes a command in the system's command line interface and returns the result.
	 *
	 * @param command the command to execute
	 * @return an array containing the exit value of the command and the output of the command as a String
	 * @throws RuntimeException if an error occurs while executing the command
	 */
	public static Object[] execute(String command){
		try{
			Process process = (System.getProperty("os.name").startsWith("Windows") ? new ProcessBuilder("cmd.exe", "/c", command) : new ProcessBuilder("sh", "-c", command)).start();
			return new Object[]{process.waitFor(), new String(process.getInputStream().readAllBytes())};
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}
