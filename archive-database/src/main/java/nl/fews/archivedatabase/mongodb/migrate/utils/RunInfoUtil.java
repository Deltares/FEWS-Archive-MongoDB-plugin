package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.shared.utils.PathUtil;
import org.json.JSONObject;
import org.json.XML;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 *
 */
public final class RunInfoUtil {

	/**
	 * Static Class
	 */
	private RunInfoUtil(){}

	/**
	 *
	 * @param metaData metaData
	 * @return RunInfoFile
	 */
	public static File getRunInfoFile(JSONObject metaData){
		if(metaData.getJSONObject(metaData.getString("metaDataType")).has("runInfo")) {
			return PathUtil.normalize(new File(metaData.getString("parentFilePath"), metaData.getJSONObject(metaData.getString("metaDataType")).getJSONObject("runInfo").getString("relativeFilePath")));
		}
		return null;
	}

	/**
	 * @param runInfoFile runInfoFile
	 * @return JSONObject
	 */
	public static JSONObject readRunInfo(File runInfoFile) {
		try{
			return runInfoFile != null ? XML.toJSONObject(new InputStreamReader(new FileInputStream(runInfoFile), StandardCharsets.UTF_8)) : new JSONObject();
		}
		catch (FileNotFoundException ex){
			throw new RuntimeException(ex);
		}
	}
}