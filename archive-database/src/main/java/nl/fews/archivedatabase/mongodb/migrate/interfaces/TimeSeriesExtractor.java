package nl.fews.archivedatabase.mongodb.migrate.interfaces;

import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.util.List;

/**
 *
 */
public interface TimeSeriesExtractor {

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @param netcdfFile netcdfFile
	 * @param timeSeriesSets timeSeriesSets
	 * @param netcdfMetaData netcdfMetaData
	 * @param runInfo runInfo
	 * @return List<Document>
	 */
	List<Document> extract(TimeSeriesType timeSeriesType, File netcdfFile, JSONArray timeSeriesSets, JSONObject netcdfMetaData, JSONObject runInfo);
}
