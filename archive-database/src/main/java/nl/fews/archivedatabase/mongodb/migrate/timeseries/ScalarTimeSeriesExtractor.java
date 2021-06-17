package nl.fews.archivedatabase.mongodb.migrate.timeseries;

import nl.fews.archivedatabase.mongodb.migrate.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.migrate.interfaces.TimeSeriesExtractor;
import nl.fews.archivedatabase.mongodb.migrate.utils.NetcdfUtil;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.utils.LogUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ScalarTimeSeriesExtractor implements TimeSeriesExtractor {

	/**
	 *
	 */
	private static final String BASE_NAMESPACE = "nl.fews.archivedatabase.mongodb.migrate";

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(ScalarTimeSeriesExtractor.class);


	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @param netcdfFile netcdfFile
	 * @param timeSeriesSets timeSeriesSets
	 * @param netcdfMetaData netcdfMetaData
	 * @param runInfo runInfo
	 * @return List<Document>
	 */
	public List<Document> extract(TimeSeriesType timeSeriesType, File netcdfFile, JSONArray timeSeriesSets, JSONObject netcdfMetaData, JSONObject runInfo){
		List<Document> timeSeries = new ArrayList<>();

		List<Document> timeSeriesDocuments = NetcdfUtil.getTimeSeriesDocuments(netcdfFile);

		if(timeSeriesDocuments.size() != timeSeriesSets.length())
			timeSeriesDocuments = timeSeriesDocuments.stream().distinct().collect(Collectors.toList());

		if(timeSeriesDocuments.size() != timeSeriesSets.length()){
			IndexOutOfBoundsException ex = new IndexOutOfBoundsException(String.format("%s: timeSeriesDocuments.size(%s) != timeSeriesSets.size(%s)", netcdfFile, timeSeriesDocuments.size(), timeSeriesSets.length()));
			JSONObject message = LogUtil.getLogMessageJson(ex, Map.of("netcdfFile", netcdfFile.toString(), "timeSeriesSets", timeSeriesSets, "netcdfMetaData", netcdfMetaData, "runInfo", runInfo));
			logger.warn(message.toString(), ex);
			return timeSeries;
		}

		for (int i = 0; i < timeSeriesDocuments.size(); i++) {
			if(!timeSeriesDocuments.get(i).getString("stationId").equals(timeSeriesSets.getJSONObject(i).getString("stationId")))
				throw new IllegalArgumentException(String.format("!timeSeriesArrays.get(%s).getHeader().getLocationId(%s).equals(timeSeriesSets.getJSONObject(%s).getString(%s))", i, timeSeriesDocuments.get(i).getString("stationId"), i, timeSeriesSets.getJSONObject(i).getString("stationId")));

			Document document = getTimeSeries(timeSeriesType, timeSeriesDocuments.get(i), timeSeriesSets.getJSONObject(i), netcdfMetaData, runInfo);
			if(document.containsKey("timeseries"))
				timeSeries.add(document);
		}
		return timeSeries;
	}

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @param timeSeriesDocument timeSeriesArray
	 * @param timeSeriesSet timeSeriesSet
	 * @param netcdfMetaData netcdfMetaData
	 * @param runInfo runInfo
	 * @return Document
	 */
	private static Document getTimeSeries(TimeSeriesType timeSeriesType, Document timeSeriesDocument, JSONObject timeSeriesSet, JSONObject netcdfMetaData, JSONObject runInfo){
		try {
			TimeSeries timeSeries = (TimeSeries) Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "timeseries", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(timeSeriesType))).getConstructor().newInstance();

			List<Document> eventDocuments = timeSeries.getEvents(timeSeriesDocument.getList("timeseries", Document.class), timeSeriesSet);
			Document metaDataDocument = timeSeries.getMetaData(timeSeriesDocument, timeSeriesSet, netcdfMetaData);
			Document runInfoDocument = timeSeries.getRunInfo(runInfo);
			Document rootDocument = timeSeries.getRoot(timeSeriesDocument, timeSeriesSet, eventDocuments, runInfoDocument);

			if(!metaDataDocument.isEmpty()) rootDocument.append("metaData", metaDataDocument);
			if(!runInfoDocument.isEmpty()) rootDocument.append("runInfo", runInfoDocument);
			if(!eventDocuments.isEmpty()) rootDocument.append("timeseries", eventDocuments);

			return rootDocument;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}
