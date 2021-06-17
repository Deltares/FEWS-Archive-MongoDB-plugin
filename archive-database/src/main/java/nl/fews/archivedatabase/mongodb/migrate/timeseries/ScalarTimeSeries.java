package nl.fews.archivedatabase.mongodb.migrate.timeseries;

import nl.fews.archivedatabase.mongodb.migrate.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.DateUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseUnitConverter;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public abstract class ScalarTimeSeries implements TimeSeries {

	/**
	 *
	 * @param timeSeriesDocument timeSeriesDocument
	 * @param timeSeriesSet timeSeriesSet
	 * @param timeSeriesDocuments timeSeriesDocuments
	 * @param runInfoDocument runInfoDocument
	 * @return Document
	 */
	public Document getRoot(Document timeSeriesDocument, JSONObject timeSeriesSet, List<Document> timeSeriesDocuments, Document runInfoDocument){
		Document document = new Document();

		String timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeString(getClass().getSimpleName());
		String moduleInstanceId = timeSeriesSet.getString("moduleInstanceId");
		String locationId = timeSeriesSet.getString("locationId");
		String parameterId = timeSeriesSet.getString("parameterId");
		String encodedTimeStepId = timeSeriesSet.getString("encodedTimeStepId");
		JSONArray q = timeSeriesSet.has("qualifierId") ? timeSeriesSet.get("qualifierId") instanceof JSONArray ? timeSeriesSet.getJSONArray("qualifierId") : new JSONArray(List.of(timeSeriesSet.getString("qualifierId"))) : null;
		List<String> qualifierIds = q != null ? q.toList().stream().filter(s -> s != null && !s.equals("") && !s.equals("none")).map(Object::toString).sorted().collect(Collectors.toList()) : new ArrayList<>();
		String qualifierId = new JSONArray(qualifierIds).toString();

		if (moduleInstanceId == null || moduleInstanceId.equals(""))
			throw new IllegalArgumentException("timeSeriesSet.getString(\"moduleInstanceId\") cannot be null or empty");

		if (locationId == null || locationId.equals(""))
			throw new IllegalArgumentException("timeSeriesSet.getString(\"locationId\") cannot be null or empty");

		if (parameterId == null || parameterId.equals(""))
			throw new IllegalArgumentException("timeSeriesSet.getString(\"parameterId\") cannot be null or empty");

		if (encodedTimeStepId == null || encodedTimeStepId.equals(""))
			throw new IllegalArgumentException("timeSeriesSet.getString(\"encodedTimeStepId\") cannot be null or empty");

		document.append("timeSeriesType", timeSeriesType);
		document.append("moduleInstanceId", moduleInstanceId);
		document.append("locationId", locationId);
		document.append("parameterId", parameterId);
		document.append("qualifierIds", qualifierIds);
		document.append("qualifierId", qualifierId);
		document.append("encodedTimeStepId", encodedTimeStepId);

		if (!timeSeriesDocuments.isEmpty()){
			document.append("startTime", timeSeriesDocuments.get(0).getDate("t"));
			document.append("endTime", timeSeriesDocuments.get(timeSeriesDocuments.size()-1).getDate("t"));

			if(timeSeriesDocuments.get(0).containsKey("lt")) document.append("localStartTime", timeSeriesDocuments.get(0).getDate("lt"));
			if(timeSeriesDocuments.get(timeSeriesDocuments.size()-1).containsKey("lt")) document.append("localEndTime", timeSeriesDocuments.get(timeSeriesDocuments.size()-1).getDate("lt"));
		}

		document.append("committed", false);

		return document;
	}

	/**
	 *
	 * @param timeSeriesDocument timeSeriesDocument
	 * @param timeSeriesSet timeSeriesSet
	 * @return Document
	 */
	public Document getMetaData(Document timeSeriesDocument, JSONObject timeSeriesSet, JSONObject netcdfMetaData){
		Document document = new Document();

		int min = timeSeriesSet.getInt("minimumStepMillis") / 1000 / 60;
		int max = timeSeriesSet.getInt("maximumStepMillis") / 1000 / 60;
		int timeStepMinutes = min == max ? min : (min / 60) % 2 == 0 ? min : max;
		String sourceId = netcdfMetaData.has("sourceId") ? netcdfMetaData.getString("sourceId") : "";
		String areaId = netcdfMetaData.has("areaId") ? netcdfMetaData.getString("areaId") : "";
		String unit = timeSeriesDocument.getString("unit");
		String parameterName = "";
		String parameterType = "";
		String timeStepLabel = timeSeriesSet.getString("timeStepLabel");
		String locationName = timeSeriesSet.getString("stationName");
		String displayUnit = Settings.get("archiveDatabaseUnitConverter") != null ? Settings.get("archiveDatabaseUnitConverter", ArchiveDatabaseUnitConverter.class).getOutputUnitType(unit) : null;
		String localTimeZone = Settings.get("archiveDatabaseTimeConverter") != null ? Settings.get("archiveDatabaseTimeConverter", ArchiveDatabaseTimeConverter.class).getLocalTimeZone().getID() : null;
		Date now = new Date();

		document.append("sourceId", sourceId);
		document.append("areaId", areaId);
		document.append("unit", unit);
		if (displayUnit != null) document.append("displayUnit", displayUnit);
		document.append("locationName", locationName);
		document.append("parameterName", parameterName);
		document.append("parameterType", parameterType);
		document.append("timeStepLabel", timeStepLabel);
		document.append("timeStepMinutes", timeStepMinutes);
		if (localTimeZone != null) document.append("localTimeZone", localTimeZone);
		document.append("archiveTime", now);
		document.append("modifiedTime", now);

		return document;
	}

	/**
	 *
	 * @param timeSeriesDocuments timeSeriesArray
	 * @param timeSeriesSet timeSeriesSet
	 * @return List<Document>
	 */
	public List<Document> getEvents(List<Document> timeSeriesDocuments, JSONObject timeSeriesSet){
		List<Document> documents = new ArrayList<>();
		Date[] localTimes = Settings.get("archiveDatabaseTimeConverter") != null ? DateUtil.getDates(Settings.get("archiveDatabaseTimeConverter", ArchiveDatabaseTimeConverter.class).convert(timeSeriesDocuments.stream().mapToLong(s -> s.getDate("t").getTime()).toArray())) : null;

		float[] displayValues = new float[timeSeriesDocuments.size()];
		for (int i = 0; i < displayValues.length; i++)
			displayValues[i] = timeSeriesDocuments.get(i).get("v", Float.class);
		displayValues = Settings.get("archiveDatabaseUnitConverter") != null ? Settings.get("archiveDatabaseUnitConverter", ArchiveDatabaseUnitConverter.class).convert(timeSeriesSet.getString("parameterId"), displayValues) : null;

		for (int i = 0; i < timeSeriesDocuments.size(); i++) {
			Object value = !Float.isNaN(timeSeriesDocuments.get(i).get("v", Float.class)) ? timeSeriesDocuments.get(i).get("v", Float.class) : null;
			Object displayValue = value != null && displayValues != null && !Float.isNaN(displayValues[i]) ? displayValues[i] : null;
			Date time = timeSeriesDocuments.get(i).getDate("t");
			Date localTime = localTimes != null ? localTimes[i] : null;
			String comment = timeSeriesDocuments.get(i).getString("c");
			Integer flag = timeSeriesDocuments.get(i).getInteger("f");

			Document document = new Document();
			document.append("t", time);
			if (localTimes != null) document.append("lt", localTime);
			document.append("v", value);
			if (displayValues != null) document.append("dv", displayValue);
			document.append("f", flag);
			if (comment != null && !comment.equals("")) document.append("c", comment);
			documents.add(document);
		}
		documents.sort(Comparator.comparing(c -> c.getDate("t")));
		return documents;
	}

	/**
	 *
	 * @return runInfo
	 */
	public Document getRunInfo(JSONObject runInfo) {
		return new Document();
	}
}