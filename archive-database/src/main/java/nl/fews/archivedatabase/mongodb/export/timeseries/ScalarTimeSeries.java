package nl.fews.archivedatabase.mongodb.export.timeseries;

import nl.fews.archivedatabase.mongodb.export.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.DateUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseUnitConverter;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 */
public abstract class ScalarTimeSeries implements TimeSeries {

	/**
	 *
	 * @param header FEWS timeseries header
	 * @param timeSeriesDocuments the sorted list of timeseries event documents
	 * @param runInfoDocument the run info document
	 * @return bson document representing the root of this timeseries
	 */
	public Document getRoot(TimeSeriesHeader header, List<Document> timeSeriesDocuments, Document runInfoDocument){
		Document document = new Document();

		String timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeString(getClass().getSimpleName());
		String moduleInstanceId = header.getModuleInstanceId();
		String locationId = header.getLocationId();
		String parameterId = header.getParameterId();
		String encodedTimeStepId = header.getTimeStep().getEncoded();
		List<String> qualifierIds = IntStream.range(0, header.getQualifierCount()).mapToObj(header::getQualifierId).sorted().collect(Collectors.toList());
		String qualifierId = new JSONArray(qualifierIds).toString();

		if (moduleInstanceId == null || moduleInstanceId.equals(""))
			throw new IllegalArgumentException("header.getModuleInstanceId() cannot be null or empty");

		if (locationId == null || locationId.equals(""))
			throw new IllegalArgumentException("header.getLocationId() cannot be null or empty");

		if (parameterId == null || parameterId.equals(""))
			throw new IllegalArgumentException("header.getParameterId() cannot be null or empty");

		if (encodedTimeStepId == null || encodedTimeStepId.equals(""))
			throw new IllegalArgumentException("header.getTimeStep().getEncoded() cannot be null or empty");

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

		return document;
	}

	/**
	 *
	 * @param header FEWS timeseries header
	 * @param sourceId sourceId
	 * @param areaId areaId
	 * @return bson document representing the meta data of this timeseries
	 */
	public Document getMetaData(TimeSeriesHeader header, String areaId, String sourceId){
		Document document = new Document();

		int min = (int)(header.getTimeStep().getMinimumStepMillis() / 1000 / 60);
		int max = (int)(header.getTimeStep().getMaximumStepMillis() / 1000 / 60);
		int timeStepMinutes = min == max ? min : (min / 60) % 2 == 0 ? min : max;
		String unit = header.getUnit() == null ? "" : header.getUnit();
		String parameterName = header.getParameterName() == null ? "" : header.getParameterName();
		String parameterType = header.getParameterType() == null || header.getParameterType().getName() == null ? "" : header.getParameterType().getName();
		String timeStepLabel = header.getTimeStep() == null || header.getTimeStep().getLabel() == null ? "" : header.getTimeStep().getLabel();
		String locationName = header.getLocationName() == null ? "" : header.getLocationName();
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
	 * @param timeSeriesArray FEWS timeseries array
	 * @return the sorted list of timeseries event documents
	 */
	public List<Document> getEvents(TimeSeriesArray<TimeSeriesHeader> timeSeriesArray){
		List<Document> documents = new ArrayList<>();
		Date[] localTimes = Settings.get("archiveDatabaseTimeConverter") != null ? DateUtil.getDates(Settings.get("archiveDatabaseTimeConverter", ArchiveDatabaseTimeConverter.class).convert(timeSeriesArray.toTimesArray())) : null;
		float[] displayValues = Settings.get("archiveDatabaseUnitConverter") != null ? Settings.get("archiveDatabaseUnitConverter", ArchiveDatabaseUnitConverter.class).convert(timeSeriesArray.getHeader().getParameterId(), timeSeriesArray.toFloatArray()) : null;
		for (int i = 0; i < timeSeriesArray.size(); i++) {
			Object value = !Float.isNaN(timeSeriesArray.getValue(i)) ? timeSeriesArray.getValue(i) : null;
			Object displayValue = value != null && displayValues != null && !Float.isNaN(displayValues[i]) ? displayValues[i] : null;
			Date time = new Date(timeSeriesArray.getTime(i));
			Date localTime = localTimes != null ? localTimes[i] : null;
			String comment = timeSeriesArray.getComment(i);

			Document document = new Document();
			document.append("t", time);
			if (localTimes != null) document.append("lt", localTime);
			document.append("v", value);
			if (displayValues != null) document.append("dv", displayValue);
			document.append("f", timeSeriesArray.getFlag(i));
			if (comment != null) document.append("c", comment);
			documents.add(document);
		}
		documents.sort(Comparator.comparing(c -> c.getDate("t")));
		return documents;
	}

	/**
	 *
	 * @return bson document representing the run info of this timeseries
	 */
	public Document getRunInfo(TimeSeriesHeader header) {
		return new Document();
	}
}
