package nl.fews.archivedatabase.mongodb.shared.timeseries;

import nl.fews.archivedatabase.mongodb.shared.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.DateUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.LogUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.archive.util.runinfo.ArchiveRunInfo;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 */
public abstract class ScalarTimeSeries implements TimeSeries {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(ScalarTimeSeries.class);

	/**
	 *
	 * @param header FEWS timeseries header
	 * @param eventDocuments the sorted list of timeseries event documents
	 * @param runInfo the run info document
	 * @return bson document representing the root of this timeseries
	 */
	public Document getRoot(TimeSeriesHeader header, List<Document> eventDocuments, Document runInfo){
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

		if (!eventDocuments.isEmpty()){
			document.append("startTime", eventDocuments.get(0).getDate("t"));
			document.append("endTime", eventDocuments.get(eventDocuments.size()-1).getDate("t"));

			if(eventDocuments.get(0).containsKey("lt")) document.append("localStartTime", eventDocuments.get(0).getDate("lt"));
			if(eventDocuments.get(eventDocuments.size()-1).containsKey("lt")) document.append("localEndTime", eventDocuments.get(eventDocuments.size()-1).getDate("lt"));
		}

		return document;
	}

	/**
	 *
	 * @param header FEWS timeseries header
	 * @param sourceId sourceId
	 * @param areaId areaId
	 * @return bson document representing the metadata of this timeseries
	 */
	public Document getMetaData(TimeSeriesHeader header, String areaId, String sourceId){
		Document document = new Document();

		int min = (int)(header.getTimeStep().getMinimumStepMillis() / 1000 / 60);
		int max = (int)(header.getTimeStep().getMaximumStepMillis() / 1000 / 60);
		int timeStepMinutes = min == max ? min : (min / 60) % 2 == 0 ? min : max;
		areaId = areaId != null ? areaId : "";
		sourceId = sourceId != null ? sourceId : "";
		String headerUnit = header.getUnit() != null && !header.getUnit().trim().equals("-") && !header.getUnit().trim().equals("") ? header.getUnit().trim() : null;

		ParameterInfo parameterInfo = Settings.get("archiveDatabaseRegionConfigInfoProvider") != null ? Settings.get("archiveDatabaseRegionConfigInfoProvider", ArchiveDatabaseRegionConfigInfoProvider.class).getParameterInfo(header.getParameterId()) : null;
		String parameterName = parameterInfo != null ? parameterInfo.getName() : "";
		String parameterUnit = parameterInfo != null ? parameterInfo.getUnit() : null;

		String unit = parameterUnit != null ? parameterUnit : headerUnit != null ? headerUnit : "";

		if(headerUnit != null && parameterUnit != null && !headerUnit.equals(parameterUnit) && !parameterUnit.equals(header.getParameterId())){
			Exception ex = new IllegalArgumentException(String.format("Units for header.getParameterId() -> %3$s not equal: header.getUnit() -> %1$s, parameterInfo.getUnit() -> %2$s. Using header.getUnit() -> %1$s.",
					header.getUnit(), parameterInfo.getUnit(), header.getParameterId()));
			logger.debug(LogUtil.getLogMessageJson(ex, Map.of("parameterId", header.getParameterId())).toJson(), ex);
		}

		String parameterType = header.getParameterType() != null && header.getParameterType().getName() != null ? header.getParameterType().getName() : "";
		String timeStepLabel = header.getTimeStep() != null && header.getTimeStep().toString() != null ? header.getTimeStep().toString() : "";

		LocationInfo locationInfo = Settings.get("archiveDatabaseRegionConfigInfoProvider") != null ? Settings.get("archiveDatabaseRegionConfigInfoProvider", ArchiveDatabaseRegionConfigInfoProvider.class).getLocationInfo(header.getLocationId()) : null;
		String locationName = locationInfo != null ? locationInfo.getName() : "";

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
	 * @param metadataDocument metadataDocument
	 * @return the sorted list of timeseries event documents
	 */
	public List<Document> getEvents(TimeSeriesArray<TimeSeriesHeader> timeSeriesArray, Document metadataDocument){
		List<Document> documents = new ArrayList<>();

		String unit = metadataDocument.getString("unit");
		Date[] localTimes = Settings.get("archiveDatabaseTimeConverter") != null ? DateUtil.getDates(Settings.get("archiveDatabaseTimeConverter", ArchiveDatabaseTimeConverter.class).convert(timeSeriesArray.toTimesArray())) : null;
		float[] displayValues = Settings.get("archiveDatabaseUnitConverter") != null ? Settings.get("archiveDatabaseUnitConverter", ArchiveDatabaseUnitConverter.class).convert(unit, timeSeriesArray.toFloatArray()) : null;

		for (int i = 0; i < timeSeriesArray.size(); i++) {
			Object value = !Float.isNaN(timeSeriesArray.getValue(i)) ? timeSeriesArray.getValue(i) : null;
			Object displayValue = value != null && displayValues != null && !Float.isNaN(displayValues[i]) ? displayValues[i] : null;

			Date time = new Date(timeSeriesArray.getTime(i));
			Date localTime = localTimes != null ? localTimes[i] : null;

			int flag = timeSeriesArray.getFlag(i);
			String comment = timeSeriesArray.getComment(i);

			Document document = new Document();

			document.append("t", time);
			if (localTimes != null) document.append("lt", localTime);

			document.append("v", value);
			if (displayValues != null) document.append("dv", displayValue);

			document.append("f", flag);
			if (comment != null) document.append("c", comment);

			documents.add(document);
		}
		documents.sort(Comparator.comparing(c -> c.getDate("t")));
		return documents;
	}

	/**
	 *
	 * @param header FEWS timeseries header
	 * @return Document
	 */
	public Document getRunInfo(TimeSeriesHeader header) {
		return new Document();
	}

	/**
	 *
	 * @param archiveRunInfo archiveRunInfo
	 * @return Document
	 */
	public Document getRunInfo(ArchiveRunInfo archiveRunInfo) {
		return new Document();
	}
}
