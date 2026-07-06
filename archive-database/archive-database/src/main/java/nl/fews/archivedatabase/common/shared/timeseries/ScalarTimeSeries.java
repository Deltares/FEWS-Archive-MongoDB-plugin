package nl.fews.archivedatabase.common.shared.timeseries;

import nl.fews.archivedatabase.common.shared.interfaces.TimeSeries;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.common.shared.utils.DateUtil;
import nl.fews.archivedatabase.common.shared.utils.LogUtil;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.archive.util.runinfo.ArchiveRunInfo;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.util.timeseries.FlagSource;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
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
	 * @return document representing the root of this timeseries
	 */
	public Map<String, Object> getRoot(FewsTimeSeriesHeader header, List<Map<String, Object>> eventDocuments, Map<String, Object> runInfo){
		var document = new LinkedHashMap<String, Object>();

		var timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeString(getClass().getSimpleName());
		var moduleInstanceId = header.getModuleInstanceId();
		var locationId = header.getLocationId();
		var parameterId = header.getParameterId();
		var encodedTimeStepId = header.getTimeStep().getEncoded();
		var qualifierIds = IntStream.range(0, header.getQualifierCount()).mapToObj(header::getQualifierId).sorted().toList();
		var qualifierId = new JSONArray(qualifierIds).toString();

		if (moduleInstanceId == null || moduleInstanceId.isEmpty())
			throw new IllegalArgumentException("header.getModuleInstanceId() cannot be null or empty");

		if (locationId == null || locationId.isEmpty())
			throw new IllegalArgumentException("header.getLocationId() cannot be null or empty");

		if (parameterId == null || parameterId.isEmpty())
			throw new IllegalArgumentException("header.getParameterId() cannot be null or empty");

		if (encodedTimeStepId == null || encodedTimeStepId.isEmpty())
			throw new IllegalArgumentException("header.getTimeStep().getEncoded() cannot be null or empty");

		document.put("timeSeriesType", timeSeriesType);
		document.put("moduleInstanceId", moduleInstanceId);
		document.put("locationId", locationId);
		document.put("parameterId", parameterId);
		document.put("qualifierIds", qualifierIds);
		document.put("qualifierId", qualifierId);
		document.put("encodedTimeStepId", encodedTimeStepId);

		if (!eventDocuments.isEmpty()){
			document.put("startTime", eventDocuments.get(0).get("t"));
			document.put("endTime", eventDocuments.get(eventDocuments.size()-1).get("t"));

			if(eventDocuments.get(0).containsKey("lt")) document.put("localStartTime", eventDocuments.get(0).get("lt"));
			if(eventDocuments.get(eventDocuments.size()-1).containsKey("lt")) document.put("localEndTime", eventDocuments.get(eventDocuments.size()-1).get("lt"));
		}

		return document;
	}

	/**
	 *
	 * @param header FEWS timeseries header
	 * @param sourceId sourceId
	 * @param areaId areaId
	 * @return document representing the metadata of this timeseries
	 */
	public Map<String, Object> getMetaData(FewsTimeSeriesHeader header, String areaId, String sourceId){
		var document = new LinkedHashMap<String, Object>();

		var min = (int)(header.getTimeStep().getMinimumStepMillis() / 1000 / 60);
		var max = (int)(header.getTimeStep().getMaximumStepMillis() / 1000 / 60);
		var timeStepMinutes = min == max ? min : (min / 60) % 2 == 0 ? min : max;
		areaId = areaId != null ? areaId : "";
		sourceId = sourceId != null ? sourceId : "";
		var headerUnit = header.getUnit() != null && !header.getUnit().trim().equals("-") && !header.getUnit().trim().isEmpty() ? header.getUnit().trim() : null;

		var parameterInfo = Settings.get("archiveDatabaseRegionConfigInfoProvider") != null ? Settings.get("archiveDatabaseRegionConfigInfoProvider", ArchiveDatabaseRegionConfigInfoProvider.class).getParameterInfo(header.getParameterId()) : null;
		var parameterName = parameterInfo != null ? parameterInfo.getName() : "";
		var parameterUnit = parameterInfo != null ? parameterInfo.getUnit() : null;

		var unit = parameterUnit != null ? parameterUnit : headerUnit != null ? headerUnit : "";

		if(headerUnit != null && parameterUnit != null && !headerUnit.equals(parameterUnit) && !parameterUnit.equals(header.getParameterId())){
			Exception ex = new IllegalArgumentException(String.format("Units for header.getParameterId() -> %3$s not equal: header.getUnit() -> %1$s, parameterInfo.getUnit() -> %2$s. Using header.getUnit() -> %1$s.",
					header.getUnit(), parameterInfo.getUnit(), header.getParameterId()));
			logger.debug(new JSONObject(LogUtil.getLogMessageJson(ex, Map.of("parameterId", header.getParameterId() == null ? "" : header.getParameterId()))).toString(), ex);
		}

		var parameterType = header.getParameterType() != null && header.getParameterType().getName() != null ? header.getParameterType().getName() : "";
		var timeStepLabel = header.getTimeStep() != null && header.getTimeStep().toString() != null ? header.getTimeStep().toString() : "";

		var locationInfo = Settings.get("archiveDatabaseRegionConfigInfoProvider") != null ? Settings.get("archiveDatabaseRegionConfigInfoProvider", ArchiveDatabaseRegionConfigInfoProvider.class).getLocationInfo(header.getLocationId()) : null;
		var locationName = locationInfo != null ? locationInfo.name() : "";

		var displayUnit = Settings.get("archiveDatabaseUnitConverter") != null ? Settings.get("archiveDatabaseUnitConverter", ArchiveDatabaseUnitConverter.class).getOutputUnitType(unit) : null;
		var localTimeZone = Settings.get("archiveDatabaseTimeConverter") != null ? Settings.get("archiveDatabaseTimeConverter", ArchiveDatabaseTimeConverter.class).getLocalTimeZone().getID() : null;
		var now = new Date();

		document.put("sourceId", sourceId);
		document.put("areaId", areaId);
		document.put("unit", unit);
		if (displayUnit != null) document.put("displayUnit", displayUnit);
		document.put("locationName", locationName);
		document.put("parameterName", parameterName);
		document.put("parameterType", parameterType);
		document.put("timeStepLabel", timeStepLabel);
		document.put("timeStepMinutes", timeStepMinutes);
		if (localTimeZone != null) document.put("localTimeZone", localTimeZone);
		document.put("archiveTime", now);
		document.put("modifiedTime", now);

		return document;
	}

	/**
	 *
	 * @param timeSeriesArray FEWS timeseries array
	 * @param metadataDocument metadataDocument
	 * @return the sorted list of timeseries event documents
	 */
	public List<Map<String, Object>> getEvents(TimeSeriesArray<FewsTimeSeriesHeader> timeSeriesArray, Map<String, Object> metadataDocument){
		var documents = new ArrayList<Map<String, Object>>();

		var unit = (String)metadataDocument.get("unit");
		var localTimes = Settings.get("archiveDatabaseTimeConverter") != null ? DateUtil.getDates(Settings.get("archiveDatabaseTimeConverter", ArchiveDatabaseTimeConverter.class).convert(timeSeriesArray.toTimesArray())) : null;
		var displayValues = Settings.get("archiveDatabaseUnitConverter") != null ? Settings.get("archiveDatabaseUnitConverter", ArchiveDatabaseUnitConverter.class).convert(unit, timeSeriesArray.toFloatArray()) : null;

		for (int i = 0; i < timeSeriesArray.size(); i++) {
			var value = !Float.isNaN(timeSeriesArray.getValue(i)) ? timeSeriesArray.getValue(i) : null;
			var displayValue = value != null && displayValues != null && !Float.isNaN(displayValues[i]) ? displayValues[i] : null;

			var time = new Date(timeSeriesArray.getTime(i));
			var localTime = localTimes != null ? localTimes[i] : null;

			var flag = timeSeriesArray.getFlag(i);
			var user = timeSeriesArray.getUser(i);
			var comment = timeSeriesArray.getComment(i);
			var flagSource = FlagSource.get(timeSeriesArray.getFlagSource(i)).getId();

			var document = new LinkedHashMap<String, Object>();

			document.put("t", time);
			if (localTimes != null) document.put("lt", localTime);

			document.put("v", value);
			if (displayValues != null) document.put("dv", displayValue);

			document.put("f", flag);
			if (user != null) document.put("u", user);
			if (comment != null) document.put("c", comment);
			if (flagSource != null) document.put("fs", flagSource);

			documents.add(document);
		}
		documents.sort(Comparator.comparing(c -> (Date)c.get("t")));
		return documents;
	}

	/**
	 *
	 * @param header FEWS timeseries header
	 * @return Map<String, Object>
	 */
	public Map<String, Object> getRunInfo(FewsTimeSeriesHeader header) {
		return new LinkedHashMap<>();
	}

	/**
	 *
	 * @param archiveRunInfo archiveRunInfo
	 * @return Map<String, Object>
	 */
	public Map<String, Object> getRunInfo(ArchiveRunInfo archiveRunInfo) {
		return new LinkedHashMap<>();
	}
}
