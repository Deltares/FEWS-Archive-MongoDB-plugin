package nl.fews.archivedatabase.mongodb.query.utils;

import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.FewsTimeSeriesHeaderProvider;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.HeaderRequest;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.Box;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.IrregularTimeStep;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import nl.wldelft.util.timeseries.TimeStepUtils;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

public class TimeSeriesArrayUtil {

	/**
	 *
	 */
	private TimeSeriesArrayUtil() {
	}

	/**
	 *
	 * @param timeSeriesValueType timeSeriesValueType
	 * @param timeSeriesType timeSeriesType
	 * @param result result
	 * @return TimeSeriesArray<TimeSeriesHeader>
	 */
	public static Box<TimeSeriesHeader, SystemActivityDescriptor> getTimeSeriesHeader(TimeSeriesValueType timeSeriesValueType, TimeSeriesType timeSeriesType, Document result){
		HeaderRequest.HeaderRequestBuilder headerRequestBuilder = new HeaderRequest.HeaderRequestBuilder();
		headerRequestBuilder.setValueType(timeSeriesValueType);
		headerRequestBuilder.setTimeSeriesType(timeSeriesType);

		if(result.containsKey("moduleInstanceId")) headerRequestBuilder.setModuleInstanceId(result.getString("moduleInstanceId"));
		if(result.containsKey("locationId")) headerRequestBuilder.setLocationId(result.getString("locationId"));
		if(result.containsKey("parameterId")) headerRequestBuilder.setParameterId(result.getString("parameterId"));
		if(result.containsKey("qualifierIds")) headerRequestBuilder.setQualifiersIds(result.getList("qualifierIds", String.class).toArray(new String[0]));
		if(result.containsKey("encodedTimeStepId")) headerRequestBuilder.setTimeStep(TimeStepUtils.decode(result.getString("encodedTimeStepId")));

		if(result.containsKey("forecastTime")) headerRequestBuilder.setExternalForecastTime(result.getDate("forecastTime").getTime());
		if(result.containsKey("ensembleId") && !result.getString("ensembleId").trim().equals("")) headerRequestBuilder.setEnsembleId(result.getString("ensembleId"));
		if(result.containsKey("ensembleMemberId") && !result.getString("ensembleMemberId").trim().equals("")) headerRequestBuilder.setEnsembleMember(result.getString("ensembleMemberId"));

		if(result.containsKey("runInfo")){
			Document runInfo = result.get("runInfo", Document.class);
			if(runInfo.containsKey("taskRunId")) headerRequestBuilder.setTaskRunId(runInfo.getString("taskRunId"));
			if(runInfo.containsKey("userId")) headerRequestBuilder.setUserId(runInfo.getString("userId"));
			if(runInfo.containsKey("workflowId")) headerRequestBuilder.setWorkflowId(runInfo.getString("workflowId"));
			if(runInfo.containsKey("dispatchTime")) headerRequestBuilder.setDispatchTime(runInfo.getDate("dispatchTime").getTime());
			if(runInfo.containsKey("time0")) headerRequestBuilder.setDispatchTime(runInfo.getDate("time0").getTime());
		}

		return Settings.get("headerProvider", FewsTimeSeriesHeaderProvider.class).getHeader(headerRequestBuilder.build());
	}

	/**
	 *
	 * @param timeSeriesHeader timeSeriesHeader
	 * @param events events
	 * @param requestTimeSeriesArray requestTimeSeriesArray
	 * @return TimeSeriesArray<TimeSeriesHeader>
	 */
	public static TimeSeriesArray<TimeSeriesHeader> getTimeSeriesArray(TimeSeriesHeader timeSeriesHeader, List<Document> events, TimeSeriesArray<TimeSeriesHeader> requestTimeSeriesArray){
		Map<Long, Document> existingEvents = new LinkedHashMap<>();
		Map<Long, Integer> existingTimeSeries = new LinkedHashMap<>();
		for (int i = 0; i < requestTimeSeriesArray.size(); i++) {
			existingEvents.put(requestTimeSeriesArray.getTime(i), new Document("v", requestTimeSeriesArray.getValue(i)).append("f", requestTimeSeriesArray.getFlag(i)).append("c", requestTimeSeriesArray.getComment(i)));
			existingTimeSeries.put(requestTimeSeriesArray.getTime(i), i);
		}

		Map<Long, Document> eventsMap = events.stream().collect(Collectors.toMap(s -> s.getDate("t").getTime(), s -> s, (k, v) -> v, LinkedHashMap::new));

		Set<Long> distinctOrderedTimes = new TreeSet<>(eventsMap.keySet());
		distinctOrderedTimes.addAll(existingEvents.keySet());

		List<Document> mergedEvents = distinctOrderedTimes.stream().map(s ->
			existingEvents.containsKey(s) && (requestTimeSeriesArray.isValueReliable(existingTimeSeries.get(s)) || !eventsMap.containsKey(s)) ? existingEvents.get(s) :
			eventsMap.get(s)).collect(Collectors.toList());

		return TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader, mergedEvents);
	}

	/**
	 *
	 * @param timeSeriesHeader timeSeriesHeader
	 * @param events events
	 * @return TimeSeriesArray<TimeSeriesHeader>
	 */
	public static TimeSeriesArray<TimeSeriesHeader> getTimeSeriesArray(TimeSeriesHeader timeSeriesHeader, List<Document> events){
		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader, timeSeriesHeader.getTimeStep());
		timeSeriesArray.setForecastTime(timeSeriesHeader.getForecastTime());

		long[] times = new long[events.size()];
		for (int i = 0; i < events.size(); i++)
			times[i] = events.get(i).getDate("t").getTime();

		if(timeSeriesHeader.getTimeStep() == IrregularTimeStep.INSTANCE)
			timeSeriesArray.ensureTimes(times);
		else
			timeSeriesArray.ensurePeriod(new Period(times[0], times[times.length-1]));

		for (int i = 0; i < events.size(); i++)  {
			timeSeriesArray.setValue(i,  events.get(i).get("v") != null ? events.get(i).getDouble("v").floatValue() : Float.NaN);
			timeSeriesArray.setFlag(i, events.get(i).getInteger("f").byteValue());
			timeSeriesArray.setComment(i, events.get(i).getString("c"));
		}
		return timeSeriesArray;
	}
}
