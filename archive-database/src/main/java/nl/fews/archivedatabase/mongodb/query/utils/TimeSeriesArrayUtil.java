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

import java.util.List;
import java.util.Map;
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
		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader, timeSeriesHeader.getTimeStep());
		timeSeriesArray.setForecastTime(timeSeriesHeader.getForecastTime());

		for (int i = 0; i < requestTimeSeriesArray.size(); i++) {
			timeSeriesArray.put(requestTimeSeriesArray.getTime(i), requestTimeSeriesArray.getValue(i));
			timeSeriesArray.setFlag(i, requestTimeSeriesArray.getFlag(i));
			timeSeriesArray.setComment(i, requestTimeSeriesArray.getComment(i));
		}
		Map<Long, Float> resultMap = events.stream().collect(Collectors.toMap(s -> s.getDate("t").getTime(), s -> s.get("v") != null ? s.getDouble("v").floatValue() : Float.NaN));
		for (int i = 0; i < timeSeriesArray.size(); i++) {
			long time = timeSeriesArray.getTime(i);
			if(!requestTimeSeriesArray.isValueReliable(i) && resultMap.containsKey(time))
				timeSeriesArray.setValue(i, resultMap.get(time));
		}

		return timeSeriesArray;
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
