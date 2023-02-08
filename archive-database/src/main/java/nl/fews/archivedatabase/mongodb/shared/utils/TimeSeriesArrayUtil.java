package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.FewsTimeSeriesHeaderProvider;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.HeaderRequest;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.Box;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.*;
import org.bson.Document;

import java.util.*;

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
		if(result.containsKey("ensembleId") && !result.getString("ensembleId").trim().equals("") && !result.getString("ensembleId").equals("none") && !result.getString("ensembleId").equals("main")) headerRequestBuilder.setEnsembleId(result.getString("ensembleId"));
		if(result.containsKey("ensembleMemberId") && !result.getString("ensembleMemberId").trim().equals("") && !result.getString("ensembleMemberId").equals("none") && !result.getString("ensembleMemberId").equals("0")) headerRequestBuilder.setEnsembleMember(result.getString("ensembleMemberId"));

		if(result.containsKey("runInfo")){
			Document runInfo = result.get("runInfo", Document.class);
			if(runInfo.containsKey("taskRunId")) headerRequestBuilder.setTaskRunId(runInfo.getString("taskRunId"));
			if(runInfo.containsKey("userId")) headerRequestBuilder.setUserId(runInfo.getString("userId"));
			if(runInfo.containsKey("workflowId")) headerRequestBuilder.setWorkflowId(runInfo.getString("workflowId"));
			if(runInfo.containsKey("dispatchTime")) headerRequestBuilder.setDispatchTime(runInfo.getDate("dispatchTime").getTime());
			if(runInfo.containsKey("time0")) headerRequestBuilder.setTimeZero(runInfo.getDate("time0").getTime());
		}

		return Settings.get("headerProvider", FewsTimeSeriesHeaderProvider.class).getHeader(headerRequestBuilder.build());
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


		if (!events.isEmpty()){
			Map<Long, Document> eventLookup = new HashMap<>();
			long[] times = new long[events.size()];
			for (int i = 0; i < events.size(); i++) {
				times[i] = events.get(i).getDate("t").getTime();
				eventLookup.put(times[i], events.get(i));
			}

			if(timeSeriesHeader.getTimeStep() == IrregularTimeStep.INSTANCE)
				timeSeriesArray.ensureTimes(times);
			else
				timeSeriesArray.ensurePeriod(new Period(times[0], times[times.length-1]));

			for (int i = 0; i < timeSeriesArray.size(); i++)  {
				if(eventLookup.containsKey(timeSeriesArray.getTime(i))){
					Document event = eventLookup.get(timeSeriesArray.getTime(i));
					timeSeriesArray.setValue(i,  event.get("v") != null ? event.getDouble("v").floatValue() : Float.NaN);
					timeSeriesArray.setFlag(i, event.getInteger("f").byteValue());
					timeSeriesArray.setComment(i, event.getString("c"));
					timeSeriesArray.setFlagSource(i, event.get("fs") != null ? event.getInteger("fs").byteValue() : timeSeriesArray.getFlagSource(i));
					timeSeriesArray.setUser(i, event.getString("u"));
				}
			}
		}
		return timeSeriesArray;
	}
}
