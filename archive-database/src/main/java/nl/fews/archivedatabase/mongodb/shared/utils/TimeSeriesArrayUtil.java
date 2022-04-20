package nl.fews.archivedatabase.mongodb.shared.utils;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

public class TimeSeriesArrayUtil {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(TimeSeriesArrayUtil.class);

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
			if(runInfo.containsKey("time0")) headerRequestBuilder.setDispatchTime(runInfo.getDate("time0").getTime());
		}

		return Settings.get("headerProvider", FewsTimeSeriesHeaderProvider.class).getHeader(headerRequestBuilder.build());
	}

	/**
	 * ASSUMPTION: events and requestTimeSeriesArray are monotonically increasing
	 * @param timeSeriesHeader timeSeriesHeader
	 * @param events events
	 * @param requestTimeSeriesArray requestTimeSeriesArray
	 * @return TimeSeriesArray<TimeSeriesHeader>
	 */
	public static TimeSeriesArray<TimeSeriesHeader> getTimeSeriesArray(TimeSeriesHeader timeSeriesHeader, List<Document> events, TimeSeriesArray<TimeSeriesHeader> requestTimeSeriesArray){
		List<Document> mergedEvents = new ArrayList<>(events.size() + requestTimeSeriesArray.size());
		int x = 0;
		int  y = 0;
		long lastDate = Long.MIN_VALUE;

		// MERGE AND DEDUPLICATE, PREFER EXISTING RELIABLE OVER ARCHIVE
		while(x < events.size() && y < requestTimeSeriesArray.size()) {
			long a = events.get(x).getDate("t").getTime();
			long b = requestTimeSeriesArray.getTime(y);
			if(a < b){
				if (a > lastDate) {
					mergedEvents.add(events.get(x));
					lastDate = a;
				}
				x++;
			}
			else if (b < a){
				if(b > lastDate){
					mergedEvents.add(new Document("t", new Date(b)).append("v", (double)requestTimeSeriesArray.getValue(y)).append("f", (int)requestTimeSeriesArray.getFlag(y)).append("c", requestTimeSeriesArray.getComment(y)));
					lastDate = b;
				}
				y++;
			}
			else{
				if (a > lastDate) {
					mergedEvents.add(requestTimeSeriesArray.isValueReliable(y) ? new Document("t", new Date(b)).append("v", (double)requestTimeSeriesArray.getValue(y)).append("f", (int)requestTimeSeriesArray.getFlag(y)).append("c", requestTimeSeriesArray.getComment(y)) : events.get(x));
					lastDate = a;
				}
				x++;
				y++;
			}
		}
		while(x < events.size()) {
			long a = events.get(x).getDate("t").getTime();
			if (a > lastDate) {
				mergedEvents.add(events.get(x));
				lastDate = a;
			}
			x++;
		}
		while(y < requestTimeSeriesArray.size()) {
			long b = requestTimeSeriesArray.getTime(y);
			if(b > lastDate) {
				mergedEvents.add(new Document("t", new Date(b)).append("v", (double)requestTimeSeriesArray.getValue(y)).append("f", (int)requestTimeSeriesArray.getFlag(y)).append("c", requestTimeSeriesArray.getComment(y)));
				lastDate = b;
			}
			y++;
		}
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

		if (!events.isEmpty()){
			List<Document> eventsDeduplicate = events.stream().collect(Collectors.groupingBy(t -> t.getDate("t"))).values().stream().map(s -> s.get(0)).sorted(Comparator.comparing(s -> s.getDate("t"))).collect(Collectors.toList());

			if(events.size() != eventsDeduplicate.size()) {
				Exception ex = new Exception(String.format("Duplicate event dates found and removed -> [%s]", events.stream().collect(Collectors.groupingBy(t -> t.getDate("t"))).entrySet().stream().filter(f -> f.getValue().size() > 1).map(s -> String.format("%s: %s", s.getKey().toString(), s.getValue().size())).collect(Collectors.joining(","))));
				logger.warn(LogUtil.getLogMessageJson(ex, Map.of("timeSeriesHeader", timeSeriesHeader.toString())).toJson(), ex);
			}

			long[] times = new long[eventsDeduplicate.size()];
			for (int i = 0; i < eventsDeduplicate.size(); i++)
				times[i] = eventsDeduplicate.get(i).getDate("t").getTime();

			if(timeSeriesHeader.getTimeStep() == IrregularTimeStep.INSTANCE)
				timeSeriesArray.ensureTimes(times);
			else
				timeSeriesArray.ensurePeriod(new Period(times[0], times[times.length-1]));

			for (int i = 0; i < eventsDeduplicate.size(); i++)  {
				timeSeriesArray.setValue(i,  eventsDeduplicate.get(i).get("v") != null ? eventsDeduplicate.get(i).getDouble("v").floatValue() : Float.NaN);
				timeSeriesArray.setFlag(i, eventsDeduplicate.get(i).getInteger("f").byteValue());
				timeSeriesArray.setComment(i, eventsDeduplicate.get(i).getString("c"));
			}
		}
		return timeSeriesArray;
	}
}
