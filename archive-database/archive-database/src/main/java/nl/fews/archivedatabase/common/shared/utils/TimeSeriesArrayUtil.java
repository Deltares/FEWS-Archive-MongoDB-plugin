package nl.fews.archivedatabase.common.shared.utils;

import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.common.shared.tuple.Pair;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.fews.archivedatabase.interfaces.FewsTimeSeriesHeaderProvider;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.HeaderRequest;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.Box;
import nl.wldelft.util.Period;
import nl.wldelft.util.ThreadUtils;
import nl.wldelft.util.timeseries.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

@SuppressWarnings("unchecked")
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
	 * @return TimeSeriesArray<FewsTimeSeriesHeader>
	 */
	public static Box<FewsTimeSeriesHeader, SystemActivityDescriptor> getTimeSeriesHeader(TimeSeriesValueType timeSeriesValueType, TimeSeriesType timeSeriesType, Map<String, Object> result){
		var headerRequestBuilder = new HeaderRequest.HeaderRequestBuilder();
		headerRequestBuilder.setValueType(timeSeriesValueType);
		headerRequestBuilder.setTimeSeriesType(timeSeriesType);

		if(result.containsKey("moduleInstanceId")) headerRequestBuilder.setModuleInstanceId((String)result.get("moduleInstanceId"));
		if(result.containsKey("locationId")) headerRequestBuilder.setLocationId((String)result.get("locationId"));
		if(result.containsKey("parameterId")) headerRequestBuilder.setParameterId((String)result.get("parameterId"));
		if(result.containsKey("qualifierIds")) headerRequestBuilder.setQualifiersIds(((List<String>)result.get("qualifierIds")).toArray(new String[0]));
		if(result.containsKey("encodedTimeStepId")) headerRequestBuilder.setTimeStep(TimeStepUtils.decode((String)result.get("encodedTimeStepId")));

		if(result.containsKey("forecastTime")) headerRequestBuilder.setExternalForecastTime(((Date)result.get("forecastTime")).getTime());
		if(result.containsKey("ensembleId") && !((String)result.get("ensembleId")).trim().isEmpty() && !result.get("ensembleId").equals("none") && !result.get("ensembleId").equals("main")) headerRequestBuilder.setEnsembleId((String)result.get("ensembleId"));
		if(result.containsKey("ensembleMemberId") && !((String)result.get("ensembleMemberId")).trim().isEmpty() && !result.get("ensembleMemberId").equals("none") && !result.get("ensembleMemberId").equals("0")) headerRequestBuilder.setEnsembleMember((String)result.get("ensembleMemberId"));

		if(result.containsKey("runInfo")){
			var runInfo = (Map<String, Object>)result.get("runInfo");
			if(runInfo.containsKey("taskRunId")) headerRequestBuilder.setTaskRunId((String)runInfo.get("taskRunId"));
			if(runInfo.containsKey("userId")) headerRequestBuilder.setUserId((String)runInfo.get("userId"));
			if(runInfo.containsKey("workflowId")) headerRequestBuilder.setWorkflowId((String)runInfo.get("workflowId"));
			if(runInfo.containsKey("dispatchTime")) headerRequestBuilder.setDispatchTime(((Date)runInfo.get("dispatchTime")).getTime());
			if(runInfo.containsKey("time0")) headerRequestBuilder.setTimeZero(((Date)runInfo.get("time0")).getTime());
		}
		return getTimeSeriesHeader(result, headerRequestBuilder);
	}

	/**
	 * BUG
	 * @param headerRequestBuilder headerRequestBuilder
	 * @return Box<FewsTimeSeriesHeader, SystemActivityDescriptor>
	 */
	private static Box<FewsTimeSeriesHeader, SystemActivityDescriptor> getTimeSeriesHeader(Map<String, Object> result, HeaderRequest.HeaderRequestBuilder headerRequestBuilder){
		var headerBox = new AtomicReference<Box<FewsTimeSeriesHeader, SystemActivityDescriptor>>();
		for (int i = 0; i < 5; i++) {
			var thread = ThreadUtils.newThread(() -> headerBox.set(Settings.get("headerProvider", FewsTimeSeriesHeaderProvider.class).getHeader(headerRequestBuilder.build())), "getTimeSeriesHeader");
			thread.start();
			try {
				thread.join(1000);
			}
			catch (InterruptedException e) {/*IGNORE*/}

			var h = headerBox.get();
			if (h != null && h.getObject0() != null && !h.getObject0().equals(FewsTimeSeriesHeader.NONE) && !h.getObject0().isNone()) {
				return h;
			}
			else{
				System.err.printf("FewsTimeSeriesHeaderProvider.getHeader(headerRequestBuilder.build()) FAILED %s%n", new JSONObject(result));
				System.out.printf("FewsTimeSeriesHeaderProvider.getHeader(headerRequestBuilder.build()) FAILED %s%n", new JSONObject(result));
				logger.info(String.format("FewsTimeSeriesHeaderProvider.getHeader(headerRequestBuilder.build()) FAILED %s", new JSONObject(result)));
			}
			try{
				Thread.sleep(100);
			}
			catch (InterruptedException e) {/*IGNORE*/}
		}
		return headerBox.get();
	}

	/**
	 *
	 * @param timeSeriesHeader timeSeriesHeader
	 * @param events events
	 * @return TimeSeriesArray<FewsTimeSeriesHeader>
	 */
	public static TimeSeriesArray<FewsTimeSeriesHeader> getTimeSeriesArray(FewsTimeSeriesHeader timeSeriesHeader, List<Map<String, Object>> events){
		var timeSeriesArray = new TimeSeriesArray<FewsTimeSeriesHeader>(timeSeriesHeader, timeSeriesHeader.getTimeStep());
		timeSeriesArray.setForecastTime(timeSeriesHeader.getForecastTime());

		if (events.isEmpty())
			return timeSeriesArray;

		var eventLookup = new LinkedHashMap<Long, Map<String, Object>>();
		long[] times = new long[events.size()];
		for (int i = 0; i < events.size(); i++) {
			times[i] = ((Date)events.get(i).get("t")).getTime();
			eventLookup.put(times[i], events.get(i));
		}

		if(timeSeriesHeader.getTimeStep() == IrregularTimeStep.INSTANCE)
			timeSeriesArray.ensureTimes(times);
		else
			timeSeriesArray.ensurePeriod(new Period(times[0], times[times.length-1]));

		for (int i = 0; i < timeSeriesArray.size(); i++)  {
			if(eventLookup.containsKey(timeSeriesArray.getTime(i))){
				var event = eventLookup.get(timeSeriesArray.getTime(i));
				timeSeriesArray.setValue(i,  event.get("v") != null ? ((Double)event.get("v")).floatValue() : Float.NaN);
				timeSeriesArray.setFlag(i, ((Integer)event.get("f")).byteValue());
				timeSeriesArray.setComment(i, (String)event.get("c"));
				timeSeriesArray.setFlagSource(i, event.get("fs") != null && FlagSource.get((String)event.get("fs")) != null ? FlagSource.get((String)event.get("fs")).toByte() : timeSeriesArray.getFlagSource(i));
				timeSeriesArray.setUser(i, (String)event.get("u"));
			}
		}
		return timeSeriesArray;
	}

	/**
	 *
	 * @param timeSeriesArray timeSeriesArray
	 * @return Map<Boolean, List<Period>>, true = missingPeriod, false = existingPeriod
	 */
	public static Map<Boolean, List<Period>> getTimeSeriesArrayExistingPeriods(TimeSeriesArray<FewsTimeSeriesHeader> timeSeriesArray){
		var existingPeriods = new LinkedHashMap<Boolean, List<Period>>(Map.of(true, new ArrayList<>(), false, new ArrayList<>()));
		long[] times = timeSeriesArray.toTimesArray();
		float[] values = timeSeriesArray.toFloatArray();

		if(values.length == 0)
			return existingPeriods;

		final Boolean[] prev = {Float.isNaN(values[0])};
		AtomicInteger group = new AtomicInteger(0);

		IntStream.range(0, times.length).mapToObj(i -> new Pair<>(times[i], values[i])).collect(Collectors.groupingBy(p -> {
			if(!prev[0].equals(Float.isNaN(p.getValue1()))){
				group.incrementAndGet();
			}
			prev[0] = Float.isNaN(p.getValue1());
			return group.get();
		})).forEach((k, v) -> existingPeriods.get(Float.isNaN(v.get(0).getValue1())).add(new Period(v.get(0).getValue0(), v.get(v.size()-1).getValue0())));
		return existingPeriods;
	}

	/**
	 * @param a timeSeriesArray
	 * @param b timeSeriesArray
	 * @return true if both empty or all time / value pairs match, else false
	 */
	public static boolean timeSeriesArrayValuesAreEqual(TimeSeriesArray<FewsTimeSeriesHeader> a, TimeSeriesArray<FewsTimeSeriesHeader> b){
		if(a.size() != b.size())
			return false;

		if(a.isEmpty())
			return true;

		if(a.getTime(a.size()-1) != b.getTime(a.size()-1))
			return false;

		if(a.getValue(a.size()-1) != b.getValue(a.size()-1))
			return false;

		for (int i = 0; i < a.size(); i++) {
			if(Float.isNaN(a.getValue(i)) && Float.isNaN(b.getValue(i)))
				continue;

			if(a.getValue(i) != b.getValue(i))
				return false;

			if(a.getTime(i) != b.getTime(i))
				return false;
		}
		return true;
	}
}
