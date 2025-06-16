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
import org.javatuples.Pair;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TimeSeriesArrayUtil {

	/**
	 *
	 */
	private static final Object mutex = new Object();

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
		if(result.containsKey("ensembleId") && !result.getString("ensembleId").trim().isEmpty() && !result.getString("ensembleId").equals("none") && !result.getString("ensembleId").equals("main")) headerRequestBuilder.setEnsembleId(result.getString("ensembleId"));
		if(result.containsKey("ensembleMemberId") && !result.getString("ensembleMemberId").trim().isEmpty() && !result.getString("ensembleMemberId").equals("none") && !result.getString("ensembleMemberId").equals("0")) headerRequestBuilder.setEnsembleMember(result.getString("ensembleMemberId"));

		if(result.containsKey("runInfo")){
			Document runInfo = result.get("runInfo", Document.class);
			if(runInfo.containsKey("taskRunId")) headerRequestBuilder.setTaskRunId(runInfo.getString("taskRunId"));
			if(runInfo.containsKey("userId")) headerRequestBuilder.setUserId(runInfo.getString("userId"));
			if(runInfo.containsKey("workflowId")) headerRequestBuilder.setWorkflowId(runInfo.getString("workflowId"));
			if(runInfo.containsKey("dispatchTime")) headerRequestBuilder.setDispatchTime(runInfo.getDate("dispatchTime").getTime());
			if(runInfo.containsKey("time0")) headerRequestBuilder.setTimeZero(runInfo.getDate("time0").getTime());
		}
		synchronized (mutex) {
			for (int i = 0; i < 5; i++) {
				var h = Settings.get("headerProvider", FewsTimeSeriesHeaderProvider.class).getHeader(headerRequestBuilder.build());
				if (h != null && h.getObject0() != null && !h.getObject0().equals(TimeSeriesHeader.NONE) && !h.getObject0().isNone()) {
					return h;
				}
				try{
					Thread.sleep(100);
				}
				catch (InterruptedException e) {/*IGNORE*/}
			}
			throw new RuntimeException("Settings.get(\"headerProvider\", FewsTimeSeriesHeaderProvider.class).getHeader(headerRequestBuilder.build()) FAILED");
		}
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

		if (events.isEmpty())
			return timeSeriesArray;

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
				timeSeriesArray.setFlagSource(i, event.get("fs") != null && FlagSource.get(event.getString("fs")) != null ? FlagSource.get(event.getString("fs")).toByte() : timeSeriesArray.getFlagSource(i));
				timeSeriesArray.setUser(i, event.getString("u"));
			}
		}
		return timeSeriesArray;
	}

	/**
	 *
	 * @param timeSeriesArray timeSeriesArray
	 * @return Map<Boolean, List<Period>>, true = missingPeriod, false = existingPeriod
	 */
	public static Map<Boolean, List<Period>> getTimeSeriesArrayExistingPeriods(TimeSeriesArray<TimeSeriesHeader> timeSeriesArray){
		Map<Boolean, List<Period>> existingPeriods = Map.of(true, new ArrayList<>(), false, new ArrayList<>());
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
	public static boolean timeSeriesArrayValuesAreEqual(TimeSeriesArray<TimeSeriesHeader> a, TimeSeriesArray<TimeSeriesHeader> b){
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

	/**
	 * @param existing timeSeriesArray
	 * @param current timeSeriesArray
	 * @return true if current contains new time / value pairs, else false
	 */
	public static boolean timeSeriesArrayValuesHasNew(TimeSeriesArray<TimeSeriesHeader> existing, TimeSeriesArray<TimeSeriesHeader> current){
		Map<Long, Float> existingNonNanTimes = IntStream.range(0, existing.size()).boxed().filter(i -> !Float.isNaN(existing.getValue(i))).collect(Collectors.toMap(existing::getTime, existing::getValue));
		return IntStream.range(0, current.size()).boxed().filter(i -> !Float.isNaN(current.getValue(i))).anyMatch(i -> !existingNonNanTimes.containsKey(current.getTime(i)));
	}
}
