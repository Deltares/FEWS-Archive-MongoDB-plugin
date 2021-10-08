package nl.fews.archivedatabase.mongodb.query.utils;

import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.FewsTimeSeriesHeaderProvider;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.HeaderRequest;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.IrregularTimeStep;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import nl.wldelft.util.timeseries.TimeStepUtils;
import org.bson.Document;

import java.util.List;

public class HeaderRequestUtil {

	private HeaderRequestUtil() {
	}

	public static TimeSeriesArray<TimeSeriesHeader> getTimeSeriesArray(TimeSeriesValueType timeSeriesValueType, TimeSeriesType timeSeriesType, Document result){
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

		if(result.containsKey("runInfo") && result.get("runInfo", Document.class).containsKey("taskRunId")) headerRequestBuilder.setTaskRunId(result.get("runInfo", Document.class).getString("taskRunId"));
		if(result.containsKey("runInfo") && result.get("runInfo", Document.class).containsKey("userId")) headerRequestBuilder.setUserId(result.get("runInfo", Document.class).getString("userId"));
		if(result.containsKey("runInfo") && result.get("runInfo", Document.class).containsKey("workflowId")) headerRequestBuilder.setWorkflowId(result.get("runInfo", Document.class).getString("workflowId"));

		//if(result.get("metaData", Document.class).containsKey("ensembleMemberIndex")) headerRequestBuilder.setEnsembleMemberIndex(result.get("metaData", Document.class).getInteger("ensembleMemberIndex"));
		//if(result.get("metaData", Document.class).containsKey("unit")) headerRequestBuilder.setUnit(result.get("metaData", Document.class).getString("unit"));
		//if(result.get("metaData", Document.class).containsKey("parameterName")) headerRequestBuilder.setParameterName(result.get("metaData", Document.class).getString("parameterName"));
		//if(result.get("metaData", Document.class).containsKey("locationName")) headerRequestBuilder.setLocationName(result.get("metaData", Document.class).getString("locationName"));
		//if(result.get("metaData", Document.class).containsKey("parameterType")) headerRequestBuilder.setParameterType(ParameterType.get(result.get("metaData", Document.class).getString("parameterType")));
		//if(result.get("metaData", Document.class).containsKey("approvedTime")) headerRequestBuilder.setApprovedTime(result.get("metaData", Document.class).getDate("approvedTime").getTime());

		TimeSeriesHeader timeSeriesHeader = Settings.get("headerProvider", FewsTimeSeriesHeaderProvider.class).getHeader(headerRequestBuilder.build());

		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader, timeSeriesHeader.getTimeStep());
		timeSeriesArray.setForecastTime(timeSeriesHeader.getForecastTime());

		List<Document> events = result.getList("timeseries", Document.class);

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
