package nl.fews.archivedatabase.mongodb.query;

import com.mongodb.client.MongoCursor;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseReadResult;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.FewsTimeSeriesHeaderProvider;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.HeaderRequest;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.timeseries.*;
import org.bson.Document;

import java.util.List;

/**
 *
 */
public class MongoDbArchiveDatabaseReadResult implements ArchiveDatabaseReadResult {

	/**
	 *
	 */
	private final MongoCursor<Document> result;

	/**
	 *
	 */
	private final TimeSeriesType timeSeriesType;

	/**
	 *
	 */
	private final TimeSeriesValueType valueType;

	/**
	 *
	 * @param result result
	 * @param valueType valueType
	 * @param timeSeriesType timeSeriesType
	 */
	public MongoDbArchiveDatabaseReadResult(MongoCursor<Document> result, TimeSeriesValueType valueType, TimeSeriesType timeSeriesType) {
		this.result = result;
		this.timeSeriesType = timeSeriesType;
		this.valueType = valueType;
	}

	/**
	 *
	 * @return TimeSeriesArrays<TimeSeriesHeader>
	 */
	@Override
	public TimeSeriesArrays<TimeSeriesHeader> next() {
		Document next = result.next();

		HeaderRequest.HeaderRequestBuilder headerRequestBuilder = new HeaderRequest.HeaderRequestBuilder();
		headerRequestBuilder.setValueType(valueType);
		headerRequestBuilder.setTimeSeriesType(timeSeriesType);
		if(next.containsKey("moduleInstanceId")) headerRequestBuilder.setModuleInstanceId(next.getString("moduleInstanceId"));
		if(next.containsKey("locationId")) headerRequestBuilder.setLocationId(next.getString("locationId"));
		if(next.containsKey("parameterId")) headerRequestBuilder.setParameterId(next.getString("parameterId"));
		if(next.containsKey("qualifierIds")) headerRequestBuilder.setQualifiersIds(next.getList("qualifierIds", String.class).toArray(new String[0]));
		if(next.containsKey("encodedTimeStepId")) headerRequestBuilder.setTimeStep(TimeStepUtils.decode(next.getString("encodedTimeStepId")));

		//if(next.containsKey("forecastTime")) headerRequestBuilder.setForecastTime(next.getDate("forecastTime").getTime());
		//if(next.containsKey("ensembleId") && !next.getString("ensembleId").trim().equals("")) headerRequestBuilder.setEnsembleId(next.getString("ensembleId"));
		//if(next.containsKey("ensembleMemberId") && !next.getString("ensembleMemberId").trim().equals("")) headerRequestBuilder.setEnsembleMemberId(next.getString("ensembleMemberId"));

		//if(next.get("metaData", Document.class).containsKey("ensembleMemberIndex")) headerRequestBuilder.setEnsembleMemberIndex(next.get("metaData", Document.class).getInteger("ensembleMemberIndex"));
		//if(next.get("metaData", Document.class).containsKey("unit")) headerRequestBuilder.setUnit(next.get("metaData", Document.class).getString("unit"));
		//if(next.get("metaData", Document.class).containsKey("parameterName")) headerRequestBuilder.setParameterName(next.get("metaData", Document.class).getString("parameterName"));
		//if(next.get("metaData", Document.class).containsKey("locationName")) headerRequestBuilder.setLocationName(next.get("metaData", Document.class).getString("locationName"));
		//if(next.get("metaData", Document.class).containsKey("parameterType")) headerRequestBuilder.setParameterType(ParameterType.get(next.get("metaData", Document.class).getString("parameterType")));
		//if(next.get("metaData", Document.class).containsKey("approvedTime")) headerRequestBuilder.setApprovedTime(next.get("metaData", Document.class).getDate("approvedTime").getTime());

		TimeSeriesHeader timeSeriesHeader = Settings.get("headerProvider", FewsTimeSeriesHeaderProvider.class).getHeader(headerRequestBuilder.build());

		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader, timeSeriesHeader.getTimeStep());
		timeSeriesArray.setForecastTime(timeSeriesHeader.getForecastTime());

		List<Document> events = next.getList("timeseries", Document.class);

		long[] times = new long[events.size()];
		for (int i = 0; i < events.size(); i++)
			times[i] = events.get(i).getDate("t").getTime();
		timeSeriesArray.ensureTimes(times);

		for (int i = 0; i < events.size(); i++)  {
			timeSeriesArray.setValue(i,  events.get(i).getDouble("v") != null ? events.get(i).getDouble("v").floatValue() : Float.NaN);
			timeSeriesArray.setFlag(i, events.get(i).getInteger("f").byteValue());
			timeSeriesArray.setComment(i, events.get(i).getString("c"));
		}
		return new TimeSeriesArrays<>(timeSeriesArray);
	}

	/**
	 *
	 * @return boolean
	 */
	@Override
	public boolean hasNext() {
		return result.hasNext();
	}
}
