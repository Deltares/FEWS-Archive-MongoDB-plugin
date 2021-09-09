package nl.fews.archivedatabase.mongodb.query;

import com.mongodb.client.MongoCursor;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseReadResult;
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
	public MongoDbArchiveDatabaseReadResult(MongoCursor<Document> result) {
		this.result = result;
	}

	/**
	 *
	 * @return TimeSeriesArrays<TimeSeriesHeader>
	 */
	@Override
	public TimeSeriesArrays<TimeSeriesHeader> next() {
		Document next = result.next();
		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();

		if(next.containsKey("moduleInstanceId")) timeSeriesHeader.setModuleInstanceId(next.getString("moduleInstanceId"));
		if(next.containsKey("locationId")) timeSeriesHeader.setLocationId(next.getString("locationId"));
		if(next.containsKey("parameterId")) timeSeriesHeader.setParameterId(next.getString("parameterId"));
		if(next.containsKey("qualifierIds")) timeSeriesHeader.setQualifierIds(next.getList("qualifierIds", String.class).toArray(new String[0]));
		if(next.containsKey("encodedTimeStepId")) timeSeriesHeader.setTimeStep(TimeStepUtils.decode(next.getString("encodedTimeStepId")));
		if(next.containsKey("forecastTime")) timeSeriesHeader.setForecastTime(next.getDate("forecastTime").getTime());
		if(next.containsKey("ensembleId") && !next.getString("ensembleId").trim().equals("")) timeSeriesHeader.setEnsembleId(next.getString("ensembleId"));
		if(next.containsKey("ensembleMemberId") && !next.getString("ensembleMemberId").trim().equals("")) timeSeriesHeader.setEnsembleMemberId(next.getString("ensembleMemberId"));

		if(next.get("metaData", Document.class).containsKey("ensembleMemberIndex")) timeSeriesHeader.setEnsembleMemberIndex(next.get("metaData", Document.class).getInteger("ensembleMemberIndex"));
		if(next.get("metaData", Document.class).containsKey("unit")) timeSeriesHeader.setUnit(next.get("metaData", Document.class).getString("unit"));
		if(next.get("metaData", Document.class).containsKey("parameterName")) timeSeriesHeader.setParameterName(next.get("metaData", Document.class).getString("parameterName"));
		if(next.get("metaData", Document.class).containsKey("locationName")) timeSeriesHeader.setLocationName(next.get("metaData", Document.class).getString("locationName"));
		if(next.get("metaData", Document.class).containsKey("parameterType")) timeSeriesHeader.setParameterType(ParameterType.get(next.get("metaData", Document.class).getString("parameterType")));
		if(next.get("metaData", Document.class).containsKey("approvedTime")) timeSeriesHeader.setApprovedTime(next.get("metaData", Document.class).getDate("approvedTime").getTime());

		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader);
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
