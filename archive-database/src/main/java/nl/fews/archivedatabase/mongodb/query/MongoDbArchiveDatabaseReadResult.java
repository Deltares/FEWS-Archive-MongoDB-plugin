package nl.fews.archivedatabase.mongodb.query;

import com.mongodb.client.MongoCursor;
import nl.fews.archivedatabase.mongodb.query.utils.TimeSeriesArrayUtil;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseReadResult;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.timeseries.*;
import org.bson.Document;

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
	private final TimeSeriesValueType timeSeriesValueType;

	/**
	 *
	 * @param result result
	 * @param timeSeriesValueType valueType
	 * @param timeSeriesType timeSeriesType
	 */
	public MongoDbArchiveDatabaseReadResult(MongoCursor<Document> result, TimeSeriesValueType timeSeriesValueType, TimeSeriesType timeSeriesType) {
		this.result = result;
		this.timeSeriesType = timeSeriesType;
		this.timeSeriesValueType = timeSeriesValueType;
	}

	/**
	 *
	 * @return TimeSeriesArrays<TimeSeriesHeader>
	 */
	@Override
	public TimeSeriesArrays<TimeSeriesHeader> next() {
		Document next = result.next();
		return new TimeSeriesArrays<>(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesValueType, timeSeriesType, next));
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
