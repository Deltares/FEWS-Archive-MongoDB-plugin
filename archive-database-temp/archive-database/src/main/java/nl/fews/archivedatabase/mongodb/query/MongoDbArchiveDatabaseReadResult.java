package nl.fews.archivedatabase.mongodb.query;

import com.mongodb.client.MongoCursor;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesArrayUtil;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseReadResult;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.Box;
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
	public Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor> next() {
		Document next = result.next();
		Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(timeSeriesValueType, timeSeriesType, next);
		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), next.getList("timeseries", Document.class));
		return new Box<>(new TimeSeriesArrays<>(timeSeriesArray), timeSeriesHeader.getObject1());
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
