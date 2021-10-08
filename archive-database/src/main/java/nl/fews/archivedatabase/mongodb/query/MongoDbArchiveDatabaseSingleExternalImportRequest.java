package nl.fews.archivedatabase.mongodb.query;

import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesHeader;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class MongoDbArchiveDatabaseSingleExternalImportRequest implements SingleExternalDataImportRequest {
	/**
	 *
	 */
	private final Period period;

	/**
	 *
	 */
	private final Map<String, List<Object>> query;

	/**
	 *
	 */
	private final TimeSeriesValueType timeSeriesValueType;

	/**
	 *
	 */
	private final TimeSeriesType timeSeriesType;

	/**
	 *
	 */
	private final TimeSeriesArray<TimeSeriesHeader> timeSeriesArray;

	/**
	 *
	 * @param period period
	 */
	public MongoDbArchiveDatabaseSingleExternalImportRequest(Period period, Map<String, List<Object>> query, TimeSeriesValueType timeSeriesValueType, TimeSeriesType timeSeriesType, TimeSeriesArray<TimeSeriesHeader> timeSeriesArray) {
		this.period = period;
		this.query = query;
		this.timeSeriesValueType = timeSeriesValueType;
		this.timeSeriesType = timeSeriesType;
		this.timeSeriesArray = timeSeriesArray;
	}

	/**
	 *
	 * @return TimeSeriesArray<TimeSeriesHeader>
	 */
	public TimeSeriesArray<TimeSeriesHeader> getTimeSeriesArray() {
		return timeSeriesArray;
	}

	/**
	 *
	 * @return Period
	 */
	@Override
	public Period getPeriod() {
		return period;
	}

	/**
	 *
	 * @return String
	 */
	public TimeSeriesType getTimeSeriesType() {
		return timeSeriesType;
	}

	/**
	 *
	 * @return Document
	 */
	public Map<String, List<Object>> getQuery() {
		return query;
	}

	/**
	 *
	 * @return TimeSeriesValueType
	 */
	public TimeSeriesValueType getTimeSeriesValueType() {
		return timeSeriesValueType;
	}
}
