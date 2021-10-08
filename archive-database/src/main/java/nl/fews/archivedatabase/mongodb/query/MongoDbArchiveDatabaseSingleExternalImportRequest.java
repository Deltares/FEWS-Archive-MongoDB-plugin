package nl.fews.archivedatabase.mongodb.query;

import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
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
	private final TimeSeriesType timeSeriesType;

	/**
	 *
	 */
	private final Map<String, List<Object>> query;

	/**
	 *
	 */
	private final TimeSeriesArray<TimeSeriesHeader> timeSeriesArray;

	/**
	 *
	 * @param period period
	 */
	public MongoDbArchiveDatabaseSingleExternalImportRequest(Period period, TimeSeriesType timeSeriesType, Map<String, List<Object>> query, TimeSeriesArray<TimeSeriesHeader> timeSeriesArray) {
		this.period = period;
		this.timeSeriesType = timeSeriesType;
		this.query = query;
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
}
