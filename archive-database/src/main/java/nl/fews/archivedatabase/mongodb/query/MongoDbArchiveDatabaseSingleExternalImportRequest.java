package nl.fews.archivedatabase.mongodb.query;

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
	private final String collection;

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
	public MongoDbArchiveDatabaseSingleExternalImportRequest(Period period, String collection, Map<String, List<Object>> query, TimeSeriesArray<TimeSeriesHeader> timeSeriesArray) {
		this.period = period;
		this.collection = collection;
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
	public String getCollection() {
		return collection;
	}

	/**
	 *
	 * @return Document
	 */
	public Map<String, List<Object>> getQuery() {
		return query;
	}
}
