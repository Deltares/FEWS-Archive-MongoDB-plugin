package nl.fews.archivedatabase.mongodb.query;

import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.ArchiveDatabaseObservedImportRequest;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.TimeSeriesHeader;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class MongoDbArchiveDatabaseObservedImportRequest extends ArchiveDatabaseObservedImportRequest {
	/**
	 *
	 */
	private final Map<String, List<Object>> query;

	/**
	 *
	 * @param period period
	 */
	public MongoDbArchiveDatabaseObservedImportRequest(List<TimeSeriesHeader> timeSeriesHeaders, Period period, Map<String, List<Object>> query) {
		super(timeSeriesHeaders, period);
		this.query = query;
	}

	/**
	 *
	 * @return Map<String, List<Object>>
	 */
	public Map<String, List<Object>> getQuery() {
		return query;
	}
}
