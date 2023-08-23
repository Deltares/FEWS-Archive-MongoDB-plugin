package nl.fews.archivedatabase.mongodb.query;

import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.ArchiveDatabaseStitchedSimulatedHistoricalImportRequest;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MongoDbArchiveDatabaseStitchedSimulatedHistoricalImportRequest extends ArchiveDatabaseStitchedSimulatedHistoricalImportRequest {
	/**
	 *
	 */
	private final Map<String, List<Object>> query;

	/**
	 *
	 */
	private final List<Period> existingPeriods;

	/**
	 *
	 * @param period period
	 */
	public MongoDbArchiveDatabaseStitchedSimulatedHistoricalImportRequest(List<TimeSeriesHeader> timeSeriesHeaders, Period period, List<Period> existingPeriods, Map<String, List<Object>> query) {
		super(timeSeriesHeaders, period);
		this.query = query;
		this.existingPeriods = existingPeriods;
	}

	/**
	 *
	 * @return Map<String, List<Object>>
	 */
	public Map<String, List<Object>> getQuery() {
		return query;
	}

	/**
	 *
	 * @return List<Period>
	 */
	public List<Period> getExistingPeriods() {
		return existingPeriods;
	}
}
