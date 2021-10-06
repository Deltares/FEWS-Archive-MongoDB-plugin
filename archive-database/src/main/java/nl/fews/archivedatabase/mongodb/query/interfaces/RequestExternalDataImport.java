package nl.fews.archivedatabase.mongodb.query.interfaces;

import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import java.util.List;

/**
 *
 */
public interface RequestExternalDataImport {
	/**
	 *
	 * @param collection collection
	 * @param period period
	 * @param timeSeriesArrays timeSeriesArrays
	 * @return List<SingleExternalDataImportRequest>
	 */
	List<SingleExternalDataImportRequest> getExternalDataImportRequests(String collection, Period period, TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays);
}
