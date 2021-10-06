package nl.fews.archivedatabase.mongodb.query.operations;

import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;

import java.util.List;

public class RequestExternalDataImportSingletons extends RequestExternalDataImportBase {

	/**
	 *
	 * @param collection collection
	 * @param period period
	 * @param timeSeriesArrays timeSeriesArrays
	 * @return List<SingleExternalDataImportRequest>
	 */
	@Override
	public List<SingleExternalDataImportRequest> getExternalDataImportRequests(String collection, Period period, TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays) {
		return null;
	}
}
