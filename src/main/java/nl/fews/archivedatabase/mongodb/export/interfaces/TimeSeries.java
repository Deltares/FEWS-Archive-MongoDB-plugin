package nl.fews.archivedatabase.mongodb.export.interfaces;

import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.List;

public interface TimeSeries {
	/**
	 *
	 * @param timeSeriesArray FEWS timeseries array
	 * @return the sorted list of timeseries event documents
	 */
	List<Document> getTimeSeries(TimeSeriesArray<TimeSeriesHeader> timeSeriesArray);
}
