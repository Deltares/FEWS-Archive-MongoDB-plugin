package nl.fews.archivedatabase.mongodb.export.interfaces;

import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import org.bson.Document;

import java.util.List;

public interface Synchronize {

	/**
	 * Inserts, updates or replaces data for timeseries
	 * @param timeSeries the entire list of all documents passed to this instance
	 * @param timeSeriesType timeSeriesType
	 */
	void synchronize(Document timeSeries, TimeSeriesType timeSeriesType);
}
