package nl.fews.archivedatabase.common.export.interfaces;

import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;

import java.util.Map;

public interface Synchronize {

	/**
	 * Inserts, updates or replaces data for timeseries
	 * @param timeSeries the entire list of all documents passed to this instance
	 * @param timeSeriesType timeSeriesType
	 */
	void synchronize(Map<String, Object> timeSeries, TimeSeriesType timeSeriesType);
}
