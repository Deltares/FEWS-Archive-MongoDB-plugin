package nl.fews.archivedatabase.mongodb.query;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseReadResult;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;

public class MongoDbArchiveDatabaseReadResult implements ArchiveDatabaseReadResult {
	@Override
	public TimeSeriesArrays<TimeSeriesHeader> next() {
		return null;
	}

	@Override
	public boolean hasNext() {
		return false;
	}
}
