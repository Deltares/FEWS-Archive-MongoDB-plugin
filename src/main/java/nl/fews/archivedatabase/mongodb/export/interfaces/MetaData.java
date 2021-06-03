package nl.fews.archivedatabase.mongodb.export.interfaces;

import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

public interface MetaData {
	/**
	 * getMetaData
	 * @param header FEWS timeseries header
	 * @return bson document representing the meta data of this timeseries
	 */
	Document getMetaData(TimeSeriesHeader header);
}
