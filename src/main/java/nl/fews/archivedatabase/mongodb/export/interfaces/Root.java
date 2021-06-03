package nl.fews.archivedatabase.mongodb.export.interfaces;

import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.List;

public interface Root {
	/**
	 *
	 * @param header FEWS timeseries header
	 * @param timeSeriesDocuments the sorted list of timeseries event documents
	 * @param runInfoDocument the run info document
	 * @return bson document representing the root of this timeseries
	 */
	Document getRoot(TimeSeriesHeader header, List<Document> timeSeriesDocuments, Document runInfoDocument);
}
