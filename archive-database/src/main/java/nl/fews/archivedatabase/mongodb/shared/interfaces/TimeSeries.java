package nl.fews.archivedatabase.mongodb.shared.interfaces;

import nl.wldelft.archive.util.runinfo.ArchiveRunInfo;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.List;

public interface TimeSeries {
	/**
	 *
	 * @param header FEWS timeseries header
	 * @param eventDocuments the sorted list of timeseries event documents
	 * @param runInfoDocument the run info document
	 * @return bson document representing the root of this timeseries
	 */
	Document getRoot(TimeSeriesHeader header, List<Document> eventDocuments, Document runInfoDocument);

	/**
	 * getMetaData
	 * @param header FEWS timeseries header
	 * @param areaId areaId
	 * @param sourceId sourceId
	 * @return bson document representing the meta data of this timeseries
	 */
	Document getMetaData(TimeSeriesHeader header, String areaId, String sourceId);

	/**
	 *
	 * @param timeSeriesArray FEWS timeseries array
	 * @param metadataDocument metadataDocument
	 * @return the sorted list of timeseries event documents
	 */
	List<Document> getEvents(TimeSeriesArray<TimeSeriesHeader> timeSeriesArray, Document metadataDocument);

	/**
	 * @param header FEWS timeseries header
	 * @return bson document representing the run info of this timeseries
	 */
	Document getRunInfo(TimeSeriesHeader header);

	/**
	 * @param archiveRunInfo archiveRunInfo
	 * @return bson document representing the run info of this timeseries
	 */
	Document getRunInfo(ArchiveRunInfo archiveRunInfo);
}
