package nl.fews.archivedatabase.common.shared.interfaces;

import nl.wldelft.archive.util.runinfo.ArchiveRunInfo;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.util.timeseries.TimeSeriesArray;

import java.util.List;
import java.util.Map;

public interface TimeSeries {
	/**
	 *
	 * @param header FEWS timeseries header
	 * @param eventDocuments the sorted list of timeseries event documents
	 * @param runInfoDocument the run info document
	 * @return document representing the root of this timeseries
	 */
	Map<String, Object> getRoot(FewsTimeSeriesHeader header, List<Map<String, Object>> eventDocuments, Map<String, Object> runInfoDocument);

	/**
	 * getMetaData
	 * @param header FEWS timeseries header
	 * @param areaId areaId
	 * @param sourceId sourceId
	 * @return document representing the metadata of this timeseries
	 */
	Map<String, Object> getMetaData(FewsTimeSeriesHeader header, String areaId, String sourceId);

	/**
	 *
	 * @param timeSeriesArray FEWS timeseries array
	 * @param metadataDocument metadataDocument
	 * @return the sorted list of timeseries event documents
	 */
	List<Map<String, Object>> getEvents(TimeSeriesArray<FewsTimeSeriesHeader> timeSeriesArray, Map<String, Object> metadataDocument);

	/**
	 * @param header FEWS timeseries header
	 * @return document representing the run info of this timeseries
	 */
	Map<String, Object> getRunInfo(FewsTimeSeriesHeader header);

	/**
	 * @param archiveRunInfo archiveRunInfo
	 * @return document representing the run info of this timeseries
	 */
	Map<String, Object> getRunInfo(ArchiveRunInfo archiveRunInfo);
}
