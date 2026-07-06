package nl.fews.archivedatabase.common.shared.timeseries;

import nl.fews.archivedatabase.common.shared.interfaces.TimeSeries;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;

import java.util.List;
import java.util.Map;

/**
 *
 */
public final class ScalarSimulatedHistoricalStitched extends ScalarTimeSeries implements TimeSeries {
	/**
	 *
	 * @param header FEWS timeseries header
	 * @param eventDocuments the sorted list of timeseries event documents
	 * @param runInfo the run info document
	 * @return document representing the root of this timeseries
	 */
	@Override
	public Map<String, Object> getRoot(FewsTimeSeriesHeader header, List<Map<String, Object>> eventDocuments, Map<String, Object> runInfo){
		var document = super.getRoot(header, eventDocuments, runInfo);

		var ensembleId = header.getEnsembleId() == null || header.getEnsembleId().equals("none") || header.getEnsembleId().equals("main") ? "" : header.getEnsembleId();
		var ensembleMemberId = header.getEnsembleMemberId() == null || header.getEnsembleMemberId().equals("none") || header.getEnsembleMemberId().equals("0") ? ensembleId : header.getEnsembleMemberId();

		document.put("ensembleId", ensembleId);
		document.put("ensembleMemberId", ensembleMemberId);

		return document;
	}

	/**
	 * @param header FEWS timeseries header
	 * @param areaId areaId
	 * @param sourceId sourceId
	 * @return document representing the metadata of this timeseries
	 */
	@Override
	public Map<String, Object> getMetaData(FewsTimeSeriesHeader header, String areaId, String sourceId){
		var document = super.getMetaData(header, areaId, sourceId);

		var ensembleMemberIndex = header.getEnsembleMemberIndex();
		document.put("ensembleMemberIndex", ensembleMemberIndex);

		return document;
	}
}
