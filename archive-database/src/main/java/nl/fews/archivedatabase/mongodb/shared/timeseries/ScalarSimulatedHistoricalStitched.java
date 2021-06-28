package nl.fews.archivedatabase.mongodb.shared.timeseries;

import nl.fews.archivedatabase.mongodb.shared.interfaces.TimeSeries;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.List;

/**
 *
 */
public final class ScalarSimulatedHistoricalStitched extends ScalarTimeSeries implements TimeSeries {
	/**
	 *
	 * @param header FEWS timeseries header
	 * @param eventDocuments the sorted list of timeseries event documents
	 * @param runInfo the run info document
	 * @return bson document representing the root of this timeseries
	 */
	@Override
	public Document getRoot(TimeSeriesHeader header, List<Document> eventDocuments, Document runInfo){
		Document document = super.getRoot(header, eventDocuments, runInfo);

		String ensembleId = header.getEnsembleId() != null && !header.getEnsembleId().equals("none") ? header.getEnsembleId() : "";
		String ensembleMemberId = header.getEnsembleMemberId() != null && !header.getEnsembleMemberId().equals("none") ? header.getEnsembleMemberId() : "";

		document.append("ensembleId", ensembleId);
		document.append("ensembleMemberId", ensembleMemberId);

		return document;
	}

	/**
	 * @param header FEWS timeseries header
	 * @param areaId areaId
	 * @param sourceId sourceId
	 * @return bson document representing the meta data of this timeseries
	 */
	@Override
	public Document getMetaData(TimeSeriesHeader header, String areaId, String sourceId){
		Document document = super.getMetaData(header, areaId, sourceId);

		int ensembleMemberIndex = header.getEnsembleMemberIndex();
		document.append("ensembleMemberIndex", ensembleMemberIndex);

		return document;
	}
}
