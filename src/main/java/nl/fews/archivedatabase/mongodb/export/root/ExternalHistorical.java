package nl.fews.archivedatabase.mongodb.export.root;

import nl.fews.archivedatabase.mongodb.export.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.export.interfaces.Root;
import nl.fews.archivedatabase.mongodb.export.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;
import org.json.JSONArray;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ExternalHistorical implements Root {

	protected final ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter;

	/**
	 *
	 * @param archiveDatabaseTimeConverter archiveDatabaseTimeConverter. null if no conversion is to be performed.
	 */
	public ExternalHistorical(ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter){
		this.archiveDatabaseTimeConverter = archiveDatabaseTimeConverter;
	}

	/**
	 *
	 * @param header FEWS timeseries header
	 * @param timeSeriesDocuments the sorted list of timeseries event documents
	 * @param runInfoDocument the run info document
	 * @return bson document representing the root of this timeseries
	 */
	public Document getRoot(TimeSeriesHeader header, List<Document> timeSeriesDocuments, Document runInfoDocument){
		Document document = new Document();

		String timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeString(TimeSeriesType.EXTERNAL_HISTORICAL);
		String moduleInstanceId = header.getModuleInstanceId() == null ? "" : header.getModuleInstanceId();
		String locationId = header.getLocationId() == null ? "" : header.getLocationId();
		String parameterId = header.getParameterId() == null ? "" : header.getParameterId();
		String encodedTimeStepId = header.getTimeStep() == null || header.getTimeStep().getEncoded() == null ? "" : header.getTimeStep().getEncoded();
		List<String> qualifierIds = IntStream.range(0, header.getQualifierCount()).mapToObj(header::getQualifierId).sorted().collect(Collectors.toList());
		String qualifierId = qualifierIds.isEmpty() ? "" : new JSONArray(qualifierIds).toString();

		document.append("timeSeriesType", timeSeriesType);
		document.append("moduleInstanceId", moduleInstanceId);
		document.append("locationId", locationId);
		document.append("parameterId", parameterId);
		document.append("qualifierIds", qualifierIds);
		document.append("qualifierId", qualifierId);
		document.append("encodedTimeStepId", encodedTimeStepId);

		if (!timeSeriesDocuments.isEmpty()){

			document.append("startTime", timeSeriesDocuments.get(0).getDate("t"));
			document.append("endTime", timeSeriesDocuments.get(timeSeriesDocuments.size()-1).getDate("t"));

			if(timeSeriesDocuments.get(0).containsKey("lt")) document.append("localStartTime", timeSeriesDocuments.get(0).getDate("lt"));
			if(timeSeriesDocuments.get(timeSeriesDocuments.size()-1).containsKey("lt")) document.append("localEndTime", timeSeriesDocuments.get(timeSeriesDocuments.size()-1).getDate("lt"));
		}

		return document;
	}
}
