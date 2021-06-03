package nl.fews.archivedatabase.mongodb.export.root;

import nl.fews.archivedatabase.mongodb.export.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.export.interfaces.Root;
import nl.fews.archivedatabase.mongodb.export.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.externaldatasource.opendatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.List;

public class SimulatedForecasting extends ExternalForecasting implements Root {

	/**
	 *
	 * @param archiveDatabaseTimeConverter archiveDatabaseTimeConverter. null if no conversion is to be performed.
	 */
	public SimulatedForecasting(ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter){
		super(archiveDatabaseTimeConverter);
	}

	/**
	 *
	 * @param header FEWS timeseries header
	 * @param timeSeriesDocuments the sorted list of timeseries event documents
	 * @param runInfoDocument the run info document
	 * @return bson document representing the root of this timeseries
	 */
	@Override
	public Document getRoot(TimeSeriesHeader header, List<Document> timeSeriesDocuments, Document runInfoDocument){
		Document document = super.getRoot(header, timeSeriesDocuments, runInfoDocument);

		String timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeString(TimeSeriesType.SIMULATED_FORECASTING);
		String taskRunId = runInfoDocument.getString("taskRunId");

		document.append("timeSeriesType", timeSeriesType);
		document.append("taskRunId", taskRunId);

		return document;
	}
}
