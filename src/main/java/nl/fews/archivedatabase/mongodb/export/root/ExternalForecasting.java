package nl.fews.archivedatabase.mongodb.export.root;

import nl.fews.archivedatabase.mongodb.export.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.export.interfaces.Root;
import nl.fews.archivedatabase.mongodb.export.utils.DateUtil;
import nl.fews.archivedatabase.mongodb.export.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.externaldatasource.opendatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.Date;
import java.util.List;

public class ExternalForecasting extends ExternalHistorical implements Root {

	/**
	 *
	 * @param archiveDatabaseTimeConverter archiveDatabaseTimeConverter. null if no conversion is to be performed.
	 */
	public ExternalForecasting(ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter){
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

		String timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeString(TimeSeriesType.EXTERNAL_FORECASTING);
		String ensembleId = header.getEnsembleId() != null && !header.getEnsembleId().equals("none") ? header.getEnsembleId() : "";
		String ensembleMemberId = header.getEnsembleMemberId() != null && !header.getEnsembleMemberId().equals("none") ? header.getEnsembleMemberId() : "";
		Date forecastTime = new Date(header.getForecastTime());
		Date localForecastTime = archiveDatabaseTimeConverter == null ? null : DateUtil.getDates(archiveDatabaseTimeConverter.convert(new long[]{header.getForecastTime()}))[0];

		document.append("timeSeriesType", timeSeriesType);
		document.append("ensembleId", ensembleId);
		document.append("ensembleMemberId", ensembleMemberId);
		document.append("forecastTime", forecastTime);
		if(localForecastTime != null) document.append("localForecastTime", localForecastTime);

		return document;
	}
}
