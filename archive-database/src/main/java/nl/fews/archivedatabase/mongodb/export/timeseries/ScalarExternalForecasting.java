package nl.fews.archivedatabase.mongodb.export.timeseries;

import nl.fews.archivedatabase.mongodb.export.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.DateUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.Date;
import java.util.List;

/**
 *
 */
public class ScalarExternalForecasting extends ScalarExternalHistorical implements TimeSeries {
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

		String ensembleId = header.getEnsembleId() != null && !header.getEnsembleId().equals("none") ? header.getEnsembleId() : "";
		String ensembleMemberId = header.getEnsembleMemberId() != null && !header.getEnsembleMemberId().equals("none") ? header.getEnsembleMemberId() : "";
		Date forecastTime = new Date(header.getForecastTime());
		Date localForecastTime = Settings.get("archiveDatabaseTimeConverter") == null ? null : DateUtil.getDates(Settings.get("archiveDatabaseTimeConverter", ArchiveDatabaseTimeConverter.class).convert(new long[]{header.getForecastTime()}))[0];

		if (forecastTime.equals(new Date(0)))
			throw new IllegalArgumentException("header.getForecastTime() cannot be null or default");

		document.append("ensembleId", ensembleId);
		document.append("ensembleMemberId", ensembleMemberId);
		document.append("forecastTime", forecastTime);
		if(localForecastTime != null) document.append("localForecastTime", localForecastTime);

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
