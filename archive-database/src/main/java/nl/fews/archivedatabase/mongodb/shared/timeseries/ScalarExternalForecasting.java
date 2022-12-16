package nl.fews.archivedatabase.mongodb.shared.timeseries;

import nl.fews.archivedatabase.mongodb.shared.interfaces.TimeSeries;
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
public class ScalarExternalForecasting extends ScalarTimeSeries implements TimeSeries {

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

		String ensembleId = header.getEnsembleId() == null || header.getEnsembleId().equals("none") || header.getEnsembleId().equals("main") ? "" : header.getEnsembleId();
		String ensembleMemberId = header.getEnsembleMemberId() == null || header.getEnsembleMemberId().equals("none") || header.getEnsembleMemberId().equals("0") ? ensembleId : header.getEnsembleMemberId();
		Date forecastTime = new Date(header.getForecastTime());

		if (header.getForecastTime() == Long.MIN_VALUE && runInfo.containsKey("time0") && runInfo.get("time0") instanceof Date)
			forecastTime = runInfo.getDate("time0");

		if (header.getForecastTime() == Long.MIN_VALUE)
			throw new IllegalArgumentException("header.getForecastTime() cannot be null or default");

		Date localForecastTime = Settings.get("archiveDatabaseTimeConverter") == null ? null : DateUtil.getDates(Settings.get("archiveDatabaseTimeConverter", ArchiveDatabaseTimeConverter.class).convert(new long[]{forecastTime.getTime()}))[0];

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
	 * @return bson document representing the metadata of this timeseries
	 */
	@Override
	public Document getMetaData(TimeSeriesHeader header, String areaId, String sourceId){
		Document document = super.getMetaData(header, areaId, sourceId);

		int ensembleMemberIndex = header.getEnsembleMemberIndex();
		document.append("ensembleMemberIndex", ensembleMemberIndex);

		return document;
	}
}
