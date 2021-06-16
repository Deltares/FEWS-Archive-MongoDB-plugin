package nl.fews.archivedatabase.mongodb.migrate.timeseries;

import nl.fews.archivedatabase.mongodb.migrate.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import org.bson.Document;
import org.json.JSONObject;

import java.util.Date;
import java.util.List;

/**
 *
 */
public class ScalarExternalForecasting extends ScalarExternalHistorical implements TimeSeries {

	/**
	 *
	 * @param timeSeriesDocument header
	 * @param timeSeriesSet timeSeriesSet
	 * @return Document
	 */
	@Override
	public Document getMetaData(Document timeSeriesDocument, JSONObject timeSeriesSet, JSONObject netcdfMetaData){
		Document document = super.getMetaData(timeSeriesDocument, timeSeriesSet, netcdfMetaData);

		int ensembleMemberIndex = timeSeriesSet.has("ensembleMemberIndex") ? timeSeriesSet.getInt("ensembleMemberIndex") : -1;
		document.append("ensembleMemberIndex", ensembleMemberIndex);

		return document;
	}

	/**
	 *
	 *
	 * @param timeSeriesDocument timeSeriesDocument
	 * @param timeSeriesSet timeSeriesSet
	 * @param timeSeriesDocuments timeSeriesDocuments
	 * @param runInfoDocument runInfoDocument
	 * @return Document
	 */
	@Override
	public Document getRoot(Document timeSeriesDocument, JSONObject timeSeriesSet, List<Document> timeSeriesDocuments, Document runInfoDocument){
		Document document = super.getRoot(timeSeriesDocument, timeSeriesSet, timeSeriesDocuments, runInfoDocument);

		String ensembleId = timeSeriesSet.has("ensembleId") ? timeSeriesSet.getString("ensembleId") : "";
		String ensembleMemberId;
		ensembleMemberId = timeSeriesSet.has("ensembleMemberId") ? timeSeriesSet.get("ensembleMemberId").toString() : "";
		Date forecastTime = timeSeriesDocument.getDate("forecastTime");
		long time = Settings.get("archiveDatabaseTimeConverter", ArchiveDatabaseTimeConverter.class).convert(new long[]{forecastTime.getTime()})[0];
		Date localForecastTime = Settings.get("archiveDatabaseTimeConverter") != null ? new Date(time) : null;

		document.append("ensembleId", ensembleId);
		document.append("ensembleMemberId", ensembleMemberId);
		document.append("forecastTime", forecastTime);
		if(localForecastTime != null) document.append("localForecastTime", localForecastTime);

		document.remove("bucket");

		return document;
	}
}
