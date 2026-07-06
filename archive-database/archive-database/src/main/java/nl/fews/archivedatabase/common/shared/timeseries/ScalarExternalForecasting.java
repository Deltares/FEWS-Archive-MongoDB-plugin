package nl.fews.archivedatabase.common.shared.timeseries;

import nl.fews.archivedatabase.common.shared.interfaces.TimeSeries;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.common.shared.utils.DateUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class ScalarExternalForecasting extends ScalarTimeSeries implements TimeSeries {

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
		var forecastTime = new Date(header.getForecastTime());

		if (header.getForecastTime() == Long.MIN_VALUE && runInfo.containsKey("time0") && runInfo.get("time0") instanceof Date)
			forecastTime = (Date)runInfo.get("time0");

		if (header.getForecastTime() == Long.MIN_VALUE)
			throw new IllegalArgumentException("header.getForecastTime() cannot be null or default");

		var localForecastTime = Settings.get("archiveDatabaseTimeConverter") == null ? null : DateUtil.getDates(Settings.get("archiveDatabaseTimeConverter", ArchiveDatabaseTimeConverter.class).convert(new long[]{forecastTime.getTime()}))[0];

		document.put("ensembleId", ensembleId);
		document.put("ensembleMemberId", ensembleMemberId);
		document.put("forecastTime", forecastTime);
		if(localForecastTime != null) document.put("localForecastTime", localForecastTime);

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
