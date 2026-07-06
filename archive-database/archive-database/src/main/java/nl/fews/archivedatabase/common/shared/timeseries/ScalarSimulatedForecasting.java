package nl.fews.archivedatabase.common.shared.timeseries;

import nl.fews.archivedatabase.common.shared.interfaces.TimeSeries;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.common.shared.utils.DateUtil;
import nl.wldelft.archive.util.runinfo.ArchiveRunInfo;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ScalarSimulatedForecasting extends ScalarTimeSeries implements TimeSeries {

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
		var taskRunId = (String)runInfo.get("taskRunId");

		if (header.getForecastTime() == Long.MIN_VALUE && runInfo.containsKey("time0") && runInfo.get("time0") instanceof Date)
			forecastTime = (Date)runInfo.get("time0");

		if (forecastTime.getTime() == Long.MIN_VALUE)
			throw new IllegalArgumentException("header.getForecastTime() cannot be null or default");

		if (taskRunId == null || taskRunId.isEmpty())
			throw new IllegalArgumentException("runInfo.getString(\"taskRunId\") cannot be null or empty");

		var localForecastTime = Settings.get("archiveDatabaseTimeConverter") == null ? null : DateUtil.getDates(Settings.get("archiveDatabaseTimeConverter", ArchiveDatabaseTimeConverter.class).convert(new long[]{forecastTime.getTime()}))[0];

		document.put("ensembleId", ensembleId);
		document.put("ensembleMemberId", ensembleMemberId);
		document.put("forecastTime", forecastTime);
		if(localForecastTime != null) document.put("localForecastTime", localForecastTime);
		document.put("taskRunId", taskRunId);

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
		var approvedTime = new Date(header.getApprovedTime());

		document.put("ensembleMemberIndex", ensembleMemberIndex);
		document.put("approvedTime", approvedTime);

		return document;
	}

	/**
	 *
	 * @param header header
	 * @return document representing the run info of this timeseries
	 */
	@Override
	public Map<String, Object> getRunInfo(FewsTimeSeriesHeader header) {
		var document = super.getRunInfo(header);

		var taskRun = header.getTaskRunDescriptor();
		var taskDescriptor = taskRun.getTaskDescriptor();

		var taskRunId = taskRun.getId();
		var dispatchTime = new Date(taskRun.getDispatchTime());
		var mcId = taskRun.getMasterControllerId();
		var userId = taskDescriptor.getTaskProperties().getUserId();
		var time0 = new Date(taskRun.getTime0());
		var workflowId = taskDescriptor.getWorkflowId();
		var configRevisionNumber = Settings.get("configRevision");

		document.put("dispatchTime", dispatchTime);
		document.put("taskRunId", taskRunId);
		document.put("mcId", mcId);
		document.put("userId", userId);
		document.put("time0", time0);
		document.put("workflowId", workflowId);
		document.put("configRevisionNumber", configRevisionNumber);

		return document;
	}

	/**
	 *
	 * @param archiveRunInfo archiveRunInfo
	 * @return document representing the run info of this timeseries
	 */
	@Override
	public Map<String, Object> getRunInfo(ArchiveRunInfo archiveRunInfo) {
		var document = super.getRunInfo(archiveRunInfo);

		var taskRunId = archiveRunInfo.getTaskRunId();
		var dispatchTime = new Date(archiveRunInfo.getDispatchTime());
		var mcId = archiveRunInfo.getMcId();
		var userId = archiveRunInfo.getUserId();
		var time0 = new Date(archiveRunInfo.getTime0());
		var workflowId = archiveRunInfo.getWorkflowId();
		var configRevisionNumber = archiveRunInfo.getConfigRevisionNumber();

		document.put("dispatchTime", dispatchTime);
		document.put("taskRunId", taskRunId);
		document.put("mcId", mcId);
		document.put("userId", userId);
		document.put("time0", time0);
		document.put("workflowId", workflowId);
		document.put("configRevisionNumber", configRevisionNumber);

		return document;
	}
}
