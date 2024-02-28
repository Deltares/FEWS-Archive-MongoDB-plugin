package nl.fews.archivedatabase.mongodb.shared.timeseries;

import nl.fews.archivedatabase.mongodb.shared.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.DateUtil;
import nl.wldelft.archive.util.runinfo.ArchiveRunInfo;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.runs.TaskDescriptor;
import nl.wldelft.fews.system.data.runs.TaskRunDescriptor;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.Date;
import java.util.List;

/**
 *
 */
public class ScalarSimulatedForecasting extends ScalarTimeSeries implements TimeSeries {

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
		String taskRunId = runInfo.getString("taskRunId");

		if (header.getForecastTime() == Long.MIN_VALUE && runInfo.containsKey("time0") && runInfo.get("time0") instanceof Date)
			forecastTime = runInfo.getDate("time0");

		if (forecastTime.getTime() == Long.MIN_VALUE)
			throw new IllegalArgumentException("header.getForecastTime() cannot be null or default");

		if (taskRunId == null || taskRunId.equals(""))
			throw new IllegalArgumentException("runInfo.getString(\"taskRunId\") cannot be null or empty");

		Date localForecastTime = Settings.get("archiveDatabaseTimeConverter") == null ? null : DateUtil.getDates(Settings.get("archiveDatabaseTimeConverter", ArchiveDatabaseTimeConverter.class).convert(new long[]{forecastTime.getTime()}))[0];

		document.append("ensembleId", ensembleId);
		document.append("ensembleMemberId", ensembleMemberId);
		document.append("forecastTime", forecastTime);
		if(localForecastTime != null) document.append("localForecastTime", localForecastTime);
		document.append("taskRunId", taskRunId);

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

		document.append("ensembleMemberIndex", "ensembleMemberIndex");
		document.append("approvedTime", "approvedTime");

		if(header instanceof FewsTimeSeriesHeader fewsHeader) {
			int ensembleMemberIndex = fewsHeader.getEnsembleMemberIndex();
			Date approvedTime = new Date(fewsHeader.getApprovedTime());

			document.append("ensembleMemberIndex", ensembleMemberIndex);
			document.append("approvedTime", approvedTime);
		}
		return document;
	}

	/**
	 *
	 * @param header header
	 * @return bson document representing the run info of this timeseries
	 */
	@Override
	public Document getRunInfo(TimeSeriesHeader header) {
		Document document = super.getRunInfo(header);

		document.append("dispatchTime", "dispatchTime");
		document.append("taskRunId", "taskRunId");
		document.append("mcId", "mcId");
		document.append("userId", "userId");
		document.append("time0", "time0");
		document.append("workflowId", "workflowId");
		document.append("configRevisionNumber", "configRevisionNumber");

		if(header instanceof FewsTimeSeriesHeader fewsHeader){
			TaskRunDescriptor taskRun = fewsHeader.getTaskRunDescriptor();
			TaskDescriptor taskDescriptor = taskRun.getTaskDescriptor();

			String taskRunId = taskRun.getId();
			Date dispatchTime = new Date(taskRun.getDispatchTime());
			String mcId = taskRun.getMasterControllerId();
			String userId = taskDescriptor.getTaskProperties().getUserId();
			Date time0 = new Date(taskRun.getTime0());
			String workflowId = taskDescriptor.getWorkflowId();
			String configRevisionNumber = Settings.get("configRevision");

			document.append("dispatchTime", dispatchTime);
			document.append("taskRunId", taskRunId);
			document.append("mcId", mcId);
			document.append("userId", userId);
			document.append("time0", time0);
			document.append("workflowId", workflowId);
			document.append("configRevisionNumber", configRevisionNumber);
		}
		return document;
	}

	/**
	 *
	 * @param archiveRunInfo archiveRunInfo
	 * @return bson document representing the run info of this timeseries
	 */
	@Override
	public Document getRunInfo(ArchiveRunInfo archiveRunInfo) {
		Document document = super.getRunInfo(archiveRunInfo);

		String taskRunId = archiveRunInfo.getTaskRunId();
		Date dispatchTime = new Date(archiveRunInfo.getDispatchTime());
		String mcId = archiveRunInfo.getMcId();
		String userId = archiveRunInfo.getUserId();
		Date time0 = new Date(archiveRunInfo.getTime0());
		String workflowId = archiveRunInfo.getWorkflowId();
		String configRevisionNumber = archiveRunInfo.getConfigRevisionNumber();

		document.append("dispatchTime", dispatchTime);
		document.append("taskRunId", taskRunId);
		document.append("mcId", mcId);
		document.append("userId", userId);
		document.append("time0", time0);
		document.append("workflowId", workflowId);
		document.append("configRevisionNumber", configRevisionNumber);

		return document;
	}
}
