package nl.fews.archivedatabase.mongodb.export.timeseries;

import nl.fews.archivedatabase.mongodb.export.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
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
public class ScalarSimulatedForecasting extends ScalarExternalForecasting implements TimeSeries {
	/**
	 *
	 * @param header FEWS timeseries header
	 * @param timeSeries the sorted list of timeseries event documents
	 * @param runInfo the run info document
	 * @return bson document representing the root of this timeseries
	 */
	@Override
	public Document getRoot(TimeSeriesHeader header, List<Document> timeSeries, Document runInfo){
		Document document = super.getRoot(header, timeSeries, runInfo);

		String taskRunId = runInfo.getString("taskRunId");

		if (taskRunId == null || taskRunId.equals(""))
			throw new IllegalArgumentException("runInfo.getString(\"taskRunId\") cannot be null or empty");

		document.append("taskRunId", taskRunId);

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

		Date approvedTime = new Date(header.getApprovedTime());
		document.append("approvedTime", approvedTime);

		return document;
	}

	/**
	 *
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

		if(header instanceof FewsTimeSeriesHeader){
			FewsTimeSeriesHeader fewsHeader = (FewsTimeSeriesHeader)header;
			TaskRunDescriptor taskRun = fewsHeader.getTaskRunDescriptor();
			TaskDescriptor taskDescriptor = taskRun.getTaskDescriptor();

			String taskRunId = taskRun.getId();
			Date dispatchTime = taskRun.getDispatchTime() != 0 ? new Date(taskRun.getDispatchTime()) : null;
			String mcId = taskRun.getMasterControllerId();
			String userId = taskDescriptor.getTaskProperties().getUserId();
			Date time0 = taskRun.getTime0() != 0 ? new Date(taskRun.getTime0()) : null;
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
}
