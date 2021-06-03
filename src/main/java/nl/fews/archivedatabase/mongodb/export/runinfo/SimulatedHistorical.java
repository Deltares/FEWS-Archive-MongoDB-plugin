package nl.fews.archivedatabase.mongodb.export.runinfo;

import nl.fews.archivedatabase.mongodb.export.interfaces.RunInfo;
import org.bson.Document;

public class SimulatedHistorical extends ExternalForecasting implements RunInfo {

	/**
	 *
	 * @return bson document representing the run info of this timeseries
	 */
	public Document getRunInfo() {
		Document document = super.getRunInfo();

		document.append("dispatchTime", "dispatchTime");
		document.append("taskRunId", "taskRunId");
		document.append("mcId", "mcId");
		document.append("userId", "userId");
		document.append("time0", "time0");
		document.append("workflowId", "workflowId");
		document.append("configRevisionNumber", "configRevisionNumber");

		return document;
	}
}
