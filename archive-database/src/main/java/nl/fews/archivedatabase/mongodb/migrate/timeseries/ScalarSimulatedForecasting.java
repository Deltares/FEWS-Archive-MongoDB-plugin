package nl.fews.archivedatabase.mongodb.migrate.timeseries;

import nl.fews.archivedatabase.mongodb.migrate.interfaces.TimeSeries;
import org.bson.Document;
import org.json.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 *
 */
public class ScalarSimulatedForecasting extends ScalarExternalForecasting implements TimeSeries {

	/**
	 *
	 * @param timeSeriesDocument header
	 * @param timeSeriesSet timeSeriesSet
	 * @param timeSeries timeSeriesDocuments
	 * @param runInfo runInfoDocument
	 * @return Document
	 */
	@Override
	public Document getRoot(Document timeSeriesDocument, JSONObject timeSeriesSet, List<Document> timeSeries, Document runInfo){
		Document document = super.getRoot(timeSeriesDocument, timeSeriesSet, timeSeries, runInfo);

		String taskRunId = runInfo.getString("taskRunId");

		if (taskRunId == null || taskRunId.equals(""))
			throw new IllegalArgumentException("runInfo.getString(\"taskRunId\") cannot be null or empty");

		document.append("taskRunId", taskRunId);

		return document;
	}

	/**
	 *
	 * @return runInfo
	 */
	@Override
	public Document getRunInfo(JSONObject runInfo) {
		try{
			Document document = super.getRunInfo(runInfo);

			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date dispatchTime = simpleDateFormat.parse(String.format("%s %s", runInfo.getJSONObject("runInfo").getJSONObject("dispatchTime").getString("date"), runInfo.getJSONObject("runInfo").getJSONObject("dispatchTime").getString("time")));
			String taskRunId = runInfo.getJSONObject("runInfo").getString("taskRunId");
			String mcId = runInfo.getJSONObject("runInfo").getString("mcId");
			String userId = runInfo.getJSONObject("runInfo").has("userId") ? runInfo.getJSONObject("runInfo").getString("userId") : "";
			Date time0 = simpleDateFormat.parse(String.format("%s %s", runInfo.getJSONObject("runInfo").getJSONObject("time0").getString("date"), runInfo.getJSONObject("runInfo").getJSONObject("time0").getString("time")));
			String workflowId = runInfo.getJSONObject("runInfo").getString("workflowId");
			String configRevisionNumber = runInfo.getJSONObject("runInfo").getString("configRevisionId");

			document.append("dispatchTime", dispatchTime);
			document.append("taskRunId", taskRunId);
			document.append("mcId", mcId);
			document.append("userId", userId);
			document.append("time0", time0);
			document.append("workflowId", workflowId);
			document.append("configRevisionNumber", configRevisionNumber);
			return document;
		}
		catch (ParseException ex){
			throw new RuntimeException(ex);
		}
	}
}