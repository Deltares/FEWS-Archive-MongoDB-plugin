package nl.fews.archivedatabase.mongodb.export.runinfo;

import nl.fews.archivedatabase.mongodb.export.interfaces.RunInfo;
import org.bson.Document;

public class ExternalHistorical implements RunInfo {

	/**
	 *
	 * @return bson document representing the run info of this timeseries
	 */
	public Document getRunInfo() {
		return new Document();
	}
}
