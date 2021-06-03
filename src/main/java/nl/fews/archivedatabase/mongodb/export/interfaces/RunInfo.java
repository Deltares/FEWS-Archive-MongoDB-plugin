package nl.fews.archivedatabase.mongodb.export.interfaces;

import org.bson.Document;

public interface RunInfo {
	/**
	 *
	 * @return bson document representing the run info of this timeseries
	 */
	Document getRunInfo();
}
