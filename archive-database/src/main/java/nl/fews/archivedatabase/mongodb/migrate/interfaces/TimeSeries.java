package nl.fews.archivedatabase.mongodb.migrate.interfaces;

import org.bson.Document;
import org.json.JSONObject;

import java.util.List;

public interface TimeSeries {
	/**
	 *
	 * @param timeSeriesDocument header
	 * @param timeSeriesSet timeSeriesSet
	 * @param timeSeriesDocuments timeSeriesDocuments
	 * @param runInfoDocument runInfoDocument
	 * @return Document
	 */
	Document getRoot(Document timeSeriesDocument, JSONObject timeSeriesSet, List<Document> timeSeriesDocuments, Document runInfoDocument);

	/**
	 * getMetaData
	 * @param timeSeriesDocument FEWS timeseries header
	 * @param timeSeriesSet timeSeriesSet
	 * @param netcdfMetaData netcdfMetaData
	 * @return bson document representing the meta data of this timeseries
	 */
	Document getMetaData(Document timeSeriesDocument, JSONObject timeSeriesSet, JSONObject netcdfMetaData);

	/**
	 *
	 * @param timeSeriesDocuments FEWS timeseries array
	 * @param timeSeriesSet timeSeriesSet
	 * @return the sorted list of timeseries event documents
	 */
	List<Document> getEvents(List<Document> timeSeriesDocuments, JSONObject timeSeriesSet);

	/**
	 * @param runInfo runInfo
	 * @return bson document representing the run info of this timeseries
	 */
	Document getRunInfo(JSONObject runInfo);
}
