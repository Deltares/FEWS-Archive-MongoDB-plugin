package nl.fews.archivedatabase.mongodb.migrate.timeseries;

import nl.fews.archivedatabase.mongodb.migrate.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.shared.utils.DateUtil;
import org.bson.Document;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.util.List;

/**
 *
 */
public class ScalarExternalHistorical extends ScalarTimeSeries implements TimeSeries {

	/**
	 *
	 * @param timeSeriesDocument header
	 * @param timeSeriesSet timeSeriesSet
	 * @param timeSeriesDocuments timeSeriesDocuments
	 * @param runInfoDocument runInfoDocument
	 * @return Document
	 */
	@Override
	public Document getRoot(Document timeSeriesDocument, JSONObject timeSeriesSet, List<Document> timeSeriesDocuments, Document runInfoDocument){
		Document document = super.getRoot(timeSeriesDocument, timeSeriesSet, timeSeriesDocuments, runInfoDocument);

		if (!document.containsKey("startTime"))
			return document;

		LocalDateTime s = DateUtil.getLocalDateTime(document.getDate("startTime"));
		LocalDateTime e = DateUtil.getLocalDateTime(document.getDate("endTime"));

		int bucket;
		//DAILY BUCKET
		if(s.getYear() == e.getYear() && s.getMonthValue() == e.getMonthValue() && s.getDayOfMonth() == e.getDayOfMonth())
			bucket = s.getYear() * 10000 + s.getMonthValue() * 100 + s.getDayOfMonth();
		//MONTHLY BUCKET
		else if(s.getYear() == e.getYear() && s.getMonthValue() == e.getMonthValue())
			bucket = s.getYear() * 100 + s.getMonthValue();
		//YEARLY BUCKET
		else if(s.getYear() == e.getYear())
			bucket = s.getYear();
		//DECADE BUCKET
		else
			bucket = s.getYear() / 10 * 10;

		document.append("bucket", bucket);

		return document;
	}
}