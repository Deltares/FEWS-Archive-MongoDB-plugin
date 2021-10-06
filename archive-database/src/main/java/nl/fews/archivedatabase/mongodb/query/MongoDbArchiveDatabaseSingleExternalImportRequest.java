package nl.fews.archivedatabase.mongodb.query;

import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.util.Period;
import org.bson.Document;

/**
 *
 */
public class MongoDbArchiveDatabaseSingleExternalImportRequest implements SingleExternalDataImportRequest {
	/**
	 *
	 */
	private final Period period;

	/**
	 *
	 */
	private final String collection;

	/**
	 *
	 */
	private final Document query;

	/**
	 *
	 * @param period period
	 */
	public MongoDbArchiveDatabaseSingleExternalImportRequest(Period period, String collection, Document query) {
		this.period = period;
		this.collection = collection;
		this.query = query;
	}

	/**
	 *
	 * @return Period
	 */
	@Override
	public Period getPeriod() {
		return period;
	}

	/**
	 *
	 * @return String
	 */
	public String getCollection() {
		return collection;
	}

	/**
	 *
	 * @return Document
	 */
	public Document getQuery() {
		return query;
	}
}
