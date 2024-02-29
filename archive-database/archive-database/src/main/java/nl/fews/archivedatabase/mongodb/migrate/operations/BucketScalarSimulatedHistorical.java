package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.interfaces.BucketHistorical;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesUtil;
import org.bson.Document;

import java.util.*;

public final class BucketScalarSimulatedHistorical extends BucketHistoricalBase implements BucketHistorical {

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param collection collection
	 * @return List<Document>
	 */
	protected List<Document> getUnwoundTimeSeries(Document bucketKeyDocument, String collection){
		return TimeSeriesUtil.getStitchedTimeSeries(bucketKeyDocument, collection);
	}
}
