package nl.fews.archivedatabase.mongodb.migrate.operations;

import nl.fews.archivedatabase.mongodb.migrate.interfaces.BucketHistorical;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesUtil;
import org.bson.Document;

import java.util.List;

public final class BucketScalarExternalHistorical extends BucketHistoricalBase implements BucketHistorical {

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param singletonCollection singletonCollection
	 * @return List<Document>
	 */
	protected List<Document> getUnwoundTimeSeries(Document bucketKeyDocument, String singletonCollection){
		return TimeSeriesUtil.getUnwoundTimeSeries(bucketKeyDocument, singletonCollection);
	}
}
