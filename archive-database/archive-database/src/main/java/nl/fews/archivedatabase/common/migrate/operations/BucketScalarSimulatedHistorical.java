package nl.fews.archivedatabase.common.migrate.operations;

import nl.fews.archivedatabase.common.migrate.interfaces.BucketHistorical;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesUtil;

import java.util.*;

public final class BucketScalarSimulatedHistorical extends BucketHistoricalBase implements BucketHistorical {

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param collection collection
	 * @return List<Map<String, Object>>
	 */
	protected List<Map<String, Object>> getUnwoundTimeSeries(Map<String, Object> bucketKeyDocument, String collection){
		return TimeSeriesUtil.getStitchedTimeSeries(bucketKeyDocument, collection);
	}
}
