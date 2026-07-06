package nl.fews.archivedatabase.common.migrate.operations;

import nl.fews.archivedatabase.common.migrate.interfaces.BucketHistorical;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesUtil;

import java.util.List;
import java.util.Map;

public final class BucketScalarExternalHistorical extends BucketHistoricalBase implements BucketHistorical {

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param singletonCollection singletonCollection
	 * @return List<Map<String, Object>>
	 */
	protected List<Map<String, Object>> getUnwoundTimeSeries(Map<String, Object> bucketKeyDocument, String singletonCollection){
		return TimeSeriesUtil.getUnwoundTimeSeries(bucketKeyDocument, singletonCollection);
	}
}
