package nl.fews.archivedatabase.mongodb.migrate.interfaces;

public interface BucketHistorical {

	/**
	 *
	 * @param singletonCollection singletonCollection
	 * @param bucketCollection bucketCollection
	 */
	void bucketGroups(String singletonCollection, String bucketCollection);
}
