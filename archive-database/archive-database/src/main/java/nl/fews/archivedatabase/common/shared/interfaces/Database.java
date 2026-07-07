package nl.fews.archivedatabase.common.shared.interfaces;

public interface Database {

	/**
	 *
	 * @param collection collection
	 */
	void ensureTable(String collection);

	/**
	 *
	 */
	void close();

	/**
	 *
	 * @param collection collection
	 */
	void drop(String collection);

	/**
	 *
	 * @param collection collection
	 * @param newName newName
	 */
	void rename(String collection, String newName);

	/**
	 *
	 * @param srcCollection srcCollection
	 * @param dstCollection dstCollection
	 */
	void replace(String srcCollection, String dstCollection);
}
