package nl.fews.archivedatabase.mongodb.migrate.interfaces;

/**
 * The interface Throwing consumer.
 * @param <T> the type parameter
 * @param <E> the type parameter
 */
public interface ThrowingConsumer<T, E extends Exception> {

	/**
	 * Accept.
	 * @param t the t
	 * @throws E the e
	 */
	void accept(T t) throws E;
}
