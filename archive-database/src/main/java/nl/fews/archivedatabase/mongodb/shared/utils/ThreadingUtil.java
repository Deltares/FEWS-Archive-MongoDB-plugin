package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.migrate.interfaces.ThrowingConsumer;

import java.util.function.Consumer;

/**
 *
 */
public final class ThreadingUtil{

	/**
	 * Static Class
	 */
	private ThreadingUtil(){}

	/**
	 * Throwing consumer.
	 *
	 * @param <T> the type parameter
	 * @param c   the c
	 * @return the consumer
	 */
	public static <T> Consumer<T> throwing(ThrowingConsumer<T, Exception> c) {
		return i -> {
			try {
				c.accept(i);
			}
			catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		};
	}
}