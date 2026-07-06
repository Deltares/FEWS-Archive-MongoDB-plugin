package nl.fews.archivedatabase.common.shared.streaming;

import java.util.Iterator;

/**
 * Adding peek functionality to iterator
 */
public class PeekingIterator<T> implements Iterator<T>, AutoCloseable{
	/**
	 * result iterator from a query
	 */
	private final Iterator<T> iterator;

	/**
	 * the peek cache 'next' document
	 */
	private T next = null;

	/**
	 * Adding peek functionality to iterator
	 * @param iterator Iterator<T>
	 */
	public PeekingIterator(Iterator<T> iterator) {
		this.iterator = iterator;
		if(iterator.hasNext())
			next = iterator.next();
	}

	/**
	 * return the 'next' cached document without advancing the iterator. returns null when empty
	 * @return T
	 */
	public T peek(){
		return next;
	}

	/**
	 *
	 */
	@Override
	public void close(){
		try {
			if (iterator instanceof AutoCloseable) {
				((AutoCloseable) iterator).close();
			}
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * true of the next document (from peek) is not null
	 * @return boolean
	 */
	@Override
	public boolean hasNext() {
		return next != null;
	}

	/**
	 * return the 'next' cached document, advancing the iterator. returns null when empty
	 * @return T
	 */
	@Override
	public T next() {
		T r = next;
		next = iterator.hasNext() ? iterator.next() : null;
		return r;
	}
}
