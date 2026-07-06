package nl.fews.archivedatabase.common.shared.interfaces;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface ClosableIterable<T> extends Iterable<T> {
	@Override
    ClosableIterator<T> iterator();

	@Override
    default void forEach(Consumer<? super T> action) {
		try (ClosableIterator<T> iterator = iterator()) {
			while (iterator.hasNext())
				action.accept(iterator.next());
		}
    }

	static <E> ClosableIterable<E> wrap(Supplier<Iterator<E>> iteratorSupplier) {
		return () -> {
			Iterator<E> iterator = iteratorSupplier.get();

			return new ClosableIterator<>() {
				@Override
				public boolean hasNext() {
					return iterator.hasNext();
				}

				@Override
				public E next() {
					return iterator.next();
				}

				@Override
				public void close() {
					if (iterator instanceof AutoCloseable) {
						try {
							((AutoCloseable) iterator).close();
						}
						catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				}
			};
		};
	}
}
