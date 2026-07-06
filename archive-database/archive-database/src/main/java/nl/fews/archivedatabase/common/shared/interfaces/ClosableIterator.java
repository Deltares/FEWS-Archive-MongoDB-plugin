package nl.fews.archivedatabase.common.shared.interfaces;

import java.util.Iterator;

public interface ClosableIterator<T> extends Iterator<T>, AutoCloseable {
    @Override
    void close();
}