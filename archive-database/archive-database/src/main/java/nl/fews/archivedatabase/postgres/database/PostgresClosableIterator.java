package nl.fews.archivedatabase.postgres.database;

import nl.fews.archivedatabase.common.shared.interfaces.ClosableIterator;

import java.sql.*;
import java.util.Map;
import java.util.NoSuchElementException;

public class PostgresClosableIterator implements ClosableIterator<Map<String, Object>> {
	private final Connection connection;
	private final PreparedStatement preparedStatement;
	private final ResultSet resultSet;
	private Boolean hasNext = null;
	private boolean closed = false;

	public PostgresClosableIterator(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) throws SQLException {
		this.connection = connection;
		this.preparedStatement = preparedStatement;
		this.resultSet = resultSet;
	}

	@Override
	public boolean hasNext() {
		if (hasNext != null)
			return hasNext;
		try {
			hasNext = resultSet.next();
			if (!hasNext)
				close();
			return hasNext;
		}
		catch (SQLException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public Map<String, Object> next() {
		if (!hasNext())
			throw new NoSuchElementException();

		try {
			hasNext = null;
			return DatabaseUtil.resultSetToMap(resultSet);
		}
		catch (SQLException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		if (closed)
			return;
		closed = true;
		try { if (resultSet != null) resultSet.close(); } catch (SQLException ignored) {}
		try { if (preparedStatement != null) preparedStatement.close(); } catch (SQLException ignored) {}
		try { if (connection != null) connection.setAutoCommit(true);  } catch (SQLException ignored) {}
		try { if (connection != null) connection.setReadOnly(false); } catch (SQLException ignored) {}
		try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
	}
}