package nl.fews.archivedatabase.postgres.database;

import org.json.JSONArray;
import org.json.JSONObject;
import org.postgresql.Driver;
import org.postgresql.util.PSQLException;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Supplier;
import java.util.regex.Pattern;

@SuppressWarnings("unchecked")
public final class DatabaseUtil {
	/**
	 *
	 */
	private static final int NUM_RETRIES = 5;

	/**
	 *
	 */
	private static final int RETRY_INTERVAL_MS = 1000;

	/**
	 * Static Class
	 */
	private DatabaseUtil(){}

	/**
	 *
	 * @param connectionString connectionString
	 * @return Set<String>
	 */
	public static Set<String> listTableNames(String connectionString){
		var sql = "SELECT table_name FROM information_schema.tables WHERE table_catalog = current_database() AND table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')";

		try (var c = DriverManager.getConnection(connectionString); var ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
			var tables = new HashSet<String>();
			while (rs.next())
				tables.add(rs.getString("table_name"));
			return tables;
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 *
	 * @param connectionString connectionString
	 * @param tableName tableName
	 * @return List<String>
	 */
	public static List<String> listColumnNames(String connectionString, String tableName){
		var sql = "SELECT column_name FROM information_schema.columns WHERE table_catalog = current_database() AND table_schema = 'public' AND table_name = ? ORDER BY ordinal_position";

		try (var c = DriverManager.getConnection(connectionString); var ps = c.prepareStatement(sql)) {
			ps.setString(1, tableName);
			var columns = new ArrayList<String>();
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next())
					columns.add(rs.getString("column_name"));
				return columns;
			}
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 *
	 * @param connectionString connectionString
	 */
	public static void ensureDatabase(String connectionString){
		var database = Driver.parseURL(connectionString, null).getProperty("PGDBNAME");
		connectionString = connectionString.replaceFirst("/" + Pattern.quote(database) + "(\\?|$)", "/postgres$1");

		if (!databaseExists(connectionString, database)) {
			try (var c = DriverManager.getConnection(connectionString); var ps = c.prepareStatement(Schema.getDatabase())) {
				ps.execute();
			}
			catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 *
	 * @param connectionString connectionString
	 * @param databaseName databaseName
	 * @return boolean
	 */
	public static boolean databaseExists(String connectionString, String databaseName) {
		var sql = "SELECT 1 FROM pg_database WHERE datname = ?";

		try (var c = DriverManager.getConnection(connectionString); var ps = c.prepareStatement(sql)) {
			ps.setString(1, databaseName);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next();
			}
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 *
	 * @param ps ps
	 * @param params params
	 * @throws SQLException SQLException
	 */
	public static void bindParams(PreparedStatement ps, List<Object> params) throws SQLException {
		for (int i = 0; i < params.size(); i++) {
			ps.setObject(i + 1, params.get(i));
		}
	}

	/**
	 *
	 * @param rs rs
	 * @return Map<String, Object>
	 * @throws SQLException SQLException
	 */
	public static Map<String, Object> resultSetToMap(ResultSet rs) throws SQLException {
		var m = rs.getMetaData();
		var r = new LinkedHashMap<String, Object>();
		for (int i = 1; i <= m.getColumnCount(); i++) {
			var value = rs.getObject(i);
			if (value instanceof java.sql.Array array)
				value = array.getArray();
			r.put(m.getColumnLabel(i), value);
		}
		return hierarchizeDocument(r, Schema.getTimeseriesFields(), Schema.getTimeseriesValueFields());
	}

	/**
	 *
	 * @param identifier identifier
	 * @return String
	 */
	public static String quoteIdentifier(String identifier) {
		return "\"" + identifier.replace(".", "_") + "\"";
	}

	/**
	 *
	 * @param document document
	 * @param timeseriesFields timeseriesFields
	 * @param prefix prefix
	 */
	public static Map<String, Object> flattenDocument(Map<String, Object> document, List<String> timeseriesFields, List<String> prefix){
		var flattened = new LinkedHashMap<String, Object>();
		document.forEach((k, v) -> {
			prefix.add(k);
			var path = String.join("_", prefix);
			if (v instanceof List){
				if (k.equals("timeseries")){
					var ts = (List<Map<String, Object>>)v;
					for(var field: timeseriesFields){
						var values = new Object[ts.size()];
						for(int i = 0; i < ts.size(); i++)
							values[i] = ts.get(i).get(field);
						flattened.put(path + "_" + field, values);
					}
				}
				else
					flattened.put(path, ((List<?>)v).toArray());
			}
			else if (v instanceof Map)
				flattened.putAll(flattenDocument((Map<String, Object>)v, timeseriesFields, prefix));
			else
				flattened.put(path, v);
			prefix.remove(prefix.size() - 1);
		});
		return flattened;
	}

	/**
	 *
	 * @param flattened flattened
	 * @param timeseriesFields timeseriesFields
	 * @return Map<String, Object>
	 */
	public static Map<String, Object> hierarchizeDocument(Map<String, Object> flattened, List<String> timeseriesFields, Set<String> timeseriesValueFields) {
		var hierarchized = new LinkedHashMap<String, Object>();
		var ts = new LinkedHashMap<String, Object[]>();
		flattened.forEach((k, v) -> {
			var prefix = List.of(k.split("_"));
			if (prefix.contains("timeseries"))
				ts.put(prefix.get(prefix.indexOf("timeseries")+1), (Object[]) v);
			else {
				Map<String, Object> current = hierarchized;
				for (int i = 0; i < prefix.size() - 1; i++)
					current = (Map<String, Object>) current.computeIfAbsent(prefix.get(i), x -> new LinkedHashMap<>());
				current.put(prefix.get(prefix.size() - 1), v);
			}
		});
		int rowCount = ts.isEmpty() ? 0 : ts.values().iterator().next().length;
		var tsList = new ArrayList<Map<String, Object>>(rowCount);
		for (int i = 0; i < rowCount; i++) {
			var row = new LinkedHashMap<String, Object>();
			for (String field : timeseriesFields) {
				var v = ts.get(field);
				if (v != null && (v[i] != null || timeseriesValueFields.contains(field)))
					row.put(field, v[i]);
			}
			tsList.add(row);
		}
		hierarchized.put("timeseries", tsList);
		return hierarchized;
	}

	/**
	 *
	 * @param operation operation
	 * @return <T>
	 * @param <T> <T>
	 */
	public static <T> T retry(String sql, Object params, Supplier<T> operation) {
		for (int i = 1; i <= NUM_RETRIES; i++) {
			try {
				return operation.get();
			}
			catch (Exception ex) {
				if (i == NUM_RETRIES || (ex instanceof PSQLException && ((PSQLException)ex).getSQLState().equals("23505"))){
					if (ex instanceof PSQLException && ((PSQLException)ex).getServerErrorMessage() != null){
						var message = ((PSQLException)ex).getServerErrorMessage().getDetail();
						params = params instanceof List ? new JSONArray(params).toString() : params instanceof Map ? new JSONObject(params).toString() : params;
						throw new RuntimeException(new JSONObject(Map.of("sql", sql, "params", params, "message", message)).toString(), ex);
					}
					else
						throw new RuntimeException(ex);
				}

				try {
					Thread.sleep(RETRY_INTERVAL_MS);
				}
				catch (InterruptedException iex) {
					Thread.currentThread().interrupt();
					throw new RuntimeException(iex);
				}
			}
		}
		throw new IllegalStateException(new JSONObject(Map.of("sql", sql, "params", params)).toString());
	}
}
