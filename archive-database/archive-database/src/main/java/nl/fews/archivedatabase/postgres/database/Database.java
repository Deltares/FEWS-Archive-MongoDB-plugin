package nl.fews.archivedatabase.postgres.database;

import nl.fews.archivedatabase.common.shared.interfaces.ClosableIterable;
import nl.fews.archivedatabase.common.shared.settings.Settings;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 *
 */
@SuppressWarnings({"unused", "ConstantConditions"})
public final class Database implements nl.fews.archivedatabase.common.shared.interfaces.Database, AutoCloseable {

	/**
	 *
	 */
	private final ConcurrentHashMap<String, List<String>> columnNamesCache = new ConcurrentHashMap<>();

	/**
	 *
	 */
	private final HikariDataSource dataSource;

	/**
	 *
	 */
	private static String connectionString = null;

	/**
	 *
	 */
	public Database(){
		var connectionString = Settings.get("connectionString", String.class);
		
		DatabaseUtil.ensureDatabase(connectionString);

		var config = new HikariConfig();
		config.setJdbcUrl(connectionString);
		config.setMaximumPoolSize(32);
		config.setMinimumIdle(2);
		config.setPoolName("postgres-pool");
		config.setConnectionTimeout(30 * 1000);
		config.setIdleTimeout(10 * 60 * 1000);
		config.setMaxLifetime(30 * 60 * 1000);

		dataSource = new HikariDataSource(config);
		if (Database.connectionString == null || !Database.connectionString.equals(connectionString))
			ensureTables();
		Database.connectionString = connectionString;
	}

	/**
	 *
	 */
	@Override
	public void close(){
		dataSource.close();
	}

	/**
	 *
	 * @param table table
	 * @return List<String>
	 */
	public List<String> getColumnNames(String table){
		return columnNamesCache.computeIfAbsent(table, t -> DatabaseUtil.listColumnNames(connectionString, t));
	}

	/**
	 *
	 */
	private void ensureTables(){
		var tableNames = DatabaseUtil.listTableNames(connectionString);
		Schema.getTables().forEach(table -> {
			if(!tableNames.contains(table))
				CompletableFuture.runAsync(() -> ensureTable(table));
			else
				ensureTable(table);
		});
	}

	/**
	 *
	 * @param table table
	 */
	@Override
	public void ensureTable(String table){
		try (var c = dataSource.getConnection(); var ps = c.prepareStatement(Schema.getSettings())) {
			ps.execute();
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
		try (var c = dataSource.getConnection(); var ps = c.prepareStatement(Schema.getSchema(table))) {
			ps.execute();
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}



	/**
	 *
	 * @param table table
	 */
	@Override
	public void drop(String table){
		var query = Sql.render(Sql.ctx().dropTableIfExists(Sql.table(table)));
		DatabaseUtil.retry(query.sql(), query.params(), () -> {
			try (var c = dataSource.getConnection(); var ps = c.prepareStatement(query.sql())) {
				DatabaseUtil.bindParams(ps, query.params());
				ps.execute();
				columnNamesCache.remove(table);
				return null;
			}
			catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	/**
	 *
	 * @param table table
	 * @param newName newName
	 */
	@Override
	public void rename(String table, String newName){
		var query = Sql.render(Sql.ctx().alterTable(Sql.table(table)).renameTo(Sql.table(newName)));
		DatabaseUtil.retry(query.sql(), query.params(), () -> {
			try (var c = dataSource.getConnection();
				 var ps = c.prepareStatement(query.sql())) {
				DatabaseUtil.bindParams(ps, query.params());
				ps.execute();
				columnNamesCache.remove(table);
				columnNamesCache.remove(newName);
				return null;
			}
			catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	/**
	 *
	 * @param srcTable srcTable
	 * @param dstTable dstTable
	 */
	@Override
	public void replace(String srcTable, String dstTable){
		var tempTableName = String.format("TEMP_%s", dstTable);
		rename(dstTable, tempTableName);
		rename(srcTable, dstTable);
		drop(tempTableName);
	}

	/**
	 *
	 * @param sql sql
	 * @param params params
	 * @return Document
	 */
	public Map<String, Object> selectOne(String sql, List<Object> params){
		return DatabaseUtil.retry(sql, params, () -> {
			try (var c = dataSource.getConnection(); var ps = c.prepareStatement(sql)) {
				DatabaseUtil.bindParams(ps, params);
				try (var rs = ps.executeQuery()) {
					if (rs.next())
						return DatabaseUtil.resultSetToMap(rs);
					else
						return null;
				}
			}
			catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	/**
	 *
	 * @param sql sql
	 * @param params params
	 * @return Document
	 */
	public ClosableIterable<Map<String, Object>> select(String sql, List<Object> params){
		return ClosableIterable.wrap(() -> DatabaseUtil.retry(sql, params, () -> {
			try {
				var c = dataSource.getConnection();
				c.setAutoCommit(false);
				c.setReadOnly(true);
				var ps = c.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				ps.setFetchSize(1000);
				DatabaseUtil.bindParams(ps, params);
				return new PostgresClosableIterator(c, ps, ps.executeQuery());
			}
			catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}));
	}

	/**
	 *
	 * @param table table
	 * @param column column
	 * @param clazz clazz
	 * @return Set<T>
	 */
	public <T> Set<T> distinct(String table, String column, Map<String, Object> whereKeys, Class<T> clazz){
		var query = Sql.render(Sql.ctx().selectDistinct(Sql.field(column)).from(Sql.table(table)).where(Sql.condition(whereKeys)));
		return DatabaseUtil.retry(query.sql(), query.params(), () -> {
			try (var c = dataSource.getConnection(); var ps = c.prepareStatement(query.sql())) {
				DatabaseUtil.bindParams(ps, query.params());
				try (var rs = ps.executeQuery()) {
					var r = new HashSet<T>();
					while (rs.next())
						r.add(rs.getObject(1, clazz));
					return r;
				}
			}
			catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	/**
	 *
	 * @param table table
	 * @param document document
	 */
	public long insertOne(String table, Map<String, Object> document){
		var flattened = DatabaseUtil.flattenDocument(document, Schema.getTimeseriesFields(), List.of());
		var values = flattened.entrySet().stream().collect(Collectors.toMap(e -> Sql.field(e.getKey()), Map.Entry::getValue, (a, b) -> b, LinkedHashMap::new));
		var query = Sql.render(Sql.ctx().insertInto(Sql.table(table)).set(values).returning(Sql.field("_id")));
		return DatabaseUtil.retry(query.sql(), query.params(), () -> {
			try (var c = dataSource.getConnection(); var ps = c.prepareStatement(query.sql())) {
				DatabaseUtil.bindParams(ps, query.params());
				try (var rs = ps.executeQuery()) {
					rs.next();
					return rs.getLong(1);
				}
			}
			catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	/**
	 *
	 * @param table table
	 * @param documents documents
	 */
	public void insertMany(String table, List<Map<String, Object>> documents){
		var flattenedDocuments = new LinkedHashMap<String, List<Map<String, Object>>>();
		documents.forEach(document -> {
			var flattened = DatabaseUtil.flattenDocument(document, Schema.getTimeseriesFields(), List.of());
			var key = flattened.keySet().stream().sorted().collect(Collectors.joining("_"));
			flattenedDocuments.computeIfAbsent(key, k -> new ArrayList<>()).add(flattened);
		});
		flattenedDocuments.forEach((k, group) -> {
			var columns = new ArrayList<>(group.get(0).keySet());
			var fields = columns.stream().map(Sql::field).toList();
			var values = columns.stream().map(c -> Sql.val(null)).toList();
			var query = Sql.render(Sql.ctx().insertInto(Sql.table(table), fields).values(values));
			DatabaseUtil.retry(query.sql(), group.get(0), () -> {
				try (var c = dataSource.getConnection(); var ps = c.prepareStatement(query.sql())) {
					for(var document: group) {
						var params = columns.stream().map(document::get).toList();
						DatabaseUtil.bindParams(ps, params);
						ps.addBatch();
					}
					return ps.executeBatch();
				}
				catch (SQLException e) {
					throw new RuntimeException(e);
				}
			});
		});
	}

	/**
	 * 
	 * @param table table
	 * @param whereKeys whereKeys
	 * @return boolean
	 */
	public boolean exists(String table, Map<String, Object> whereKeys){
		var query = Sql.render(Sql.ctx().selectOne().from(Sql.table(table)).where(Sql.condition(whereKeys)).limit(1));
		return DatabaseUtil.retry(query.sql(), query.params(), () -> {
			try (var c = dataSource.getConnection(); var ps = c.prepareStatement(query.sql())) {
				DatabaseUtil.bindParams(ps, query.params());
				try (var rs = ps.executeQuery()){
					return rs.next();
				}
			}
			catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	/**
	 * 
	 * @param table table
	 * @param whereKeys whereKeys
	 * @param setValues setValues
	 */
	public void upsert(String table, Map<String, Object> whereKeys, Map<String, Object> setValues){
		var document = new LinkedHashMap<String, Object>();
		document.putAll(whereKeys);
		document.putAll(setValues);

		var flattened = DatabaseUtil.flattenDocument(document, Schema.getTimeseriesFields(), List.of());
		var insertValues = flattened.entrySet().stream().collect(Collectors.toMap(e -> Sql.field(e.getKey()), Map.Entry::getValue, (a, b) -> b, LinkedHashMap::new));
		var conflictFields = whereKeys.keySet().stream().map(Sql::field).toList();
		var updateValues = setValues.keySet().stream().collect(Collectors.toMap(Sql::field, Sql::excluded, (a, b) -> b, LinkedHashMap::new));

		var insert = Sql.ctx().insertInto(Sql.table(table)).set(insertValues).onConflict(conflictFields);
		var query = Sql.render(setValues.isEmpty() ? insert.doNothing() : insert.doUpdate().set(updateValues));

		DatabaseUtil.retry(query.sql(), query.params(), () -> {
			try (var c = dataSource.getConnection(); var ps = c.prepareStatement(query.sql())) {
				DatabaseUtil.bindParams(ps, query.params());
				return ps.executeUpdate();
			}
			catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	/**
	 * 
	 * @param table table
	 * @param whereKeys whereKeys
	 * @param setValues setValues
	 */
	public void update(String table, Map<String, Object> whereKeys, Map<String, Object> setValues){
		var updates = setValues.entrySet().stream().collect(Collectors.toMap(e -> Sql.field(e.getKey()), Map.Entry::getValue, (a, b) -> b, LinkedHashMap::new));
		var query = Sql.render(Sql.ctx().update(Sql.table(table)).set(updates).where(Sql.condition(whereKeys)));
		DatabaseUtil.retry(query.sql(), query.params(), () -> {
			try (var c = dataSource.getConnection(); var ps = c.prepareStatement(query.sql())) {
				DatabaseUtil.bindParams(ps, query.params());
				return ps.executeUpdate();
			}
			catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}

	/**
	 *
	 * @param table table
	 * @param whereKeys whereKeys
	 */
	public void delete(String table,  Map<String, Object> whereKeys){
		var query = Sql.render(Sql.ctx().deleteFrom(Sql.table(table)).where(Sql.condition(whereKeys)));
		DatabaseUtil.retry(query.sql(), query.params(), () -> {
			try (var c = dataSource.getConnection(); var ps = c.prepareStatement(query.sql())) {
				DatabaseUtil.bindParams(ps, query.params());
				return ps.executeUpdate();
			}
			catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}
}
