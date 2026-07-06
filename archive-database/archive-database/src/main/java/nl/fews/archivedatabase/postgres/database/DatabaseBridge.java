package nl.fews.archivedatabase.postgres.database;

import nl.fews.archivedatabase.common.shared.interfaces.ClosableIterable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;


public final class DatabaseBridge implements nl.fews.archivedatabase.common.shared.interfaces.DatabaseBridge {

	private final Database db = new Database();

	@Override
	public void ensureCollection(String table){
		db.ensureCollection(table);
	}

	@Override
	public void close(){
		db.close();
	}

	@Override
	public void drop(String table) {
		db.drop(table);
	}

	@Override
	public void rename(String table, String newName){
		db.rename(table, newName);
	}

	@Override
	public void replace(String srcCollection, String dstCollection){
		db.replace(srcCollection, dstCollection);
	}

	@Override
	public void deleteOne(String table, Map<String, Object> whereKeys){
		db.delete(table, whereKeys);
	}

	@Override
	public void deleteMany(String table, Map<String, Object> whereKeys) {
		db.delete(table, whereKeys);
	}

	@Override
	public void updateOne(String table, Map<String, Object> whereKeys, Map<String, Object> setValues, boolean upsert){
		if (upsert)
			db.upsert(table, whereKeys, setValues);
		else
			db.update(table, whereKeys, setValues);
	}

	@Override
	public void updateOne(String table, Map<String, Object> whereKeys, Map<String, Object> setValues){
		db.update(table, whereKeys, setValues);
	}

	@Override
	public void updateMany(String table, Map<String, Object> whereKeys, Map<String, Object> setValues){
		db.update(table, whereKeys, setValues);
	}

	@Override
	public Object insertOne(String table, Map<String, Object> document) {
		return db.insertOne(table, document);
	}

	@Override
	public void insertMany(String table, List<Map<String, Object>> documents) {
		db.insertMany(table, documents);
	}

	@Override
	public <T> Set<T> distinct(String table, String column, Map<String, Object> whereKeys, Class<T> clazz){
		return db.distinct(table, column, whereKeys, clazz);
	}

	@Override
	public Map<String, Object> findOne(String table, Map<String, Object> whereKeys, Map<String, Object> projection){
		var query = Sql.render(Sql.ctx().select(Sql.selectFields(db.getColumnNames(table), projection)).from(Sql.table(table)).where(Sql.condition(whereKeys)).limit(1));
		return db.selectOne(query.sql(), query.params());
	}

	@Override
	public Map<String, Object> findOne(String table, Map<String, Object> query){
		return findOne(table, query, Map.of());
	}

	@Override
	public ClosableIterable<Map<String, Object>> find(String table, Map<String, Object> whereKeys){
		return find(table, whereKeys, Map.of());
	}

	@Override
	public ClosableIterable<Map<String, Object>> find(String table, Map<String, Object> whereKeys, Map<String, Object> projection){
		var query = Sql.render(Sql.ctx().
				select(Sql.selectFields(db.getColumnNames(table), projection)).
				from(Sql.table(table)).
				where(Sql.condition(whereKeys)));
		return db.select(query.sql(), query.params());
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateStitchedTimeSeries(String table, Map<String, Object> match, Map<String, Object> sort, Map<String, Object> projection){
		var query = Sql.render(Sql.ctx().
				select(Sql.selectFields(db.getColumnNames(table), projection)).
				from(Sql.table(table)).
				where(Sql.condition(match)).
				orderBy(Sql.sortFields(sort)));
		return db.select(query.sql(), query.params());
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateReadBuckets(String table, Map<String, Object> match, Map<String, Object> sort){
		var query = Sql.render(Sql.ctx().
				select().
				from(Sql.table(table)).
				where(Sql.condition(match)).
				orderBy(Sql.sortFields(sort)));
		return db.select(query.sql(), query.params());
	}

	//TODO: In Progress
	@Override
	public ClosableIterable<Map<String, Object>> aggregateAvailableYears(String table, Map<String, Object> match, Map<String, Object> sort, Map<String, Object> groupId){
		var query = Sql.render(Sql.ctx().
				select(Sql.aliasedFields(groupId)).
				from(Sql.table(table)).
				where(Sql.condition(match)).
				groupBy(Sql.groupFields(groupId)).
				orderBy(Sql.sortFields(sort)));
		return db.select(query.sql(), query.params());
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateExternalForecastTimes(String table, Map<String, Object> match, String groupId, Map<String, Object> sort, int limit){
		var query = Sql.render(Sql.ctx().
				select(Sql.field(groupId)).
				from(Sql.table(table)).
				where(Sql.condition(match)).
				groupBy(Sql.field(groupId)).
				orderBy(Sql.sortFields(sort)).
				limit(limit));
		return db.select(query.sql(), query.params());
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateSimulatedTaskRunInfos(String table, Map<String, Object> match, Map<String, Object> groupId, Map<String, Object> sort, int limit){
		var query = Sql.render(Sql.ctx().
				select(Sql.aliasedFields(groupId)).
				from(Sql.table(table)).
				where(Sql.condition(match)).
				groupBy(Sql.groupFields(groupId)).
				orderBy(Sql.sortFields(sort)).
				limit(limit));
		return db.select(query.sql(), query.params());
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateUnwoundTimeSeries(String table, Map<String, Object> match, String unwind){
		var timeseriesFields = Schema.getTimeseriesFields();
		var selectFields = timeseriesFields.stream().map(f -> Sql.field("u", "timeseries_" + f)).toList();
		var unnestArgs = timeseriesFields.stream().map(f -> Sql.field("t", "timeseries_" + f)).toList();
		var placeholders = LongStream.range(0, timeseriesFields.size()).mapToObj(i -> String.format("{%d}", i)).toList();
		var aliases = timeseriesFields.stream().map(f -> "\"" + Sql.columnName("timeseries_" + f) + "\"").collect(Collectors.joining(", "));
		var unnest = Sql.table(String.format("LATERAL UNNEST(%s) AS u(%s)", String.join(",", placeholders), aliases), unnestArgs);
		var query = Sql.render(Sql.ctx().
				select(selectFields).
				from(Sql.table(table).as("t")).
				crossJoin(unnest).
				where(Sql.condition(match, "t")));
		return db.select(query.sql(), query.params());
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateTimeSeriesGroups(String table, Map<String, Object> sort, Map<String, Object> groupId, Map<String, Object> first, Map<String, Object> hint){
		var rowNumber = Sql.rowNumber().over().partitionBy(Sql.groupFields(groupId, "t")).orderBy(Sql.sortFields(sort, "t")).as("rowNumber");
		var ranked = Sql.ctx().select(Sql.asterisk(), rowNumber).from(Sql.table(table).as("t")).asTable("f");
		var selectFields = Sql.selectFieldList();
		groupId.forEach((alias, source) -> selectFields.add(Sql.field("f", source.toString()).as(alias)));
		first.forEach((alias, source) -> selectFields.add(Sql.field("f", source.toString()).as(alias)));
		var query = Sql.render(Sql.ctx().
				select(selectFields).
				from(ranked).
				where(Sql.field("f", "rowNumber").eq(1)));
		return db.select(query.sql(), query.params());
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateTimeSeriesIndex(String table, Map<String, Object> sort, Map<String, Object> groupId, Map<String, Object> addFields){
		var selectFields = new ArrayList<>(Sql.aliasedFields(groupId));
		addFields.forEach((k, v) -> selectFields.add(Sql.val(v).as(k)));
		var query = Sql.render(Sql.ctx().
				select(selectFields).
				from(Sql.table(table)).
				groupBy(Sql.groupFields(groupId)).
				orderBy(Sql.sortFields(sort)));
		return db.select(query.sql(), query.params());
	}

	@Override
	public Map<String, Object> aggregateOneMultiFieldGroupCount(String table, Map<String, Object> match, Map<String, Object> sum){
		var query = Sql.render(Sql.ctx().
				select(Sql.count(sum.keySet().iterator().next())).
				from(Sql.table(table)).
				where(Sql.condition(match)));
		return db.selectOne(query.sql(), query.params());
	}

	@Override
	public Map<String, Object> aggregateOneMultiFieldGroupCount(String table, Map<String, Object> match, Map<String, Object> groupId, Map<String, Object> sum){
		var grouped = Sql.ctx().
				selectOne().
				from(Sql.table(table)).
				where(Sql.condition(match)).
				groupBy(Sql.groupFields(groupId)).asTable("g");
		var query = Sql.render(Sql.ctx().
				select(Sql.count(sum.keySet().iterator().next())).
				from(grouped));
		return db.selectOne(query.sql(), query.params());
	}
}