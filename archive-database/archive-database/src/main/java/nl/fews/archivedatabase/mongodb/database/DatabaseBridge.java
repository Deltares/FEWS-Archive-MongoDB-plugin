package nl.fews.archivedatabase.mongodb.database;

import nl.fews.archivedatabase.common.shared.interfaces.ClosableIterable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class DatabaseBridge implements nl.fews.archivedatabase.common.shared.interfaces.DatabaseBridge {

	private final Database db = Database.instance();

	@Override
	public void ensureCollection(String collection){
		db.ensureCollection(collection);
	}

	@Override
	public void close(){
		db.close();
	}

	@Override
	public void drop(String collection) {
		db.drop(collection);
	}

	@Override
	public void rename(String collection, String newName){
		db.rename(collection, newName);
	}

	@Override
	public void replace(String srcCollection, String dstCollection){
		db.replace(srcCollection, dstCollection);
	}

	@Override
	public void deleteOne(String collection, Map<String, Object> query){
		db.deleteOne(collection, query);
	}

	@Override
	public void deleteMany(String collection, Map<String, Object> query) {
		db.deleteMany(collection, query);
	}

	@Override
	public void updateOne(String collection, Map<String, Object> query, Map<String, Object> document, boolean upsert){
		db.updateOne(collection, query, Map.of("$set", document), upsert);
	}

	@Override
	public void updateOne(String collection, Map<String, Object> query, Map<String, Object> document){
		db.updateOne(collection, query, document);
	}

	@Override
	public void updateMany(String collection, Map<String, Object> query, Map<String, Object> document){
		db.updateMany(collection, query, document);
	}

	@Override
	public Object insertOne(String collection, Map<String, Object> document) {
		return db.insertOne(collection, document);
	}

	@Override
	public void insertMany(String collection, List<Map<String, Object>> documents) {
		db.insertMany(collection, documents);
	}

	@Override
	public <T> Set<T> distinct(String collection, String field, Map<String, Object> query, Class<T> clazz){
		return db.distinct(collection, field, query, clazz);
	}

	@Override
	public Map<String, Object> findOne(String collection, Map<String, Object> query, Map<String, Object> projection){
		return db.findOne(collection, MongoDb.match(query), projection);
	}

	@Override
	public Map<String, Object> findOne(String collection, Map<String, Object> query){
		return db.findOne(collection, MongoDb.match(query));
	}

	@Override
	public ClosableIterable<Map<String, Object>> find(String collection, Map<String, Object> query, Map<String, Object> projection){
		return db.find(collection, MongoDb.match(query), projection);
	}

	@Override
	public ClosableIterable<Map<String, Object>> find(String collection, Map<String, Object> query){
		return db.find(collection, MongoDb.match(query));
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateStitchedTimeSeries(String collection, Map<String, Object> match, Map<String, Object> sort, Map<String, Object> projection) {
		return db.aggregate(collection, List.of(
				Map.of("$match", MongoDb.match(match)),
				Map.of("$sort", sort),
				Map.of("$project", projection)));
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateAvailableYears(String collection, Map<String, Object> match, Map<String, Object> sort, Map<String, Object> groupId){
		return db.aggregate(collection, List.of(
				Map.of("$match", MongoDb.match(match)),
				Map.of("$sort", sort),
				Map.of("$group", MongoDb.group(groupId)),
				Map.of("$project", MongoDb.project(groupId))));
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateExternalForecastTimes(String collection, Map<String, Object> match, String groupId, Map<String, Object> sort, int limit) {
		return db.aggregate(collection, List.of(
				Map.of("$match", MongoDb.match(match)),
				Map.of("$group", MongoDb.group(groupId)),
				Map.of("$project", MongoDb.project(groupId)),
				Map.of("$sort", sort),
				Map.of("$limit", limit)
		));
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateSimulatedTaskRunInfos(String collection, Map<String, Object> match, Map<String, Object> groupId, Map<String, Object> sort, int limit) {
		return db.aggregate(collection, List.of(
				Map.of("$match", MongoDb.match(match)),
				Map.of("$group", MongoDb.group(groupId)),
				Map.of("$project", MongoDb.project(groupId)),
				Map.of("$sort", sort),
				Map.of("$limit", limit)
		));
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateUnwoundTimeSeries(String collection, Map<String, Object> match, String unwind) {
		return db.aggregate(collection, List.of(
				Map.of("$match", MongoDb.match(match)),
				Map.of("$unwind", MongoDb.field(unwind)),
				Map.of("$replaceRoot", MongoDb.replaceRoot(unwind))));
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateReadBuckets(String collection, Map<String, Object> match, Map<String, Object> sort){
		return db.aggregate(collection, List.of(
				Map.of("$match", MongoDb.match(match)),
				Map.of("$sort", sort)));
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateTimeSeriesGroups(String collection, Map<String, Object> sort, Map<String, Object> groupId, Map<String, Object> first, Map<String, Object> hint){
		var group = MongoDb.group(groupId);
		first.forEach((k, v) -> group.put(k, Map.of("$first", MongoDb.field(v))));
		return db.aggregate(collection, List.of(
				Map.of("$sort", sort),
				Map.of("$group", group),
				Map.of("$project", MongoDb.project(groupId, first))), hint);
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateTimeSeriesIndex(String collection, Map<String, Object> sort, Map<String, Object> groupId, Map<String, Object> addFields) {
		return db.aggregate(collection, List.of(
				Map.of("$sort", sort),
				Map.of("$group", MongoDb.group(groupId)),
				Map.of("$replaceRoot", Map.of("newRoot", groupId.keySet().stream().collect(Collectors.toMap(s -> s, s -> String.format("$_id.%s", s))))),
				Map.of("$addFields", addFields)));
	}

	@Override
	public Map<String, Object> aggregateOneMultiFieldGroupCount(String collection, Map<String, Object> match, Map<String, Object> sum){
		var group = new LinkedHashMap<String, Object>();
		group.put("_id", null);
		sum.forEach((k, v) -> group.put(k, Map.of("$sum", v)));
		return db.aggregateOne(collection, List.of(
				Map.of("$match", MongoDb.match(match)),
				Map.of("$group", group)));
	}

	@Override
	public Map<String, Object> aggregateOneMultiFieldGroupCount(String collection, Map<String, Object> match, Map<String, Object> groupId, Map<String, Object> sum){
		var group = new LinkedHashMap<String, Object>();
		group.put("_id", null);
		sum.forEach((k, v) -> group.put(k, Map.of("$sum", v)));
		return db.aggregateOne(collection, List.of(
				Map.of("$match", MongoDb.match(match)),
				Map.of("$group", MongoDb.group(groupId)),
				Map.of("$group", group)));
	}
}