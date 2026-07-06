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
		return db.findOne(collection, query, projection);
	}

	@Override
	public Map<String, Object> findOne(String collection, Map<String, Object> query){
		return db.findOne(collection, query);
	}

	@Override
	public ClosableIterable<Map<String, Object>> find(String collection, Map<String, Object> query, Map<String, Object> projection){
		return db.find(collection, query, projection);
	}

	@Override
	public ClosableIterable<Map<String, Object>> find(String collection, Map<String, Object> query){
		return db.find(collection, query);
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateAvailableYears(String collection, Map<String, Object> match, Map<String, Object> sort, Map<String, Object> projection){
		return db.aggregate(collection, List.of(Map.of("$match", match), Map.of("$sort", sort), Map.of("$project", projection)));
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateReadBuckets(String collection, Map<String, Object> match, Map<String, Object> sort){
		match = match.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, s -> s.getValue() instanceof Map ? ((Map<?, ?>)s.getValue()).entrySet().stream().collect(Collectors.toMap(x -> String.format("$%s", x.getKey()), Map.Entry::getValue)) : s.getValue()));
		return db.aggregate(collection, List.of(Map.of("$match", match), Map.of("$sort", sort)));
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregate(String collection, Map<String, Object> match, String unwind, String replaceRoot){
		return db.aggregate(collection, List.of(Map.of("$match", match), Map.of("$unwind", String.format("$%s", unwind)), Map.of("$replaceRoot", Map.of("newRoot", String.format("$%s", replaceRoot)))));
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregateTimeSeriesGroups(String collection, Map<String, Object> sort, Map<String, Object> groupId, Map<String, Object> first, Map<String, Object> hint){
		var g = new LinkedHashMap<String, Object>(Map.of("_id", groupId));
		g.putAll(first.keySet().stream().collect(Collectors.toMap(s -> s, s -> Map.of("$first", String.format("$%s", first.get(s))))));
		return db.aggregate(collection, List.of(Map.of("$sort", sort), Map.of("$group", g)), hint);
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregate(String collection, List<Map<String, Object>> pipeline){
		return db.aggregate(collection, pipeline);
	}

	@Override
	public ClosableIterable<Map<String, Object>> aggregate(String collection, Map<String, Object> sort, Map<String, Object> groupId, String replaceRoot, Map<String, Object> addFields){
		return db.aggregate(collection, List.of(
			Map.of("$sort", sort),
			Map.of("$group", Map.of("_id", groupId.keySet().stream().collect(Collectors.toMap(s -> s, s -> String.format("$%s", groupId.get(s)))))),
			Map.of("$replaceRoot", Map.of("newRoot", String.format("$%s", replaceRoot))),
			Map.of("$addFields", addFields)));
	}

	@Override
	public Map<String, Object> aggregateOneMultiFieldGroupCount(String collection, Map<String, Object> match, Map<String, Object> sum){
		var group = new LinkedHashMap<String, Object>();
		group.put("_id", null);
		sum.forEach((k, v) -> group.put(k, Map.of("$sum", v)));
		return db.aggregateOne(collection, List.of(Map.of("$match", match), Map.of("$group", group)));
	}

	@Override
	public Map<String, Object> aggregateOneMultiFieldGroupCount(String collection, Map<String, Object> match, Map<String, Object> groupId, Map<String, Object> sum){
		var g1 = Map.of("_id", groupId.keySet().stream().collect(Collectors.toMap(s -> s, s -> String.format("$%s", groupId.get(s)), (a, b) -> b, LinkedHashMap::new)));

		var g2 = new LinkedHashMap<String, Object>();
		g2.put("_id", null);
		sum.forEach((k, v) -> g2.put(k, Map.of("$sum", v)));

		return db.aggregateOne(collection, List.of(Map.of("$match", match), Map.of("$group", g1), Map.of("$group", g2)));
	}
}