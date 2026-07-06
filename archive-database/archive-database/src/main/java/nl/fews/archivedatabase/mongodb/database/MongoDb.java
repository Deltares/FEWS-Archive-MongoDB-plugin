package nl.fews.archivedatabase.mongodb.database;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public final class MongoDb {
	private MongoDb() {}

	/**
	 *
	 * @param match match
	 * @return Map<String, Object>
	 */
	public static Map<String, Object> match(Map<String, Object> match) {
		var result = new LinkedHashMap<String, Object>();
			match.forEach((k, v) -> {
			if ("and".equals(k) || "or".equals(k))
				result.put("$" + k, ((List<Map<String, Object>>)v).stream().map(MongoDb::match).toList());
			else if (v instanceof Map<?, ?> ops)
				result.put(k, operator((Map<String, Object>) ops));
			else
				result.put(k, v);
		});
		return result;
	}

	/**
	 *
	 * @param ops ops
	 * @return Map<String, Object>
	 */
	private static Map<String, Object> operator(Map<String, Object> ops) {
		var result = new LinkedHashMap<String, Object>();
		ops.forEach((k, v) -> {
			var op = switch (k) {
				case "gte" -> "$gte";
				case "lte" -> "$lte";
				case "gt" -> "$gt";
				case "lt" -> "$lt";
				case "eq" -> "$eq";
				case "in" -> "$in";
				default -> throw new IllegalArgumentException(k);
			};
			result.put(op, v);
		});
		return result;
	}

	/**
	 *
	 * @param value value
	 * @return Object
	 */
	public static Object field(Object value) {
		return value instanceof String s ? "$" + s : value;
	}

	/**
	 *
	 * @param groupId groupId
	 * @param aggregate aggregate
	 * @return Map<String, Object>
	 */
	public static Map<String, Object> project(Map<String, Object> groupId, Map<String, Object> aggregate) {
		var project = new LinkedHashMap<String, Object>(Map.of("_id", 0));
		groupId.keySet().forEach(k -> project.put(k, "$_id." + k));
		aggregate.keySet().forEach(k -> project.put(k, "$" + k));
		return project;
	}

	/**
	 *
	 * @param groupId groupId
	 * @return Map<String, Object>
	 */
	public static Map<String, Object> project(Map<String, Object> groupId) {
		var project = new LinkedHashMap<String, Object>(Map.of("_id", 0));
		groupId.keySet().forEach(k -> project.put(k, "$_id." + k));
		return project;
	}

	/**
	 *
	 * @param source source
	 * @param fields fields
	 * @return Map<String, Object>
	 */
	public static Map<String, Object> project(String source, List<String> fields) {
		var project = new LinkedHashMap<String, Object>(Map.of("_id", 0));
		fields.forEach(f -> project.put(f, "$" + source + "." + f));
		return project;
	}

	/**
	 *
	 * @param groupId groupId
	 * @return Map<String, Object>
	 */
	public static Map<String, Object> project(String groupId){
		return new LinkedHashMap<>(Map.of("_id", 0, groupId, "$_id"));
	}

	/**
	 *
	 * @param groupId groupId
	 * @return Map<String, Object>
	 */
	public static Map<String, Object> group(Map<String, Object> groupId){
		return new LinkedHashMap<>(Map.of("_id", groupId.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> MongoDb.field(e.getValue()), (a, b) -> b, LinkedHashMap::new))));
	}

	/**
	 *
	 * @param groupId groupId
	 * @return Map<String, Object>
	 */
	public static Map<String, Object> group(String groupId){
		return new LinkedHashMap<>(Map.of("_id", MongoDb.field(groupId)));
	}

	/**
	 *
	 * @param unwind unwind
	 * @return Map<String, Object>
	 */
	public static Map<String, Object> replaceRoot(String unwind){
		return Map.of("newRoot", MongoDb.field(unwind));
	}
}
