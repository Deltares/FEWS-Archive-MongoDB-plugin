package nl.fews.archivedatabase.mongodb.query.operations;

import nl.fews.archivedatabase.mongodb.shared.database.Database;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class SummarizeSingletons extends SummarizeBase {
	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate event startDate, inclusive
	 * @param endDate event endDate, inclusive
	 * @return int
	 */
	@Override
	public int getMultiFieldGroupCount(String collection, Map<String, List<Object>> query, Date startDate, Date endDate, List<String> fields){
		Document match = new Document();
		match.append("forecastTime", new Document("$gte", startDate).append("$lte", endDate));
		query.forEach((k, v) -> {
			if(!v.isEmpty())
				match.append(k, v.size() == 1 ? v.get(0) : new Document("$in", v));
		});

		Document group = new Document();
		group.append("_id", new Document());
		fields.forEach(field -> group.get("_id", Document.class).append(field, String.format("$%s", field)));

		Document count = new Document();
		count.append("_id", null).append("count", new Document("$sum", 1));

		Document result = Database.aggregate(collection, List.of(
				new Document("$match", match),
				new Document("$group", group),
				new Document("$group", count))).first();
		return result != null ? result.getInteger("count") : 0;
	}

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate event startDate, inclusive
	 * @param endDate event endDate, inclusive
	 * @return int
	 */
	@Override
	public int getSingleFieldDistinctCount(String collection, Map<String, List<Object>> query, Date startDate, Date endDate, String field){
		Document document = new Document();
		document.append("forecastTime", new Document("$gte", startDate).append("$lte", endDate));
		query.forEach((k, v) -> {
			if(!v.isEmpty())
				document.append(k, v.size() == 1 ? v.get(0) : new Document("$in", v));
		});
		List<String> count = new ArrayList<>();
		for (String s : Database.distinct(collection, field, document, String.class))
			count.add(s);
		return count.size();
	}
}
