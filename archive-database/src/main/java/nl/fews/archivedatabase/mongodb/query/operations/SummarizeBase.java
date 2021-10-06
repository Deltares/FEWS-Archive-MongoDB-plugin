package nl.fews.archivedatabase.mongodb.query.operations;

import nl.fews.archivedatabase.mongodb.query.interfaces.Summarize;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public abstract class SummarizeBase implements Summarize {
	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param match the filter over which distinct counts are to be made
	 * @return int
	 */
	protected int getMultiFieldGroupCount(String collection, Document match, List<String> fields){
		Document group = new Document();
		group.append("_id", new Document());
		fields.forEach(field -> group.get("_id", Document.class).append(field, String.format("$%s", field)));

		Document count = new Document();
		count.append("_id", null).append("count", new Document("$sum", 1));

		Document result;
		if(fields.isEmpty()){
			result = Database.aggregate(collection, List.of(
					new Document("$match", match),
					new Document("$group", count))).first();
		}
		else{
			result = Database.aggregate(collection, List.of(
					new Document("$match", match),
					new Document("$group", group),
					new Document("$group", count))).first();
		}
		return result != null ? result.getInteger("count") : 0;
	}

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param match the filter over which distinct counts are to be made
	 * @return int
	 */
	protected int getSingleFieldDistinctCount(String collection, Document match, String field){
		List<String> count = new ArrayList<>();
		for (String s : Database.distinct(collection, field, match, String.class))
			count.add(s);
		return count.size();
	}
}
