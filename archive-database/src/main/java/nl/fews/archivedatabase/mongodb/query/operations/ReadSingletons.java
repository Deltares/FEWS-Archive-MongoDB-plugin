package nl.fews.archivedatabase.mongodb.query.operations;

import com.mongodb.client.MongoCursor;
import nl.fews.archivedatabase.mongodb.query.interfaces.Read;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import org.bson.Document;

import java.util.*;

/**
 * Provides streaming capability for singleton timeseries
 */
public final class ReadSingletons implements Read {

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate forecast startDate, inclusive
	 * @param endDate forecast endDate, inclusive
	 * @return MongoCursor<Document>
	 */
	public MongoCursor<Document> read(String collection, Map<String, List<Object>> query, Date startDate, Date endDate) {
		Document document = new Document();

		if(startDate != null && endDate != null)
			document.append("forecastTime", new Document("$gte", startDate).append("$lte", endDate));
		else if(startDate != null)
			document.append("forecastTime", new Document("$gte", startDate));
		else if(endDate != null)
			document.append("forecastTime", new Document("$lte", endDate));

		query.forEach((k, v) -> {
			if(!v.isEmpty())
				document.append(k, v.size() == 1 ? v.get(0) : new Document("$in", v));
		});
		return Database.find(collection, document).iterator();
	}
}
