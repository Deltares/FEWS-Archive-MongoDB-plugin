package nl.fews.archivedatabase.mongodb.query.operations;

import nl.fews.archivedatabase.mongodb.query.interfaces.HasData;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import org.bson.Document;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Provides streaming capability for singleton timeseries
 */
public final class HasDataSingletons implements HasData {

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate forecast startDate, inclusive
	 * @param endDate forecast endDate, inclusive
	 * @return MongoCursor<Document>
	 */
	public boolean hasData(String collection, Map<String, List<Object>> query, Date startDate, Date endDate) {
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
		return Database.findOne(collection, document, new Document("_id", 1)) != null;
	}
}
