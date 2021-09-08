package nl.fews.archivedatabase.mongodb.export.operations;

import nl.fews.archivedatabase.mongodb.export.interfaces.Synchronize;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;
import org.javatuples.Triplet;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class SynchronizeBase implements Synchronize {

	/**
	 * Inserts, updates or replaces data for bucketed (observed) timeseries
	 * @param timeSeries the entire list of all documents passed to this instance
	 * @param timeSeriesType timeSeriesType
	 */
	public void synchronize(List<Document> timeSeries, TimeSeriesType timeSeriesType){
		Map<String, Triplet<List<Document>, List<Document>, List<Document>>> insertUpdateRemove = getInsertUpdateRemove(timeSeries, timeSeriesType);
		synchronize(
				timeSeriesType,
				insertUpdateRemove.values().stream().flatMap(s -> s.getValue0().stream()).collect(Collectors.toList()),
				insertUpdateRemove.values().stream().flatMap(s -> s.getValue1().stream()).collect(Collectors.toList()),
				insertUpdateRemove.values().stream().flatMap(s -> s.getValue2().stream()).collect(Collectors.toList()));
	}

	/**
	 * Bulk operations
	 * @param timeSeriesType timeSeriesType
	 * @param insert insert documents
	 * @param replace replace documents
	 * @param remove remove documents
	 */
	private static void synchronize(TimeSeriesType timeSeriesType, List<Document> insert, List<Document> replace, List<Document> remove){
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		if(!insert.isEmpty())
			Database.insertMany(collection, insert);

		if(!replace.isEmpty()) {
			Database.deleteMany(collection, new Document("_id", new Document("$in", replace.stream().map(s -> s.get("_id")).collect(Collectors.toList()))));
			Database.insertMany(collection, replace);
		}

		if (!remove.isEmpty())
			Database.deleteMany(collection, new Document("_id", new Document("$in", remove.stream().map(s -> s.get("_id")).collect(Collectors.toList()))));
	}

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param timeSeriesType timeSeriesType
	 * @return Map<String, Triplet<List<Document>, List<Document>, List<Document>>>
	 */
	protected abstract Map<String, Triplet<List<Document>, List<Document>, List<Document>>> getInsertUpdateRemove(List<Document> timeSeries, TimeSeriesType timeSeriesType);
}
