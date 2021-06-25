package nl.fews.archivedatabase.mongodb.export.operations;

import nl.fews.archivedatabase.mongodb.export.interfaces.Synchronize;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.utils.BucketUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;
import org.javatuples.Triplet;

import java.util.List;
import java.util.stream.Collectors;

public abstract class SynchronizeBase implements Synchronize {

	/**
	 * Inserts, updates or replaces data for bucketed (observed) timeseries
	 * @param timeSeries the entire list of all documents passed to this instance
	 * @param timeSeriesType timeSeriesType
	 */
	public void synchronize(List<Document> timeSeries, TimeSeriesType timeSeriesType){
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		List<String> keys = Database.getCollectionKeys(collection);

		Triplet<List<Document>, List<Document>, List<Document>> triplet = synchronize(timeSeries, collection, keys);
		List<Document> insert = triplet.getValue0();
		List<Document> replace = triplet.getValue1();
		List<Document> remove = triplet.getValue2();

		if (insert.stream().filter(s -> s.getString("encodedTimeStepId").equals("nonequidistant")).anyMatch(d -> d.getList("timeseries", Document.class).size() > BucketUtil.TIME_SERIES_MAX_ENTRY_COUNT) ||
				replace.stream().filter(s -> s.getString("encodedTimeStepId").equals("nonequidistant")).anyMatch(d -> d.getList("timeseries", Document.class).size() > BucketUtil.TIME_SERIES_MAX_ENTRY_COUNT)) {
			for (Document document: insert)
				BucketUtil.ensureBucketSize(timeSeriesType, document);
			for (Document document: replace)
				BucketUtil.ensureBucketSize(timeSeriesType, document);

			triplet = synchronize(timeSeries, collection, keys);
			insert = triplet.getValue0();
			replace = triplet.getValue1();
			remove = triplet.getValue2();
		}
		if (insert.stream().filter(s -> s.getString("encodedTimeStepId").equals("nonequidistant")).anyMatch(d -> d.getList("timeseries", Document.class).size() > BucketUtil.TIME_SERIES_MAX_ENTRY_COUNT) ||
				replace.stream().filter(s -> s.getString("encodedTimeStepId").equals("nonequidistant")).anyMatch(d -> d.getList("timeseries", Document.class).size() > BucketUtil.TIME_SERIES_MAX_ENTRY_COUNT))
			throw new IllegalStateException("Cannot resolve nonequidistant bucket size.");

		synchronize(collection, insert, replace, remove);
	}

	/**
	 * Bulk operations
	 * @param collection collection
	 * @param insert insert documents
	 * @param replace replace documents
	 * @param remove remove documents
	 */
	private static void synchronize(String collection, List<Document> insert, List<Document> replace, List<Document> remove){
		if(!insert.isEmpty())
			Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).insertMany(insert);

		if(!replace.isEmpty()) {
			Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).deleteMany(new Document("_id", new Document("$in", replace.stream().map(s -> s.get("_id")).collect(Collectors.toList()))));
			Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).insertMany(replace);
		}

		if (!remove.isEmpty())
			Database.create().getDatabase(Database.getDatabaseName()).getCollection(collection).deleteMany(new Document("_id", new Document("$in", remove.stream().map(s -> s.get("_id")).collect(Collectors.toList()))));
	}

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param collection collection
	 * @param keys keys
	 * @return Triplet<List<Document>, List<Document>, List<Document>>
	 */
	protected abstract Triplet<List<Document>, List<Document>, List<Document>> synchronize(List<Document> timeSeries, String collection, List<String> keys);
}
