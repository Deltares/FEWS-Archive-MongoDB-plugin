package nl.fews.archivedatabase.mongodb.export.operations;

import com.mongodb.MongoWriteException;
import nl.fews.archivedatabase.mongodb.export.interfaces.Synchronize;
import nl.fews.archivedatabase.mongodb.shared.database.Collection;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import org.bson.Document;
import org.javatuples.Triplet;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class SynchronizeBase implements Synchronize {

	/**
	 *
	 */
	private static final Map<Document, Object> timeSeriesIndex = new ConcurrentHashMap<>();

	static{
		Database.find(Collection.TimeSeriesIndex.toString(), new Document(), new Document("_id", 0)).forEach(document -> timeSeriesIndex.put(document, "null"));
	}

	/**
	 *
	 * @param document document
	 * @param collection collection
	 * @return Document
	 */
	private static Document getTimeSeriesIndexKey(Document document, String collection){
		String moduleInstanceId = document.containsKey("moduleInstanceId") ? document.getString("moduleInstanceId") : "";
		String parameterId = document.containsKey("parameterId") ? document.getString("parameterId") : "";
		String encodedTimeStepId = document.containsKey("encodedTimeStepId") ? document.getString("encodedTimeStepId") : "";
		String areaId = document.containsKey("metaData") && document.get("metaData", Document.class).containsKey("areaId") ? document.get("metaDate", Document.class).getString("areaId") : "";
		String sourceId = document.containsKey("metaData") && document.get("metaData", Document.class).containsKey("sourceId") ? document.get("metaData", Document.class).getString("sourceId") : "";
		return new Document("moduleInstanceId", moduleInstanceId).append("parameterId", parameterId).append("encodedTimeStepId", encodedTimeStepId).append("areaId", areaId).append("sourceId", sourceId).append("collection", collection);
	}

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
		Map<Document, Object> missingTimeSeriesIndexes = new ConcurrentHashMap<>();
		if(!insert.isEmpty())
		{
			insert.parallelStream().forEach(ts -> Database.insertOne(collection, ts));
			missingTimeSeriesIndexes.putAll(insert.stream().map(s -> SynchronizeBase.getTimeSeriesIndexKey(s, collection)).filter(key -> !timeSeriesIndex.containsKey(key)).distinct().collect(Collectors.toMap(s -> s, s -> s)));
		}

		if(!replace.isEmpty()) {
			Database.deleteMany(collection, new Document("_id", new Document("$in", replace.stream().map(s -> s.get("_id")).collect(Collectors.toList()))));
			replace.parallelStream().forEach(ts -> Database.insertOne(collection, ts));
			missingTimeSeriesIndexes.putAll(replace.stream().map(s -> SynchronizeBase.getTimeSeriesIndexKey(s, collection)).filter(key -> !timeSeriesIndex.containsKey(key)).distinct().collect(Collectors.toMap(s -> s, s -> s)));
		}

		if (!remove.isEmpty())
			Database.deleteMany(collection, new Document("_id", new Document("$in", remove.stream().map(s -> s.get("_id")).collect(Collectors.toList()))));

		SynchronizeBase.addMissingTimeSeriesIndexes(missingTimeSeriesIndexes);
	}

	/**
	 *
	 * @param missingTimeSeriesIndexes missingTimeSeriesIndexes
	 */
	private static synchronized void addMissingTimeSeriesIndexes(Map<Document, Object> missingTimeSeriesIndexes){
		missingTimeSeriesIndexes.forEach((k, v) -> {
			try{
				Database.insertOne(Collection.TimeSeriesIndex.toString(), k);
				timeSeriesIndex.put(k, "");
			}
			catch (MongoWriteException ex){
				//IGNORE
			}
		});
	}

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param timeSeriesType timeSeriesType
	 * @return Map<String, Triplet<List<Document>, List<Document>, List<Document>>>
	 */
	protected abstract Map<String, Triplet<List<Document>, List<Document>, List<Document>>> getInsertUpdateRemove(List<Document> timeSeries, TimeSeriesType timeSeriesType);
}
