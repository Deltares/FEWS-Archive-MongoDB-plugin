package nl.fews.archivedatabase.common.export.operations;

import nl.fews.archivedatabase.common.export.interfaces.Synchronize;
import nl.fews.archivedatabase.common.shared.database.Index;
import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.tuple.Triplet;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesTypeUtil;

import java.util.*;

@SuppressWarnings({"unused", "unchecked"})
public final class SynchronizeSingletons extends SynchronizeBase implements Synchronize {

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param timeSeriesType timeSeriesType
	 * @return Triplet<List<Map<String, Object>>, List<Map<String, Object>>, List<Map<String, Object>>>
	 */
	@Override
	protected Map<String, Triplet<List<Map<String, Object>>, List<Map<String, Object>>, List<Map<String, Object>>>> getInsertUpdateRemove(Map<String, Object> timeSeries, TimeSeriesType timeSeriesType){
		var insertUpdateRemove = new LinkedHashMap<String, Triplet<List<Map<String, Object>>, List<Map<String, Object>>, List<Map<String, Object>>>>();
		var existingDocuments = getExistingDocuments(getExistingQueries(timeSeries, timeSeriesType), timeSeriesType);

		getDocumentsByKey(timeSeries, timeSeriesType).forEach((key, documents) -> {
			var insert = new ArrayList<Map<String, Object>>();
			var replace = new ArrayList<Map<String, Object>>();
			var remove = new ArrayList<Map<String, Object>>();

			var document = documents.get(documents.size() - 1);
			var existingDocument = existingDocuments.getOrDefault(key, null);
			if (existingDocument == null) {
				if(!((List<Map<?, ?>>)document.get("timeseries")).isEmpty())
					insert.add(document);
			}
			else {
				((Map<String, Object>)document.get("metaData")).put("archiveTime", ((Map<String, Object>)existingDocument.get("metaData")).get("archiveTime"));
				document.put("_id", existingDocument.get("_id"));
				if(((List<Map<?, ?>>)document.get("timeseries")).isEmpty())
					remove.add(document);
				else
					replace.add(document);
			}
			insertUpdateRemove.put(key, new Triplet<>(insert, replace, remove));
		});
		return insertUpdateRemove;
	}

	/**
	 *
	 * @param existingQueries existingQueries
	 * @param timeSeriesType timeSeriesType
	 * @return existingDocuments
	 */
	private Map<String, Map<String, Object>> getExistingDocuments(List<Map<String, Object>> existingQueries, TimeSeriesType timeSeriesType){
		var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		var keys = Index.getKeys(collection);
		var existingDocuments = new LinkedHashMap<String, Map<String, Object>>();
		if(!existingQueries.isEmpty()) {
			database.find(collection, Map.of("or", existingQueries), Map.of("timeseries", 0)).forEach(document -> {
				var key = Index.getKey(Index.getKeyDocument(keys, document));
				if(((Date)((Map<String, Object>)document.get("metaData")).get("archiveTime")).compareTo((Date)((Map<String, Object>)existingDocuments.getOrDefault(key, document).get("metaData")).get("archiveTime")) >= 0)
					existingDocuments.put(key, document);
			});
		}
		return existingDocuments;
	}

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param timeSeriesType timeSeriesType
	 * @return existingQueries
	 */
	private List<Map<String, Object>> getExistingQueries(Map<String, Object> timeSeries, TimeSeriesType timeSeriesType){
		var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		var keys = Index.getKeys(collection);
		var existingQueries = new ArrayList<Map<String, Object>>();
		getDocumentsByKey(timeSeries, timeSeriesType).forEach((key, value) -> {
			var document = value.get(value.size() - 1);
			existingQueries.add(Index.getKeyDocument(keys, document));
		});
		return existingQueries;
	}

	/**
	 * Gets the documents for each set of unique key fields.
	 * @param timeSeries the entire list of all documents passed to this instance
	 * @param timeSeriesType the document key fields matching the database unique collection key
	 * @return Map<String, List<Map<String, Object>>> document map keyed by their JSON string key representation of unique key fields.
	 */
	private Map<String, List<Map<String, Object>>> getDocumentsByKey(Map<String, Object> timeSeries, TimeSeriesType timeSeriesType){
		var keyBucketDocuments = new LinkedHashMap<String, List<Map<String, Object>>>();
		var key = Index.getKey(Index.getKeyDocument(Index.getKeys(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType)), timeSeries));
		keyBucketDocuments.putIfAbsent(key, new ArrayList<>());
		keyBucketDocuments.get(key).add(timeSeries);
		return keyBucketDocuments;
	}
}
