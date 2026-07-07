package nl.fews.archivedatabase.common.shared.utils;

import nl.fews.archivedatabase.common.shared.database.Database;
import nl.fews.archivedatabase.common.shared.database.Index;
import nl.fews.archivedatabase.common.shared.enums.BucketSize;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public final class TimeSeriesUtil {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(TimeSeriesUtil.class);

	/**
	 * Static Class
	 */
	private TimeSeriesUtil(){}

	/**
	 *
	 * @param collection collection
	 * @param bucketKeyFields bucketKeyFields
	 * @return List<Map<String, Object>>
	 */
	public static List<Map<String, Object>> getTimeSeriesGroups(String collection, List<String> bucketKeyFields){
		var timeSeriesGroups = new ArrayList<Map<String, Object>>();

		var hint = bucketKeyFields.stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new));
		hint.put("metaData.timeStepMinutes", 1);

		var groups = Database.instance().aggregateTimeSeriesGroups(
			collection,
			bucketKeyFields.stream().collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)),
			bucketKeyFields.stream().collect(Collectors.toMap(s -> s, s -> s, (k, v) -> v, LinkedHashMap::new)),
			Map.of("timeStepMinutes", "metaData.timeStepMinutes"),
			Map.of("hint", hint)
		);

		groups.forEach(timeSeriesGroup -> {
			var groupResults = (Map<String, Object>)timeSeriesGroup.get("_id");
			var bucketSize = groupResults.get("encodedTimeStepId").equals("NETS") ?
				BucketUtil.getNetsBucketSize(collection, Index.getKey(Index.getKeyDocument(bucketKeyFields, groupResults))) :
				BucketUtil.getBucketSize((int)timeSeriesGroup.get("timeStepMinutes"));
			timeSeriesGroups.add(Map.of("timeSeriesGroup", groupResults,"bucketSize", bucketSize.toString()));
		});

		return timeSeriesGroups;
	}

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param timeSeriesBuckets timeSeriesBuckets
	 * @param bucketSize bucketSize
	 * @param collection collection
	 */
	public static List<Map<String, Object>> getTimeSeriesDocuments(Map<String, Object> bucketKeyDocument, Map<Long, List<Map<String, Object>>> timeSeriesBuckets, BucketSize bucketSize, String collection){
		var timeSeriesDocuments = new ArrayList<Map<String, Object>>();

		var baseDocument = Database.instance().findOne(collection, bucketKeyDocument, Map.of("timeseries", 0, "_id", 0, "localStartTime", 0, "localEndTime", 0));
		if(baseDocument != null){
			var baseDocumentJson = new JSONObject(baseDocument).toString();
			timeSeriesBuckets.forEach((bucketValue, timeSeries) -> {
				var document = new JSONObject(baseDocumentJson).toMap();
				document.put("bucketSize", bucketSize.toString());
				document.put("bucket", bucketValue);
				document.put("timeseries", timeSeries);
				document.put("startTime", timeSeries.get(0).get("t"));
				document.put("endTime", timeSeries.get(timeSeries.size() - 1).get("t"));
				if (timeSeries.get(0).containsKey("lt")) document.put("localStartTime", timeSeries.get(0).get("lt"));
				if (timeSeries.get(timeSeries.size() - 1).containsKey("lt")) document.put("localEndTime", timeSeries.get(timeSeries.size() - 1).get("lt"));
				timeSeriesDocuments.add(document);
			});
		}

		return timeSeriesDocuments;
	}

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param collection collection
	 * @return List<Map<String, Object>>
	 */
	public static List<Map<String, Object>> getStitchedTimeSeries(Map<String, Object> bucketKeyDocument, String collection){
		var timeSeries = new LinkedHashMap<Date, Map<String, Object>>();

		var results = Database.instance().aggregateStitchedTimeSeries(collection, bucketKeyDocument, Map.of("forecastTime", 1), Map.of("_id", 0, "timeseries", 1));
		results.forEach(result -> timeSeries.putAll(((List<Map<String, Object>>)result.get("timeseries")).stream().collect(Collectors.toMap(x -> (Date)x.get("t"), x-> x))));
		return timeSeries.values().stream().sorted(Comparator.comparing(s -> (Date)s.get("t"))).toList();
	}

	/**
	 *
	 * @param bucketKeyDocument bucketKeyDocument
	 * @param collection collection
	 * @return List<Map<String, Object>>
	 */
	public static List<Map<String, Object>> getUnwoundTimeSeries(Map<String, Object> bucketKeyDocument, String collection){
		var events = new ArrayList<Map<String, Object>>();

		Database.instance().aggregateUnwoundTimeSeries(collection, bucketKeyDocument, "timeseries").forEach(events::add);

		var eventsDeduplicate = TimeSeriesUtil.deduplicateEvents(events);

		if(events.size() != eventsDeduplicate.size()){
			Exception ex = new Exception("Duplicate event dates found and removed");
			logger.warn(new JSONObject(LogUtil.getLogMessageJson(ex, Map.of("collection", collection, "bucketKeyDocument", bucketKeyDocument))).toString(), ex);
		}

		return eventsDeduplicate;
	}

	/**
	 *
	 * @param timeSeries timeSeries
	 * @param bucketSize bucketSize
	 * @return Map<Integer, List<Map<String, Object>>>
	 */
	public static Map<Long, List<Map<String, Object>>> getTimeSeriesBuckets(List<Map<String, Object>> timeSeries, BucketSize bucketSize){
		var timeSeriesBuckets = new LinkedHashMap<Long, List<Map<String, Object>>>();

		timeSeries.forEach(ts -> {
			long bucketValue = BucketUtil.getBucketValue((Date)ts.get("t"), bucketSize);
			timeSeriesBuckets.putIfAbsent(bucketValue, new ArrayList<>());
			timeSeriesBuckets.get(bucketValue).add(ts);
		});

		return timeSeriesBuckets;
	}

	/**
	 *
	 * @param events events
	 * @return List<Map<String, Object>>
	 */
	private static List<Map<String, Object>> deduplicateEvents(List<Map<String, Object>> events){
		var eventsDuplicate  = new LinkedHashMap<Date, Map<String, Object>>();
		var eventsDeduplicate = new ArrayList<Map<String, Object>>();

		var sorted = true;
		var lastDate = new Date(Long.MIN_VALUE);

		for (Map<String, Object> e: events) {
			var eventDate = (Date)e.get("t");
			if(!eventsDuplicate.containsKey(eventDate)) {
				eventsDuplicate.put(eventDate, e);
				eventsDeduplicate.add(e);

				if(eventDate.compareTo(lastDate) < 0)
					sorted = false;
				lastDate = eventDate;
			}
		}
		if(!sorted)
			eventsDeduplicate.sort(Comparator.comparing(s -> (Date)s.get("t")));
		return eventsDeduplicate;
	}

	/**
	 * Trims null values from the beginning and end of a timeseries
	 * @param timeseries timeseries
	 * @return List<Map<String, Object>>
	 */
	public static List<Map<String, Object>> trimNullValues(List<Map<String, Object>> timeseries){
		var begin = 0;
		var end = timeseries.size();

		while(begin < end && timeseries.get(begin).get("v") == null)
			begin++;

		while(end > begin && timeseries.get(end-1).get("v") == null)
			end--;

		return timeseries.subList(begin, end);
	}
}