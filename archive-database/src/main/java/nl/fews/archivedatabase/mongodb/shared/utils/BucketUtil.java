package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.util.Date;

public final class BucketUtil {

	/**
	 *
	 */
	private static final JSONArray bucketDefinitions = new JSONArray(Settings.get("bucketDefinitions", String.class));

	/**
	 * Static class
	 */
	private BucketUtil(){}

	/**
	 *
	 * @param timeSeriesGroup document
	 * @return Document
	 */
	public static Document getBucket(Document timeSeriesGroup){
		String bucketSize = BucketSize.YEARLY.toString();
		boolean collapse = false;
		for(var i = 0; i < bucketDefinitions.length(); i++){
			JSONObject b = bucketDefinitions.getJSONObject(i);
			if (
				(!b.getJSONObject("definition").has("moduleInstanceId") || b.getJSONObject("definition").getJSONArray("moduleInstanceId").isEmpty() || b.getJSONObject("definition").getJSONArray("moduleInstanceId").toList().contains(timeSeriesGroup.getString("moduleInstanceId"))) &&
				(!b.getJSONObject("definition").has("locationId") || b.getJSONObject("definition").getJSONArray("locationId").isEmpty() || b.getJSONObject("definition").getJSONArray("locationId").toList().contains(timeSeriesGroup.getString("locationId"))) &&
				(!b.getJSONObject("definition").has("parameterId") || b.getJSONObject("definition").getJSONArray("parameterId").isEmpty() || b.getJSONObject("definition").getJSONArray("parameterId").toList().contains(timeSeriesGroup.getString("parameterId"))) &&
				(!b.getJSONObject("definition").has("qualifierId") || b.getJSONObject("definition").getJSONArray("qualifierId").isEmpty() || b.getJSONObject("definition").getJSONArray("qualifierId").toList().contains(timeSeriesGroup.getString("qualifierId"))) &&
				(!b.getJSONObject("definition").has("encodedTimeStepId") || b.getJSONObject("definition").getJSONArray("encodedTimeStepId").isEmpty() || b.getJSONObject("definition").getJSONArray("encodedTimeStepId").toList().contains(timeSeriesGroup.getString("encodedTimeStepId")))){
				bucketSize = b.getString("bucketSize");
				collapse = b.getBoolean("collapse");
			}
		}
		return new Document("bucketSize", bucketSize).append("collapse", collapse);
	}

	/**
	 *
	 * @param date date
	 * @param bucketSize bucketSize
	 * @return int
	 */
	public static int getBucketValue(Date date, BucketSize bucketSize){
		LocalDateTime localDateTime = DateUtil.getLocalDateTime(date);
		switch(bucketSize) {
			case DECADE:
				return localDateTime.getYear() / 10 * 10;
			case YEARLY:
				return localDateTime.getYear();
			case MONTHLY:
				return localDateTime.getYear() * 100 + localDateTime.getMonthValue();
			case DAILY:
				return localDateTime.getYear() * 10000 + localDateTime.getMonthValue() * 100 + localDateTime.getDayOfMonth();
			default:
				throw new java.lang.IllegalArgumentException(bucketSize.toString());
		}
	}
}
