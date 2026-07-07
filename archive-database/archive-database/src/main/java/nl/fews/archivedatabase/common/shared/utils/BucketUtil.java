package nl.fews.archivedatabase.common.shared.utils;

import nl.fews.archivedatabase.common.shared.database.Collection;
import nl.fews.archivedatabase.common.shared.database.Database;
import nl.fews.archivedatabase.common.shared.database.Index;
import nl.fews.archivedatabase.common.shared.enums.BucketSize;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SuppressWarnings("ALL")
public abstract class BucketUtil {

	/**
	 * HOURLY DATA / YEAR BUCKET
	 */
	private static final long BSON_TARGET_ELEMENT_COUNT = 365L * 24L;

	/**
	 * 16MB
	 */
	private static final long MAX_BSON_DOCUMENT_BYTES = 16L * 1024L * 1024L;

	/**
	 * 1MB
	 */
	private static final long BSON_DOCUMENT_BUFFER_BYTES = 1024L * 1024L;

	/**
	 * 15MB
	 */
	private static final long BSON_DOCUMENT_BYTES = MAX_BSON_DOCUMENT_BYTES - BSON_DOCUMENT_BUFFER_BYTES;

	/**
	 * date + date + float + float + int + string(0)
	 */
	private static final long BYTES_PER_TIME_SERIES_ENTRY = 64L + 64L + 32L + 32L + 32L;

	/**
	 * 70,217
	 */
	public static final long TIME_SERIES_MAX_ENTRY_COUNT = BSON_DOCUMENT_BYTES / BYTES_PER_TIME_SERIES_ENTRY;

	/**
	 * DEFAULT_BUCKET_SIZE
	 */
	private static final BucketSize DEFAULT_BUCKET_SIZE = BucketSize.MONTH;

	/**
	 * Static class
	 */
	private BucketUtil(){}

	/**
	 *
	 * @param bucketKey, bucketKey
	 * @return BucketSize
	 */
	public static synchronized BucketSize getNetsBucketSize(String bucketCollection, String bucketKey){
		var result = Database.instance().findOne(Collection.BucketSize.toString(), Map.of("bucketCollection", bucketCollection, "bucketKey", bucketKey), Map.of("_id", 0,"bucketSize", 1));
		return result == null ? BucketSize.YEAR : BucketSize.valueOf((String)result.get("bucketSize"));
	}

	/**
	 *
	 * @param bucketCollection bucketCollection
	 * @return List<String></String>
	 */
	public static List<String> getBucketKeyFields(String bucketCollection){
		return Index.getKeys(bucketCollection).stream().filter(s -> !s.equals("bucketSize") && !s.equals("bucket")).toList();
	}

	/**
	 *
	 * @param timeStepMinutes document
	 * @return BucketSize
	 */
	public static BucketSize getBucketSize(int timeStepMinutes){

		if(timeStepMinutes <= 0)
			return DEFAULT_BUCKET_SIZE;
		else if(1.0 / timeStepMinutes * 60L * 24L * 365L * 1000000000L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.AEON;
		else if(1.0 / timeStepMinutes * 60L * 24L * 365L * 1000000L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.MEGANNUM;
		else if(1.0 / timeStepMinutes * 60L * 24L * 365L * 1000L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.MILLENNIA;
		else if(1.0 / timeStepMinutes * 60L * 24L * 365L * 100L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.CENTURY;
		else if(1.0 / timeStepMinutes * 60L * 24L * 365L * 10L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.DECADE;
		else if(1.0 / timeStepMinutes * 60L * 24L * 365L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.YEAR;
		else if(1.0 / timeStepMinutes * 60L * 24L * 32L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.MONTH;
		else if(1.0 / timeStepMinutes * 60L * 24L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.DAY;
		else if(1.0 / timeStepMinutes * 60L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.HOUR;
		else if(1.0 / timeStepMinutes <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.MINUTE;
		else if(1.0 / timeStepMinutes / 60L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.SECOND;
		else
			return DEFAULT_BUCKET_SIZE;
	}

	/**
	 *
	 * @param startTime startTime
	 * @param endTime endTime
	 * @param count count
	 * @return BucketSize
	 */
	public static BucketSize getEstimatedBucketSize(Date startTime, Date endTime, long count){
		var s = DateUtil.getLocalDateTime(startTime);
		var e = DateUtil.getLocalDateTime(endTime);

		if(!e.isAfter(s) || count <= 0)
			throw new IllegalArgumentException(String.format("startTime: [%s] endTime: [%s] count: [%s]. startTime must be greater than end time and count must be greater than 0.", startTime, endTime, count));
		else if((double)count / Duration.between(s, e).getSeconds() * 60L * 60L * 24L * 365L * 1000000000L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.AEON;
		else if((double)count / Duration.between(s, e).getSeconds() * 60L * 60L * 24L * 365L * 1000000L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.MEGANNUM;
		else if((double)count / Duration.between(s, e).getSeconds() * 60L * 60L * 24L * 365L * 1000L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.MILLENNIA;
		else if((double)count / Duration.between(s, e).getSeconds() * 60L * 60L * 24L * 365L * 100L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.CENTURY;
		else if((double)count / Duration.between(s, e).getSeconds() * 60L * 60L * 24L * 365L * 10L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.DECADE;
		else if((double)count / Duration.between(s, e).getSeconds() * 60L * 60L * 24L * 365L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.YEAR;
		else if((double)count / Duration.between(s, e).getSeconds() * 60L * 60L * 24L * 32L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.MONTH;
		else if((double)count / Duration.between(s, e).getSeconds() * 60L * 60L * 24L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.DAY;
		else if((double)count / Duration.between(s, e).getSeconds() * 60L * 60L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.HOUR;
		else if((double)count / Duration.between(s, e).getSeconds() * 60L <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.MINUTE;
		else if((double)count / Duration.between(s, e).getSeconds() <= BSON_TARGET_ELEMENT_COUNT)
			return BucketSize.SECOND;
		else
			throw new IllegalArgumentException(String.format("startTime: [%s] endTime: [%s] count: [%s]. Cannot fit sample data into a bucket.", startTime, endTime, count));
	}

	/**
	 *
	 * @param date date
	 * @param bucketSize bucketSize
	 * @return int
	 */
	public static long getBucketValue(Date date, BucketSize bucketSize){
		var localDateTime = DateUtil.getLocalDateTime(date);
		return switch (bucketSize) {
			case AEON -> localDateTime.getYear() / 1000000000L * 1000000000L;
			case MEGANNUM -> localDateTime.getYear() / 1000000L * 1000000L;
			case MILLENNIA -> localDateTime.getYear() / 1000L * 1000L;
			case CENTURY -> localDateTime.getYear() / 100L * 100L;
			case DECADE -> localDateTime.getYear() / 10L * 10L;
			case YEAR -> localDateTime.getYear();
			case MONTH -> localDateTime.getYear() * 100L + localDateTime.getMonthValue();
			case DAY -> localDateTime.getYear() * 10000L + localDateTime.getMonthValue() * 100L + localDateTime.getDayOfMonth();
			case HOUR -> localDateTime.getYear() * 1000000L + localDateTime.getMonthValue() * 10000L + localDateTime.getDayOfMonth() * 100L + localDateTime.getHour();
			case MINUTE -> localDateTime.getYear() * 100000000L + localDateTime.getMonthValue() * 1000000L + localDateTime.getDayOfMonth() * 10000L + localDateTime.getHour() * 100L + localDateTime.getMinute();
			case SECOND -> localDateTime.getYear() * 10000000000L + localDateTime.getMonthValue() * 100000000L + localDateTime.getDayOfMonth() * 1000000L + localDateTime.getHour() * 10000L + localDateTime.getMinute() * 100L + localDateTime.getSecond();
			default -> throw new IllegalArgumentException(bucketSize.toString());
		};
	}

	/**
	 *
	 * @param bucketSize bucketSize
	 * @param bucketKeyDocument bucketKey
	 * @param bucketKey bucketKey
	 */
	private static synchronized void resizeExistingBuckets(String bucketCollection, Map<String, Object> bucketKeyDocument, String bucketKey, BucketSize bucketSize){
		var timeSeries = TimeSeriesUtil.getUnwoundTimeSeries(bucketKeyDocument, bucketCollection);
		var timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
		var timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(bucketKeyDocument, timeSeriesBuckets, bucketSize, bucketCollection);

		Database.instance().deleteMany(bucketCollection, bucketKeyDocument);
		if(!timeSeriesDocuments.isEmpty())
			Database.instance().insertMany(bucketCollection, timeSeriesDocuments);
		Database.instance().updateOne(Collection.BucketSize.toString(), Map.of("bucketCollection", bucketCollection, "bucketKey", bucketKey), Map.of("bucketSize", bucketSize.toString()), true);
	}

	/**
	 *
	 * @param documents documents
	 * @return Map<String, Object>
	 */
	private static Map<String, Object> getMaxSizeDocument(List<Map<String, Object>> documents){
		Map<String, Object> maxSizeDocument = null;
		for (var document: documents) {
			if(document.get("encodedTimeStepId").equals("NETS"))
				maxSizeDocument = maxSizeDocument == null || ((List<Map<String, Object>>)document.get("timeseries")).size() > ((List<Map<String, Object>>)maxSizeDocument.get("timeseries")).size() ? document : maxSizeDocument;
		}
		return maxSizeDocument;
	}

	/**
	 *
	 * @param bucketCollection bucketCollection
	 * @param documents documents
	 */
	public static BucketSize ensureBucketSize(String bucketCollection, List<Map<String, Object>> documents){
		BucketSize bucketSize = null;
		var document = getMaxSizeDocument(documents);
		if(document != null && ((List<Map<String, Object>>)document.get("timeseries")).size() > BucketUtil.TIME_SERIES_MAX_ENTRY_COUNT) {
			bucketSize = BucketUtil.getEstimatedBucketSize((Date)document.get("startTime"), (Date)document.get("endTime"), ((List<Map<String, Object>>)document.get("timeseries")).size());
			var bucketKeyDocument = Index.getKeyDocument(BucketUtil.getBucketKeyFields(bucketCollection), document);
			var bucketKey = Index.getKey(bucketKeyDocument);
			resizeExistingBuckets(bucketCollection, bucketKeyDocument, bucketKey, bucketSize);
		}
		return bucketSize;
	}
}
