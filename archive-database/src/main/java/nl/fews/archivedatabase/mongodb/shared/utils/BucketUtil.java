package nl.fews.archivedatabase.mongodb.shared.utils;

import com.mongodb.client.model.UpdateOptions;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class BucketUtil {

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
	 * @param timeSeriesGroup document
	 * @return BucketSize
	 */
	public static BucketSize getBucketSize(Document timeSeriesGroup){
		Document bucketSize = Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("bucketSizeCollection")).find(timeSeriesGroup).projection(new Document("bucketSize", 1)).first();
		if (bucketSize != null)
			return BucketSize.valueOf(bucketSize.getString("bucketSize"));
		return DEFAULT_BUCKET_SIZE;
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
		LocalDateTime s = DateUtil.getLocalDateTime(startTime);
		LocalDateTime e = DateUtil.getLocalDateTime(endTime);

		if(e.compareTo(s) <= 0 || count <= 0)
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
		LocalDateTime localDateTime = DateUtil.getLocalDateTime(date);
		switch(bucketSize) {
			case AEON:
				return localDateTime.getYear() / 1000000000L * 1000000000L;
			case MEGANNUM:
				return localDateTime.getYear() / 1000000L * 1000000L;
			case MILLENNIA:
				return localDateTime.getYear() / 1000L * 1000L;
			case CENTURY:
				return localDateTime.getYear() / 100L * 100L;
			case DECADE:
				return localDateTime.getYear() / 10L * 10L;
			case YEAR:
				return localDateTime.getYear();
			case MONTH:
				return localDateTime.getYear() * 100L + localDateTime.getMonthValue();
			case DAY:
				return localDateTime.getYear() * 10000L + localDateTime.getMonthValue() * 100L + localDateTime.getDayOfMonth();
			case HOUR:
				return localDateTime.getYear() * 1000000L + localDateTime.getMonthValue() * 10000L + localDateTime.getDayOfMonth() * 100L + localDateTime.getHour();
			case MINUTE:
				return localDateTime.getYear() * 100000000L + localDateTime.getMonthValue() * 1000000L + localDateTime.getDayOfMonth() * 10000L + localDateTime.getHour() * 100L + localDateTime.getMinute();
			case SECOND:
				return localDateTime.getYear() * 10000000000L + localDateTime.getMonthValue() * 100000000L + localDateTime.getDayOfMonth() * 1000000L + localDateTime.getHour() * 10000L + localDateTime.getMinute() * 100L + localDateTime.getSecond();
			default:
				throw new java.lang.IllegalArgumentException(bucketSize.toString());
		}
	}

	/**
	 *
	 * @param bucketSize bucketSize
	 * @param timeSeriesGroup timeSeriesGroup
	 */
	public static void resizeBuckets(TimeSeriesType timeSeriesType, Document timeSeriesGroup, BucketSize bucketSize){
		if(timeSeriesType != TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL && timeSeriesType != TimeSeriesType.SCALAR_SIMULATED_HISTORICAL)
			throw new IllegalArgumentException(String.format("[%s] Only [%s] and [%s] support bucketing", timeSeriesType, TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, TimeSeriesType.SCALAR_SIMULATED_HISTORICAL));

		List<Document> timeSeries = TimeSeriesUtil.getTimeSeries(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType));
		Map<Long, List<Document>> timeSeriesBuckets = TimeSeriesUtil.getTimeSeriesBuckets(timeSeries, bucketSize);
		List<Document> timeSeriesDocuments = TimeSeriesUtil.getTimeSeriesDocuments(timeSeriesGroup, timeSeriesBuckets, bucketSize, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType));

		TimeSeriesUtil.removeTimeSeriesDocuments(timeSeriesGroup, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType));
		TimeSeriesUtil.saveTimeSeriesDocuments(timeSeriesDocuments, TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType));

		Database.create().getDatabase(Database.getDatabaseName()).getCollection(Settings.get("bucketSizeCollection")).updateOne(timeSeriesGroup, new Document("bucketSize", bucketSize.toString()), new UpdateOptions().upsert(true));
	}

	/**
	 *
	 * @param timeSeriesType timeSeriesType
	 * @param document document
	 */
	public static void ensureBucketSize(TimeSeriesType timeSeriesType, Document document){
		if(document.getString("encodedTimeStepId").equals("nonequidistant") && document.getList("timeseries", Document.class).size() > BucketUtil.TIME_SERIES_MAX_ENTRY_COUNT){
			BucketSize bucketSize = BucketUtil.getEstimatedBucketSize(document.getDate("startTime"), document.getDate("endTime"), document.getList("timeseries", Document.class).size());
			Document timeSeriesGroup = Database.getCollectionIndexes(Settings.get("bucketSizeCollection"))[0];
			timeSeriesGroup = new Document(timeSeriesGroup.keySet().stream().filter(s -> !s.equals("unique")).collect(Collectors.toMap(s -> s, document::get, (k, v) -> v, LinkedHashMap::new)));
			resizeBuckets(timeSeriesType, timeSeriesGroup,bucketSize);
		}
	}
}
