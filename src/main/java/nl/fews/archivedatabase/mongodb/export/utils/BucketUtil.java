package nl.fews.archivedatabase.mongodb.export.utils;

import nl.fews.archivedatabase.mongodb.export.enums.BucketSize;
import org.bson.Document;

import java.time.Duration;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;

public class BucketUtil {

	/**
	 * The minimum (default) entry in the timeStepMinutesBucketThreshold lookup
	 */
	private static final int TIME_STEP_MINUTES_BUCKET_THRESHOLD_MINIMUM = 0;

	/**
	 * The time step threshold equal or above which to apply the specified Bucket Size
	 */
	private static final Map<Integer, BucketSize> timeStepMinutesBucketThreshold = Map.of(
			TIME_STEP_MINUTES_BUCKET_THRESHOLD_MINIMUM, BucketSize.MONTHLY,
			15, BucketSize.YEARLY
	);

	private BucketUtil(){

	}

	/**
	 * derive the Bucket Size to be used based on the time step minutes
	 * @param document the root document from which to determine the bucket size
	 * @return BucketSize according to timeStepMinutesBucketThreshold
	 */
	public static BucketSize getBucketSize(Document document){
		int timeStepMinutes = document.get("metaData", Document.class).getInteger("timeStepMinutes");
		Date startTime = document.getDate("startTime");
		Date endTime = document.getDate("endTime");
		int timeseriesSize = document.getList("timeseries", Document.class).size();

		timeseriesSize = timeseriesSize <= 0 ? 1 : timeseriesSize;
		timeStepMinutes = timeStepMinutes <= 0 ? (int)((Duration.between(DateUtil.getLocalDateTime(startTime), DateUtil.getLocalDateTime(endTime)).toMinutes()+1)/timeseriesSize) : timeStepMinutes;

		int finalTimeStepMinutes = timeStepMinutes;
		Integer threshold = timeStepMinutesBucketThreshold.keySet().stream().filter(s -> finalTimeStepMinutes > s).max(Comparator.comparing(s -> s)).orElse(TIME_STEP_MINUTES_BUCKET_THRESHOLD_MINIMUM);
		return timeStepMinutesBucketThreshold.get(threshold);
	}

	/**
	 * gets the integer bucket representation: the year or year-month (year*100+month).  months are represented 1-12.
	 * @param date the date from which to determine the corresponding bucket
	 * @param bucketSize the bucketSize to calculate
	 * @return the int representation of this bucket
	 */
	public static int getBucket(Date date, BucketSize bucketSize){
		return bucketSize == BucketSize.YEARLY ?
				DateUtil.getLocalDateTime(date).getYear() :
				DateUtil.getLocalDateTime(date).getYear() * 100 + DateUtil.getLocalDateTime(date).getMonthValue();
	}
}
