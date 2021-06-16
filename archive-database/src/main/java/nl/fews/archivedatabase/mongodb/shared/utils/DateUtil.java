package nl.fews.archivedatabase.mongodb.shared.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public final class DateUtil {

	/**
	 * Static Class
	 */
	private DateUtil(){

	}

	/**
	 * The lock to support the static lookup caches for safe multi-threaded synchronization
	 */
	private static final Object lock = new Object();

	/**
	 * The Date Cache for converting long to Date type required by mongo.
	 */
	private static final Map<Long, Date> dateCache = new HashMap<>();

	/**
	 * The Local Date Time Cache used for extracting year, month and date from Date for bucket definitions.
	 */
	private static final Map<Date, LocalDateTime> localDateTimeCache = new HashMap<>();

	/**
	 * Typically used to get the local time as a Date object based on utc ms from epoch.
	 * @param utcTimes the utc time milliseconds from the epoch to convert from
	 * @return the dates based on the utc value passed
	 */
	public static Date[] getDates(long[] utcTimes){
		synchronized (lock){
			Date[] dates = new Date[utcTimes.length];
			for (int i = 0; i < utcTimes.length; i++) {
				dateCache.putIfAbsent(utcTimes[i], new Date(utcTimes[i]));
				dates[i] = dateCache.get(utcTimes[i]);
			}
			return dates;
		}
	}

	/**
	 * Get a LocalDateTime object by date for accessing year, month, day.
	 * @param date the date to convert from
	 * @return the LocalDateTime
	 */
	public static LocalDateTime getLocalDateTime(Date date){
		synchronized (lock){
			localDateTimeCache.computeIfAbsent(date, x -> x.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime());
			return localDateTimeCache.get(date);
		}
	}
}
