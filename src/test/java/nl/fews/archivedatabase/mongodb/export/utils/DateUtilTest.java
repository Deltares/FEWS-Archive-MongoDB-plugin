package nl.fews.archivedatabase.mongodb.export.utils;

import junit.framework.TestCase;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

public class DateUtilTest extends TestCase {

	public void testGetDates() {
		Date now = new Date();
		Date[] dates = DateUtil.getDates(new long[]{0, now.getTime()});
		assertEquals(new Date(0), dates[0]);
		assertEquals(now, dates[1]);
	}

	public void testGetLocalDateTime() {
		Date now = new Date();
		LocalDateTime localDateTime = DateUtil.getLocalDateTime(now);
		assertEquals(now.getTime(), localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
	}
}