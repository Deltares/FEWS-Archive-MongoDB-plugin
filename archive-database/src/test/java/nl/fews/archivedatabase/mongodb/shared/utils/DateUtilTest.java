package nl.fews.archivedatabase.mongodb.shared.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

class DateUtilTest {

	@Test
	void testGetDates() {
		Date now = new Date();
		Date[] dates = DateUtil.getDates(new long[]{0, now.getTime()});
		Assertions.assertEquals(new Date(0), dates[0]);
		Assertions.assertEquals(now, dates[1]);
	}

	@Test
	void testGetLocalDateTime() {
		Date now = new Date();
		LocalDateTime localDateTime = DateUtil.getLocalDateTime(now);
		Assertions.assertEquals(now.getTime(), localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
	}
}